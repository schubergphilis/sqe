//! Shared worker bootstrap.
//!
//! Both the standalone `sqe-worker` binary and `sqe-server --mode worker` must
//! build an identically-wired [`WorkerFlightService`]: shared worker secret,
//! Parquet footer cache, credential store, IPC compression, and a running
//! heartbeat task. Before this module the two paths diverged: `run_worker` in
//! sqe-server dropped `.with_worker_secret()`, the footer cache, and never
//! started the heartbeat, so Helm-deployed workers (which run `--mode worker`)
//! were unauthenticated, uncached, and invisible to the coordinator (#219).
//!
//! One function ([`build_worker_service`]) now wires the service, derives the
//! advertise URL, starts the heartbeat, and emits the security warnings. Each
//! binary keeps its own TLS-build and `serve` loop (they differ: sqe-server
//! adds a health server and graceful shutdown).

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use prometheus::Counter;
use sqe_catalog::FooterCache;
use sqe_core::{parse_memory_limit, FlightCompression, SqeConfig};
use sqe_metrics::WorkerMetricsRegistry;
use sqe_spill::{
    LocalSegmentStore, MemoryGovernor, ResizableFairSpillPool, S3SegmentStore, S3SpillConfig,
    SpillManager, TieredSegmentStore,
};

use crate::advertise::derive_advertise_url;
use crate::config_reload::{
    resolve_memory_budgets, spawn_config_reload_watch, BootConfigIdentity, HotReloadHandles,
    WorkerHotConfig, DEFAULT_CONFIG_RELOAD_INTERVAL,
};
use crate::flight_service::WorkerFlightService;
use crate::heartbeat;

/// Build the fully-wired worker Flight service and start its heartbeat task.
///
/// Wiring (kept identical across both binaries):
/// - worker secret (authenticates inbound scan tickets / credential refresh);
/// - Parquet footer cache sized from `storage.footer_cache_size`;
/// - DoGet + shuffle IPC compression from `coordinator.shuffle_compression`;
/// - scan timeout from `worker.scan_timeout_secs`;
/// - a background heartbeat to `worker.coordinator_url` advertising a routable
///   URL derived via [`derive_advertise_url`].
///
/// Fails loudly when a configured key would otherwise be silently inert: an
/// undeliverable advertise URL aborts startup rather than letting the worker
/// run invisibly. `SqeConfig::validate()` (called by each binary before this)
/// already rejects the empty-secret and plaintext-transport cases.
pub fn build_worker_service(
    config: &SqeConfig,
    metrics: Arc<WorkerMetricsRegistry>,
    session_ctx: SessionContext,
) -> anyhow::Result<WorkerFlightService> {
    build_worker_service_with_hot_reload(config, metrics, session_ctx, None, None)
}

/// Like [`build_worker_service`], with optional resizable pool + config path for
/// memory hot-reload and live cgroup need tracking.
pub fn build_worker_service_with_hot_reload(
    config: &SqeConfig,
    metrics: Arc<WorkerMetricsRegistry>,
    session_ctx: SessionContext,
    memory_pool: Option<Arc<ResizableFairSpillPool>>,
    config_path: Option<&str>,
) -> anyhow::Result<WorkerFlightService> {
    let shuffle_compression =
        FlightCompression::from_config(&config.coordinator.shuffle_compression)
            .unwrap_or(FlightCompression::Zstd);

    // Parquet footer cache: avoids re-reading file metadata from S3 on every
    // scan. Sized from storage.footer_cache_size. Counters are standalone
    // (the cache works whether or not they are scraped); register them on the
    // worker metrics registry so footer hit-rate is observable.
    let footer_cache = build_footer_cache(config, &metrics);

    let budgets = resolve_memory_budgets(config)?;
    let hot = Arc::new(WorkerHotConfig::new(
        budgets.memory_limit_bytes,
        budgets.shuffle_budget_bytes,
        budgets.configured_need_bytes,
        budgets.scan_timeout_secs,
    ));
    // Fail-closed cgroup check + live cgroup watch (shares hot.configured_need).
    validate_process_headroom(
        config,
        budgets.configured_need_bytes,
        Arc::clone(&hot.configured_need_bytes),
    )?;

    let spill_manager = build_spill_manager(config)?;
    let memory_governor = Arc::new(MemoryGovernor::new(budgets.governor_pool_bytes));
    tracing::info!(
        pool_bytes = memory_governor.pool_bytes(),
        distributable = memory_governor.distributable_bytes(),
        headroom = memory_governor.headroom_bytes(),
        "Worker memory governor ready"
    );

    let mut service = WorkerFlightService::new(metrics, session_ctx)
        .with_scan_timeout(config.worker.scan_timeout_secs)
        .with_flight_compression(shuffle_compression)
        .with_shuffle_compression(shuffle_compression)
        .with_footer_cache(footer_cache)
        .with_worker_secret(config.worker.worker_secret.clone())
        .with_shuffle_memory_budget_atom(Arc::clone(&hot.shuffle_budget_bytes))
        .with_scan_timeout_atom(Arc::clone(&hot.scan_timeout_secs))
        .with_memory_governor(memory_governor.clone());
    if let Some(sm) = spill_manager {
        service = service.with_spill_manager(sm);
    }

    if let (Some(pool), Some(path)) = (memory_pool, config_path) {
        let handles = HotReloadHandles {
            pool,
            governor: memory_governor,
            hot,
            boot_identity: BootConfigIdentity::from_config(config),
        };
        let _ = spawn_config_reload_watch(
            PathBuf::from(path),
            handles,
            DEFAULT_CONFIG_RELOAD_INTERVAL,
        );
    }

    // Plaintext warning (config validation already fail-closes on non-loopback
    // distributed setups without TLS or the opt-in; this covers the waived /
    // loopback case so the operator still sees it).
    if !config.coordinator.tls.is_enabled() {
        tracing::warn!(
            "WARNING: the worker Flight service is PLAINTEXT (no TLS). User S3 \
             credentials and the worker secret travel in cleartext. Set \
             [coordinator.tls] cert_file/key_file to enable TLS, or do not run \
             workers on untrusted networks."
        );
    }

    if config.worker.worker_secret.is_empty() && config.worker.allow_unauthenticated {
        tracing::warn!(
            "WARNING: worker.allow_unauthenticated = true -- any TCP-reachable \
             client may send scan tickets or refresh S3 credentials on this \
             worker. Set worker.worker_secret for production."
        );
    }

    // Heartbeat to the coordinator. Only started when a coordinator URL is
    // configured. The advertise URL is derived once at startup and must be
    // routable: an undeliverable URL aborts boot (fail loudly) instead of
    // poisoning the coordinator's registry with 0.0.0.0.
    if !config.worker.coordinator_url.is_empty() {
        let advertise_url = derive_advertise_url(config).map_err(|e| {
            anyhow::anyhow!(
                "cannot start worker heartbeat: {e}. (worker.coordinator_url is set, \
                 so the worker must advertise a routable address)"
            )
        })?;
        let interval = Duration::from_secs(config.worker.heartbeat_interval_secs);
        tracing::info!(
            coordinator = %config.worker.coordinator_url,
            advertise_url = %advertise_url,
            interval_secs = config.worker.heartbeat_interval_secs,
            "Starting heartbeat to coordinator"
        );
        heartbeat::start_heartbeat_task(
            config.worker.coordinator_url.clone(),
            advertise_url,
            interval,
            config.worker.worker_secret.clone(),
        );
    } else {
        // A worker_secret with no coordinator_url is inert: nothing to
        // heartbeat. Warn so the operator notices the half-configured state.
        if !config.worker.worker_secret.is_empty() {
            tracing::warn!(
                "worker.worker_secret is set but worker.coordinator_url is empty: \
                 this worker will not heartbeat any coordinator. Set \
                 worker.coordinator_url to join a cluster."
            );
        }
    }

    Ok(service)
}

/// Fail closed when configured need exceeds a known **enforced** cgroup limit.
/// Logs a full OS/container/cgroup/host snapshot. Starts a live cgroup watch
/// that reads `configured_need` from `need_atom` (updated by hot config reload).
fn validate_process_headroom(
    config: &SqeConfig,
    need: u64,
    need_atom: Arc<std::sync::atomic::AtomicU64>,
) -> anyhow::Result<()> {
    let limit = parse_memory_limit(&config.worker.memory_limit).unwrap_or(0) as u64;
    let headroom = need.saturating_sub(limit);
    need_atom.store(need, Ordering::Release);
    let mem = sqe_core::runtime_memory_info();

    tracing::info!(
        os = %mem.os,
        container = %mem.container,
        cgroup_version = %mem.cgroup.version,
        cgroup_path = mem.cgroup.path.as_deref().unwrap_or(""),
        cgroup_limit_file = mem.cgroup.limit_file.as_deref().unwrap_or(""),
        cgroup_memory_max_bytes = mem.cgroup.memory_max_bytes.unwrap_or(0),
        cgroup_memory_current_bytes = mem.cgroup.memory_current_bytes.unwrap_or(0),
        host_total_bytes = mem.host.total_bytes.unwrap_or(0),
        host_available_bytes = mem.host.available_bytes.unwrap_or(0),
        process_rss_bytes = mem.process_rss_bytes.unwrap_or(0),
        enforced_memory_limit_bytes = mem.enforced_memory_limit_bytes.unwrap_or(0),
        enforced_source = mem.enforced_memory_limit_source,
        worker_memory_limit = limit,
        process_headroom = headroom,
        "Runtime memory environment (container/cgroup/OS)"
    );

    match mem.enforced_memory_limit_bytes {
        Some(enforced) if enforced > 0 && need > enforced => {
            return Err(anyhow::anyhow!(
                "worker.memory_limit ({limit}) + process_headroom ({headroom}) = {need} \
                 exceeds enforced memory limit ({enforced}, source={}, os={}, container={}, \
                 cgroup={}). Lower memory_limit or raise the container/cgroup limit.",
                mem.enforced_memory_limit_source,
                mem.os,
                mem.container,
                mem.cgroup.version,
            ));
        }
        Some(enforced) => {
            tracing::info!(
                memory_limit = limit,
                process_headroom = headroom,
                enforced_memory_limit = enforced,
                source = mem.enforced_memory_limit_source,
                container = %mem.container,
                cgroup_version = %mem.cgroup.version,
                "Process headroom validated against container/cgroup limit"
            );
        }
        None => {
            tracing::info!(
                memory_limit = limit,
                process_headroom = headroom,
                source = mem.enforced_memory_limit_source,
                os = %mem.os,
                container = %mem.container,
                host_total_bytes = mem.host.total_bytes.unwrap_or(0),
                "No enforced cgroup memory limit; skipping headroom fail-closed check"
            );
        }
    }

    let _ = sqe_core::spawn_runtime_memory_watch(
        need_atom,
        sqe_core::DEFAULT_RUNTIME_MEMORY_WATCH_INTERVAL,
    );

    Ok(())
}

/// Open a local [`SpillManager`] when spill is enabled, or `None` when disabled.
///
/// Runs orphan cleanup on start when configured. Fails loudly on unsupported
/// backends or unusable spill directories so misconfig cannot silently disable
/// spill in production.
fn build_spill_manager(config: &SqeConfig) -> anyhow::Result<Option<Arc<SpillManager>>> {
    let spill = &config.worker.spill;
    // Respect both the legacy spill_to_disk flag and the new spill.enabled.
    if !spill.enabled || !config.worker.spill_to_disk {
        tracing::info!("Worker spill substrate disabled");
        return Ok(None);
    }
    let max_bytes = parse_memory_limit(&spill.max_bytes).map_err(|e| {
        anyhow::anyhow!("worker.spill.max_bytes: {e}")
    })? as u64;
    let orphan_age = spill.orphan_age_duration().map_err(|e| anyhow::anyhow!("{e}"))?;

    let store: Arc<dyn sqe_spill::SegmentStore> = match spill.backend.as_str() {
        "local" => {
            let dir = spill
                .resolved_directory(&config.worker.spill_dir)
                .to_string();
            if dir.is_empty() {
                return Err(anyhow::anyhow!(
                    "spill enabled but no directory configured \
                     (set worker.spill.directory or worker.spill_dir)"
                ));
            }
            let min_free = parse_memory_limit(&spill.min_free_bytes).map_err(|e| {
                anyhow::anyhow!("worker.spill.min_free_bytes: {e}")
            })? as u64;
            Arc::new(LocalSegmentStore::open(
                &dir,
                max_bytes,
                min_free,
                spill.max_concurrent_writes,
                spill.max_concurrent_reads,
            )?)
        }
        "s3" => {
            let s3 = &spill.s3;
            let s3_cfg = S3SpillConfig {
                bucket: s3.bucket.clone(),
                prefix: s3.prefix.clone(),
                region: s3.region.clone(),
                endpoint: s3.endpoint.clone(),
                access_key_id: s3.access_key_id.clone(),
                secret_access_key: s3.secret_access_key.expose().to_string(),
                allow_http: s3.allow_http,
                path_style: s3.path_style || !s3.endpoint.is_empty(),
                max_bytes,
                max_objects: s3.max_objects,
                max_concurrent_writes: spill.max_concurrent_writes,
                max_concurrent_reads: spill.max_concurrent_reads,
            };
            Arc::new(S3SegmentStore::from_config(&s3_cfg)?)
        }
        "tiered" => {
            let dir = spill
                .resolved_directory(&config.worker.spill_dir)
                .to_string();
            if dir.is_empty() {
                return Err(anyhow::anyhow!(
                    "tiered spill requires worker.spill.directory (or spill_dir)"
                ));
            }
            let min_free = parse_memory_limit(&spill.min_free_bytes).map_err(|e| {
                anyhow::anyhow!("worker.spill.min_free_bytes: {e}")
            })? as u64;
            let local = Arc::new(LocalSegmentStore::open(
                &dir,
                max_bytes,
                min_free,
                spill.max_concurrent_writes,
                spill.max_concurrent_reads,
            )?);
            let s3c = &spill.s3;
            let s3_cfg = S3SpillConfig {
                bucket: s3c.bucket.clone(),
                prefix: s3c.prefix.clone(),
                region: s3c.region.clone(),
                endpoint: s3c.endpoint.clone(),
                access_key_id: s3c.access_key_id.clone(),
                secret_access_key: s3c.secret_access_key.expose().to_string(),
                allow_http: s3c.allow_http,
                path_style: s3c.path_style || !s3c.endpoint.is_empty(),
                max_bytes,
                max_objects: s3c.max_objects,
                max_concurrent_writes: spill.max_concurrent_writes,
                max_concurrent_reads: spill.max_concurrent_reads,
            };
            let s3 = Arc::new(S3SegmentStore::from_config(&s3_cfg)?);
            Arc::new(TieredSegmentStore::new(local, s3))
        }
        other => {
            return Err(anyhow::anyhow!(
                "worker.spill.backend = {other:?} is unknown \
                 (supported: local, s3, tiered)"
            ));
        }
    };

    let manager = Arc::new(SpillManager::new(store, orphan_age));
    // Orphan cleanup runs async on first use of the manager; avoid
    // block_on during bootstrap (the runtime may already be running).
    if spill.cleanup_on_start {
        let mgr = manager.clone();
        tokio::spawn(async move {
            match mgr.cleanup_orphans_on_start().await {
                Ok(n) => tracing::info!(orphans_cleaned = n, "Spill orphan cleanup complete"),
                Err(e) => tracing::warn!(error = %e, "Spill orphan cleanup failed"),
            }
        });
    }
    tracing::info!(
        backend = %spill.backend,
        max_bytes,
        cleanup_on_start = spill.cleanup_on_start,
        "Worker spill manager ready"
    );
    Ok(Some(manager))
}

/// Build the Parquet footer cache and register its hit/miss counters on the
/// worker metrics registry so the footer hit-rate is scrapeable.
fn build_footer_cache(
    config: &SqeConfig,
    metrics: &WorkerMetricsRegistry,
) -> Arc<FooterCache> {
    let size_bytes = parse_memory_limit(&config.storage.footer_cache_size).unwrap_or_else(|e| {
        tracing::warn!(
            value = %config.storage.footer_cache_size,
            error = %e,
            "Invalid catalog.footer_cache_size, defaulting to 256MB"
        );
        256 * 1024 * 1024
    });

    let hits = Counter::new(
        "sqe_worker_footer_cache_hits_total",
        "Total Parquet footer cache hits on this worker",
    )
    .expect("static counter opts are valid");
    let misses = Counter::new(
        "sqe_worker_footer_cache_misses_total",
        "Total Parquet footer cache misses on this worker",
    )
    .expect("static counter opts are valid");
    // Best-effort registration: a duplicate registration (e.g. two workers in
    // one process during tests) must not abort the worker.
    if let Err(e) = metrics.registry.register(Box::new(hits.clone())) {
        tracing::debug!(error = %e, "footer_cache_hits already registered");
    }
    if let Err(e) = metrics.registry.register(Box::new(misses.clone())) {
        tracing::debug!(error = %e, "footer_cache_misses already registered");
    }

    Arc::new(FooterCache::new(size_bytes as u64, hits, misses))
}
