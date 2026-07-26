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

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use prometheus::Counter;
use sqe_catalog::FooterCache;
use sqe_core::{parse_memory_limit, FlightCompression, SqeConfig};
use sqe_metrics::WorkerMetricsRegistry;
use sqe_spill::{LocalSegmentStore, SpillManager};

use crate::advertise::derive_advertise_url;
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
    let shuffle_compression =
        FlightCompression::from_config(&config.coordinator.shuffle_compression)
            .unwrap_or(FlightCompression::Zstd);

    // Parquet footer cache: avoids re-reading file metadata from S3 on every
    // scan. Sized from storage.footer_cache_size. Counters are standalone
    // (the cache works whether or not they are scraped); register them on the
    // worker metrics registry so footer hit-rate is observable.
    let footer_cache = build_footer_cache(config, &metrics);

    let spill_manager = build_spill_manager(config)?;

    let mut service = WorkerFlightService::new(metrics, session_ctx)
        .with_scan_timeout(config.worker.scan_timeout_secs)
        .with_flight_compression(shuffle_compression)
        .with_shuffle_compression(shuffle_compression)
        .with_footer_cache(footer_cache)
        .with_worker_secret(config.worker.worker_secret.clone());
    if let Some(sm) = spill_manager {
        service = service.with_spill_manager(sm);
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
    match spill.backend.as_str() {
        "local" => {}
        other => {
            return Err(anyhow::anyhow!(
                "worker.spill.backend = {other:?} is not available in this build \
                 (Phase 3 implements local only)"
            ));
        }
    }
    let dir = spill
        .resolved_directory(&config.worker.spill_dir)
        .to_string();
    if dir.is_empty() {
        return Err(anyhow::anyhow!(
            "spill enabled but no directory configured \
             (set worker.spill.directory or worker.spill_dir)"
        ));
    }
    let max_bytes = parse_memory_limit(&spill.max_bytes).map_err(|e| {
        anyhow::anyhow!("worker.spill.max_bytes: {e}")
    })? as u64;
    let min_free = parse_memory_limit(&spill.min_free_bytes).map_err(|e| {
        anyhow::anyhow!("worker.spill.min_free_bytes: {e}")
    })? as u64;
    let store = Arc::new(LocalSegmentStore::open(
        &dir,
        max_bytes,
        min_free,
        spill.max_concurrent_writes,
        spill.max_concurrent_reads,
    )?);
    let orphan_age = spill.orphan_age_duration().map_err(|e| anyhow::anyhow!("{e}"))?;
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
        spill_dir = %dir,
        max_bytes,
        cleanup_on_start = spill.cleanup_on_start,
        "Worker spill manager ready (local backend)"
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
