//! Hot-reload of **selected** worker settings from `sqe.toml`.
//!
//! Polls the config file mtime (same pattern as API-key reload). On change:
//!
//! - Reloads and re-applies **memory** knobs that have live handles:
//!   `worker.memory_limit`, `[worker.memory]` sub-budgets, scan timeout.
//! - Re-validates against the live cgroup limit (same fail-closed rule as boot).
//! - Resizes [`ResizableFairSpillPool`] + [`MemoryGovernor`].
//! - Updates the shared `configured_need` atom used by the cgroup watch.
//!
//! Fields that require process restart (ports, spill backend/dir, secrets, TLS)
//! are logged when they change; previous values keep running.
//!
//! **Ports never hot-apply.** Changing `worker.flight_port` or
//! `metrics.prometheus_port` in the file only emits a warning; the process
//! keeps listening on the sockets opened at boot until restart.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use datafusion::execution::memory_pool::MemoryPool;
use sqe_core::{parse_memory_limit, runtime_memory_info, SqeConfig};
use sqe_spill::{MemoryGovernor, ResizableFairSpillPool};
use tracing::{error, info, warn};

/// Default poll interval for config file mtime.
pub const DEFAULT_CONFIG_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

/// Live-updatable worker budgets shared with Flight + memory watch.
#[derive(Debug)]
pub struct WorkerHotConfig {
    pub memory_limit_bytes: Arc<AtomicUsize>,
    pub shuffle_budget_bytes: Arc<AtomicUsize>,
    /// `memory_limit + process_headroom` for cgroup fail-closed / watch.
    pub configured_need_bytes: Arc<AtomicU64>,
    pub scan_timeout_secs: Arc<AtomicU64>,
}

impl WorkerHotConfig {
    pub fn new(
        memory_limit_bytes: usize,
        shuffle_budget_bytes: usize,
        configured_need_bytes: u64,
        scan_timeout_secs: u64,
    ) -> Self {
        Self {
            memory_limit_bytes: Arc::new(AtomicUsize::new(memory_limit_bytes.max(1))),
            shuffle_budget_bytes: Arc::new(AtomicUsize::new(shuffle_budget_bytes.max(64 * 1024))),
            configured_need_bytes: Arc::new(AtomicU64::new(configured_need_bytes)),
            scan_timeout_secs: Arc::new(AtomicU64::new(scan_timeout_secs)),
        }
    }
}

/// Handles required to apply a memory hot-reload.
#[derive(Clone)]
pub struct HotReloadHandles {
    pub pool: Arc<ResizableFairSpillPool>,
    pub governor: Arc<MemoryGovernor>,
    pub hot: Arc<WorkerHotConfig>,
    /// Snapshot of non-hot fields at boot (for restart-required warnings).
    pub boot_identity: BootConfigIdentity,
}

/// Config identity that cannot be hot-reloaded safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootConfigIdentity {
    /// Bound at process start; port changes in sqe.toml are **ignored**.
    pub flight_port: u16,
    /// Metrics scrape port bound at start (if metrics server is up).
    pub prometheus_port: u16,
    pub coordinator_url: String,
    pub worker_secret_set: bool,
    pub allow_unauthenticated: bool,
    pub spill_enabled: bool,
    pub spill_backend: String,
    pub spill_dir: String,
    pub spill_to_disk: bool,
}

impl BootConfigIdentity {
    pub fn from_config(config: &SqeConfig) -> Self {
        Self {
            flight_port: config.worker.flight_port,
            prometheus_port: config.metrics.prometheus_port,
            coordinator_url: config.worker.coordinator_url.clone(),
            worker_secret_set: !config.worker.worker_secret.is_empty(),
            allow_unauthenticated: config.worker.allow_unauthenticated,
            spill_enabled: config.worker.spill.enabled,
            spill_backend: config.worker.spill.backend.clone(),
            spill_dir: config
                .worker
                .spill
                .resolved_directory(&config.worker.spill_dir)
                .to_string(),
            spill_to_disk: config.worker.spill_to_disk,
        }
    }
}

/// Resolve memory_limit, governor pool, shuffle budget, and configured need.
pub fn resolve_memory_budgets(config: &SqeConfig) -> anyhow::Result<ResolvedMemoryBudgets> {
    let memory_limit = parse_memory_limit(&config.worker.memory_limit).map_err(|e| {
        anyhow::anyhow!(
            "Invalid worker.memory_limit '{}': {e}",
            config.worker.memory_limit
        )
    })?;
    let resolved = config
        .worker
        .memory
        .resolve_bytes(memory_limit)
        .map_err(|e| anyhow::anyhow!("worker.memory: {e}"))?;
    let governor_pool = resolved
        .operator_budget
        .saturating_add(resolved.shuffle_memory_budget)
        .max(1024 * 1024);
    let configured_need = (memory_limit as u64).saturating_add(resolved.process_headroom as u64);
    Ok(ResolvedMemoryBudgets {
        memory_limit_bytes: memory_limit,
        governor_pool_bytes: governor_pool,
        shuffle_budget_bytes: resolved.shuffle_memory_budget.max(64 * 1024),
        process_headroom_bytes: resolved.process_headroom,
        configured_need_bytes: configured_need,
        scan_timeout_secs: config.worker.scan_timeout_secs,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedMemoryBudgets {
    pub memory_limit_bytes: usize,
    pub governor_pool_bytes: usize,
    pub shuffle_budget_bytes: usize,
    pub process_headroom_bytes: usize,
    pub configured_need_bytes: u64,
    pub scan_timeout_secs: u64,
}

/// Result of applying a hot-reload snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotReloadOutcome {
    /// All requested windows applied.
    FullyApplied,
    /// At least one smaller window was ignored because current use is larger.
    /// Caller should **not** advance mtime so the next poll retries when load drops.
    ShrinkIgnoredPendingUse,
}

/// Apply memory budgets to live handles after cgroup validation.
///
/// **Shrinks below current use are ignored** (error log, keep previous window):
/// - DataFusion pool when `new_limit < reserved`
/// - Governor when active grants/minima cannot fit the new pool
///
/// Growths, timeouts, and shrinks that still fit usage apply. Partial apply
/// returns [`HotReloadOutcome::ShrinkIgnoredPendingUse`] (not an Err) so a
/// blocked shrink does not block scan_timeout updates.
pub fn apply_memory_hot_reload(
    handles: &HotReloadHandles,
    budgets: &ResolvedMemoryBudgets,
) -> anyhow::Result<HotReloadOutcome> {
    // Same fail-closed rule as boot: need must fit live cgroup when known.
    // This rejects *raising* configured need above the kernel limit, not
    // in-process shrinks under load (those are ignored separately below).
    let mem = runtime_memory_info();
    if let Some(enforced) = mem.enforced_memory_limit_bytes {
        if enforced > 0 && budgets.configured_need_bytes > enforced {
            return Err(anyhow::anyhow!(
                "hot-reload rejected: worker.memory_limit + process_headroom ({}) \
                 exceeds live cgroup limit ({enforced}, source={}). Raise the cgroup \
                 limit or lower memory settings.",
                budgets.configured_need_bytes,
                mem.enforced_memory_limit_source
            ));
        }
    }

    let prev_limit = handles.pool.pool_size();
    let reserved = handles.pool.reserved();
    let mut applied_limit = prev_limit;
    let mut pool_shrink_ignored = false;
    match handles.pool.try_set_pool_size(budgets.memory_limit_bytes) {
        Ok(_) => {
            applied_limit = handles.pool.pool_size();
        }
        Err(e) => {
            pool_shrink_ignored = true;
            error!(
                requested_memory_limit_bytes = e.requested_bytes,
                reserved_bytes = e.reserved_bytes,
                current_limit_bytes = e.current_limit_bytes,
                "Ignoring hot-reload shrink of worker.memory_limit: current use is \
                 larger than the new window. Keeping previous limit until usage drops \
                 or the process restarts."
            );
        }
    }

    let prev_gov = handles.governor.pool_bytes();
    let mut applied_gov = prev_gov;
    let mut governor_shrink_ignored = false;
    match handles
        .governor
        .try_resize_pool(budgets.governor_pool_bytes)
    {
        Ok(()) => {
            applied_gov = handles.governor.pool_bytes();
        }
        Err(e) => {
            governor_shrink_ignored = true;
            error!(
                requested_governor_pool_bytes = budgets.governor_pool_bytes,
                current_governor_pool_bytes = prev_gov,
                granted_sum = handles.governor.granted_sum(),
                reason = %e,
                "Ignoring hot-reload shrink of governor pool: current grants/use \
                 exceed the new window. Keeping previous governor capacity."
            );
        }
    }

    // Only publish atoms for windows we actually applied. If pool shrink was
    // ignored, keep previous memory_limit + configured_need so the cgroup
    // watch matches the live pool.
    if !pool_shrink_ignored {
        handles
            .hot
            .memory_limit_bytes
            .store(applied_limit, Ordering::Release);
        handles
            .hot
            .configured_need_bytes
            .store(budgets.configured_need_bytes, Ordering::Release);
    }
    if !governor_shrink_ignored {
        handles
            .hot
            .shuffle_budget_bytes
            .store(budgets.shuffle_budget_bytes, Ordering::Release);
    }
    handles
        .hot
        .scan_timeout_secs
        .store(budgets.scan_timeout_secs, Ordering::Release);

    if pool_shrink_ignored || governor_shrink_ignored {
        error!(
            requested_memory_limit_bytes = budgets.memory_limit_bytes,
            applied_memory_limit_bytes = applied_limit,
            pool_reserved_bytes = reserved,
            requested_governor_pool_bytes = budgets.governor_pool_bytes,
            applied_governor_pool_bytes = applied_gov,
            pool_shrink_ignored,
            governor_shrink_ignored,
            scan_timeout_secs = budgets.scan_timeout_secs,
            "Hot-reload partially applied: one or more smaller windows ignored \
             because current use is larger; will retry while config stays smaller"
        );
        Ok(HotReloadOutcome::ShrinkIgnoredPendingUse)
    } else {
        info!(
            memory_limit_bytes = applied_limit,
            governor_pool_bytes = applied_gov,
            shuffle_budget_bytes = budgets.shuffle_budget_bytes,
            process_headroom_bytes = budgets.process_headroom_bytes,
            configured_need_bytes = budgets.configured_need_bytes,
            scan_timeout_secs = budgets.scan_timeout_secs,
            enforced_cgroup = mem.enforced_memory_limit_bytes.unwrap_or(0),
            "Applied worker memory hot-reload"
        );
        Ok(HotReloadOutcome::FullyApplied)
    }
}

/// Log restart-required fields that changed in `next` vs boot identity.
///
/// Port / listen-address changes **never** take effect on reload: the process
/// keeps the sockets opened at start. Returns `true` if any such field differed.
pub fn warn_restart_required(boot: &BootConfigIdentity, next: &SqeConfig) -> bool {
    let now = BootConfigIdentity::from_config(next);
    if boot == &now {
        return false;
    }
    let mut any = false;
    if boot.flight_port != now.flight_port {
        any = true;
        warn!(
            listening_on = boot.flight_port,
            config_value = now.flight_port,
            "worker.flight_port changed in config but does NOT take effect on \
             hot-reload — still listening on the boot port. Restart the worker \
             to bind the new port."
        );
    }
    if boot.prometheus_port != now.prometheus_port {
        any = true;
        warn!(
            listening_on = boot.prometheus_port,
            config_value = now.prometheus_port,
            "metrics.prometheus_port changed in config but does NOT take effect \
             on hot-reload — still scraping the boot port. Restart to rebind."
        );
    }
    if boot.coordinator_url != now.coordinator_url {
        any = true;
        warn!(
            boot_coordinator_url = %boot.coordinator_url,
            config_coordinator_url = %now.coordinator_url,
            "worker.coordinator_url changed; restart required (heartbeat keeps boot URL)"
        );
    }
    if boot.worker_secret_set != now.worker_secret_set
        || boot.allow_unauthenticated != now.allow_unauthenticated
    {
        any = true;
        warn!("worker auth settings changed; restart required (not hot-reloaded)");
    }
    if boot.spill_enabled != now.spill_enabled
        || boot.spill_backend != now.spill_backend
        || boot.spill_dir != now.spill_dir
        || boot.spill_to_disk != now.spill_to_disk
    {
        any = true;
        warn!(
            "worker spill backend/directory/enable changed; restart required \
             (open stores are not re-created)"
        );
    }
    any
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// Poll `config_path` and apply memory hot-reload when the file changes.
pub fn spawn_config_reload_watch(
    config_path: PathBuf,
    handles: HotReloadHandles,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let interval = if interval.is_zero() {
        DEFAULT_CONFIG_RELOAD_INTERVAL
    } else {
        interval
    };
    tokio::spawn(async move {
        let mut last_mtime = file_mtime(&config_path);
        info!(
            config_path = %config_path.display(),
            interval_secs = interval.as_secs(),
            "Watching sqe.toml for worker memory hot-reload"
        );
        loop {
            tokio::time::sleep(interval).await;
            let mtime = file_mtime(&config_path);
            if mtime.is_none() || mtime == last_mtime {
                continue;
            }
            match SqeConfig::load(config_path.to_str().unwrap_or_default()) {
                Ok(new_config) => {
                    warn_restart_required(&handles.boot_identity, &new_config);
                    match resolve_memory_budgets(&new_config) {
                        Ok(budgets) => {
                            let prev_limit = handles.hot.memory_limit_bytes.load(Ordering::Acquire);
                            let prev_need =
                                handles.hot.configured_need_bytes.load(Ordering::Acquire);
                            if budgets.memory_limit_bytes == prev_limit
                                && budgets.configured_need_bytes == prev_need
                                && budgets.shuffle_budget_bytes
                                    == handles.hot.shuffle_budget_bytes.load(Ordering::Acquire)
                                && budgets.scan_timeout_secs
                                    == handles.hot.scan_timeout_secs.load(Ordering::Acquire)
                            {
                                info!(
                                    config_path = %config_path.display(),
                                    "Config file changed but memory budgets unchanged"
                                );
                                last_mtime = mtime;
                                continue;
                            }
                            match apply_memory_hot_reload(&handles, &budgets) {
                                Ok(HotReloadOutcome::FullyApplied) => {
                                    last_mtime = mtime;
                                }
                                Ok(HotReloadOutcome::ShrinkIgnoredPendingUse) => {
                                    // Keep last_mtime behind so we re-attempt every
                                    // interval until usage falls under the new window.
                                }
                                Err(e) => {
                                    error!(
                                        error = %e,
                                        config_path = %config_path.display(),
                                        "Memory hot-reload rejected; keeping previous budgets"
                                    );
                                    last_mtime = mtime;
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                config_path = %config_path.display(),
                                "Failed to parse memory budgets on reload; keeping previous"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        config_path = %config_path.display(),
                        "Failed to load config on reload; keeping previous"
                    );
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handles(
        pool_bytes: usize,
        gov_bytes: usize,
    ) -> (
        HotReloadHandles,
        Arc<ResizableFairSpillPool>,
        Arc<MemoryGovernor>,
        Arc<WorkerHotConfig>,
    ) {
        let pool = Arc::new(ResizableFairSpillPool::new(pool_bytes));
        let governor = Arc::new(MemoryGovernor::new(gov_bytes));
        let hot = Arc::new(WorkerHotConfig::new(
            pool_bytes,
            50 * 1024 * 1024,
            pool_bytes as u64 + 256 * 1024 * 1024,
            600,
        ));
        let handles = HotReloadHandles {
            pool: pool.clone(),
            governor: governor.clone(),
            hot: hot.clone(),
            boot_identity: BootConfigIdentity {
                flight_port: 50052,
                prometheus_port: 9090,
                coordinator_url: String::new(),
                worker_secret_set: false,
                allow_unauthenticated: true,
                spill_enabled: false,
                spill_backend: "local".into(),
                spill_dir: String::new(),
                spill_to_disk: false,
            },
        };
        (handles, pool, governor, hot)
    }

    fn minimal_cfg(worker_flight_port: u16, prometheus_port: u16) -> SqeConfig {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        write!(
            f,
            r#"
[coordinator]
[auth]
[catalog]
catalog_url = ""

[worker]
flight_port = {worker_flight_port}
allow_unauthenticated = true

[metrics]
prometheus_port = {prometheus_port}
"#
        )
        .expect("write");
        SqeConfig::load(f.path().to_str().unwrap()).expect("minimal worker config")
    }

    #[test]
    fn port_change_warns_and_does_not_mutate_boot_listen_identity() {
        let boot_cfg = minimal_cfg(50052, 9090);
        let boot = BootConfigIdentity::from_config(&boot_cfg);
        assert_eq!(boot.flight_port, 50052);
        assert_eq!(boot.prometheus_port, 9090);

        let next = minimal_cfg(50099, 9191);
        assert!(
            warn_restart_required(&boot, &next),
            "port deltas must surface restart-required warnings"
        );
        // Boot identity is immutable for the process lifetime — hot-reload
        // never rebinds sockets, so the effective listen ports stay at boot.
        assert_eq!(boot.flight_port, 50052);
        assert_eq!(boot.prometheus_port, 9090);
        assert_ne!(
            BootConfigIdentity::from_config(&next).flight_port,
            boot.flight_port
        );
    }

    #[test]
    fn unchanged_ports_do_not_warn_restart() {
        let boot_cfg = minimal_cfg(50052, 9090);
        let boot = BootConfigIdentity::from_config(&boot_cfg);
        let next = minimal_cfg(50052, 9090);
        assert!(!warn_restart_required(&boot, &next));
    }

    #[test]
    fn apply_memory_hot_reload_resizes_pool_and_governor() {
        let (handles, pool, governor, hot) = test_handles(200 * 1024 * 1024, 150 * 1024 * 1024);

        let budgets = ResolvedMemoryBudgets {
            memory_limit_bytes: 400 * 1024 * 1024,
            governor_pool_bytes: 250 * 1024 * 1024,
            shuffle_budget_bytes: 80 * 1024 * 1024,
            process_headroom_bytes: 256 * 1024 * 1024,
            configured_need_bytes: 400 * 1024 * 1024 + 256 * 1024 * 1024,
            scan_timeout_secs: 120,
        };
        assert_eq!(
            apply_memory_hot_reload(&handles, &budgets).expect("apply"),
            HotReloadOutcome::FullyApplied
        );
        assert_eq!(pool.pool_size(), 400 * 1024 * 1024);
        assert_eq!(governor.pool_bytes(), 250 * 1024 * 1024);
        assert_eq!(
            hot.memory_limit_bytes.load(Ordering::Acquire),
            400 * 1024 * 1024
        );
        assert_eq!(
            hot.shuffle_budget_bytes.load(Ordering::Acquire),
            80 * 1024 * 1024
        );
        assert_eq!(hot.scan_timeout_secs.load(Ordering::Acquire), 120);
        assert_eq!(
            hot.configured_need_bytes.load(Ordering::Acquire),
            budgets.configured_need_bytes
        );
    }

    #[test]
    fn apply_ignores_pool_shrink_when_reserved_exceeds_new_window() {
        use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

        let (handles, pool, _governor, hot) = test_handles(64 * 1024 * 1024, 32 * 1024 * 1024);
        let dyn_pool: Arc<dyn MemoryPool> = pool.clone();
        let consumer = MemoryConsumer::new("busy").with_can_spill(true);
        let r = consumer.register(&dyn_pool);
        r.try_grow(48 * 1024 * 1024).expect("reserve half+");

        let prev_limit = pool.pool_size();
        let prev_need = hot.configured_need_bytes.load(Ordering::Acquire);
        let budgets = ResolvedMemoryBudgets {
            memory_limit_bytes: 16 * 1024 * 1024, // below reserved
            governor_pool_bytes: 32 * 1024 * 1024,
            shuffle_budget_bytes: 8 * 1024 * 1024,
            process_headroom_bytes: 1024 * 1024,
            configured_need_bytes: 17 * 1024 * 1024,
            scan_timeout_secs: 30,
        };
        assert_eq!(
            apply_memory_hot_reload(&handles, &budgets).expect("partial ok"),
            HotReloadOutcome::ShrinkIgnoredPendingUse
        );
        assert_eq!(pool.pool_size(), prev_limit, "pool limit kept");
        assert_eq!(
            hot.memory_limit_bytes.load(Ordering::Acquire),
            prev_limit,
            "hot atom not lowered"
        );
        assert_eq!(
            hot.configured_need_bytes.load(Ordering::Acquire),
            prev_need,
            "need atom not lowered when pool shrink ignored"
        );
        // Timeout still applies.
        assert_eq!(hot.scan_timeout_secs.load(Ordering::Acquire), 30);
    }
}
