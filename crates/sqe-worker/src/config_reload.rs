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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
            shuffle_budget_bytes: Arc::new(AtomicUsize::new(
                shuffle_budget_bytes.max(64 * 1024),
            )),
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
    pub flight_port: u16,
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

/// Apply memory budgets to live handles after cgroup validation.
pub fn apply_memory_hot_reload(
    handles: &HotReloadHandles,
    budgets: &ResolvedMemoryBudgets,
) -> anyhow::Result<()> {
    // Same fail-closed rule as boot: need must fit live cgroup when known.
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

    handles
        .governor
        .try_resize_pool(budgets.governor_pool_bytes)
        .map_err(|e| anyhow::anyhow!("hot-reload governor resize: {e}"))?;

    handles.pool.set_pool_size(budgets.memory_limit_bytes);

    handles
        .hot
        .memory_limit_bytes
        .store(budgets.memory_limit_bytes, Ordering::Release);
    handles
        .hot
        .shuffle_budget_bytes
        .store(budgets.shuffle_budget_bytes, Ordering::Release);
    handles
        .hot
        .configured_need_bytes
        .store(budgets.configured_need_bytes, Ordering::Release);
    handles
        .hot
        .scan_timeout_secs
        .store(budgets.scan_timeout_secs, Ordering::Release);

    info!(
        memory_limit_bytes = budgets.memory_limit_bytes,
        governor_pool_bytes = budgets.governor_pool_bytes,
        shuffle_budget_bytes = budgets.shuffle_budget_bytes,
        process_headroom_bytes = budgets.process_headroom_bytes,
        configured_need_bytes = budgets.configured_need_bytes,
        scan_timeout_secs = budgets.scan_timeout_secs,
        enforced_cgroup = mem.enforced_memory_limit_bytes.unwrap_or(0),
        "Applied worker memory hot-reload"
    );
    Ok(())
}

fn warn_restart_required(boot: &BootConfigIdentity, next: &SqeConfig) {
    let now = BootConfigIdentity::from_config(next);
    if boot == &now {
        return;
    }
    if boot.flight_port != now.flight_port {
        warn!(
            previous = boot.flight_port,
            new = now.flight_port,
            "worker.flight_port changed; restart required (not hot-reloaded)"
        );
    }
    if boot.coordinator_url != now.coordinator_url {
        warn!("worker.coordinator_url changed; restart required (heartbeat keeps boot URL)");
    }
    if boot.worker_secret_set != now.worker_secret_set
        || boot.allow_unauthenticated != now.allow_unauthenticated
    {
        warn!("worker auth settings changed; restart required (not hot-reloaded)");
    }
    if boot.spill_enabled != now.spill_enabled
        || boot.spill_backend != now.spill_backend
        || boot.spill_dir != now.spill_dir
        || boot.spill_to_disk != now.spill_to_disk
    {
        warn!(
            "worker spill backend/directory/enable changed; restart required \
             (open stores are not re-created)"
        );
    }
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
                            if let Err(e) = apply_memory_hot_reload(&handles, &budgets) {
                                error!(
                                    error = %e,
                                    config_path = %config_path.display(),
                                    "Memory hot-reload rejected; keeping previous budgets"
                                );
                                // Do not advance mtime so a later fix re-tries? Or advance
                                // so we do not spin. Advance: operator can touch file again.
                                last_mtime = mtime;
                                continue;
                            }
                            last_mtime = mtime;
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

    #[test]
    fn apply_memory_hot_reload_resizes_pool_and_governor() {
        let pool = Arc::new(ResizableFairSpillPool::new(200 * 1024 * 1024));
        let governor = Arc::new(MemoryGovernor::new(150 * 1024 * 1024));
        let hot = Arc::new(WorkerHotConfig::new(
            200 * 1024 * 1024,
            50 * 1024 * 1024,
            200 * 1024 * 1024 + 256 * 1024 * 1024,
            600,
        ));
        let handles = HotReloadHandles {
            pool: pool.clone(),
            governor: governor.clone(),
            hot: hot.clone(),
            boot_identity: BootConfigIdentity {
                flight_port: 50052,
                coordinator_url: String::new(),
                worker_secret_set: false,
                allow_unauthenticated: true,
                spill_enabled: false,
                spill_backend: "local".into(),
                spill_dir: String::new(),
                spill_to_disk: false,
            },
        };

        let budgets = ResolvedMemoryBudgets {
            memory_limit_bytes: 400 * 1024 * 1024,
            governor_pool_bytes: 250 * 1024 * 1024,
            shuffle_budget_bytes: 80 * 1024 * 1024,
            process_headroom_bytes: 256 * 1024 * 1024,
            configured_need_bytes: 400 * 1024 * 1024 + 256 * 1024 * 1024,
            scan_timeout_secs: 120,
        };
        apply_memory_hot_reload(&handles, &budgets).expect("apply");
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
}
