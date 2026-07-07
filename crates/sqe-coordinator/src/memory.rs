//! Memory pressure monitoring and admission control for the coordinator.
//!
//! Monitors the coordinator's DataFusion [`FairSpillPool`] memory usage and
//! classifies it into pressure levels. When memory pressure reaches `Red`
//! (>95% utilization), new queries are rejected to prevent OOM.

use std::sync::{Arc, OnceLock};

use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnv;

/// Memory pressure classification based on pool utilization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressure {
    /// < 70% utilization — normal operation
    Green,
    /// 70-85% utilization — warning, consider scaling
    Yellow,
    /// 85-95% utilization — high pressure, spilling expected
    Orange,
    /// > 95% utilization — critical, reject new queries
    Red,
}

impl MemoryPressure {
    /// Classify memory pressure from used bytes and total limit.
    pub fn from_usage(used: usize, limit: usize) -> Self {
        if limit == 0 {
            return Self::Green;
        }
        let pct = (used as f64 / limit as f64) * 100.0;
        match pct as u64 {
            0..=69 => Self::Green,
            70..=84 => Self::Yellow,
            85..=94 => Self::Orange,
            _ => Self::Red,
        }
    }

    /// Returns `true` if new queries should be admitted at this pressure level.
    pub fn admits_new_query(&self) -> bool {
        !matches!(self, Self::Red)
    }

    /// Returns the pressure level as a numeric gauge value for Prometheus.
    /// 0=Green, 1=Yellow, 2=Orange, 3=Red.
    pub fn as_gauge(&self) -> f64 {
        match self {
            Self::Green => 0.0,
            Self::Yellow => 1.0,
            Self::Orange => 2.0,
            Self::Red => 3.0,
        }
    }

    /// Returns the pressure level name for logging.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Green => "green",
            Self::Yellow => "yellow",
            Self::Orange => "orange",
            Self::Red => "red",
        }
    }
}

impl std::fmt::Display for MemoryPressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Check current memory pressure level from the DataFusion [`MemoryPool`].
///
/// Uses `MemoryPool::reserved()` for current usage and
/// `MemoryPool::memory_limit()` for the configured limit.
/// Returns `MemoryPressure::Green` for unlimited pools.
pub fn check_pressure(pool: &Arc<dyn MemoryPool>) -> MemoryPressure {
    let used = pool.reserved();
    let limit = match pool.memory_limit() {
        MemoryLimit::Finite(n) => n,
        _ => return MemoryPressure::Green,
    };
    MemoryPressure::from_usage(used, limit)
}

/// Returns the current memory usage in bytes from the pool, or 0 for unlimited.
pub fn used_bytes(pool: &Arc<dyn MemoryPool>) -> usize {
    pool.reserved()
}

/// Returns the memory limit in bytes from the pool, or 0 for unlimited.
pub fn limit_bytes(pool: &Arc<dyn MemoryPool>) -> usize {
    match pool.memory_limit() {
        MemoryLimit::Finite(n) => n,
        _ => 0,
    }
}

// ── Per-query memory observation ─────────────────────────────────────────
//
// Phase 0 of `openspec/changes/scan-throughput-memory-safety`: make memory
// growth attributable instead of guessed at. One INFO line per completed
// query carries the pool residue and process RSS; residue above a threshold
// additionally logs the pool's top consumers by name. Two kernel OOM kills at
// SF10 (2026-07-06) happened below the configured pool cap, so RSS-vs-pool
// divergence across a query sweep is the primary retention signal.

/// The coordinator's concrete tracked pool, kept alongside the `dyn` handle in
/// the runtime so [`tracked_pool_report`] can name consumers. One coordinator
/// runtime per process; set once at startup, `None` under `memory_pool =
/// "fair"` (FairSpillPool has no consumer tracking).
static TRACKED_POOL: OnceLock<Arc<TrackConsumersPool<GreedyMemoryPool>>> = OnceLock::new();

/// Record the concrete tracked pool for consumer reporting. Later calls are
/// ignored (tests build multiple runtimes in one process; the first wins).
pub fn set_tracked_pool(pool: Arc<TrackConsumersPool<GreedyMemoryPool>>) {
    let _ = TRACKED_POOL.set(pool);
}

/// The top `n` pool consumers by reserved bytes, or `None` when the runtime
/// was built without consumer tracking.
pub fn tracked_pool_report(n: usize) -> Option<String> {
    TRACKED_POOL.get().map(|p| p.report_top(n))
}

/// Current process resident set size in bytes, from `/proc/self/status`
/// (`VmRSS`). Returns `None` where procfs is unavailable (macOS dev boxes);
/// the retention gates run on Linux.
pub fn process_rss_bytes() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Pool residue above this after a query completes triggers a consumer-report
/// WARN. Concurrent queries legitimately hold reservations, so the report is
/// a sequential-sweep diagnostic, not an alert; the threshold keeps it quiet
/// during normal concurrent operation.
const RESIDUE_REPORT_BYTES: usize = 64 * 1024 * 1024;

/// Log pool residue and process RSS at query completion. Call exactly once
/// per query, after the tracker records the terminal state (the
/// `StreamFinalizer` for streamed queries, the batch path otherwise).
pub fn observe_query_end(pool: &Arc<dyn MemoryPool>, query_id: &dyn std::fmt::Display) {
    let pool_residue = pool.reserved();
    let rss = process_rss_bytes();
    tracing::info!(
        query_id = %query_id,
        pool_residue_bytes = pool_residue,
        process_rss_bytes = rss.unwrap_or(0),
        "query memory at completion"
    );
    if pool_residue > RESIDUE_REPORT_BYTES {
        if let Some(report) = tracked_pool_report(5) {
            tracing::warn!(
                query_id = %query_id,
                pool_residue_bytes = pool_residue,
                "pool residue after query completion; top consumers: {report}"
            );
        }
    }
}

/// Per-user memory account: tracks bytes reserved by in-flight queries
/// keyed by username and enforces a per-user budget on admission.
///
/// Reservations are recorded via [`PerUserMemoryRegistry::try_reserve`]; the
/// returned [`PerUserReservation`] guard releases the bytes when it drops
/// (the streaming result wrapper carries it for the stream's lifetime).
/// Setting `budget_bytes = 0` disables the cap entirely.
#[derive(Debug, Default)]
pub struct PerUserMemoryRegistry {
    used: dashmap::DashMap<String, usize>,
}

#[derive(Debug)]
pub struct PerUserReservation {
    registry: Arc<PerUserMemoryRegistry>,
    user: String,
    bytes: usize,
}

impl PerUserMemoryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes currently reserved by the given user (0 if unknown).
    pub fn used_bytes(&self, user: &str) -> usize {
        self.used.get(user).map(|v| *v).unwrap_or(0)
    }

    /// Attempt to record a new reservation of `bytes` for `user`. Returns
    /// `None` if the user is already at or above `budget_bytes`; the caller
    /// should reject the query with a per-user pressure error in that case.
    /// `budget_bytes == 0` disables the cap and always returns `Some`.
    pub fn try_reserve(
        self: &Arc<Self>,
        user: &str,
        bytes: usize,
        budget_bytes: usize,
    ) -> Option<PerUserReservation> {
        if budget_bytes == 0 {
            return Some(PerUserReservation {
                registry: self.clone(),
                user: user.to_string(),
                bytes: 0,
            });
        }
        let mut entry = self.used.entry(user.to_string()).or_insert(0);
        if *entry + bytes > budget_bytes {
            return None;
        }
        *entry += bytes;
        Some(PerUserReservation {
            registry: self.clone(),
            user: user.to_string(),
            bytes,
        })
    }
}

impl Drop for PerUserReservation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        if let Some(mut entry) = self.registry.used.get_mut(&self.user) {
            *entry = entry.saturating_sub(self.bytes);
        }
    }
}

/// Spawn a background task that updates memory metrics every second.
///
/// This ensures Prometheus/Grafana always sees current memory usage,
/// even between queries. Without this, gauges only update at query start
/// and show 0 between queries (because the 5s scrape interval misses
/// the brief query execution window).
pub fn spawn_metrics_reporter(
    runtime: Arc<RuntimeEnv>,
    metrics: Arc<sqe_metrics::MetricsRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            let pool = &runtime.memory_pool;
            let used = pool.reserved();
            let limit = match pool.memory_limit() {
                MemoryLimit::Finite(n) => n,
                _ => 0,
            };
            let pressure = MemoryPressure::from_usage(used, limit);
            metrics.coordinator_memory_used_bytes.set(used as f64);
            metrics.coordinator_memory_limit_bytes.set(limit as f64);
            metrics.coordinator_memory_pressure.set(pressure.as_gauge());
            if let Some(rss) = process_rss_bytes() {
                metrics.coordinator_rss_bytes.set(rss as f64);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pressure_green_at_zero() {
        assert_eq!(MemoryPressure::from_usage(0, 1000), MemoryPressure::Green);
    }

    #[test]
    fn test_pressure_green_below_70() {
        assert_eq!(
            MemoryPressure::from_usage(690, 1000),
            MemoryPressure::Green
        );
    }

    #[test]
    fn test_pressure_yellow_at_70() {
        assert_eq!(
            MemoryPressure::from_usage(700, 1000),
            MemoryPressure::Yellow
        );
    }

    #[test]
    fn test_pressure_yellow_at_84() {
        assert_eq!(
            MemoryPressure::from_usage(840, 1000),
            MemoryPressure::Yellow
        );
    }

    #[test]
    fn test_pressure_orange_at_85() {
        assert_eq!(
            MemoryPressure::from_usage(850, 1000),
            MemoryPressure::Orange
        );
    }

    #[test]
    fn test_pressure_orange_at_94() {
        assert_eq!(
            MemoryPressure::from_usage(940, 1000),
            MemoryPressure::Orange
        );
    }

    #[test]
    fn test_pressure_red_at_95() {
        assert_eq!(
            MemoryPressure::from_usage(950, 1000),
            MemoryPressure::Red
        );
    }

    #[test]
    fn test_pressure_red_at_100() {
        assert_eq!(
            MemoryPressure::from_usage(1000, 1000),
            MemoryPressure::Red
        );
    }

    #[test]
    fn test_pressure_red_over_limit() {
        // Can happen briefly before spill kicks in
        assert_eq!(
            MemoryPressure::from_usage(1100, 1000),
            MemoryPressure::Red
        );
    }

    #[test]
    fn test_pressure_green_for_zero_limit() {
        // Unlimited pool should always be Green
        assert_eq!(MemoryPressure::from_usage(999, 0), MemoryPressure::Green);
    }

    #[test]
    fn test_admits_new_query() {
        assert!(MemoryPressure::Green.admits_new_query());
        assert!(MemoryPressure::Yellow.admits_new_query());
        assert!(MemoryPressure::Orange.admits_new_query());
        assert!(!MemoryPressure::Red.admits_new_query());
    }

    #[test]
    fn test_gauge_values() {
        assert_eq!(MemoryPressure::Green.as_gauge(), 0.0);
        assert_eq!(MemoryPressure::Yellow.as_gauge(), 1.0);
        assert_eq!(MemoryPressure::Orange.as_gauge(), 2.0);
        assert_eq!(MemoryPressure::Red.as_gauge(), 3.0);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", MemoryPressure::Green), "green");
        assert_eq!(format!("{}", MemoryPressure::Red), "red");
    }

    #[test]
    fn test_check_pressure_with_fair_spill_pool() {
        use datafusion::execution::memory_pool::FairSpillPool;

        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024 * 1024)); // 1MB
        // Fresh pool should have zero usage
        let pressure = check_pressure(&pool);
        assert_eq!(pressure, MemoryPressure::Green);
    }

    #[test]
    fn test_used_bytes_and_limit_bytes() {
        use datafusion::execution::memory_pool::FairSpillPool;

        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024 * 1024));
        assert_eq!(used_bytes(&pool), 0);
        assert_eq!(limit_bytes(&pool), 1024 * 1024);
    }

    #[test]
    fn per_user_memory_admits_when_under_budget() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        let r1 = reg.try_reserve("alice", 100, 500);
        assert!(r1.is_some());
        assert_eq!(reg.used_bytes("alice"), 100);
        let r2 = reg.try_reserve("alice", 200, 500);
        assert!(r2.is_some());
        assert_eq!(reg.used_bytes("alice"), 300);
    }

    #[test]
    fn per_user_memory_rejects_when_budget_exceeded() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        let _r1 = reg.try_reserve("alice", 400, 500).unwrap();
        let r2 = reg.try_reserve("alice", 200, 500);
        assert!(r2.is_none(), "second reservation should exceed budget");
        assert_eq!(reg.used_bytes("alice"), 400);
    }

    #[test]
    fn per_user_memory_releases_on_drop() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        {
            let _r = reg.try_reserve("alice", 400, 500).unwrap();
            assert_eq!(reg.used_bytes("alice"), 400);
        }
        assert_eq!(reg.used_bytes("alice"), 0);
    }

    #[test]
    fn per_user_memory_is_independent_per_user() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        let _a = reg.try_reserve("alice", 400, 500).unwrap();
        let b = reg.try_reserve("bob", 400, 500);
        assert!(b.is_some(), "bob's budget is independent from alice");
        assert_eq!(reg.used_bytes("alice"), 400);
        assert_eq!(reg.used_bytes("bob"), 400);
    }

    #[test]
    fn process_rss_reads_on_linux_and_degrades_elsewhere() {
        let rss = process_rss_bytes();
        if cfg!(target_os = "linux") {
            assert!(rss.expect("procfs available") > 0);
        }
        // Non-Linux: None is the contract; nothing further to assert.
    }

    #[test]
    fn observe_query_end_handles_residue_and_empty_pools() {
        use datafusion::execution::memory_pool::{MemoryConsumer, GreedyMemoryPool};
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 30));
        // Empty pool: must not panic.
        observe_query_end(&pool, &"q-empty");
        // Live reservation above the report threshold: must not panic even
        // when the global tracked pool belongs to another runtime (or none).
        let mut r = MemoryConsumer::new("residue-test").register(&pool);
        r.try_grow(RESIDUE_REPORT_BYTES + 1).expect("grow");
        observe_query_end(&pool, &"q-residue");
    }

    #[test]
    fn per_user_memory_zero_budget_disables_cap() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        let r = reg.try_reserve("alice", usize::MAX / 2, 0);
        assert!(r.is_some(), "budget = 0 should accept any reservation");
        // Zero-byte tracking entry: used_bytes stays 0 because we never updated.
        assert_eq!(reg.used_bytes("alice"), 0);
    }
}
