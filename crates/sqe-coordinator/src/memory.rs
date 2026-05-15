//! Memory pressure monitoring and admission control for the coordinator.
//!
//! Monitors the coordinator's DataFusion [`FairSpillPool`] memory usage and
//! classifies it into pressure levels. When memory pressure reaches `Red`
//! (>95% utilization), new queries are rejected to prevent OOM.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
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
    fn per_user_memory_zero_budget_disables_cap() {
        let reg = Arc::new(PerUserMemoryRegistry::new());
        let r = reg.try_reserve("alice", usize::MAX / 2, 0);
        assert!(r.is_some(), "budget = 0 should accept any reservation");
        // Zero-byte tracking entry: used_bytes stays 0 because we never updated.
        assert_eq!(reg.used_bytes("alice"), 0);
    }
}
