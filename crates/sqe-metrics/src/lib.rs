pub mod server;
pub mod audit;
pub mod otel;
pub mod propagation;

use prometheus::{
    Counter, CounterVec, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, Opts, Registry,
};

/// Trait for types that expose a Prometheus [`Registry`] for metrics serving.
pub trait HasRegistry: Send + Sync + 'static {
    fn prometheus_registry(&self) -> &Registry;
}

// ---------------------------------------------------------------------------
// Coordinator metrics
// ---------------------------------------------------------------------------

/// Central metrics registry for the SQE coordinator.
#[derive(Clone)]
pub struct MetricsRegistry {
    pub registry: Registry,
    pub query_count: CounterVec,
    pub query_duration: HistogramVec,
    pub rows_returned: Counter,
    pub active_sessions: IntGauge,
    pub healthy_workers: IntGauge,
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub cache_invalidations: Counter,
    pub cache_size_bytes: Gauge,
    pub cache_entries: Gauge,
    // Scheduler metrics
    pub scheduler_decisions: IntCounterVec,
    pub scheduler_task_count: Histogram,
    pub scheduler_task_size_mb: Histogram,
    pub scheduler_stragglers: IntCounter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();

        let query_count = CounterVec::new(
            Opts::new("sqe_query_count_total", "Total queries executed"),
            &["status", "statement_type"],
        )
        .unwrap();
        registry.register(Box::new(query_count.clone())).unwrap();

        let query_duration = HistogramVec::new(
            HistogramOpts::new("sqe_query_duration_seconds", "Query execution duration")
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["statement_type"],
        )
        .unwrap();
        registry.register(Box::new(query_duration.clone())).unwrap();

        let rows_returned = Counter::new(
            "sqe_rows_returned_total",
            "Total rows returned across all queries",
        )
        .unwrap();
        registry.register(Box::new(rows_returned.clone())).unwrap();

        let active_sessions = IntGauge::new(
            "sqe_active_sessions",
            "Number of active sessions",
        )
        .unwrap();
        registry.register(Box::new(active_sessions.clone())).unwrap();

        let healthy_workers = IntGauge::new(
            "sqe_healthy_workers",
            "Number of healthy workers",
        )
        .unwrap();
        registry.register(Box::new(healthy_workers.clone())).unwrap();

        let cache_hits = Counter::new("sqe_cache_hits_total", "Total cache hits").unwrap();
        let cache_misses = Counter::new("sqe_cache_misses_total", "Total cache misses").unwrap();
        let cache_invalidations = Counter::new("sqe_cache_invalidations_total", "Total cache invalidations").unwrap();
        let cache_size_bytes = Gauge::new("sqe_cache_size_bytes", "Current cache memory usage in bytes").unwrap();
        let cache_entries = Gauge::new("sqe_cache_entries", "Current number of cached entries").unwrap();

        registry.register(Box::new(cache_hits.clone())).unwrap();
        registry.register(Box::new(cache_misses.clone())).unwrap();
        registry.register(Box::new(cache_invalidations.clone())).unwrap();
        registry.register(Box::new(cache_size_bytes.clone())).unwrap();
        registry.register(Box::new(cache_entries.clone())).unwrap();

        let scheduler_decisions = IntCounterVec::new(
            Opts::new("sqe_scheduler_decisions_total", "Scheduling decisions by type"),
            &["decision"],
        )
        .unwrap();
        registry.register(Box::new(scheduler_decisions.clone())).unwrap();

        let scheduler_task_count = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_scheduler_task_count",
                "Number of tasks per distributed query",
            )
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0]),
        )
        .unwrap();
        registry.register(Box::new(scheduler_task_count.clone())).unwrap();

        let scheduler_task_size_mb = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_scheduler_task_size_mb",
                "Size of individual scan tasks in MB",
            )
            .buckets(vec![1.0, 10.0, 50.0, 100.0, 256.0, 512.0, 1024.0, 5120.0]),
        )
        .unwrap();
        registry.register(Box::new(scheduler_task_size_mb.clone())).unwrap();

        let scheduler_stragglers = IntCounter::new(
            "sqe_scheduler_stragglers_total",
            "Number of straggler fragments detected",
        )
        .unwrap();
        registry.register(Box::new(scheduler_stragglers.clone())).unwrap();

        Self {
            registry,
            query_count,
            query_duration,
            rows_returned,
            active_sessions,
            healthy_workers,
            cache_hits,
            cache_misses,
            cache_invalidations,
            cache_size_bytes,
            cache_entries,
            scheduler_decisions,
            scheduler_task_count,
            scheduler_task_size_mb,
            scheduler_stragglers,
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HasRegistry for MetricsRegistry {
    fn prometheus_registry(&self) -> &Registry {
        &self.registry
    }
}

// ---------------------------------------------------------------------------
// Worker metrics
// ---------------------------------------------------------------------------

/// Metrics registry for SQE workers.
///
/// Tracks per-worker counters for fragments executed, rows scanned, bytes read,
/// and a histogram of fragment execution durations.
#[derive(Clone)]
pub struct WorkerMetricsRegistry {
    pub registry: Registry,
    /// Total number of scan fragments executed by this worker.
    pub fragments_executed: Counter,
    /// Total rows scanned across all fragments.
    pub rows_scanned: Counter,
    /// Total bytes read from storage across all fragments.
    pub bytes_read: Counter,
    /// Histogram of per-fragment execution time in seconds.
    pub fragment_duration: Histogram,
}

impl WorkerMetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();

        let fragments_executed = Counter::new(
            "sqe_worker_fragments_executed_total",
            "Total number of scan fragments executed",
        )
        .unwrap();
        registry
            .register(Box::new(fragments_executed.clone()))
            .unwrap();

        let rows_scanned = Counter::new(
            "sqe_worker_rows_scanned_total",
            "Total rows scanned across all fragments",
        )
        .unwrap();
        registry
            .register(Box::new(rows_scanned.clone()))
            .unwrap();

        let bytes_read = Counter::new(
            "sqe_worker_bytes_read_total",
            "Total bytes read from storage",
        )
        .unwrap();
        registry
            .register(Box::new(bytes_read.clone()))
            .unwrap();

        let fragment_duration = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_worker_fragment_duration_seconds",
                "Per-fragment execution time",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
        )
        .unwrap();
        registry
            .register(Box::new(fragment_duration.clone()))
            .unwrap();

        Self {
            registry,
            fragments_executed,
            rows_scanned,
            bytes_read,
            fragment_duration,
        }
    }
}

impl Default for WorkerMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HasRegistry for WorkerMetricsRegistry {
    fn prometheus_registry(&self) -> &Registry {
        &self.registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Coordinator MetricsRegistry tests ────────────────────────

    #[test]
    fn test_metrics_registry_creation() {
        let metrics = MetricsRegistry::new();
        // Touch each metric so Prometheus includes it in gather()
        metrics.query_count.with_label_values(&["success", "query"]).inc_by(0.0);
        metrics.query_duration.with_label_values(&["query"]).observe(0.0);
        metrics.rows_returned.inc_by(0.0);
        metrics.active_sessions.set(0);
        metrics.healthy_workers.set(0);
        metrics.cache_hits.inc_by(0.0);
        metrics.cache_misses.inc_by(0.0);
        metrics.cache_invalidations.inc_by(0.0);
        metrics.cache_size_bytes.set(0.0);
        metrics.cache_entries.set(0.0);
        metrics.scheduler_decisions.with_label_values(&["local"]).inc_by(0);
        metrics.scheduler_task_count.observe(0.0);
        metrics.scheduler_task_size_mb.observe(0.0);
        metrics.scheduler_stragglers.inc_by(0);
        assert!(metrics.registry.gather().len() >= 14);
    }

    #[test]
    fn test_query_count_increment() {
        let metrics = MetricsRegistry::new();
        metrics.query_count.with_label_values(&["success", "query"]).inc();
        let count = metrics.query_count.with_label_values(&["success", "query"]).get();
        assert_eq!(count, 1.0);
    }

    #[test]
    fn test_query_duration_observe() {
        let metrics = MetricsRegistry::new();
        metrics.query_duration.with_label_values(&["query"]).observe(0.5);
        let count = metrics.query_duration.with_label_values(&["query"]).get_sample_count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_active_sessions_gauge() {
        let metrics = MetricsRegistry::new();
        metrics.active_sessions.inc();
        metrics.active_sessions.inc();
        assert_eq!(metrics.active_sessions.get(), 2);
        metrics.active_sessions.dec();
        assert_eq!(metrics.active_sessions.get(), 1);
    }

    // ── WorkerMetricsRegistry tests ─────────────────────────────

    #[test]
    fn test_worker_metrics_registry_creation() {
        let m = WorkerMetricsRegistry::new();
        // Touch each metric so Prometheus includes it in gather()
        m.fragments_executed.inc_by(0.0);
        m.rows_scanned.inc_by(0.0);
        m.bytes_read.inc_by(0.0);
        m.fragment_duration.observe(0.0);
        assert!(m.registry.gather().len() >= 4);
    }

    #[test]
    fn test_worker_fragments_executed_counter() {
        let m = WorkerMetricsRegistry::new();
        m.fragments_executed.inc();
        m.fragments_executed.inc();
        assert_eq!(m.fragments_executed.get(), 2.0);
    }

    #[test]
    fn test_worker_rows_scanned_counter() {
        let m = WorkerMetricsRegistry::new();
        m.rows_scanned.inc_by(500.0);
        m.rows_scanned.inc_by(300.0);
        assert_eq!(m.rows_scanned.get(), 800.0);
    }

    #[test]
    fn test_worker_bytes_read_counter() {
        let m = WorkerMetricsRegistry::new();
        m.bytes_read.inc_by(1024.0);
        m.bytes_read.inc_by(2048.0);
        assert_eq!(m.bytes_read.get(), 3072.0);
    }

    #[test]
    fn test_worker_fragment_duration_histogram() {
        let m = WorkerMetricsRegistry::new();
        m.fragment_duration.observe(0.1);
        m.fragment_duration.observe(0.5);
        m.fragment_duration.observe(2.0);
        assert_eq!(m.fragment_duration.get_sample_count(), 3);
        let sum = m.fragment_duration.get_sample_sum();
        assert!((sum - 2.6).abs() < 1e-9);
    }

    #[test]
    fn test_worker_metrics_default() {
        let m = WorkerMetricsRegistry::default();
        // Confirm Default trait works
        m.fragments_executed.inc();
        assert_eq!(m.fragments_executed.get(), 1.0);
    }
}
