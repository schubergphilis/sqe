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
    // Footer cache metrics
    pub footer_cache_hits: Counter,
    pub footer_cache_misses: Counter,
    pub footer_cache_size_bytes: Gauge,
    // Coordinator memory pressure metrics
    pub coordinator_memory_used_bytes: Gauge,
    pub coordinator_memory_limit_bytes: Gauge,
    pub coordinator_memory_pressure: Gauge,

    // Spill metrics
    pub sort_spill_count: Counter,
    pub sort_spill_bytes: Counter,
    pub join_spill_count: Counter,
    pub join_spill_bytes: Counter,

    // Shuffle metrics (Phase B — registered now, incremented when shuffle lands)
    pub shuffle_bytes_sent: Counter,
    pub shuffle_bytes_received: Counter,
    pub shuffle_partitions: Counter,

    // Late materialization metrics
    pub late_mat_bytes_predicate: Counter,
    pub late_mat_bytes_projection: Counter,
    pub late_mat_selectivity: Histogram,

    // Pruning metrics
    pub files_pruned_minmax: Counter,
    pub files_pruned_bloom: Counter,
    pub pages_pruned_index: Counter,

    // Latency
    pub time_to_first_row: Histogram,

    // S3 I/O metrics
    pub s3_requests_total: IntCounterVec,
    pub s3_bytes_read_total: IntCounter,
    pub s3_bytes_written_total: IntCounter,
    pub s3_request_duration_seconds: Histogram,

    // Auth metrics
    pub auth_attempts_total: IntCounterVec,
    pub auth_duration_seconds: Histogram,
    pub token_refresh_total: IntCounterVec,

    // Adaptive sort metrics
    pub sorts_stripped_total: IntCounterVec,
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

        let footer_cache_hits = Counter::new(
            "sqe_footer_cache_hits_total",
            "Total Parquet footer cache hits",
        )
        .unwrap();
        registry.register(Box::new(footer_cache_hits.clone())).unwrap();

        let footer_cache_misses = Counter::new(
            "sqe_footer_cache_misses_total",
            "Total Parquet footer cache misses",
        )
        .unwrap();
        registry.register(Box::new(footer_cache_misses.clone())).unwrap();

        let footer_cache_size_bytes = Gauge::new(
            "sqe_footer_cache_size_bytes",
            "Current estimated size of Parquet footer cache in bytes",
        )
        .unwrap();
        registry.register(Box::new(footer_cache_size_bytes.clone())).unwrap();

        let coordinator_memory_used_bytes = Gauge::new(
            "sqe_coordinator_memory_used_bytes",
            "Current coordinator DataFusion memory pool usage in bytes",
        )
        .unwrap();
        registry.register(Box::new(coordinator_memory_used_bytes.clone())).unwrap();

        let coordinator_memory_limit_bytes = Gauge::new(
            "sqe_coordinator_memory_limit_bytes",
            "Coordinator DataFusion memory pool limit in bytes",
        )
        .unwrap();
        registry.register(Box::new(coordinator_memory_limit_bytes.clone())).unwrap();

        let coordinator_memory_pressure = Gauge::new(
            "sqe_coordinator_memory_pressure",
            "Coordinator memory pressure level (0=green, 1=yellow, 2=orange, 3=red)",
        )
        .unwrap();
        registry.register(Box::new(coordinator_memory_pressure.clone())).unwrap();

        // Spill metrics
        let sort_spill_count = Counter::new(
            "sqe_sort_spill_count_total",
            "Number of sort spill events",
        )
        .unwrap();
        registry.register(Box::new(sort_spill_count.clone())).unwrap();

        let sort_spill_bytes = Counter::new(
            "sqe_sort_spill_bytes_total",
            "Bytes spilled for sorts",
        )
        .unwrap();
        registry.register(Box::new(sort_spill_bytes.clone())).unwrap();

        let join_spill_count = Counter::new(
            "sqe_join_spill_count_total",
            "Join spill events (SortMergeJoin)",
        )
        .unwrap();
        registry.register(Box::new(join_spill_count.clone())).unwrap();

        let join_spill_bytes = Counter::new(
            "sqe_join_spill_bytes_total",
            "Bytes spilled for joins",
        )
        .unwrap();
        registry.register(Box::new(join_spill_bytes.clone())).unwrap();

        // Shuffle metrics (Phase B — registered now, incremented when shuffle lands)
        let shuffle_bytes_sent = Counter::new(
            "sqe_shuffle_bytes_sent_total",
            "Bytes sent via DoExchange",
        )
        .unwrap();
        registry.register(Box::new(shuffle_bytes_sent.clone())).unwrap();

        let shuffle_bytes_received = Counter::new(
            "sqe_shuffle_bytes_received_total",
            "Bytes received via DoExchange",
        )
        .unwrap();
        registry.register(Box::new(shuffle_bytes_received.clone())).unwrap();

        let shuffle_partitions = Counter::new(
            "sqe_shuffle_partitions_total",
            "Shuffle partitions created",
        )
        .unwrap();
        registry.register(Box::new(shuffle_partitions.clone())).unwrap();

        // Late materialization metrics
        let late_mat_bytes_predicate = Counter::new(
            "sqe_late_mat_bytes_predicate_total",
            "Bytes read for predicate evaluation",
        )
        .unwrap();
        registry.register(Box::new(late_mat_bytes_predicate.clone())).unwrap();

        let late_mat_bytes_projection = Counter::new(
            "sqe_late_mat_bytes_projection_total",
            "Bytes read for projection",
        )
        .unwrap();
        registry.register(Box::new(late_mat_bytes_projection.clone())).unwrap();

        let late_mat_selectivity = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_late_mat_selectivity",
                "Late materialization selectivity (rows surviving / total rows)",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]),
        )
        .unwrap();
        registry.register(Box::new(late_mat_selectivity.clone())).unwrap();

        // Pruning metrics
        let files_pruned_minmax = Counter::new(
            "sqe_files_pruned_minmax_total",
            "Files skipped by min/max pruning",
        )
        .unwrap();
        registry.register(Box::new(files_pruned_minmax.clone())).unwrap();

        let files_pruned_bloom = Counter::new(
            "sqe_files_pruned_bloom_total",
            "Files skipped by bloom filter",
        )
        .unwrap();
        registry.register(Box::new(files_pruned_bloom.clone())).unwrap();

        let pages_pruned_index = Counter::new(
            "sqe_pages_pruned_index_total",
            "Pages skipped by page index",
        )
        .unwrap();
        registry.register(Box::new(pages_pruned_index.clone())).unwrap();

        // Latency
        let time_to_first_row = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_time_to_first_row_seconds",
                "Time from query submit to first result row",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        )
        .unwrap();
        registry.register(Box::new(time_to_first_row.clone())).unwrap();

        // S3 I/O metrics
        let s3_requests_total = IntCounterVec::new(
            Opts::new("sqe_s3_requests_total", "Total S3 requests by operation and status"),
            &["operation", "status"],
        )
        .unwrap();
        registry.register(Box::new(s3_requests_total.clone())).unwrap();

        let s3_bytes_read_total = IntCounter::new(
            "sqe_s3_bytes_read_total",
            "Total bytes fetched from S3",
        )
        .unwrap();
        registry.register(Box::new(s3_bytes_read_total.clone())).unwrap();

        let s3_bytes_written_total = IntCounter::new(
            "sqe_s3_bytes_written_total",
            "Total bytes written to S3",
        )
        .unwrap();
        registry.register(Box::new(s3_bytes_written_total.clone())).unwrap();

        let s3_request_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_s3_request_duration_seconds",
                "S3 request latency in seconds",
            )
            .buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        )
        .unwrap();
        registry.register(Box::new(s3_request_duration_seconds.clone())).unwrap();

        // Auth metrics
        let auth_attempts_total = IntCounterVec::new(
            Opts::new(
                "sqe_auth_attempts_total",
                "Total authentication attempts by provider and status",
            ),
            &["provider", "status"],
        )
        .unwrap();
        registry.register(Box::new(auth_attempts_total.clone())).unwrap();

        let auth_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "sqe_auth_duration_seconds",
                "Authentication handshake latency in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        )
        .unwrap();
        registry.register(Box::new(auth_duration_seconds.clone())).unwrap();

        let token_refresh_total = IntCounterVec::new(
            Opts::new(
                "sqe_token_refresh_total",
                "Total token refresh attempts by status",
            ),
            &["status"],
        )
        .unwrap();
        registry.register(Box::new(token_refresh_total.clone())).unwrap();

        // Adaptive sort stripping metric
        let sorts_stripped_total = IntCounterVec::new(
            Opts::new(
                "sqe_sorts_stripped_total",
                "Total sort operations stripped by adaptive sort rule",
            ),
            &["mode", "reason"],
        )
        .unwrap();
        registry.register(Box::new(sorts_stripped_total.clone())).unwrap();

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
            footer_cache_hits,
            footer_cache_misses,
            footer_cache_size_bytes,
            coordinator_memory_used_bytes,
            coordinator_memory_limit_bytes,
            coordinator_memory_pressure,
            sort_spill_count,
            sort_spill_bytes,
            join_spill_count,
            join_spill_bytes,
            shuffle_bytes_sent,
            shuffle_bytes_received,
            shuffle_partitions,
            late_mat_bytes_predicate,
            late_mat_bytes_projection,
            late_mat_selectivity,
            files_pruned_minmax,
            files_pruned_bloom,
            pages_pruned_index,
            time_to_first_row,
            s3_requests_total,
            s3_bytes_read_total,
            s3_bytes_written_total,
            s3_request_duration_seconds,
            auth_attempts_total,
            auth_duration_seconds,
            token_refresh_total,
            sorts_stripped_total,
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MetricsRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsRegistry").finish_non_exhaustive()
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
        metrics.footer_cache_hits.inc_by(0.0);
        metrics.footer_cache_misses.inc_by(0.0);
        metrics.footer_cache_size_bytes.set(0.0);
        // New streaming execution metrics
        metrics.sort_spill_count.inc_by(0.0);
        metrics.sort_spill_bytes.inc_by(0.0);
        metrics.join_spill_count.inc_by(0.0);
        metrics.join_spill_bytes.inc_by(0.0);
        metrics.shuffle_bytes_sent.inc_by(0.0);
        metrics.shuffle_bytes_received.inc_by(0.0);
        metrics.shuffle_partitions.inc_by(0.0);
        metrics.late_mat_bytes_predicate.inc_by(0.0);
        metrics.late_mat_bytes_projection.inc_by(0.0);
        metrics.late_mat_selectivity.observe(0.5);
        metrics.files_pruned_minmax.inc_by(0.0);
        metrics.files_pruned_bloom.inc_by(0.0);
        metrics.pages_pruned_index.inc_by(0.0);
        metrics.time_to_first_row.observe(0.1);
        // S3 I/O metrics
        metrics.s3_requests_total.with_label_values(&["get", "success"]).inc_by(0);
        metrics.s3_bytes_read_total.inc_by(0);
        metrics.s3_bytes_written_total.inc_by(0);
        metrics.s3_request_duration_seconds.observe(0.01);
        // Auth metrics
        metrics.auth_attempts_total.with_label_values(&["oidc", "success"]).inc_by(0);
        metrics.auth_duration_seconds.observe(0.1);
        metrics.token_refresh_total.with_label_values(&["success"]).inc_by(0);
        // Adaptive sort metric
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc_by(0);
        // 17 original + 14 streaming + 7 new (S3 + auth) + 1 adaptive sort = 39 minimum
        assert!(metrics.registry.gather().len() >= 39);
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

    #[test]
    fn test_spill_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.sort_spill_count.inc_by(5.0);
        metrics.sort_spill_bytes.inc_by(1024.0);
        metrics.join_spill_count.inc_by(3.0);
        metrics.join_spill_bytes.inc_by(2048.0);
        assert_eq!(metrics.sort_spill_count.get(), 5.0);
        assert_eq!(metrics.sort_spill_bytes.get(), 1024.0);
        assert_eq!(metrics.join_spill_count.get(), 3.0);
        assert_eq!(metrics.join_spill_bytes.get(), 2048.0);
    }

    #[test]
    fn test_shuffle_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.shuffle_bytes_sent.inc_by(4096.0);
        metrics.shuffle_bytes_received.inc_by(8192.0);
        metrics.shuffle_partitions.inc_by(10.0);
        assert_eq!(metrics.shuffle_bytes_sent.get(), 4096.0);
        assert_eq!(metrics.shuffle_bytes_received.get(), 8192.0);
        assert_eq!(metrics.shuffle_partitions.get(), 10.0);
    }

    #[test]
    fn test_late_materialization_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.late_mat_bytes_predicate.inc_by(500.0);
        metrics.late_mat_bytes_projection.inc_by(1500.0);
        assert_eq!(metrics.late_mat_bytes_predicate.get(), 500.0);
        assert_eq!(metrics.late_mat_bytes_projection.get(), 1500.0);

        metrics.late_mat_selectivity.observe(0.25);
        metrics.late_mat_selectivity.observe(0.75);
        assert_eq!(metrics.late_mat_selectivity.get_sample_count(), 2);
    }

    #[test]
    fn test_pruning_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.files_pruned_minmax.inc_by(10.0);
        metrics.files_pruned_bloom.inc_by(5.0);
        metrics.pages_pruned_index.inc_by(20.0);
        assert_eq!(metrics.files_pruned_minmax.get(), 10.0);
        assert_eq!(metrics.files_pruned_bloom.get(), 5.0);
        assert_eq!(metrics.pages_pruned_index.get(), 20.0);
    }

    #[test]
    fn test_time_to_first_row_histogram() {
        let metrics = MetricsRegistry::new();
        metrics.time_to_first_row.observe(0.05);
        metrics.time_to_first_row.observe(0.5);
        metrics.time_to_first_row.observe(5.0);
        assert_eq!(metrics.time_to_first_row.get_sample_count(), 3);
        let sum = metrics.time_to_first_row.get_sample_sum();
        assert!((sum - 5.55).abs() < 1e-9);
    }

    #[test]
    fn test_s3_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.s3_requests_total.with_label_values(&["get", "success"]).inc();
        metrics.s3_requests_total.with_label_values(&["get", "success"]).inc();
        metrics.s3_requests_total.with_label_values(&["put", "success"]).inc();
        metrics.s3_requests_total.with_label_values(&["get", "error"]).inc();
        assert_eq!(
            metrics.s3_requests_total.with_label_values(&["get", "success"]).get(),
            2
        );
        assert_eq!(
            metrics.s3_requests_total.with_label_values(&["put", "success"]).get(),
            1
        );
        assert_eq!(
            metrics.s3_requests_total.with_label_values(&["get", "error"]).get(),
            1
        );

        metrics.s3_bytes_read_total.inc_by(4096);
        metrics.s3_bytes_written_total.inc_by(2048);
        assert_eq!(metrics.s3_bytes_read_total.get(), 4096);
        assert_eq!(metrics.s3_bytes_written_total.get(), 2048);

        metrics.s3_request_duration_seconds.observe(0.05);
        metrics.s3_request_duration_seconds.observe(0.15);
        assert_eq!(metrics.s3_request_duration_seconds.get_sample_count(), 2);
    }

    #[test]
    fn test_auth_metrics() {
        let metrics = MetricsRegistry::new();
        metrics.auth_attempts_total.with_label_values(&["oidc", "success"]).inc();
        metrics.auth_attempts_total.with_label_values(&["oidc", "failed"]).inc();
        metrics.auth_attempts_total.with_label_values(&["bearer", "success"]).inc();
        assert_eq!(
            metrics.auth_attempts_total.with_label_values(&["oidc", "success"]).get(),
            1
        );
        assert_eq!(
            metrics.auth_attempts_total.with_label_values(&["oidc", "failed"]).get(),
            1
        );
        assert_eq!(
            metrics.auth_attempts_total.with_label_values(&["bearer", "success"]).get(),
            1
        );

        metrics.auth_duration_seconds.observe(0.25);
        metrics.auth_duration_seconds.observe(1.5);
        assert_eq!(metrics.auth_duration_seconds.get_sample_count(), 2);

        metrics.token_refresh_total.with_label_values(&["success"]).inc();
        metrics.token_refresh_total.with_label_values(&["failed"]).inc();
        assert_eq!(metrics.token_refresh_total.with_label_values(&["success"]).get(), 1);
        assert_eq!(metrics.token_refresh_total.with_label_values(&["failed"]).get(), 1);
    }

    #[test]
    fn test_sorts_stripped_metric() {
        let metrics = MetricsRegistry::new();
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc();
        metrics.sorts_stripped_total.with_label_values(&["partition_only", "partition_only"]).inc();
        metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).inc();
        assert_eq!(
            metrics.sorts_stripped_total.with_label_values(&["adaptive", "memory_pressure"]).get(),
            2
        );
        assert_eq!(
            metrics.sorts_stripped_total.with_label_values(&["partition_only", "partition_only"]).get(),
            1
        );
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
