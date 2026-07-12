//! SQE metrics, OpenTelemetry, and the OCSF audit-logging subsystem.
//!
//! Exposes the Prometheus registry + `/metrics` server ([`server`]), OTel
//! init ([`otel`]), and the structured audit pipeline ([`audit`]).

pub mod audit;
pub mod otel;
pub mod propagation;
pub mod server;

use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, Opts, Registry,
};

fn register_counter(
    registry: &Registry,
    name: &str,
    help: &str,
) -> Result<Counter, prometheus::Error> {
    let m = Counter::new(name, help)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<CounterVec, prometheus::Error> {
    let m = CounterVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_gauge(registry: &Registry, name: &str, help: &str) -> Result<Gauge, prometheus::Error> {
    let m = Gauge::new(name, help)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_gauge_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<GaugeVec, prometheus::Error> {
    let m = GaugeVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_histogram(
    registry: &Registry,
    opts: HistogramOpts,
) -> Result<Histogram, prometheus::Error> {
    let m = Histogram::with_opts(opts)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_histogram_vec(
    registry: &Registry,
    opts: HistogramOpts,
    labels: &[&str],
) -> Result<HistogramVec, prometheus::Error> {
    let m = HistogramVec::new(opts, labels)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_int_counter(
    registry: &Registry,
    name: &str,
    help: &str,
) -> Result<IntCounter, prometheus::Error> {
    let m = IntCounter::new(name, help)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_int_counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntCounterVec, prometheus::Error> {
    let m = IntCounterVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

fn register_int_gauge(
    registry: &Registry,
    name: &str,
    help: &str,
) -> Result<IntGauge, prometheus::Error> {
    let m = IntGauge::new(name, help)?;
    registry.register(Box::new(m.clone()))?;
    Ok(m)
}

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
    /// Process resident set size. Divergence between this and the pool
    /// gauge across a query sweep is the untracked-memory/retention signal
    /// (phase 0 of scan-throughput-memory-safety).
    pub coordinator_rss_bytes: Gauge,

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

    // Write path: orphan parquet files from cancelled/failed writes (COORD-06).
    // Labelled by op (ctas/insert/...) and cleanup outcome (deleted/leaked) so
    // operators can alert on `outcome="leaked"` accumulation in S3.
    pub write_orphan_files_total: IntCounterVec,

    // Catalog (Polaris) roundtrip + circuit breaker state
    pub catalog_request_duration_seconds: HistogramVec,
    pub catalog_circuit_breaker_state: GaugeVec,

    // Policy backend (OPA / Cedar) roundtrip + cache
    pub policy_resolve_duration_seconds: HistogramVec,
    pub policy_cache_hits_total: IntCounterVec,
    pub policy_cache_misses_total: IntCounterVec,
    pub policy_circuit_breaker_state: GaugeVec,

    // Audit export (OTLP shipper) metrics
    pub audit_export_records_total: IntCounterVec,
    pub audit_export_batch_failures_total: IntCounter,
    pub audit_export_spool_lag_bytes: Gauge,
    pub audit_export_cursor_seq: Gauge,
    pub audit_export_last_success_timestamp: Gauge,

    // Dashboard auth metrics
    /// Anonymous dashboard denial counter -- incremented instead of writing an
    /// audit line when no bearer token is present (Unauthorized). Prevents
    /// health-port probe flood from polluting the audit spool and SIEM.
    pub dashboard_auth_anonymous_denied_total: IntCounter,
    /// Auth-success counter for dashboard requests. Incremented on EVERY
    /// successful admin bearer check, including within-window deduplicated
    /// requests that do not write an audit line. This keeps access observable
    /// in Prometheus even when the audit-coalesce window suppresses the line.
    pub dashboard_auth_success_total: IntCounter,
}

impl MetricsRegistry {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let query_count = register_counter_vec(
            &registry,
            "sqe_query_count_total",
            "Total queries executed",
            &["status", "statement_type", "error_code"],
        )?;

        let query_duration = register_histogram_vec(
            &registry,
            HistogramOpts::new("sqe_query_duration_seconds", "Query execution duration").buckets(
                vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
            ),
            &["statement_type"],
        )?;

        let rows_returned = register_counter(
            &registry,
            "sqe_rows_returned_total",
            "Total rows returned across all queries",
        )?;

        let active_sessions = register_int_gauge(
            &registry,
            "sqe_active_sessions",
            "Number of active sessions",
        )?;

        let healthy_workers = register_int_gauge(
            &registry,
            "sqe_healthy_workers",
            "Number of healthy workers",
        )?;

        let cache_hits = register_counter(&registry, "sqe_cache_hits_total", "Total cache hits")?;
        let cache_misses =
            register_counter(&registry, "sqe_cache_misses_total", "Total cache misses")?;
        let cache_invalidations = register_counter(
            &registry,
            "sqe_cache_invalidations_total",
            "Total cache invalidations",
        )?;
        let cache_size_bytes = register_gauge(
            &registry,
            "sqe_cache_size_bytes",
            "Current cache memory usage in bytes",
        )?;
        let cache_entries = register_gauge(
            &registry,
            "sqe_cache_entries",
            "Current number of cached entries",
        )?;

        let scheduler_decisions = register_int_counter_vec(
            &registry,
            "sqe_scheduler_decisions_total",
            "Scheduling decisions by type",
            &["decision"],
        )?;

        let scheduler_task_count = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_scheduler_task_count",
                "Number of tasks per distributed query",
            )
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 100.0]),
        )?;

        let scheduler_task_size_mb = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_scheduler_task_size_mb",
                "Size of individual scan tasks in MB",
            )
            .buckets(vec![1.0, 10.0, 50.0, 100.0, 256.0, 512.0, 1024.0, 5120.0]),
        )?;

        let scheduler_stragglers = register_int_counter(
            &registry,
            "sqe_scheduler_stragglers_total",
            "Number of straggler fragments detected",
        )?;

        let footer_cache_hits = register_counter(
            &registry,
            "sqe_footer_cache_hits_total",
            "Total Parquet footer cache hits",
        )?;

        let footer_cache_misses = register_counter(
            &registry,
            "sqe_footer_cache_misses_total",
            "Total Parquet footer cache misses",
        )?;

        let footer_cache_size_bytes = register_gauge(
            &registry,
            "sqe_footer_cache_size_bytes",
            "Current estimated size of Parquet footer cache in bytes",
        )?;

        let coordinator_memory_used_bytes = register_gauge(
            &registry,
            "sqe_coordinator_memory_used_bytes",
            "Current coordinator DataFusion memory pool usage in bytes",
        )?;

        let coordinator_memory_limit_bytes = register_gauge(
            &registry,
            "sqe_coordinator_memory_limit_bytes",
            "Coordinator DataFusion memory pool limit in bytes",
        )?;

        let coordinator_rss_bytes = register_gauge(
            &registry,
            "sqe_coordinator_rss_bytes",
            "Coordinator process resident set size in bytes",
        )?;

        let coordinator_memory_pressure = register_gauge(
            &registry,
            "sqe_coordinator_memory_pressure",
            "Coordinator memory pressure level (0=green, 1=yellow, 2=orange, 3=red)",
        )?;

        // Spill metrics
        let sort_spill_count = register_counter(
            &registry,
            "sqe_sort_spill_count_total",
            "Number of sort spill events",
        )?;

        let sort_spill_bytes = register_counter(
            &registry,
            "sqe_sort_spill_bytes_total",
            "Bytes spilled for sorts",
        )?;

        let join_spill_count = register_counter(
            &registry,
            "sqe_join_spill_count_total",
            "Join spill events (SortMergeJoin)",
        )?;

        let join_spill_bytes = register_counter(
            &registry,
            "sqe_join_spill_bytes_total",
            "Bytes spilled for joins",
        )?;

        // Shuffle metrics (Phase B — registered now, incremented when shuffle lands)
        let shuffle_bytes_sent = register_counter(
            &registry,
            "sqe_shuffle_bytes_sent_total",
            "Bytes sent via DoExchange",
        )?;

        let shuffle_bytes_received = register_counter(
            &registry,
            "sqe_shuffle_bytes_received_total",
            "Bytes received via DoExchange",
        )?;

        let shuffle_partitions = register_counter(
            &registry,
            "sqe_shuffle_partitions_total",
            "Shuffle partitions created",
        )?;

        // Late materialization metrics
        let late_mat_bytes_predicate = register_counter(
            &registry,
            "sqe_late_mat_bytes_predicate_total",
            "Bytes read for predicate evaluation",
        )?;

        let late_mat_bytes_projection = register_counter(
            &registry,
            "sqe_late_mat_bytes_projection_total",
            "Bytes read for projection",
        )?;

        let late_mat_selectivity = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_late_mat_selectivity",
                "Late materialization selectivity (rows surviving / total rows)",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1.0]),
        )?;

        // Pruning metrics
        let files_pruned_minmax = register_counter(
            &registry,
            "sqe_files_pruned_minmax_total",
            "Files skipped by min/max pruning",
        )?;

        let files_pruned_bloom = register_counter(
            &registry,
            "sqe_files_pruned_bloom_total",
            "Files skipped by bloom filter",
        )?;

        let pages_pruned_index = register_counter(
            &registry,
            "sqe_pages_pruned_index_total",
            "Pages skipped by page index",
        )?;

        // Latency
        let time_to_first_row = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_time_to_first_row_seconds",
                "Time from query submit to first result row",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        )?;

        // S3 I/O metrics
        let s3_requests_total = register_int_counter_vec(
            &registry,
            "sqe_s3_requests_total",
            "Total S3 requests by operation and status",
            &["operation", "status"],
        )?;

        let s3_bytes_read_total = register_int_counter(
            &registry,
            "sqe_s3_bytes_read_total",
            "Total bytes fetched from S3",
        )?;

        let s3_bytes_written_total = register_int_counter(
            &registry,
            "sqe_s3_bytes_written_total",
            "Total bytes written to S3",
        )?;

        let s3_request_duration_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_s3_request_duration_seconds",
                "S3 request latency in seconds",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
        )?;

        // Auth metrics
        let auth_attempts_total = register_int_counter_vec(
            &registry,
            "sqe_auth_attempts_total",
            "Total authentication attempts by provider and status",
            &["provider", "status"],
        )?;

        let auth_duration_seconds = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_auth_duration_seconds",
                "Authentication handshake latency in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
        )?;

        let token_refresh_total = register_int_counter_vec(
            &registry,
            "sqe_token_refresh_total",
            "Total token refresh attempts by status",
            &["status"],
        )?;

        // Adaptive sort stripping metric
        let sorts_stripped_total = register_int_counter_vec(
            &registry,
            "sqe_sorts_stripped_total",
            "Total sort operations stripped by adaptive sort rule",
            &["mode", "reason"],
        )?;

        let catalog_request_duration_seconds = register_histogram_vec(
            &registry,
            HistogramOpts::new(
                "sqe_catalog_request_duration_seconds",
                "Catalog (Polaris REST) roundtrip latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["op", "status"],
        )?;

        let catalog_circuit_breaker_state = register_gauge_vec(
            &registry,
            "sqe_catalog_circuit_breaker_state",
            "Catalog circuit breaker state (0=closed, 1=half_open, 2=open)",
            &["circuit"],
        )?;

        let policy_resolve_duration_seconds = register_histogram_vec(
            &registry,
            HistogramOpts::new(
                "sqe_policy_resolve_duration_seconds",
                "Policy backend (OPA / Cedar) resolve latency in seconds",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
            ]),
            &["backend", "status"],
        )?;

        let policy_cache_hits_total = register_int_counter_vec(
            &registry,
            "sqe_policy_cache_hits_total",
            "Policy backend cache hits by backend",
            &["backend"],
        )?;

        let policy_cache_misses_total = register_int_counter_vec(
            &registry,
            "sqe_policy_cache_misses_total",
            "Policy backend cache misses by backend",
            &["backend"],
        )?;

        let policy_circuit_breaker_state = register_gauge_vec(
            &registry,
            "sqe_policy_circuit_breaker_state",
            "Policy backend circuit breaker state (0=closed, 1=half_open, 2=open)",
            &["backend"],
        )?;

        let write_orphan_files_total = register_int_counter_vec(
            &registry,
            "sqe_write_orphan_files_total",
            "Orphan parquet files from cancelled/failed writes, by op and cleanup outcome",
            &["op", "outcome"],
        )?;

        // Audit export (OTLP shipper) metrics
        let audit_export_records_total = register_int_counter_vec(
            &registry,
            "sqe_audit_export_records_total",
            "Total audit records shipped by status (success or failure)",
            &["status"],
        )?;

        let audit_export_batch_failures_total = register_int_counter(
            &registry,
            "sqe_audit_export_batch_failures_total",
            "Total failed audit export batch attempts",
        )?;

        let audit_export_spool_lag_bytes = register_gauge(
            &registry,
            "sqe_audit_export_spool_lag_bytes",
            "Bytes in the audit spool not yet shipped (file size minus committed offset)",
        )?;

        let audit_export_cursor_seq = register_gauge(
            &registry,
            "sqe_audit_export_cursor_seq",
            "Sequence number of the last successfully acked audit export record",
        )?;

        let audit_export_last_success_timestamp = register_gauge(
            &registry,
            "sqe_audit_export_last_success_timestamp",
            "Unix timestamp (seconds) of the last successful audit export batch",
        )?;

        let dashboard_auth_anonymous_denied_total = register_int_counter(
            &registry,
            "sqe_dashboard_auth_anonymous_denied_total",
            "Anonymous dashboard access denials (no bearer token / invalid scheme). \
             These are NOT written to the audit spool.",
        )?;

        let dashboard_auth_success_total = register_int_counter(
            &registry,
            "sqe_dashboard_auth_success_total",
            "Successful admin bearer authentications for dashboard requests. \
             Incremented on every success, including within-window deduplicated \
             requests that do not write an audit line.",
        )?;

        Ok(Self {
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
            coordinator_rss_bytes,
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
            catalog_request_duration_seconds,
            catalog_circuit_breaker_state,
            policy_resolve_duration_seconds,
            policy_cache_hits_total,
            policy_cache_misses_total,
            policy_circuit_breaker_state,
            write_orphan_files_total,
            audit_export_records_total,
            audit_export_batch_failures_total,
            audit_export_spool_lag_bytes,
            audit_export_cursor_seq,
            audit_export_last_success_timestamp,
            dashboard_auth_anonymous_denied_total,
            dashboard_auth_success_total,
        })
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new().expect("metrics registry must initialize at startup")
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
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();

        let fragments_executed = register_counter(
            &registry,
            "sqe_worker_fragments_executed_total",
            "Total number of scan fragments executed",
        )?;

        let rows_scanned = register_counter(
            &registry,
            "sqe_worker_rows_scanned_total",
            "Total rows scanned across all fragments",
        )?;

        let bytes_read = register_counter(
            &registry,
            "sqe_worker_bytes_read_total",
            "Total bytes read from storage",
        )?;

        let fragment_duration = register_histogram(
            &registry,
            HistogramOpts::new(
                "sqe_worker_fragment_duration_seconds",
                "Per-fragment execution time",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
        )?;

        Ok(Self {
            registry,
            fragments_executed,
            rows_scanned,
            bytes_read,
            fragment_duration,
        })
    }
}

impl Default for WorkerMetricsRegistry {
    fn default() -> Self {
        Self::new().expect("worker metrics registry must initialize at startup")
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
        let metrics = MetricsRegistry::new().unwrap();
        // Touch each metric so Prometheus includes it in gather()
        metrics
            .query_count
            .with_label_values(&["success", "query", ""])
            .inc_by(0.0);
        metrics
            .query_duration
            .with_label_values(&["query"])
            .observe(0.0);
        metrics.rows_returned.inc_by(0.0);
        metrics.active_sessions.set(0);
        metrics.healthy_workers.set(0);
        metrics.cache_hits.inc_by(0.0);
        metrics.cache_misses.inc_by(0.0);
        metrics.cache_invalidations.inc_by(0.0);
        metrics.cache_size_bytes.set(0.0);
        metrics.cache_entries.set(0.0);
        metrics
            .scheduler_decisions
            .with_label_values(&["local"])
            .inc_by(0);
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
        metrics
            .s3_requests_total
            .with_label_values(&["get", "success"])
            .inc_by(0);
        metrics.s3_bytes_read_total.inc_by(0);
        metrics.s3_bytes_written_total.inc_by(0);
        metrics.s3_request_duration_seconds.observe(0.01);
        // Auth metrics
        metrics
            .auth_attempts_total
            .with_label_values(&["oidc", "success"])
            .inc_by(0);
        metrics.auth_duration_seconds.observe(0.1);
        metrics
            .token_refresh_total
            .with_label_values(&["success"])
            .inc_by(0);
        // Adaptive sort metric
        metrics
            .sorts_stripped_total
            .with_label_values(&["adaptive", "memory_pressure"])
            .inc_by(0);
        // Write-path orphan cleanup (COORD-06)
        metrics
            .write_orphan_files_total
            .with_label_values(&["ctas", "leaked"])
            .inc_by(0);
        metrics.dashboard_auth_anonymous_denied_total.inc_by(0);
        // 17 original + 14 streaming + 7 new (S3 + auth) + 1 adaptive sort + 1 dashboard = 40 minimum
        assert!(metrics.registry.gather().len() >= 40);
    }

    #[test]
    fn test_write_orphan_files_total() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .write_orphan_files_total
            .with_label_values(&["insert", "leaked"])
            .inc_by(3);
        metrics
            .write_orphan_files_total
            .with_label_values(&["insert", "deleted"])
            .inc_by(5);
        assert_eq!(
            metrics
                .write_orphan_files_total
                .with_label_values(&["insert", "leaked"])
                .get(),
            3
        );
        assert_eq!(
            metrics
                .write_orphan_files_total
                .with_label_values(&["insert", "deleted"])
                .get(),
            5
        );
    }

    #[test]
    fn test_query_count_increment() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .query_count
            .with_label_values(&["success", "query", ""])
            .inc();
        let count = metrics
            .query_count
            .with_label_values(&["success", "query", ""])
            .get();
        assert_eq!(count, 1.0);
    }

    #[test]
    fn test_query_duration_observe() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .query_duration
            .with_label_values(&["query"])
            .observe(0.5);
        let count = metrics
            .query_duration
            .with_label_values(&["query"])
            .get_sample_count();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_active_sessions_gauge() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics.active_sessions.inc();
        metrics.active_sessions.inc();
        assert_eq!(metrics.active_sessions.get(), 2);
        metrics.active_sessions.dec();
        assert_eq!(metrics.active_sessions.get(), 1);
    }

    #[test]
    fn test_spill_metrics() {
        let metrics = MetricsRegistry::new().unwrap();
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
        let metrics = MetricsRegistry::new().unwrap();
        metrics.shuffle_bytes_sent.inc_by(4096.0);
        metrics.shuffle_bytes_received.inc_by(8192.0);
        metrics.shuffle_partitions.inc_by(10.0);
        assert_eq!(metrics.shuffle_bytes_sent.get(), 4096.0);
        assert_eq!(metrics.shuffle_bytes_received.get(), 8192.0);
        assert_eq!(metrics.shuffle_partitions.get(), 10.0);
    }

    #[test]
    fn test_late_materialization_metrics() {
        let metrics = MetricsRegistry::new().unwrap();
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
        let metrics = MetricsRegistry::new().unwrap();
        metrics.files_pruned_minmax.inc_by(10.0);
        metrics.files_pruned_bloom.inc_by(5.0);
        metrics.pages_pruned_index.inc_by(20.0);
        assert_eq!(metrics.files_pruned_minmax.get(), 10.0);
        assert_eq!(metrics.files_pruned_bloom.get(), 5.0);
        assert_eq!(metrics.pages_pruned_index.get(), 20.0);
    }

    #[test]
    fn test_time_to_first_row_histogram() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics.time_to_first_row.observe(0.05);
        metrics.time_to_first_row.observe(0.5);
        metrics.time_to_first_row.observe(5.0);
        assert_eq!(metrics.time_to_first_row.get_sample_count(), 3);
        let sum = metrics.time_to_first_row.get_sample_sum();
        assert!((sum - 5.55).abs() < 1e-9);
    }

    #[test]
    fn test_s3_metrics() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .s3_requests_total
            .with_label_values(&["get", "success"])
            .inc();
        metrics
            .s3_requests_total
            .with_label_values(&["get", "success"])
            .inc();
        metrics
            .s3_requests_total
            .with_label_values(&["put", "success"])
            .inc();
        metrics
            .s3_requests_total
            .with_label_values(&["get", "error"])
            .inc();
        assert_eq!(
            metrics
                .s3_requests_total
                .with_label_values(&["get", "success"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .s3_requests_total
                .with_label_values(&["put", "success"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .s3_requests_total
                .with_label_values(&["get", "error"])
                .get(),
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
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .auth_attempts_total
            .with_label_values(&["oidc", "success"])
            .inc();
        metrics
            .auth_attempts_total
            .with_label_values(&["oidc", "failed"])
            .inc();
        metrics
            .auth_attempts_total
            .with_label_values(&["bearer", "success"])
            .inc();
        assert_eq!(
            metrics
                .auth_attempts_total
                .with_label_values(&["oidc", "success"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .auth_attempts_total
                .with_label_values(&["oidc", "failed"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .auth_attempts_total
                .with_label_values(&["bearer", "success"])
                .get(),
            1
        );

        metrics.auth_duration_seconds.observe(0.25);
        metrics.auth_duration_seconds.observe(1.5);
        assert_eq!(metrics.auth_duration_seconds.get_sample_count(), 2);

        metrics
            .token_refresh_total
            .with_label_values(&["success"])
            .inc();
        metrics
            .token_refresh_total
            .with_label_values(&["failed"])
            .inc();
        assert_eq!(
            metrics
                .token_refresh_total
                .with_label_values(&["success"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .token_refresh_total
                .with_label_values(&["failed"])
                .get(),
            1
        );
    }

    #[test]
    fn test_sorts_stripped_metric() {
        let metrics = MetricsRegistry::new().unwrap();
        metrics
            .sorts_stripped_total
            .with_label_values(&["adaptive", "memory_pressure"])
            .inc();
        metrics
            .sorts_stripped_total
            .with_label_values(&["partition_only", "partition_only"])
            .inc();
        metrics
            .sorts_stripped_total
            .with_label_values(&["adaptive", "memory_pressure"])
            .inc();
        assert_eq!(
            metrics
                .sorts_stripped_total
                .with_label_values(&["adaptive", "memory_pressure"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .sorts_stripped_total
                .with_label_values(&["partition_only", "partition_only"])
                .get(),
            1
        );
    }

    // ── WorkerMetricsRegistry tests ─────────────────────────────

    #[test]
    fn test_worker_metrics_registry_creation() {
        let m = WorkerMetricsRegistry::new().unwrap();
        // Touch each metric so Prometheus includes it in gather()
        m.fragments_executed.inc_by(0.0);
        m.rows_scanned.inc_by(0.0);
        m.bytes_read.inc_by(0.0);
        m.fragment_duration.observe(0.0);
        assert!(m.registry.gather().len() >= 4);
    }

    #[test]
    fn test_worker_fragments_executed_counter() {
        let m = WorkerMetricsRegistry::new().unwrap();
        m.fragments_executed.inc();
        m.fragments_executed.inc();
        assert_eq!(m.fragments_executed.get(), 2.0);
    }

    #[test]
    fn test_worker_rows_scanned_counter() {
        let m = WorkerMetricsRegistry::new().unwrap();
        m.rows_scanned.inc_by(500.0);
        m.rows_scanned.inc_by(300.0);
        assert_eq!(m.rows_scanned.get(), 800.0);
    }

    #[test]
    fn test_worker_bytes_read_counter() {
        let m = WorkerMetricsRegistry::new().unwrap();
        m.bytes_read.inc_by(1024.0);
        m.bytes_read.inc_by(2048.0);
        assert_eq!(m.bytes_read.get(), 3072.0);
    }

    #[test]
    fn test_worker_fragment_duration_histogram() {
        let m = WorkerMetricsRegistry::new().unwrap();
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
