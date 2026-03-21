pub mod server;
pub mod audit;
pub mod otel;
pub mod propagation;

use prometheus::{
    Counter, CounterVec, HistogramOpts, HistogramVec, IntGauge, Opts, Registry,
};

/// Central metrics registry for the SQE coordinator.
#[derive(Clone)]
pub struct MetricsRegistry {
    pub registry: Registry,
    pub query_count: CounterVec,
    pub query_duration: HistogramVec,
    pub rows_returned: Counter,
    pub active_sessions: IntGauge,
    pub healthy_workers: IntGauge,
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

        Self {
            registry,
            query_count,
            query_duration,
            rows_returned,
            active_sessions,
            healthy_workers,
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_creation() {
        let metrics = MetricsRegistry::new();
        // Touch each metric so Prometheus includes it in gather()
        metrics.query_count.with_label_values(&["success", "query"]).inc_by(0.0);
        metrics.query_duration.with_label_values(&["query"]).observe(0.0);
        metrics.rows_returned.inc_by(0.0);
        metrics.active_sessions.set(0);
        metrics.healthy_workers.set(0);
        assert!(metrics.registry.gather().len() >= 5);
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
}
