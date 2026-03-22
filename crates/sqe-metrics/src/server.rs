use std::sync::Arc;

use axum::{Router, routing::get, extract::State, response::IntoResponse};
use prometheus::Encoder;
use tracing::{info, error};

use crate::HasRegistry;

/// Start an HTTP metrics server that serves Prometheus metrics at `/metrics`.
///
/// Works with any registry type that implements [`HasRegistry`] — both the
/// coordinator's [`MetricsRegistry`](crate::MetricsRegistry) and the worker's
/// [`WorkerMetricsRegistry`](crate::WorkerMetricsRegistry).
pub fn start_metrics_server<R: HasRegistry + Clone>(
    metrics: Arc<R>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler::<R>))
            .with_state(metrics);

        let addr = format!("0.0.0.0:{port}");
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(addr = %addr, error = %e, "Failed to bind metrics server");
                return;
            }
        };

        info!("Metrics server listening on {addr}");

        if let Err(e) = axum::serve(listener, app).await {
            error!(error = %e, "Metrics server exited with error");
        }
    })
}

async fn metrics_handler<R: HasRegistry>(
    State(metrics): State<Arc<R>>,
) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.prometheus_registry().gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        buffer,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricsRegistry;

    #[tokio::test]
    async fn test_metrics_handler_returns_text() {
        let metrics = Arc::new(MetricsRegistry::new());
        metrics.query_count.with_label_values(&["success", "query"]).inc();
        // Observe a duration sample so the histogram family appears in gather()
        metrics.query_duration.with_label_values(&["query"]).observe(0.1);

        let encoder = prometheus::TextEncoder::new();
        let metric_families = metrics.prometheus_registry().gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("sqe_query_count_total"));
        assert!(output.contains("sqe_query_duration_seconds"));
    }

    #[tokio::test]
    async fn test_worker_metrics_handler_returns_text() {
        use crate::WorkerMetricsRegistry;

        let metrics = Arc::new(WorkerMetricsRegistry::new());
        metrics.fragments_executed.inc();
        metrics.rows_scanned.inc_by(42.0);
        metrics.bytes_read.inc_by(1024.0);
        metrics.fragment_duration.observe(0.123);

        let encoder = prometheus::TextEncoder::new();
        let metric_families = metrics.prometheus_registry().gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("sqe_worker_fragments_executed_total"));
        assert!(output.contains("sqe_worker_rows_scanned_total"));
        assert!(output.contains("sqe_worker_bytes_read_total"));
        assert!(output.contains("sqe_worker_fragment_duration_seconds"));
    }
}
