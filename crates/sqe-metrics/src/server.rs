use std::sync::Arc;

use axum::{Router, routing::get, extract::State, response::IntoResponse};
use prometheus::Encoder;
use tracing::info;

use crate::MetricsRegistry;

pub fn start_metrics_server(
    metrics: Arc<MetricsRegistry>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(metrics);

        let addr = format!("0.0.0.0:{port}");
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

        info!("Metrics server listening on {addr}");

        axum::serve(listener, app).await.unwrap();
    })
}

async fn metrics_handler(
    State(metrics): State<Arc<MetricsRegistry>>,
) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.registry.gather();
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

    #[tokio::test]
    async fn test_metrics_handler_returns_text() {
        let metrics = Arc::new(MetricsRegistry::new());
        metrics.query_count.with_label_values(&["success", "query"]).inc();
        // Observe a duration sample so the histogram family appears in gather()
        metrics.query_duration.with_label_values(&["query"]).observe(0.1);

        let encoder = prometheus::TextEncoder::new();
        let metric_families = metrics.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("sqe_query_count_total"));
        assert!(output.contains("sqe_query_duration_seconds"));
    }
}
