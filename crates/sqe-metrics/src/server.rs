//! Prometheus metrics HTTP server exposing the `/metrics` scrape endpoint.

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use prometheus::Encoder;
use tracing::{error, info};

use crate::HasRegistry;

/// Start an HTTP metrics server that serves Prometheus metrics at `/metrics`.
///
/// Works with any registry type that implements [`HasRegistry`] — both the
/// coordinator's [`MetricsRegistry`](crate::MetricsRegistry) and the worker's
/// [`WorkerMetricsRegistry`](crate::WorkerMetricsRegistry).
///
/// `bind` is the interface to listen on; the caller derives it (finding 764:
/// an `--mcp-anonymous` gateway keeps its ops listeners on loopback too).
pub fn start_metrics_server<R: HasRegistry + Clone>(
    metrics: Arc<R>,
    bind: std::net::IpAddr,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    let addr = std::net::SocketAddr::new(bind, port);
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    info!("Metrics server listening on {addr}");
    Ok(tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::from_std(std_listener) {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "Failed to convert metrics listener");
                return;
            }
        };
        let app = Router::new()
            .route("/metrics", get(metrics_handler::<R>))
            .with_state(metrics);
        if let Err(e) = axum::serve(listener, app).await {
            error!(error = %e, "Metrics server exited with error");
        }
    }))
}

async fn metrics_handler<R: HasRegistry>(State(metrics): State<Arc<R>>) -> impl IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.prometheus_registry().gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!(error = %e, "Failed to encode Prometheus metrics");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; charset=utf-8",
            )],
            format!("Failed to encode metrics: {e}").into_bytes(),
        );
    }

    (
        axum::http::StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        buffer,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetricsRegistry;

    #[tokio::test]
    async fn test_metrics_handler_returns_text() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        metrics
            .query_count
            .with_label_values(&["success", "query", ""])
            .inc();
        // Observe a duration sample so the histogram family appears in gather()
        metrics
            .query_duration
            .with_label_values(&["query"])
            .observe(0.1);

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

        let metrics = Arc::new(WorkerMetricsRegistry::new().unwrap());
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

    #[test]
    fn second_bind_on_same_port_fails() {
        let metrics = Arc::new(MetricsRegistry::new().unwrap());
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let err = start_metrics_server(metrics, std::net::Ipv4Addr::UNSPECIFIED.into(), port)
            .expect_err("port is taken");
        assert!(
            matches!(
                err.kind(),
                std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
            ),
            "unexpected error: {err}"
        );
    }
}
