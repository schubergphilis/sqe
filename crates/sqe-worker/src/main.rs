use std::sync::Arc;
use std::time::Duration;

use sqe_core::SqeConfig;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_worker::flight_service::WorkerFlightService;
use sqe_worker::heartbeat;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;

    let _otel_guard = sqe_metrics::otel::init_telemetry(
        "sqe-worker",
        &config.metrics.otlp_endpoint,
    );

    // Worker-specific Prometheus metrics
    let worker_metrics = Arc::new(WorkerMetricsRegistry::new());
    sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    );

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    tracing::info!("Starting SQE worker on port {port}");

    // Start heartbeat to coordinator if a coordinator URL is configured.
    if !config.worker.coordinator_url.is_empty() {
        let worker_url = format!("http://0.0.0.0:{port}");
        let interval = Duration::from_secs(config.worker.heartbeat_interval_secs);
        tracing::info!(
            coordinator = %config.worker.coordinator_url,
            interval_secs = config.worker.heartbeat_interval_secs,
            "Starting heartbeat to coordinator"
        );
        heartbeat::start_heartbeat_task(
            config.worker.coordinator_url.clone(),
            worker_url,
            interval,
        );
    }

    let flight_service = WorkerFlightService::new(worker_metrics);

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
