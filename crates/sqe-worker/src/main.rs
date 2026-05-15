use std::sync::Arc;
use std::time::Duration;

use sqe_core::SqeConfig;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_worker::flight_service::WorkerFlightService;
use sqe_worker::heartbeat;
use sqe_worker::runtime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sqe.toml".to_string());
    let config = SqeConfig::load(&config_path)?;

    let _otel_guard = sqe_metrics::otel::init_telemetry_with_sampling(
        "sqe-worker",
        &config.metrics.otlp_endpoint,
        config.metrics.trace_sample_rate,
    );

    // Worker-specific Prometheus metrics
    let worker_metrics = Arc::new(WorkerMetricsRegistry::new());
    sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    );

    // Build a configured DataFusion SessionContext with memory limits and spill-to-disk.
    // The context is created early to fail fast on invalid config (e.g. bad memory_limit).
    // It is passed into WorkerFlightService so every scan execution respects the pool.
    let session_ctx = runtime::build_session_context(&config.worker)?;

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    tracing::info!("Starting SQE worker on port {port}");

    // Validate the worker config before binding any sockets. The check
    // refuses to boot a coordinator-connected worker with an empty secret
    // unless the operator explicitly waives via
    // `worker.allow_unauthenticated = true`.
    config.validate()?;

    if config.worker.worker_secret.is_empty() && config.worker.allow_unauthenticated {
        tracing::warn!(
            "WARNING: worker.allow_unauthenticated = true -- any TCP-reachable \
             client may send scan tickets or refresh S3 credentials on this \
             worker. Set worker.worker_secret for production."
        );
    }

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
            config.worker.worker_secret.clone(),
        );
    }

    // Parse Flight IPC compression from coordinator config (shared in sqe.toml).
    // Workers use shuffle_compression for both DoGet (worker->coordinator) and
    // DoExchange (shuffle) since all worker traffic is internal.
    let shuffle_compression = sqe_core::FlightCompression::from_config(
        &config.coordinator.shuffle_compression,
    )
    .unwrap_or(sqe_core::FlightCompression::Zstd);

    let flight_service = WorkerFlightService::new(worker_metrics, session_ctx)
        .with_scan_timeout(config.worker.scan_timeout_secs)
        .with_flight_compression(shuffle_compression)
        .with_shuffle_compression(shuffle_compression)
        .with_worker_secret(config.worker.worker_secret.clone());

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
