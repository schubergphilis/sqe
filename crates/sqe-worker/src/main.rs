use std::sync::Arc;

use sqe_core::SqeConfig;
use sqe_metrics::WorkerMetricsRegistry;
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
    // `worker.allow_unauthenticated = true`, and refuses plaintext on a
    // non-loopback distributed setup unless `security.allow_insecure_transport
    // = true`.
    config.validate()?;

    // Build the fully-wired Flight service (worker secret + footer cache +
    // credential store + compression) and start the heartbeat task. Shared
    // with `sqe-server --mode worker` so both worker paths stay identical
    // (#219). The advertise URL is derived here and must be routable: an
    // undeliverable URL aborts boot instead of poisoning the registry (#220).
    let flight_service =
        sqe_worker::bootstrap::build_worker_service(&config, worker_metrics, session_ctx)?;

    // Optional TLS (QUACK-07): workers reuse the coordinator's TLS config.
    let tls_config = sqe_worker::tls::build_server_tls_config(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("Worker Flight listening on {addr} (TLS)");
    } else {
        tracing::info!("Worker Flight listening on {addr} (plaintext)");
    }

    server_builder
        .add_service(flight_service.into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

/// Resolve once a SIGINT or SIGTERM arrives. Drives `serve_with_shutdown` so
/// tonic stops accepting new connections and lets in-flight scans finish at
/// the graceful boundary instead of being hard-killed on signal (#225).
/// Mirrors the `shutdown_signal` helper in `sqe-coordinator/src/bin/sqe_server.rs`.
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, shutting down"),
        _ = terminate => tracing::info!("Received SIGTERM, shutting down"),
    }
}
