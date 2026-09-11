use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use sqe_core::SqeConfig;
use sqe_metrics::WorkerMetricsRegistry;
use sqe_worker::runtime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn parse_config_path() -> String {
    // nosemgrep: opt.aikido.semgrep-rules.rust.lang.security.args -- CLI argument parse for --config; value is only used as a config file path
    let mut args = std::env::args().skip(1);
    let mut positional = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                return args.next().unwrap_or_else(|| "sqe.toml".to_string());
            }
            "--help" | "-h" => {
                eprintln!("sqe-worker --config <path>");
                std::process::exit(0);
            }
            other if other.starts_with('-') => {
                eprintln!("unknown argument {other}; use --config <path>");
                std::process::exit(2);
            }
            other => {
                eprintln!("warning: positional config path is deprecated; use --config");
                positional = Some(other.to_string());
            }
        }
    }
    positional
        .or_else(|| std::env::var("SQE_CONFIG").ok())
        .unwrap_or_else(|| "sqe.toml".to_string())
}

fn main() -> anyhow::Result<()> {
    // ring is the process TLS provider (installed pre-main by sqe-core; this
    // makes it explicit and logs which case happened).
    sqe_core::tls::ensure_crypto_provider();
    let config_path = parse_config_path();
    let config = SqeConfig::load(&config_path)?;
    config.validate()?;
    let runtime = sqe_core::build_server_runtime(&config.coordinator, "sqe-worker")
        .map_err(|e| anyhow::anyhow!("Failed to build tokio runtime: {e}"))?;
    runtime.block_on(async_main(config, config_path))
}

async fn async_main(config: SqeConfig, config_path: String) -> anyhow::Result<()> {
    let _otel_guard = sqe_metrics::otel::init_telemetry_with_sampling(
        "sqe-worker",
        &config.metrics.otlp_endpoint,
        &config.metrics.traces_otlp_endpoint,
        config.metrics.trace_sample_rate,
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    let worker_metrics = Arc::new(
        WorkerMetricsRegistry::new()
            .map_err(|e| anyhow::anyhow!("failed to initialize worker metrics registry: {e}"))?,
    );
    let _metrics_server = sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    )
    .map_err(|e| anyhow::anyhow!("failed to bind metrics server: {e}"))?;

    let ready = Arc::new(AtomicBool::new(false));
    let health_port = config.metrics.prometheus_port.saturating_add(1);
    tokio::spawn(serve_health(health_port, ready.clone()));
    tracing::info!("Health endpoints on port {health_port} (/healthz, /readyz)");

    let (session_ctx, memory_pool) = runtime::build_session_context(&config.worker)?;

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    tracing::info!("Starting SQE worker on port {port}");

    let flight_service = sqe_worker::bootstrap::build_worker_service_with_hot_reload(
        &config,
        worker_metrics,
        session_ctx,
        Some(memory_pool),
        Some(&config_path),
    )?;

    ready.store(true, Ordering::Relaxed);

    // Optional TLS, from the one shared builder in `sqe_core::tls` (the
    // hand-copied mirror in `sqe_worker::tls` is gone). `[worker.tls]` when
    // the worker carries its own certificate, else the inherited
    // `[coordinator.tls]`: a single certificate with the coordinator's SAN
    // cannot validate for a worker's own URL (finding 603).
    let worker_tls = config.worker.effective_tls(&config.coordinator.tls).clone();
    let tls_material = sqe_core::ServerTlsMaterial::load_optional(&worker_tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(material) = tls_material.as_ref() {
        server_builder = server_builder.tls_config(material.tonic_server())?;
        tracing::info!(
            mtls = material.requires_client_cert(),
            cert_fingerprint = material.cert_fingerprint(),
            // tonic cannot swap a live acceptor; rotation needs a restart.
            hot_reload = false,
            "Worker Flight listening on {addr} (TLS)"
        );
    } else {
        tracing::info!("Worker Flight listening on {addr} (plaintext)");
    }

    server_builder
        .add_service(
            flight_service.into_server(config.coordinator.transport.max_decoding_message_bytes),
        )
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

async fn serve_health(port: u16, ready: Arc<AtomicBool>) {
    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(port, error = %e, "failed to bind worker health port");
            return;
        }
    };
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let ready = ready.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 256];
            let _ = sock.read(&mut buf).await;
            let req = String::from_utf8_lossy(&buf);
            let is_readyz = req.contains(" /readyz");
            let ok = if is_readyz {
                ready.load(Ordering::Relaxed)
            } else {
                true
            };
            let (status, body) = if ok {
                ("200 OK", "ok")
            } else {
                ("503 Service Unavailable", "not ready")
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}

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
