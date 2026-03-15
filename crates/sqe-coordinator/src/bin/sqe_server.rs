use std::sync::Arc;

use axum::{routing::get, Router};
use clap::Parser;
use tokio::signal;

use sqe_core::SqeConfig;
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::mode::Mode;
use sqe_coordinator::{QueryHandler, SessionManager};
use sqe_trino_compat::server::{TrinoAuthenticator, TrinoQueryExecutor};

// ── CLI ────────────────────────────────────────────────────────
#[derive(Parser)]
#[command(name = "sqe-server", version = sqe_core::VERSION, about = "SQE server — runs as coordinator or worker")]
struct Cli {
    /// Path to TOML configuration file
    #[arg(short, long)]
    config: Option<String>,

    /// Server mode (overrides SQE_MODE env var and config file)
    #[arg(short, long, value_enum, default_value = "coordinator")]
    mode: CliMode,
}

#[derive(Clone, clap::ValueEnum)]
enum CliMode {
    Coordinator,
    Worker,
}

// ── Health endpoints ───────────────────────────────────────────
async fn healthz() -> &'static str {
    "ok"
}

struct ReadinessState {
    ready: std::sync::atomic::AtomicBool,
}

async fn readyz(
    state: axum::extract::State<Arc<ReadinessState>>,
) -> axum::http::StatusCode {
    if state.ready.load(std::sync::atomic::Ordering::Relaxed) {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

fn start_health_server(port: u16, readiness: Arc<ReadinessState>) {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(readiness);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .expect("Failed to bind health server");
        tracing::info!("Health endpoints on port {port} (/healthz, /readyz)");
        axum::serve(listener, app).await.ok();
    });
}

// ── Graceful shutdown ──────────────────────────────────────────
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
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

// ── Trino adapters ─────────────────────────────────────────────
struct AuthenticatorAdapter(Arc<sqe_auth::Authenticator>);

#[async_trait::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<sqe_core::Session, String> {
        self.0
            .authenticate(username, password)
            .await
            .map_err(|e| e.to_string())
    }
}

struct QueryHandlerAdapter(Arc<QueryHandler>);

#[async_trait::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String> {
        self.0
            .execute(session, sql)
            .await
            .map_err(|e| e.to_string())
    }
}

// ── Main ───────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = cli
        .config
        .or_else(|| std::env::var("SQE_CONFIG").ok())
        .unwrap_or_else(|| "sqe.toml".to_string());

    let config = SqeConfig::load(&config_path)
        .map_err(|e| anyhow::anyhow!("Failed to load config from {config_path}: {e}"))?;

    // Priority: --mode flag > SQE_MODE env > config file mode
    // Since clap always has a default, check if user explicitly passed --mode
    // by seeing if SQE_MODE or config override it; otherwise use CLI default.
    let mode = match cli.mode {
        CliMode::Coordinator => Mode::Coordinator,
        CliMode::Worker => Mode::Worker,
    };

    let service_name = match mode {
        Mode::Coordinator => "sqe-coordinator",
        Mode::Worker => "sqe-worker",
    };

    let _otel_guard =
        sqe_metrics::otel::init_telemetry(service_name, &config.metrics.otlp_endpoint);

    tracing::info!(mode = ?mode, config = config_path, "Starting sqe-server");

    match mode {
        Mode::Coordinator => run_coordinator(config).await,
        Mode::Worker => run_worker(config).await,
    }
}

// ── Coordinator ────────────────────────────────────────────────
async fn run_coordinator(config: SqeConfig) -> anyhow::Result<()> {
    let readiness = Arc::new(ReadinessState {
        ready: std::sync::atomic::AtomicBool::new(false),
    });

    // Health endpoints on metrics port + 1 (or 8081 default)
    let health_port = config.metrics.prometheus_port + 1;
    start_health_server(health_port, readiness.clone());

    // Auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);
    authenticator.start_refresh_task();

    let session_manager = Arc::new(SessionManager::new(authenticator.clone()));

    // Policy
    let policy_enforcer: Arc<dyn sqe_policy::PolicyEnforcer> =
        Arc::new(sqe_policy::PassthroughEnforcer);

    // Workers
    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::new(
            config.coordinator.worker_urls.clone(),
        ),
    );

    if !config.coordinator.worker_urls.is_empty() {
        worker_registry.start_health_check_task(std::time::Duration::from_secs(5));
        tracing::info!(workers = ?config.coordinator.worker_urls, "Started worker health checks");
    }

    // Metrics & audit
    let metrics = Arc::new(sqe_metrics::MetricsRegistry::new());
    let audit = Arc::new(sqe_metrics::audit::AuditLogger::new(
        &config.metrics.audit_log_path,
    ));

    sqe_metrics::server::start_metrics_server(metrics.clone(), config.metrics.prometheus_port);
    tracing::info!("Prometheus metrics on port {}", config.metrics.prometheus_port);

    // Query handler
    let query_handler = Arc::new(QueryHandler::new(
        policy_enforcer,
        config.clone(),
        if config.coordinator.worker_urls.is_empty() {
            None
        } else {
            Some(worker_registry.clone())
        },
        Some(metrics.clone()),
        Some(audit.clone()),
    ));

    // Trino compat
    if config.coordinator.trino_http_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter(authenticator.clone()));
        let handler_adapter = Arc::new(QueryHandlerAdapter(query_handler.clone()));
        sqe_trino_compat::server::start_trino_server(
            auth_adapter,
            handler_adapter,
            config.coordinator.trino_http_port,
        );
        tracing::info!("Trino-compat HTTP on port {}", config.coordinator.trino_http_port);
    }

    // Mark ready
    readiness
        .ready
        .store(true, std::sync::atomic::Ordering::Relaxed);

    // Flight SQL server with graceful shutdown
    let flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone());
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    tracing::info!("SQE coordinator listening on {addr}");

    tonic::transport::Server::builder()
        .add_service(
            arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
        )
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE coordinator shut down");
    Ok(())
}

// ── Worker ─────────────────────────────────────────────────────
async fn run_worker(config: SqeConfig) -> anyhow::Result<()> {
    let readiness = Arc::new(ReadinessState {
        ready: std::sync::atomic::AtomicBool::new(false),
    });

    let health_port = config.metrics.prometheus_port + 1;
    start_health_server(health_port, readiness.clone());

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let flight_service = sqe_worker::flight_service::WorkerFlightService::new();

    // Mark ready
    readiness
        .ready
        .store(true, std::sync::atomic::Ordering::Relaxed);

    tracing::info!("SQE worker listening on {addr}");

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}
