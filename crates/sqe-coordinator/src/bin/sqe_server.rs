use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::response::Json;
use axum::routing::get;
use axum::Router;
use clap::Parser;
use serde::Serialize;
use tokio::signal;

use sqe_core::SqeConfig;
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::mode::Mode;
use sqe_coordinator::{QueryHandler, SessionManager};
use sqe_trino_compat::server::{NodeContext, TrinoAuthenticator, TrinoQueryExecutor};

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

struct HealthState {
    ready: Arc<AtomicBool>,
    started_at: Instant,
    role: &'static str,
    worker_registry: Option<Arc<sqe_coordinator::worker_registry::WorkerRegistry>>,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(
    state: axum::extract::State<Arc<HealthState>>,
) -> axum::http::StatusCode {
    if state.ready.load(Ordering::Relaxed) {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

// ── Ballista/DataFusion-style /api/v1/status ─────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterStatus {
    status: &'static str,
    node: NodeStatus,
    workers: Option<WorkersStatus>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeStatus {
    role: &'static str,
    version: &'static str,
    datafusion_version: &'static str,
    uptime_seconds: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkersStatus {
    total: usize,
    healthy: usize,
    healthy_urls: Vec<String>,
}

async fn cluster_status(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<ClusterStatus> {
    let ready = state.ready.load(Ordering::Relaxed);

    let workers = if let Some(ref registry) = state.worker_registry {
        Some(WorkersStatus {
            total: registry.total_workers().await,
            healthy: registry.healthy_workers().await.len(),
            healthy_urls: registry.healthy_workers().await,
        })
    } else {
        None
    };

    Json(ClusterStatus {
        status: if ready { "ACTIVE" } else { "STARTING" },
        node: NodeStatus {
            role: state.role,
            version: sqe_core::VERSION,
            datafusion_version: "51",
            uptime_seconds: state.started_at.elapsed().as_secs(),
        },
        workers,
    })
}

fn start_health_server(port: u16, state: Arc<HealthState>) {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/status", get(cluster_status))
        .with_state(state);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .expect("Failed to bind health server");
        tracing::info!("Health endpoints on port {port} (/healthz, /readyz, /api/v1/status)");
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
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    // Health endpoints on metrics port + 1 (or 9091 default)
    let health_port = config.metrics.prometheus_port + 1;

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

    // Health server (start early so probes work during init)
    let health_state = Arc::new(HealthState {
        ready: ready.clone(),
        started_at,
        role: "coordinator",
        worker_registry: if config.coordinator.worker_urls.is_empty() {
            None
        } else {
            Some(worker_registry.clone())
        },
    });
    start_health_server(health_port, health_state);

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
            NodeContext {
                version: sqe_core::VERSION.to_string(),
                ready: ready.clone(),
                started_at,
            },
        );
        tracing::info!("Trino-compat HTTP on port {}", config.coordinator.trino_http_port);
    }

    // Mark ready
    ready.store(true, Ordering::Relaxed);

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
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    let health_port = config.metrics.prometheus_port + 1;
    let health_state = Arc::new(HealthState {
        ready: ready.clone(),
        started_at,
        role: "worker",
        worker_registry: None,
    });
    start_health_server(health_port, health_state);

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let worker_metrics = Arc::new(sqe_metrics::WorkerMetricsRegistry::new());
    sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    );

    let flight_service = sqe_worker::flight_service::WorkerFlightService::new(worker_metrics);

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    tracing::info!("SQE worker listening on {addr}");

    tonic::transport::Server::builder()
        .add_service(flight_service.into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_status_serialization_coordinator() {
        let status = ClusterStatus {
            status: "ACTIVE",
            node: NodeStatus {
                role: "coordinator",
                version: "0.1.0",
                datafusion_version: "51",
                uptime_seconds: 120,
            },
            workers: Some(WorkersStatus {
                total: 3,
                healthy: 2,
                healthy_urls: vec![
                    "http://worker1:50052".to_string(),
                    "http://worker2:50052".to_string(),
                ],
            }),
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "ACTIVE");
        assert_eq!(json["node"]["role"], "coordinator");
        assert_eq!(json["node"]["version"], "0.1.0");
        assert_eq!(json["node"]["datafusionVersion"], "51");
        assert_eq!(json["node"]["uptimeSeconds"], 120);
        assert_eq!(json["workers"]["total"], 3);
        assert_eq!(json["workers"]["healthy"], 2);
        assert_eq!(json["workers"]["healthyUrls"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_cluster_status_serialization_worker() {
        let status = ClusterStatus {
            status: "ACTIVE",
            node: NodeStatus {
                role: "worker",
                version: "0.1.0",
                datafusion_version: "51",
                uptime_seconds: 60,
            },
            workers: None,
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["node"]["role"], "worker");
        assert!(json["workers"].is_null());
    }

    #[test]
    fn test_cluster_status_starting() {
        let status = ClusterStatus {
            status: "STARTING",
            node: NodeStatus {
                role: "coordinator",
                version: "0.1.0",
                datafusion_version: "51",
                uptime_seconds: 0,
            },
            workers: None,
        };

        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["status"], "STARTING");
    }

    #[tokio::test]
    async fn test_cluster_status_handler_no_workers() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "ACTIVE");
        assert_eq!(status.node.role, "coordinator");
        assert!(status.workers.is_none());
    }

    #[tokio::test]
    async fn test_cluster_status_handler_starting() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "worker",
            worker_registry: None,
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "STARTING");
        assert_eq!(status.node.role, "worker");
    }

    #[tokio::test]
    async fn test_cluster_status_handler_with_workers() {
        let registry = Arc::new(sqe_coordinator::worker_registry::WorkerRegistry::new(vec![
            "http://w1:50052".to_string(),
            "http://w2:50052".to_string(),
        ]));
        // Mark one worker healthy
        registry.mark_healthy("http://w1:50052").await;

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: Some(registry),
        });

        let Json(status) = cluster_status(axum::extract::State(state)).await;
        assert_eq!(status.status, "ACTIVE");
        let workers = status.workers.unwrap();
        assert_eq!(workers.total, 2);
        assert_eq!(workers.healthy, 1);
        assert_eq!(workers.healthy_urls, vec!["http://w1:50052"]);
    }

    #[tokio::test]
    async fn test_readyz_ready() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
        });

        let code = readyz(axum::extract::State(state)).await;
        assert_eq!(code, axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readyz_not_ready() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
        });

        let code = readyz(axum::extract::State(state)).await;
        assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }
}
