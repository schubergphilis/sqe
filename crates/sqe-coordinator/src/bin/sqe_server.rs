use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use axum::Router;
use clap::Parser;
use serde::Serialize;
use tokio::signal;

use sqe_catalog::grant_chameleon::ChameleonGrantBackend;
use sqe_core::SqeConfig;
use sqe_coordinator::flight_sql::SqeFlightSqlService;
use sqe_coordinator::mode::Mode;
use sqe_coordinator::{QueryHandler, SessionManager};
use sqe_policy::grants::{polaris::PolarisGrantBackend, GrantBackend};
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
    polaris_url: String,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(
    state: axum::extract::State<Arc<HealthState>>,
) -> Response {
    if !state.ready.load(Ordering::Relaxed) {
        return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response();
    }

    let mut checks = serde_json::Map::new();
    let mut all_healthy = true;

    // Check Polaris catalog reachability
    if !state.polaris_url.is_empty() {
        let polaris_ok = reqwest::Client::new()
            .get(format!("{}/api/catalog/v1/config", state.polaris_url))
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
            .map(|r| r.status().is_success() || r.status().as_u16() == 401)
            .unwrap_or(false);

        checks.insert(
            "polaris".to_string(),
            serde_json::Value::String(if polaris_ok {
                "ok".to_string()
            } else {
                "unreachable".to_string()
            }),
        );
        if !polaris_ok {
            all_healthy = false;
        }
    }

    if all_healthy {
        (
            axum::http::StatusCode::OK,
            Json(serde_json::json!({"status": "healthy", "checks": checks})),
        )
            .into_response()
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status": "unhealthy", "checks": checks})),
        )
            .into_response()
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
        axum::serve(listener, app).await.unwrap_or_else(|e| tracing::error!(error = %e, "Health server terminated unexpectedly"));
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
struct AuthenticatorAdapter {
    authenticator: Arc<sqe_auth::Authenticator>,
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
}

#[async_trait::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<sqe_core::Session, String> {
        self.authenticator
            .authenticate(username, password)
            .await
            .map_err(|e| e.to_string())
    }

    async fn authenticate_bearer(&self, token: &str) -> Result<sqe_core::Session, String> {
        let provider = self
            .bearer_provider
            .as_ref()
            .ok_or_else(|| "Bearer token authentication is not configured".to_string())?;

        let credentials = sqe_auth::FlightCredentials {
            bearer_token: Some(token.to_string()),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&credentials)
            .await
            .map_err(|e| format!("Bearer token validation failed: {e}"))?;

        // Convert Identity to Session: use the JWT itself as the catalog token
        // (passthrough to Polaris), and the identity fields for user/roles.
        let token_expiry = chrono::Utc::now() + chrono::Duration::hours(1);
        Ok(sqe_core::Session::new(
            identity.user_id,
            identity.catalog_token.unwrap_or_else(|| token.to_string()),
            None,
            token_expiry,
            identity.roles,
        ))
    }
}

struct QueryHandlerAdapter {
    handler: Arc<QueryHandler>,
    rate_limiter: Arc<sqe_coordinator::rate_limiter::QueryRateLimiter>,
}

#[async_trait::async_trait]
impl TrinoQueryExecutor for QueryHandlerAdapter {
    async fn execute(
        &self,
        session: &sqe_core::Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|e| e.to_string())?;
        self.handler
            .execute(session, sql)
            .await
            .map_err(|e| e.to_string())
    }
}

// ── Main ───────────────────────────────────────────────────────
//
// We build the tokio runtime manually instead of using `#[tokio::main]` so we
// can set a larger thread stack. The default 2 MiB worker stack is enough for
// most query plans but overflows on deep AST trees — notably, DataFusion
// re-parses WHERE clauses produced by our CoW DML rewrites, and the SQL
// grammar parses `a OR b OR c OR ...` as a left-leaning chain of depth N.
// For N in the thousands (e.g. TPC-E `trade_result_update_holding`, which
// materialises `(ca, sym) IN (SELECT ...)` into an O(N) OR chain over every
// pending trade), DataFusion's own AST walkers exhaust the 2 MiB stack and
// SIGABRT the coordinator. The coordinator must never crash — we spill,
// stream, and absorb large plans instead. An 8 MiB worker stack gives us ~4x
// the headroom at zero runtime cost.
fn main() -> anyhow::Result<()> {
    const WORKER_STACK_BYTES: usize = 8 * 1024 * 1024;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(WORKER_STACK_BYTES)
        .thread_name("sqe-coordinator")
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build tokio runtime: {e}"))?;

    runtime.block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = cli
        .config
        .or_else(|| std::env::var("SQE_CONFIG").ok())
        .unwrap_or_else(|| "sqe.toml".to_string());

    let config = SqeConfig::load(&config_path)
        .map_err(|e| anyhow::anyhow!("Failed to load config from {config_path}: {e}"))?;
    config
        .validate()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Security warnings for production readiness
    if !config.coordinator.tls.is_enabled() {
        tracing::warn!("WARNING: TLS is DISABLED -- Flight SQL and worker connections are unencrypted. Set [coordinator.tls] cert_file and key_file for production.");
    }
    if !config.rate_limit.enabled {
        tracing::warn!("WARNING: Rate limiting is DISABLED -- no protection against query flooding. Set [rate_limit] enabled = true for production.");
    }
    if config.auth.should_skip_tls_verify() {
        tracing::warn!("WARNING: TLS certificate verification is DISABLED for auth endpoints -- vulnerable to MITM. Set auth.tls_skip_verify = false (or auth.ssl_verification = true) for production.");
    }
    if !config.coordinator.worker_urls.is_empty() && config.coordinator.worker_secret.is_empty() {
        tracing::error!("SECURITY: worker_urls configured but worker_secret is empty -- any client can register as a worker. Set worker_secret for distributed mode.");
    }

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

    let _otel_guard = sqe_metrics::otel::init_telemetry_with_sampling(
        service_name,
        &config.metrics.otlp_endpoint,
        config.metrics.trace_sample_rate,
    );

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
        polaris_url: config.catalog.polaris_url.clone(),
    });
    start_health_server(health_port, health_state);

    // Metrics & audit
    let metrics = Arc::new(sqe_metrics::MetricsRegistry::new());
    let audit = Arc::new(
        sqe_metrics::audit::AuditLogger::new(&config.metrics.audit_log_path)
            .map_err(|e| anyhow::anyhow!(e))?,
    );

    sqe_metrics::server::start_metrics_server(metrics.clone(), config.metrics.prometheus_port);
    tracing::info!("Prometheus metrics on port {}", config.metrics.prometheus_port);

    // Credential refresh tracker — shared between query handler and background task
    let credential_tracker = Arc::new(
        sqe_coordinator::credential_refresh::CredentialRefreshTracker::new(),
    );

    // Start background credential refresh loop (checks every 60s)
    if !config.coordinator.worker_urls.is_empty() {
        sqe_coordinator::credential_refresh::start_credential_refresh_task(
            credential_tracker.clone(),
            std::time::Duration::from_secs(60),
            |_fragment| async {
                // Credential vending is deferred to Step 5 (Pluggable Catalogs):
                // the CatalogBackend trait will expose a `vend_credentials(table)`
                // method that reloads the table from Polaris to obtain fresh STS
                // tokens scoped to the fragment's data files.
                // Until then, workers use the original session credentials.
                // Tracking: nextsteps.md Step 5, openspec/changes/pluggable-catalogs/
                None
            },
        );
        tracing::info!("Started credential refresh background task (60s interval)");
    }

    // Session expiry sweeper — runs every 60s to remove idle/absolute-expired sessions
    {
        let sm = session_manager.clone();
        let idle = config.session.idle_timeout_secs;
        let absolute = config.session.absolute_timeout_secs;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            tick.tick().await; // skip first immediate tick
            loop {
                tick.tick().await;
                sm.sweep_expired_sessions(idle, absolute);
            }
        });
        tracing::info!(
            idle_timeout_secs = config.session.idle_timeout_secs,
            absolute_timeout_secs = config.session.absolute_timeout_secs,
            "Started session expiry sweeper (60s interval)"
        );
    }

    // File-based session persistence — optional, off by default
    if config.session.persistence == "file" {
        tracing::warn!(
            path = %config.session.persistence_path,
            "⚠ Session file persistence writes access tokens to disk in plaintext. \
             Ensure the persistence file has restrictive permissions (chmod 600). \
             Consider using memory persistence in production unless restart recovery is required."
        );

        // Try to restore sessions from the last snapshot on startup (best-effort).
        // Runs in spawn_blocking to avoid blocking the Tokio worker thread with std::fs I/O.
        {
            let sm = session_manager.clone();
            let restore_path = config.session.persistence_path.clone();
            let _ = tokio::task::spawn_blocking(move || sm.restore_from_file(&restore_path)).await;
        }

        // Spawn background task to periodically snapshot sessions to disk
        let sm = session_manager.clone();
        let path = config.session.persistence_path.clone();
        let interval = config.session.snapshot_interval_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval));
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                let sm_inner = sm.clone();
                let path_inner = path.clone();
                let result = tokio::task::spawn_blocking(move || {
                    sm_inner.snapshot_to_file(&path_inner)
                }).await;
                match result {
                    Ok(Err(e)) => tracing::warn!(error = %e, "Session snapshot failed"),
                    Err(e) => tracing::warn!(error = %e, "Session snapshot task panicked"),
                    Ok(Ok(())) => {}
                }
            }
        });
        tracing::info!(
            path = %config.session.persistence_path,
            interval_secs = config.session.snapshot_interval_secs,
            "File-based session persistence enabled"
        );
    }

    // Query tracker and result cache
    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );
    let query_cache = if config.query_cache.enabled {
        Some(Arc::new(sqe_coordinator::query_cache::ResultCache::new(&config.query_cache, Some(metrics.clone()))))
    } else {
        None
    };
    tracing::info!(
        history_max_entries = config.query_history.max_entries,
        cache_enabled = config.query_cache.enabled,
        "Initialized query tracker and result cache"
    );

    // Manifest-list and manifest caching is delegated to iceberg-rust's
    // per-`Table` `ObjectCache`. Because `TableMetadataCache` (built below)
    // caches `Table` instances globally, the per-table object cache persists
    // across queries and sessions.

    // Build the global table metadata cache (shared across all sessions and queries).
    // Table metadata is user-independent — schema, partitions, and snapshots are the
    // same regardless of who queries. The cache is invalidated on DDL/DML operations.
    let table_cache = sqe_catalog::TableMetadataCache::new(config.catalog.metadata_cache_ttl_secs);
    tracing::info!(
        metadata_cache_ttl_secs = config.catalog.metadata_cache_ttl_secs,
        "Initialized global table metadata cache (shared across all sessions)"
    );

    // Select the grant backend based on access_control.backend config.
    // "chameleon" (default for existing deployments), "polaris" (3-step
    // Management API), or "none" (access control disabled).
    let grant_backend: Option<Arc<dyn GrantBackend>> = match config
        .access_control
        .backend
        .as_str()
    {
        "chameleon" if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "chameleon",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            let client = Arc::new(sqe_catalog::AccessControlClient::new(
                &config.access_control.url,
            )?);
            Some(Arc::new(ChameleonGrantBackend::new(client)))
        }
        "polaris" if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "polaris",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            Some(Arc::new(PolarisGrantBackend::new(
                &config.access_control.url,
                config.access_control.client_id.clone(),
                config.access_control.client_secret.clone(),
            )?))
        }
        _ => None,
    };

    // Query handler
    let query_handler = Arc::new(
        QueryHandler::new(
            policy_enforcer,
            None, // policy_store — wired when policy engine is enabled
            config.clone(),
            if config.coordinator.worker_urls.is_empty() {
                None
            } else {
                Some(worker_registry.clone())
            },
            if config.coordinator.worker_urls.is_empty() {
                None
            } else {
                Some(credential_tracker)
            },
            Some(metrics.clone()),
            Some(audit.clone()),
            query_tracker,
            query_cache,
            grant_backend,
        )?
        .with_table_cache(table_cache)
        .with_session_manager(session_manager.clone()),
    );

    // Spawn background memory metrics reporter (updates gauges every 1s for Grafana)
    sqe_coordinator::memory::spawn_metrics_reporter(
        query_handler.runtime().clone(),
        metrics.clone(),
    );

    // Build bearer token auth chain for Trino-compat HTTP bearer token validation.
    let bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>> =
        match sqe_auth::build_auth_chain(&config.auth).await {
            Ok(chain) => {
                tracing::info!("Bearer token auth chain built for Trino-compat endpoint");
                Some(Arc::new(chain))
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to build bearer auth chain; bearer token auth will be disabled \
                     for Trino-compat"
                );
                None
            }
        };

    // Construct OAuth2 external auth state from [auth.external] config (if present).
    let oauth2_state: Option<Arc<sqe_trino_compat::oauth2::OAuth2State>> =
        if let Some(ref ext) = config.auth.external {
            match build_oauth2_state(ext, &config) {
                Ok(state) => {
                    tracing::info!(
                        issuer = %ext.issuer,
                        "External auth (OAuth2) enabled for Trino SSO"
                    );
                    Some(Arc::new(state))
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to initialize external auth; Trino SSO will be disabled"
                    );
                    None
                }
            }
        } else {
            None
        };

    // Rate limiter — shared between Flight SQL and Trino paths
    let rate_limiter = Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
        &config.rate_limit,
    ));

    // Trino compat
    if config.coordinator.trino_http_port > 0 {
        let auth_adapter = Arc::new(AuthenticatorAdapter {
            authenticator: authenticator.clone(),
            bearer_provider: bearer_provider.clone(),
        });
        let handler_adapter = Arc::new(QueryHandlerAdapter {
            handler: query_handler.clone(),
            rate_limiter: Arc::clone(&rate_limiter),
        });
        sqe_trino_compat::server::start_trino_server(
            auth_adapter,
            handler_adapter,
            config.coordinator.trino_http_port,
            NodeContext {
                version: sqe_core::VERSION.to_string(),
                ready: ready.clone(),
                started_at,
            },
            oauth2_state,
        );
        tracing::info!("Trino-compat HTTP on port {}", config.coordinator.trino_http_port);
    }

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // Flight SQL server with graceful shutdown
    let flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone())
            .with_rate_limiter(rate_limiter);
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    // Optional TLS
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("SQE coordinator listening on {addr} (TLS)");
    } else {
        tracing::info!("SQE coordinator listening on {addr} (plaintext)");
    }

    server_builder
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
        polaris_url: String::new(), // workers do not connect to Polaris directly
    });
    start_health_server(health_port, health_state);

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let worker_metrics = Arc::new(sqe_metrics::WorkerMetricsRegistry::new());
    sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    );

    let session_ctx = sqe_worker::runtime::build_session_context(&config.worker)?;
    let shuffle_compression = sqe_core::FlightCompression::from_config(
        &config.coordinator.shuffle_compression,
    )
    .unwrap_or(sqe_core::FlightCompression::Zstd);
    let flight_service =
        sqe_worker::flight_service::WorkerFlightService::new(worker_metrics, session_ctx)
            .with_scan_timeout(config.worker.scan_timeout_secs)
            .with_flight_compression(shuffle_compression)
            .with_shuffle_compression(shuffle_compression);

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // Optional TLS (reuse coordinator TLS config for workers)
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("SQE worker listening on {addr} (TLS)");
    } else {
        tracing::info!("SQE worker listening on {addr} (plaintext)");
    }

    server_builder
        .add_service(flight_service.into_server())
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

// ── External auth (OAuth2) construction ───────────────────────

/// Build the [`OAuth2State`] from the `[auth.external]` config section.
///
/// This creates:
/// - An [`OidcDiscovery`] instance (lazy-fetches `.well-known/openid-configuration`)
/// - An [`AuthCodeService`] for the Authorization Code + PKCE flow (Trino SSO)
/// - A [`PendingAuthStore`] to track in-flight auth sessions
///
/// The base URL for redirect/token URLs is derived from the coordinator's
/// Trino HTTP port (scheme defaults to `http`; in production, TLS termination
/// or a reverse proxy provides HTTPS).
fn build_oauth2_state(
    ext: &sqe_core::config::ExternalAuthConfig,
    config: &SqeConfig,
) -> anyhow::Result<sqe_trino_compat::oauth2::OAuth2State> {
    let discovery_config = sqe_auth::OidcDiscoveryConfig {
        issuer: ext.issuer.clone(),
        authorization_endpoint_override: ext.authorization_endpoint.clone(),
        token_endpoint_override: ext.token_endpoint.clone(),
        device_authorization_endpoint_override: ext.device_authorization_endpoint.clone(),
        accept_invalid_certs: ext.accept_invalid_certs,
    };

    let discovery = Arc::new(
        sqe_auth::OidcDiscovery::new(discovery_config)
            .map_err(|e| anyhow::anyhow!("OIDC discovery init failed: {e}"))?,
    );

    let auth_code_service = Arc::new(sqe_auth::AuthCodeService::new(
        discovery.clone(),
        ext.client_id.clone(),
        ext.client_secret.clone(),
        ext.redirect_uri.clone(),
        ext.scopes.clone(),
    ));

    let pending_store = Arc::new(sqe_auth::PendingAuthStore::new(
        std::time::Duration::from_secs(ext.challenge_timeout_secs),
    ));

    // Derive the base URL from the Trino HTTP port. In production, a reverse
    // proxy or TLS terminator provides the external HTTPS URL; for dev, use
    // the configured redirect_uri's scheme+host or fall back to localhost.
    let base_url = if ext.redirect_uri.contains("://") {
        // Extract scheme + host from redirect_uri (e.g. "https://sqe.example.com")
        let uri = ext.redirect_uri.trim_end_matches('/');
        match uri.find("://") {
            Some(idx) => {
                let after_scheme = &uri[idx + 3..];
                let host_end = after_scheme.find('/').unwrap_or(after_scheme.len());
                format!("{}://{}", &uri[..idx], &after_scheme[..host_end])
            }
            None => format!("http://localhost:{}", config.coordinator.trino_http_port),
        }
    } else {
        format!("http://localhost:{}", config.coordinator.trino_http_port)
    };

    Ok(sqe_trino_compat::oauth2::OAuth2State {
        auth_code_service,
        pending_store,
        base_url,
    })
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
            polaris_url: String::new(),
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
            polaris_url: String::new(),
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
            polaris_url: String::new(),
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
        // With an empty polaris_url, no Polaris check is performed — always healthy.
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            polaris_url: String::new(),
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readyz_not_ready() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            polaris_url: String::new(),
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(response.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }
}
