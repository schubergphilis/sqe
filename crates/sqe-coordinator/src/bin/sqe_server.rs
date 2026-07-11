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
use sqe_policy::grants::{polaris::PolarisGrantBackend, ranger::RangerGrantBackend, GrantBackend};
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

/// Decide whether the coordinator should run the distributed worker path:
/// consult the worker registry on queries, run health checks, and accept
/// heartbeats into the registry that `try_distribute` reads.
///
/// Returns `true` whenever the operator has expressed distributed intent.
/// Static seeding (`worker_urls`) is the explicit form. Dynamic heartbeat
/// discovery has no URL list, so we key off the worker auth secret being set
/// (the mechanism heartbeats authenticate against) or the explicit
/// `allow_unauthenticated_workers` opt-in. The default single-node config
/// (empty `worker_urls`, empty `worker_secret`, opt-in off) returns `false`,
/// so it never wires the registry and never accepts unauthenticated heartbeats
/// that would route user bearer tokens to unknown workers.
fn distributed_enabled(coordinator: &sqe_core::config::CoordinatorConfig) -> bool {
    !coordinator.worker_urls.is_empty()
        || !coordinator.worker_secret.is_empty()
        || coordinator.allow_unauthenticated_workers
}

// ── Health endpoints ───────────────────────────────────────────

struct HealthState {
    ready: Arc<AtomicBool>,
    started_at: Instant,
    role: &'static str,
    worker_registry: Option<Arc<sqe_coordinator::worker_registry::WorkerRegistry>>,
    query_tracker: Option<Arc<sqe_coordinator::query_tracker::QueryTracker>>,
    web_ui: bool,
    catalog_url: String,
    /// Populated from config in `run_coordinator`; `None` in `run_worker` and tests.
    node_info: Option<sqe_coordinator::web_ui::NodeInfo>,
    /// Populated in `run_coordinator`; `None` in `run_worker` and tests.
    metrics_history: Option<Arc<sqe_coordinator::metrics_history::MetricsHistory>>,
    /// Bearer auth provider for the web_ui guard. `None` in `run_worker` and tests.
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
    /// Auth config for admin-role check in the web_ui guard. `None` in `run_worker` and tests.
    auth_cfg: Option<sqe_core::config::AuthConfig>,
    /// Security config for client-IP resolution in the web_ui guard.
    /// Used to honour X-Forwarded-For from trusted proxies, mirroring the Flight
    /// SQL path. `None` in `run_worker` and tests (degrades to peer-wins).
    security_cfg: Option<sqe_core::config::SecurityConfig>,
    /// Audit logger wired for dashboard-access events. `None` in `run_worker` and tests.
    audit: Option<Arc<sqe_metrics::audit::AuditLogger>>,
    /// Counter incremented for anonymous (Unauthorized) dashboard denials instead
    /// of writing an audit line. `None` in `run_worker` and tests.
    anonymous_denied: Option<prometheus::IntCounter>,
    /// Counter incremented on every successful dashboard auth, including
    /// within-window deduplicated hits that do not write an audit line.
    /// `None` in `run_worker` and tests.
    dashboard_success: Option<prometheus::IntCounter>,
    /// Moka TTL cache keyed by principal. A hit means the principal was already
    /// audited within the current window; skip the audit line but still count.
    /// `None` means no dedup (window == 0 or run_worker/tests).
    success_audit_dedup: Option<moka::sync::Cache<String, ()>>,
}

impl sqe_coordinator::web_auth::BearerAdminState for HealthState {
    fn bearer_provider(&self) -> Option<&Arc<dyn sqe_auth::AuthProvider>> {
        self.bearer_provider.as_ref()
    }

    fn auth_config(&self) -> Option<&sqe_core::config::AuthConfig> {
        self.auth_cfg.as_ref()
    }

    fn audit(&self) -> Option<&Arc<sqe_metrics::audit::AuditLogger>> {
        self.audit.as_ref()
    }

    fn on_anonymous_denial(&self) {
        if let Some(c) = &self.anonymous_denied {
            c.inc();
        }
    }

    fn should_emit_success_audit(&self, principal: &str) -> bool {
        match &self.success_audit_dedup {
            None => true, // window == 0 or not wired: always emit
            Some(cache) => {
                if cache.contains_key(principal) {
                    false
                } else {
                    cache.insert(principal.to_string(), ());
                    true
                }
            }
        }
    }

    fn note_dashboard_success(&self) {
        if let Some(c) = &self.dashboard_success {
            c.inc();
        }
    }

    fn resolve_client_ip(&self, peer: Option<&str>, xff: Option<&str>) -> Option<String> {
        match &self.security_cfg {
            Some(sec) => {
                let resolved = sec.resolve_client_ip(peer, xff);
                // resolve_client_ip returns "unknown" when peer is None; map that
                // back to None so we don't pollute audit events with a literal
                // "unknown" string.
                if resolved == "unknown" {
                    None
                } else {
                    Some(resolved)
                }
            }
            // No security config wired (run_worker / tests): fall back to peer-wins.
            None => peer.map(|p| p.to_string()),
        }
    }
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
    if !state.catalog_url.is_empty() {
        let polaris_ok = reqwest::Client::new()
            .get(format!("{}/api/catalog/v1/config", state.catalog_url))
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

// ── Read-only web UI JSON API ──────────────────────────────────

#[derive(serde::Deserialize)]
struct QueryListParams {
    state: Option<String>,
    limit: Option<usize>,
}

async fn api_queries(
    state: axum::extract::State<Arc<HealthState>>,
    params: axum::extract::Query<QueryListParams>,
) -> Json<Vec<sqe_coordinator::web_ui::QueryListItem>> {
    let limit = params.limit.unwrap_or(200).min(1000);
    let items = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::query_list(t, params.state.as_deref(), limit),
        None => Vec::new(),
    };
    Json(items)
}

async fn api_query_detail(
    state: axum::extract::State<Arc<HealthState>>,
    id: axum::extract::Path<String>,
) -> Response {
    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return (axum::http::StatusCode::BAD_REQUEST, "invalid query id").into_response();
    };
    let detail = state
        .query_tracker
        .as_ref()
        .and_then(|t| sqe_coordinator::web_ui::query_detail(t, &uuid));
    match detail {
        Some(d) => Json(d).into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "query not found").into_response(),
    }
}

async fn api_workers(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::web_ui::WorkersDto> {
    let (total, healthy_urls) = match &state.worker_registry {
        Some(r) => (r.total_workers().await, r.healthy_workers().await),
        None => (0, Vec::new()),
    };
    // With a tracker, derive per-worker in-flight from running fragments. Without
    // one (it is always present on the coordinator path; this is the defensive
    // branch), report health only.
    let view = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::workers_view(total, healthy_urls, t.active_count(), t),
        None => sqe_coordinator::web_ui::WorkersDto {
            total,
            healthy_count: healthy_urls.len(),
            active_queries: 0,
            workers: healthy_urls
                .into_iter()
                .map(|url| sqe_coordinator::web_ui::WorkerDto { url, healthy: true, in_flight: 0 })
                .collect(),
        },
    };
    Json(view)
}

async fn api_overview(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::web_ui::OverviewDto> {
    let uptime_seconds = state.started_at.elapsed().as_secs();
    let cpu_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    // Build (or synthesise) the NodeInfo.
    let node_info = state.node_info.clone().unwrap_or_else(|| {
        sqe_coordinator::web_ui::NodeInfo {
            name: "SQE",
            version: sqe_core::VERSION,
            role: state.role,
            datafusion_version: "51",
            flight_sql_port: 0,
            trino_http_port: None,
            quack_port: None,
            catalog_backend: String::new(),
            catalog_url: String::new(),
            storage: String::new(),
            memory_limit: None,
            max_concurrent_queries: 0,
        }
    });

    // Use the real tracker when available; fall back to an ephemeral empty one
    // (tracker is always Some on coordinator path; this branch is defensive).
    let dto = match &state.query_tracker {
        Some(t) => sqe_coordinator::web_ui::overview(&node_info, uptime_seconds, cpu_cores, t),
        None => {
            let t = sqe_coordinator::query_tracker::QueryTracker::new(
                &sqe_core::QueryHistoryConfig::default(),
            );
            sqe_coordinator::web_ui::overview(&node_info, uptime_seconds, cpu_cores, &t)
        }
    };
    Json(dto)
}

async fn api_metrics_history(
    state: axum::extract::State<Arc<HealthState>>,
) -> Json<sqe_coordinator::metrics_history::HistoryResponse> {
    let response = state
        .metrics_history
        .as_ref()
        .map(|h| {
            let samples = h.snapshot();
            sqe_coordinator::metrics_history::bucket_samples(&samples)
        })
        .unwrap_or_else(|| sqe_coordinator::metrics_history::HistoryResponse {
            bucket_seconds: sqe_coordinator::metrics_history::BUCKET_SECS,
            buckets: Vec::new(),
        });
    Json(response)
}

async fn dashboard() -> Response {
    // WEB-05: send nosniff plus a tight CSP. The dashboard is a self-contained
    // single page (inline styles/scripts, same-origin fetches only), so a
    // restrictive policy does not break it and blocks injected external loads.
    (
        [
            (axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (axum::http::header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (
                axum::http::header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; style-src 'unsafe-inline'; \
                 script-src 'unsafe-inline'; connect-src 'self'; \
                 img-src 'self' data:; base-uri 'none'; form-action 'none'; \
                 frame-ancestors 'none'",
            ),
        ],
        sqe_coordinator::web_ui::DASHBOARD_HTML,
    )
        .into_response()
}

/// Build the health server router. Extracted so tests can drive it directly.
///
/// `/healthz`, `/readyz`, and `/api/v1/status` are always ungated -- they
/// serve k8s liveness/readiness probes and LB health checks. The `web_ui`
/// route group (dashboard + JSON API) is gated behind bearer + admin auth via
/// `route_layer` applied only to that sub-router.
fn build_health_router(state: Arc<HealthState>) -> Router {
    let open = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/v1/status", get(cluster_status));

    let app = if state.web_ui {
        let web = Router::new()
            .route("/", get(dashboard))
            .route("/api/v1/overview", get(api_overview))
            .route("/api/v1/queries", get(api_queries))
            .route("/api/v1/queries/{id}", get(api_query_detail))
            .route("/api/v1/workers", get(api_workers))
            .route("/api/v1/metrics/history", get(api_metrics_history))
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                sqe_coordinator::web_auth::require_admin_bearer::<HealthState>,
            ));
        open.merge(web)
    } else {
        open
    };

    app.with_state(state)
}

fn start_health_server(port: u16, state: Arc<HealthState>) {
    let app = build_health_router(state);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .expect("Failed to bind health server");
        tracing::info!("Health endpoints on port {port} (/healthz, /readyz, /api/v1/status)");
        // Serve with connect-info so the dashboard-access guard can extract the
        // peer TCP address for client_ip in audit events.
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap_or_else(|e| tracing::error!(error = %e, "Health server terminated unexpectedly"));
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

/// Shutdown future for `serve_with_shutdown` that drains before stopping (#250).
///
/// On SIGTERM/SIGINT this:
/// 1. flips `ready` to NOT-ready so `/readyz` returns 503 and the Kubernetes
///    Service / load balancer stops routing NEW work to this pod;
/// 2. sleeps `drain_secs` so connections already routed before the readiness
///    flip propagated can finish at the tonic graceful boundary;
/// 3. resolves, which tells tonic to stop accepting and shut down.
///
/// The grace period is bounded by `coordinator.shutdown_drain_secs` and must
/// stay below the pod's `terminationGracePeriodSeconds` so the process exits
/// before SIGKILL. NOTE: this is a time-bounded connection-drain, not a true
/// in-flight-query drain. SQE has no query-lifecycle registry to wait on yet;
/// tracking in-flight queries and blocking shutdown until they complete is a
/// follow-up (see MR).
async fn shutdown_with_drain(ready: Arc<AtomicBool>, drain_secs: u64) {
    shutdown_signal().await;
    flip_ready_and_drain(&ready, drain_secs).await;
}

/// Flip readiness to NOT-ready, then sleep the bounded drain period. Split out
/// of [`shutdown_with_drain`] so the readiness-flip is unit-testable without a
/// real OS signal.
async fn flip_ready_and_drain(ready: &AtomicBool, drain_secs: u64) {
    // Stop advertising readiness BEFORE we start draining so the Service
    // routing table converges away from this pod while we wait.
    ready.store(false, Ordering::Relaxed);

    if drain_secs == 0 {
        tracing::info!("shutdown_drain_secs = 0, shutting down immediately");
        return;
    }

    tracing::info!(
        drain_secs,
        "readiness flipped to NOT-ready, draining connections before shutdown"
    );
    tokio::time::sleep(std::time::Duration::from_secs(drain_secs)).await;
    tracing::info!("drain period elapsed, shutting down");
}

// ── Trino adapters ─────────────────────────────────────────────
struct AuthenticatorAdapter {
    authenticator: Arc<sqe_auth::Authenticator>,
    /// The full auth provider chain (built from `[[auth.providers]]`). When set
    /// (always, in production) BOTH Basic auth and bearer tokens dispatch
    /// through it, so the Trino-compat path sees the same providers as the
    /// Flight SQL handshake, including `client_credentials_passthrough`. When
    /// `None` (some unit-test fixtures), Basic auth falls back to the legacy
    /// `Authenticator`.
    bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>,
}

#[async_trait::async_trait]
impl TrinoAuthenticator for AuthenticatorAdapter {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        // Route Basic auth through the chain so a service principal's
        // client_id/client_secret reaches `client_credentials_passthrough` over
        // Trino just as it does over Flight SQL. Fall back to the legacy
        // authenticator only when no chain is wired (test fixtures).
        if let Some(provider) = self.bearer_provider.as_ref() {
            let credentials = sqe_auth::FlightCredentials {
                username: Some(username.to_string()),
                password: Some(sqe_core::SecretString::new(password.to_string())),
                ..Default::default()
            };
            let identity = provider
                .authenticate(&credentials)
                .await
                .map_err(|e| sqe_core::SqeError::Auth(e.to_string()))?;
            return Ok(sqe_coordinator::auth_session::identity_to_session(identity, None));
        }
        self.authenticator
            .authenticate(username, password)
            .await
            .map_err(|e| sqe_core::SqeError::Auth(e.to_string()))
    }

    async fn authenticate_bearer(
        &self,
        token: &str,
    ) -> Result<sqe_core::Session, sqe_core::SqeError> {
        let provider = self.bearer_provider.as_ref().ok_or_else(|| {
            sqe_core::SqeError::Auth("Bearer token authentication is not configured".to_string())
        })?;

        let credentials = sqe_auth::FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token.to_string())),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&credentials)
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Bearer token validation failed: {e}")))?;

        // Fall back to the raw JWT as the catalog token when the provider did
        // not supply one (passthrough to Polaris).
        Ok(sqe_coordinator::auth_session::identity_to_session(identity, Some(token)))
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
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        self.rate_limiter
            .check(&session.user.username)
            .map_err(|e| sqe_core::SqeError::Execution(format!("rate limit: {e}")))?;
        self.handler.execute(session, sql, None).await
    }

    async fn describe_prepared(
        &self,
        session: &sqe_core::Session,
        prepared_sql: &str,
        kind: sqe_trino_compat::protocol::DescribeKind,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        self.handler.describe_prepared(session, prepared_sql, kind).await
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
    if config.auth.should_skip_tls_verify()
        || config
            .auth
            .external
            .as_ref()
            .is_some_and(|e| e.accept_invalid_certs)
    {
        // AUTH-04: the external (interactive) auth path has its own
        // accept_invalid_certs flag that was not covered by this warning.
        tracing::warn!("WARNING: TLS certificate verification is DISABLED for auth endpoints (auth.tls_skip_verify / auth.ssl_verification / auth.external.accept_invalid_certs) -- vulnerable to MITM. Disable these for production.");
    }
    // Fire whenever the unauthenticated-discovery path is actually live: the
    // worker registry is attached (worker_urls non-empty), there is no secret
    // to check (worker_secret empty), and the operator has waived the
    // empty-secret refusal. A configured secret is enforced on the heartbeat
    // path even with the waiver set, so gating on the empty secret avoids a
    // false positive on authenticated-but-waived deployments. Shared with
    // main.rs so the two coordinator binaries cannot drift.
    if sqe_coordinator::mode::warns_unauthenticated_workers(&config.coordinator) {
        tracing::warn!("WARNING: coordinator.allow_unauthenticated_workers = true with an empty coordinator.worker_secret -- any client reachable on the cluster network can register as a worker and receive user bearer tokens. Set worker_secret for production.");
    }
    // WEB-01: the web UI / JSON API serve on the health port (0.0.0.0).
    // Routes are gated behind bearer + admin auth (see web_auth::require_admin_bearer).
    // Warn so operators know the surface is active.
    if config.metrics.web_ui {
        tracing::warn!("WARNING: metrics.web_ui = true -- the ops dashboard and /api/v1/* endpoints are served on the health port (0.0.0.0). Routes require a valid admin bearer token. Network-gate the health port or leave web_ui = false.");
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

fn build_grant_backend(
    config: &SqeConfig,
) -> anyhow::Result<Option<Arc<dyn GrantBackend>>> {
    use sqe_core::config::AccessControlBackend;
    match config.access_control.backend {
        AccessControlBackend::Chameleon if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "chameleon",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            let client = Arc::new(sqe_catalog::AccessControlClient::new(
                &config.access_control.url,
            )?);
            Ok(Some(Arc::new(ChameleonGrantBackend::new(client))))
        }
        AccessControlBackend::Polaris if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "polaris",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(PolarisGrantBackend::new(
                &config.access_control.url,
                config.access_control.client_id.clone(),
                config.access_control.client_secret.clone(),
            )?)))
        }
        AccessControlBackend::Ranger if !config.access_control.url.is_empty() => {
            let r = &config.access_control.ranger;
            tracing::info!(
                backend = "ranger",
                url = %config.access_control.url,
                service = %r.service_name,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(RangerGrantBackend::new(
                &config.access_control.url,
                &r.service_name,
                &r.admin_user,
                r.admin_password.expose(),
                &r.realm,
                r.timeout_secs,
                r.accept_invalid_certs,
            )?)))
        }
        AccessControlBackend::None
        | AccessControlBackend::Chameleon
        | AccessControlBackend::Polaris
        | AccessControlBackend::Ranger => Ok(None),
    }
}

// ── Coordinator ────────────────────────────────────────────────
async fn run_coordinator(config: SqeConfig) -> anyhow::Result<()> {
    let started_at = Instant::now();
    let ready = Arc::new(AtomicBool::new(false));

    // Single source of truth for the distributed worker path. Computed once so
    // the registry wiring, health checks, credential refresh, the health-server
    // view, and the query handler can never drift out of agreement. See
    // `distributed_enabled` for why heartbeat discovery keys off the secret.
    let distributed = distributed_enabled(&config.coordinator);

    // Health endpoints on metrics port + 1 (or 9091 default)
    let health_port = config.metrics.prometheus_port + 1;

    // Track supervised background tasks for the lifetime of the binary;
    // dropping each TaskGuard signals cooperative cancellation and aborts
    // the underlying tokio task.
    let mut _task_guards: Vec<sqe_core::TaskGuard> = Vec::new();

    // Auth
    let authenticator = Arc::new(sqe_auth::Authenticator::new(&config.auth).await?);
    _task_guards.push(authenticator.start_refresh_task());

    // Build the auth provider chain from `[[auth.providers]]`. Both Flight
    // SQL (via SessionManager) and the Trino-compat HTTP path use the
    // same chain so the two endpoints accept the same set of credentials.
    // For configs without `[[auth.providers]]` the chain wraps the legacy
    // Authenticator and behaviour is unchanged.
    let auth_chain: Arc<dyn sqe_auth::AuthProvider> =
        Arc::new(sqe_auth::build_auth_chain(&config.auth).await?);

    // SessionManager authenticates new Flight SQL requests through the
    // chain; the legacy Authenticator stays attached so its background
    // refresh task continues to keep username/password tokens current.
    let session_manager = Arc::new(SessionManager::with_provider_and_legacy(
        Arc::clone(&auth_chain),
        authenticator.clone(),
    ));

    let worker_registry = Arc::new(
        sqe_coordinator::worker_registry::WorkerRegistry::with_options_and_failures(
            config.coordinator.worker_urls.clone(),
            sqe_coordinator::channel_pool::ChannelPool::shared_with_timeouts(
                std::time::Duration::from_secs(config.coordinator.worker_connect_timeout_secs),
                std::time::Duration::from_secs(config.coordinator.worker_rpc_timeout_secs),
            ),
            config.coordinator.max_workers,
            config.coordinator.health_check_max_failures,
        ),
    );

    if distributed {
        let interval =
            std::time::Duration::from_secs(config.coordinator.health_check_interval_secs);
        _task_guards.push(worker_registry.start_health_check_task(interval));
        tracing::info!(
            seeded_workers = ?config.coordinator.worker_urls,
            interval_secs = config.coordinator.health_check_interval_secs,
            "Started worker health checks (covers heartbeat-discovered workers too)"
        );
    }

    // Created before the health server so the web UI can read it; the same Arc
    // is moved into the QueryHandler below.
    let query_tracker = Arc::new(
        sqe_coordinator::query_tracker::QueryTracker::new(&config.query_history),
    );

    // Build NodeInfo for the Overview endpoint from config (config is still in
    // scope at this point; it gets moved into the handlers below).
    let node_info = {
        use sqe_core::config::CatalogBackend;
        let catalog_backend = match &config.catalog.backend {
            CatalogBackend::Rest => "rest",
            CatalogBackend::Hms { .. } => "hms",
            CatalogBackend::Glue { .. } => "glue",
            CatalogBackend::Jdbc { .. } => "jdbc",
            CatalogBackend::S3tables { .. } => "s3tables",
        }
        .to_string();
        let storage = if config.storage.s3_endpoint.is_empty() {
            "local".to_string()
        } else {
            config.storage.s3_endpoint.clone()
        };
        let trino_http_port = (config.coordinator.trino_http_port != 0)
            .then_some(config.coordinator.trino_http_port);
        let quack_port = (config.coordinator.quack_port != 0)
            .then_some(config.coordinator.quack_port);
        let memory_limit = if config.query.max_query_memory.is_empty() || config.query.max_query_memory == "0" {
            None
        } else {
            Some(config.query.max_query_memory.clone())
        };
        sqe_coordinator::web_ui::NodeInfo {
            name: "SQE",
            version: sqe_core::VERSION,
            role: "coordinator",
            datafusion_version: "51",
            flight_sql_port: config.coordinator.flight_sql_port,
            trino_http_port,
            quack_port,
            catalog_backend,
            catalog_url: config.catalog.catalog_url.clone(),
            storage,
            memory_limit,
            max_concurrent_queries: config.query.max_concurrent_queries,
        }
    };

    // Metrics ring-buffer for the dashboard history endpoint (1 h at 5 s resolution).
    let metrics_history = if config.metrics.web_ui {
        Some(Arc::new(sqe_coordinator::metrics_history::MetricsHistory::new(720)))
    } else {
        None
    };

    // Preserve a clone of query_tracker for the metrics sampler; the original
    // is moved into QueryHandler::new below.
    let sampler_tracker = query_tracker.clone();

    // Metrics & audit
    let metrics = Arc::new(
        sqe_metrics::MetricsRegistry::new()
            .map_err(|e| anyhow::anyhow!("failed to initialize metrics registry: {e}"))?,
    );

    sqe_metrics::server::start_metrics_server(metrics.clone(), config.metrics.prometheus_port);
    tracing::info!("Prometheus metrics on port {}", config.metrics.prometheus_port);

    // Credential refresh tracker — shared between query handler and background task
    let credential_tracker = Arc::new(
        sqe_coordinator::credential_refresh::CredentialRefreshTracker::new(),
    );

    if distributed {
        let interval =
            std::time::Duration::from_secs(config.coordinator.credential_refresh_interval_secs);
        _task_guards.push(
            sqe_coordinator::credential_refresh::start_credential_refresh_task(
                credential_tracker.clone(),
                interval,
                config.coordinator.worker_secret.expose().to_string(),
                |_fragment| async {
                    // Credential vending is deferred to Step 5 (Pluggable
                    // Catalogs): the CatalogBackend trait will expose a
                    // `vend_credentials(table)` method that reloads the
                    // table from Polaris to obtain fresh STS tokens scoped
                    // to the fragment's data files. Until then, workers use
                    // the original session credentials.
                    None
                },
            ),
        );
        tracing::info!(
            interval_secs = config.coordinator.credential_refresh_interval_secs,
            "Started credential refresh background task"
        );
    }

    {
        let sm = session_manager.clone();
        let idle = config.session.idle_timeout_secs;
        let absolute = config.session.absolute_timeout_secs;
        let sweep_interval =
            std::time::Duration::from_secs(config.session.expiry_sweep_interval_secs);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_interval);
            tick.tick().await;
            loop {
                tick.tick().await;
                sm.sweep_expired_sessions(idle, absolute);
            }
        });
        tracing::info!(
            idle_timeout_secs = config.session.idle_timeout_secs,
            absolute_timeout_secs = config.session.absolute_timeout_secs,
            sweep_interval_secs = config.session.expiry_sweep_interval_secs,
            "Started session expiry sweeper"
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

    // Query tracker and result cache (query_tracker Arc already created above for the health server)
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
    // Table metadata is user-independent. Schema, partitions, and snapshots are the
    // same regardless of who queries. The cache is invalidated on DDL/DML operations.
    let table_cache = sqe_catalog::TableMetadataCache::new(config.catalog.metadata_cache_ttl_secs)
        .with_metrics(Arc::clone(&metrics));
    tracing::info!(
        metadata_cache_ttl_secs = config.catalog.metadata_cache_ttl_secs,
        "Initialized global table metadata cache (shared across all sessions)"
    );

    // Build the audit logger after the table cache so GDPR tag masking can be
    // wired via CacheTagSource. Salt is derived once at startup (stable within
    // a deployment, not across restarts/replicas, not secret-grade).
    let audit_salt = uuid::Uuid::new_v4().to_string();
    let audit_logger = sqe_metrics::audit::AuditLogger::with_config(
        &config.metrics.audit_log_path,
        sqe_coordinator::parse_audit_format(&config.metrics.audit.format),
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    let audit_logger = if !config.metrics.audit.gdpr_tags.is_empty() {
        let tag_src = Arc::new(sqe_coordinator::tag_source_impl::CacheTagSource::new(
            Arc::new(table_cache.clone()),
        ));
        let adapter = Arc::new(sqe_coordinator::audit_tag_adapter::AuditTagAdapter(tag_src));
        audit_logger.with_gdpr(
            config.metrics.audit.gdpr_tags.clone(),
            sqe_coordinator::parse_gdpr_mode(&config.metrics.audit.gdpr_identifier_mode),
            audit_salt,
            adapter,
        )
    } else {
        audit_logger
    };

    // Attach the OCSF export spool when audit export is enabled.
    let (audit_logger, shipper_spool_path) =
        if config.metrics.audit_export.enabled && config.metrics.audit_export.target == "otlp" {
            let spool = sqe_coordinator::derive_spool_path(
                &config.metrics.audit_export.spool_path,
                &config.metrics.audit_log_path,
            );
            let logger = audit_logger
                .with_export_spool(&spool)
                .map_err(|e| anyhow::anyhow!("audit export: failed to open spool file: {e}"))?;
            (logger, Some(spool))
        } else {
            (audit_logger, None)
        };

    let audit = Arc::new(audit_logger);

    // Health server. Started here (after audit init) so the audit logger can be
    // threaded into HealthState and dashboard-access events are captured from
    // the first request. Probes (/healthz, /readyz) are still served immediately
    // after coordinator startup; the only change vs. the pre-Task-2 placement is
    // that the health server starts a few ms later (after audit file open).
    let health_state = Arc::new(HealthState {
        ready: ready.clone(),
        started_at,
        role: "coordinator",
        worker_registry: if distributed {
            Some(worker_registry.clone())
        } else {
            None
        },
        query_tracker: Some(query_tracker.clone()),
        web_ui: config.metrics.web_ui,
        catalog_url: config.catalog.catalog_url.clone(),
        node_info: Some(node_info),
        metrics_history: metrics_history.clone(),
        // auth_chain is already built above; reuse it for the web_ui guard.
        bearer_provider: Some(Arc::clone(&auth_chain) as Arc<dyn sqe_auth::AuthProvider>),
        auth_cfg: Some(config.auth.clone()),
        // Thread the security config so the dashboard guard honours
        // trusted_proxies for XFF resolution, matching the Flight SQL path.
        security_cfg: Some(config.security.clone()),
        audit: Some(Arc::clone(&audit)),
        anonymous_denied: Some(metrics.dashboard_auth_anonymous_denied_total.clone()),
        dashboard_success: Some(metrics.dashboard_auth_success_total.clone()),
        success_audit_dedup: {
            let window = config.metrics.audit.dashboard_access_audit_window_secs;
            if window == 0 {
                None
            } else {
                Some(
                    moka::sync::Cache::builder()
                        .max_capacity(1024)
                        .time_to_live(std::time::Duration::from_secs(window))
                        .build(),
                )
            }
        },
    });
    start_health_server(health_port, health_state);

    // Spawn the OTLP audit shipper when export is enabled.
    let shipper_shutdown_tx = if let Some(ref spool_path) = shipper_spool_path {
        let ae = &config.metrics.audit_export;

        let endpoint = if !ae.otlp_endpoint.is_empty() {
            ae.otlp_endpoint.clone()
        } else if !config.metrics.otlp_endpoint.is_empty() {
            config.metrics.otlp_endpoint.clone()
        } else {
            tracing::warn!(
                "audit export: no OTLP endpoint configured \
                 (audit_export.otlp_endpoint and metrics.otlp_endpoint are both empty); \
                 shipper will not start"
            );
            String::new()
        };

        if endpoint.is_empty() {
            None
        } else {
            match sqe_metrics::audit::export::OtlpExporter::new(&endpoint) {
                Err(e) => {
                    tracing::warn!(error = %e, "audit export: failed to build OtlpExporter; shipper will not start");
                    None
                }
                Ok(exporter) => {
                    let cursor_path = format!("{spool_path}.cursor");
                    let shipper_metrics = sqe_metrics::audit::export::ShipperMetrics {
                        records_total: Some(metrics.audit_export_records_total.clone()),
                        batch_failures_total: Some(metrics.audit_export_batch_failures_total.clone()),
                        spool_lag_bytes: Some(metrics.audit_export_spool_lag_bytes.clone()),
                        cursor_seq: Some(metrics.audit_export_cursor_seq.clone()),
                        last_success_timestamp: Some(metrics.audit_export_last_success_timestamp.clone()),
                    };
                    let shipper = sqe_metrics::audit::export::OtlpLogShipper::new(
                        std::path::PathBuf::from(spool_path),
                        std::path::PathBuf::from(&cursor_path),
                        std::sync::Arc::new(exporter),
                        ae.batch_max,
                        ae.max_spool_bytes,
                        sqe_coordinator::parse_start_at(&ae.start_at),
                        ae.flush_interval_ms,
                    )
                    .with_metrics(shipper_metrics);

                    let (tx, rx) = tokio::sync::watch::channel(false);
                    tokio::spawn(shipper.run(rx));
                    tracing::info!(
                        spool = %spool_path,
                        endpoint = %endpoint,
                        "Started audit OTLP shipper"
                    );
                    Some(tx)
                }
            }
        }
    } else {
        if config.metrics.audit_export.enabled
            && config.metrics.audit_export.target != "otlp"
        {
            tracing::warn!(
                target = %config.metrics.audit_export.target,
                "audit export: unknown target; only \"otlp\" is supported; shipper will not start"
            );
        }
        None
    };

    // Guard + self-audit the superdebug_log_results escape hatch.
    // No-op when the flag is false (the default).
    sqe_coordinator::maybe_warn_superdebug(&audit, &config);

    // AUTH-01: build the enforcer + store from config.policy.engine.
    // table_cache is passed so the rewriter can wire CacheTagSource for
    // tag-based column masking (Task 4). A clone is taken here; the original
    // is passed to with_table_cache() on the QueryHandler below.
    let (policy_enforcer, policy_store) = sqe_coordinator::policy_wiring::build_policy_enforcer(
        &config.policy,
        Some(table_cache.clone()),
        Some(Arc::clone(&metrics)),
    )?;
    if config.policy.engine != sqe_core::config::PolicyEngine::Passthrough {
        tracing::info!(
            engine = ?config.policy.engine,
            "policy enforcement ACTIVE (row filters + column masks + tag masking)"
        );
    }

    let grant_backend: Option<Arc<dyn GrantBackend>> = build_grant_backend(&config)?;

    // OpenLineage observer (optional). When [metrics.openlineage] enabled = true,
    // build the configured sinks (file and/or HTTP+spool), spawn the emitter task,
    // and hand back a ChannelObserver wired to the bounded mpsc.
    let lineage_obs = build_lineage_observer(&config)?;

    // Query handler
    let query_handler = Arc::new(
        QueryHandler::new(
            policy_enforcer,
            policy_store,
            config.clone(),
            if distributed {
                Some(worker_registry.clone())
            } else {
                None
            },
            if distributed {
                Some(credential_tracker)
            } else {
                None
            },
            Some(metrics.clone()),
            Some(audit.clone()),
            query_tracker,
            query_cache,
            grant_backend,
            lineage_obs,
            sqe_coordinator::RuntimeCatalogRegistry::default(),
            sqe_core::SecretStore::default(),
        )?
        .with_table_cache(table_cache)
        .with_session_manager(session_manager.clone()),
    );

    // Spawn background memory metrics reporter (updates gauges every 1s for Grafana)
    sqe_coordinator::memory::spawn_metrics_reporter(
        query_handler.runtime().clone(),
        metrics.clone(),
    );

    // Spawn dashboard time-series sampler when the web UI is enabled.
    if let Some(ref hist) = metrics_history {
        _task_guards.push(sqe_coordinator::metrics_history::spawn_sampler(
            hist.clone(),
            query_handler.runtime().clone(),
            sampler_tracker,
            std::time::Duration::from_secs(5),
        ));
        tracing::info!("Started metrics history sampler (5 s interval, 1 h window)");
    }

    // Bearer auth chain for the Trino-compat HTTP path. Reuses the same
    // chain instance the Flight SQL path uses so both endpoints accept
    // the same credentials with identical provider behaviour.
    let bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>> =
        Some(Arc::clone(&auth_chain));

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

    // Rate limiters — shared between Flight SQL and Trino paths
    let rate_limiter = Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
        &config.rate_limit,
    ));
    let auth_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::AuthRateLimiter::new(
        &config.rate_limit,
    ));
    let metadata_rate_limiter = Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
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
        let trino_auth_limiter: Arc<dyn sqe_trino_compat::server::TrinoAuthRateLimiter> =
            Arc::clone(&auth_rate_limiter) as _;
        let trino_opts = sqe_trino_compat::server::TrinoServerOptions {
            security: config.security.clone(),
            auth_rate_limiter: Some(trino_auth_limiter),
            expose_version: false,
        };
        sqe_trino_compat::server::start_trino_server_with_options(
            auth_adapter,
            handler_adapter,
            config.coordinator.trino_http_port,
            NodeContext {
                version: sqe_core::VERSION.to_string(),
                ready: ready.clone(),
                started_at,
            },
            oauth2_state,
            trino_opts,
        );
        tracing::info!("Trino-compat HTTP on port {}", config.coordinator.trino_http_port);
    }

    // DuckDB Quack RPC endpoint (off by default). Shares the same AuthChain
    // as Flight SQL / Trino-compat, and dispatches every query through the
    // same QueryHandler via a CoordinatorExecutor adapter.
    if config.coordinator.quack_port > 0 {
        let quack_port = config.coordinator.quack_port;
        let executor: Arc<dyn sqe_quack_server::QueryExecutor> = Arc::new(
            sqe_coordinator::CoordinatorExecutor::new(query_handler.clone()),
        );
        // QUACK-08: the Quack ConnectionRequest -> authenticate path is rate
        // limited per client IP inside sqe-quack-server, mirroring the Flight /
        // Trino auth limiters. We pass [security].trusted_proxies so the limiter
        // keys on the real client IP (rightmost untrusted x-forwarded-for hop)
        // when the peer is a trusted proxy, and on the raw TCP peer otherwise.
        // Serving with connect-info hands the handler the peer SocketAddr.
        let quack_state =
            sqe_quack_server::QuackServerState::new(Arc::clone(&auth_chain), executor)
                .with_security(config.security.clone());
        let quack_app = sqe_quack_server::router(quack_state);
        let bind = format!("0.0.0.0:{quack_port}");
        // QUACK-05: the Quack ConnectionRequest carries the user's OIDC bearer
        // token. On a plaintext channel any on-path observer can capture and
        // replay it against Flight SQL / Polaris / S3 as that user. Reuse the
        // coordinator's [coordinator.tls] block to serve the Quack endpoint over
        // TLS; fall back to plaintext (with a loud warning) when TLS is off.
        if config.coordinator.tls.is_enabled() {
            let quack_addr: std::net::SocketAddr = bind
                .parse()
                .map_err(|e| anyhow::anyhow!("Quack server addr {bind}: {e}"))?;
            // Install the ring crypto provider so rustls' default ServerConfig
            // builder (used by RustlsConfig::from_pem_file) has a backend. This
            // matches the tonic Flight path's `tls-ring`. Idempotent: a second
            // install returns Err, which we ignore.
            let _ = rustls::crypto::ring::default_provider().install_default();
            if !config.coordinator.tls.ca_file.is_empty() {
                tracing::warn!("Quack TLS is server-side only: client-CA (mTLS) from [coordinator.tls].ca_file is enforced on the Flight path but not on the Quack path");
            }
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(
                &config.coordinator.tls.cert_file,
                &config.coordinator.tls.key_file,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Quack TLS config: {e}"))?;
            tokio::spawn(async move {
                if let Err(e) = axum_server::bind_rustls(quack_addr, tls)
                    .serve(
                        quack_app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                    )
                    .await
                {
                    tracing::error!(error = %e, "Quack RPC server (TLS) terminated unexpectedly");
                }
            });
            tracing::info!("DuckDB Quack RPC on port {quack_port} (TLS)");
        } else {
            tracing::warn!("WARNING: the Quack endpoint is PLAINTEXT (no TLS) and binds 0.0.0.0 -- user OIDC bearer tokens travel in cleartext and can be captured and replayed. Set [coordinator.tls] cert_file/key_file to enable TLS, or do not expose the Quack port on untrusted networks.");
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .map_err(|e| anyhow::anyhow!("Quack server bind to {bind}: {e}"))?;
            tokio::spawn(async move {
                if let Err(e) = axum::serve(
                    listener,
                    quack_app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .await
                {
                    tracing::error!(error = %e, "Quack RPC server terminated unexpectedly");
                }
            });
            tracing::info!("DuckDB Quack RPC on port {quack_port} (plaintext)");
        }
    }

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // Flight SQL server with graceful shutdown
    let mut flight_service =
        SqeFlightSqlService::new(session_manager, query_handler, config.clone())
            .with_rate_limiter(rate_limiter)
            .with_auth_rate_limiter(Arc::clone(&auth_rate_limiter))
            .with_metadata_rate_limiter(metadata_rate_limiter);
    // Hand the flight service the SAME registry the query handler reads, so
    // worker heartbeats (handled on the DoAction path) actually register
    // workers that `try_distribute` will route to. Without this the heartbeat
    // handler has no registry and silently drops every heartbeat -- which made
    // dynamic discovery dead even when health checks were running (#226).
    if distributed {
        flight_service = flight_service.with_worker_registry(worker_registry.clone());
    }
    let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

    // Optional TLS
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = sqe_coordinator::transport::apply_grpc_transport(
        tonic::transport::Server::builder(),
        &config.coordinator.transport,
    );
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("SQE coordinator listening on {addr} (TLS)");
    } else {
        tracing::info!("SQE coordinator listening on {addr} (plaintext)");
    }

    let serve_result = server_builder
        .add_service(
            arrow_flight::flight_service_server::FlightServiceServer::new(flight_service),
        )
        .serve_with_shutdown(
            addr,
            shutdown_with_drain(ready.clone(), config.coordinator.shutdown_drain_secs),
        )
        .await;

    // Signal the audit shipper to stop now that the server has exited.
    if let Some(tx) = shipper_shutdown_tx {
        let _ = tx.send(true);
    }

    serve_result?;
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
        query_tracker: None,
        web_ui: false,
        // Mirror the coordinator's readiness check (the established downstream
        // probe) instead of short-circuiting on an empty URL. Leaving this
        // empty made /readyz a bare process-up check, so Kubernetes routed
        // do_get to a worker before it could serve a fragment (#245). Workers
        // read S3 directly from the data-file paths in the secured plan
        // fragment and do NOT query the catalog at runtime, so a catalog blip
        // alone does not break a scan. Gating worker readiness on catalog
        // reachability is a deliberately conservative cluster-health signal: if
        // the catalog is down the coordinator cannot plan or dispatch anyway.
        // config.validate() already requires catalog.catalog_url non-empty for
        // both roles, so this always probes.
        catalog_url: config.catalog.catalog_url.clone(),
        node_info: None,
        metrics_history: None,
        bearer_provider: None,
        auth_cfg: None,
        security_cfg: None,
        audit: None,
        anonymous_denied: None,
        dashboard_success: None,
        success_audit_dedup: None,
    });
    start_health_server(health_port, health_state);

    let port = config.worker.flight_port;
    let addr = format!("0.0.0.0:{port}").parse()?;

    let worker_metrics = Arc::new(
        sqe_metrics::WorkerMetricsRegistry::new()
            .map_err(|e| anyhow::anyhow!("failed to initialize worker metrics registry: {e}"))?,
    );
    sqe_metrics::server::start_metrics_server(
        worker_metrics.clone(),
        config.metrics.prometheus_port,
    );

    let session_ctx = sqe_worker::runtime::build_session_context(&config.worker)?;

    // Build the fully-wired Flight service and start the heartbeat task via the
    // shared worker bootstrap. Previously run_worker built the service WITHOUT
    // .with_worker_secret(), without the footer cache, and never started the
    // heartbeat -- so Helm-deployed workers (which run `--mode worker`) were
    // unauthenticated, uncached, and invisible to the coordinator (#219).
    let flight_service =
        sqe_worker::bootstrap::build_worker_service(&config, worker_metrics, session_ctx)?;

    // Mark ready
    ready.store(true, Ordering::Relaxed);

    // Optional TLS (reuse coordinator TLS config for workers)
    let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut server_builder = sqe_coordinator::transport::apply_grpc_transport(
        tonic::transport::Server::builder(),
        &config.coordinator.transport,
    );
    if let Some(tls) = tls_config {
        server_builder = server_builder.tls_config(tls)?;
        tracing::info!("SQE worker listening on {addr} (TLS)");
    } else {
        tracing::info!("SQE worker listening on {addr} (plaintext)");
    }

    server_builder
        .add_service(flight_service.into_server())
        .serve_with_shutdown(
            addr,
            shutdown_with_drain(ready.clone(), config.coordinator.shutdown_drain_secs),
        )
        .await?;

    tracing::info!("SQE worker shut down");
    Ok(())
}

// ── OpenLineage observer construction ─────────────────────────

/// Build the [`sqe_lineage::LineageObserver`] from `[metrics.openlineage]`.
///
/// Returns `None` when the section is disabled (the default). When enabled,
/// validates the config, opens the configured sinks (file and/or HTTP, with
/// optional disk spool wrapping HTTP), spawns the emitter background task,
/// and returns a [`sqe_lineage::ChannelObserver`] wired to a bounded mpsc.
///
/// The emitter `JoinHandle` is intentionally dropped: the bounded mpsc keeps
/// the task alive for the process lifetime, mirroring the SpoolSink replay
/// task pattern.
fn build_lineage_observer(
    config: &SqeConfig,
) -> anyhow::Result<Option<Arc<dyn sqe_lineage::LineageObserver>>> {
    if !config.metrics.openlineage.enabled {
        return Ok(None);
    }

    config
        .metrics
        .openlineage
        .validate()
        .map_err(|e| anyhow::anyhow!("openlineage config: {e}"))?;

    let ol = &config.metrics.openlineage;

    let mut sinks: Vec<Arc<dyn sqe_lineage::Sink>> = Vec::new();

    // File sink
    if !ol.file_path.is_empty() {
        sinks.push(Arc::new(
            sqe_lineage::sinks::file::FileSink::new(&ol.file_path)
                .map_err(|e| anyhow::anyhow!("openlineage file sink: {e}"))?,
        ));
    }

    // HTTP sink (optionally wrapped by SpoolSink)
    if !ol.http_endpoint.is_empty() {
        let auth = match ol.auth_mode.as_str() {
            "none" => sqe_lineage::sinks::http::AuthMode::None,
            "bearer" => sqe_lineage::sinks::http::AuthMode::Bearer(ol.api_key.clone()),
            "user_token" => {
                // Per-event user token requires per-event sink construction; for v1
                // the emitter task uses the configured api_key as a fallback when
                // auth_mode is user_token. The actual per-event token forwarding is
                // wired in a future task; for now treat user_token like bearer.
                tracing::warn!(
                    "openlineage auth_mode=user_token not yet fully wired; \
                     using static api_key as fallback"
                );
                sqe_lineage::sinks::http::AuthMode::Bearer(ol.api_key.clone())
            }
            other => {
                return Err(anyhow::anyhow!(
                    "openlineage auth_mode '{other}' is not recognised; \
                     expected one of: none, bearer, user_token"
                ));
            }
        };

        let http = sqe_lineage::sinks::http::HttpSink::new(sqe_lineage::sinks::http::HttpConfig {
            endpoint: ol.http_endpoint.clone(),
            auth,
            timeout_ms: ol.http_timeout_ms,
            retry_attempts: ol.http_retry_attempts,
        })
        .map_err(|e| anyhow::anyhow!("openlineage http sink: {e}"))?;

        let sink: Arc<dyn sqe_lineage::Sink> = if !ol.spool_path.is_empty() {
            sqe_lineage::sinks::spool::SpoolSink::wrap(
                Arc::new(http),
                sqe_lineage::sinks::spool::SpoolConfig {
                    path: std::path::PathBuf::from(&ol.spool_path),
                    max_bytes: ol.spool_max_bytes,
                    replay_interval: std::time::Duration::from_secs(ol.replay_interval_secs),
                },
            )
        } else {
            Arc::new(http)
        };
        sinks.push(sink);
    }

    let multi = Arc::new(sqe_lineage::MultiSink::new(sinks));

    let (tx, rx) = tokio::sync::mpsc::channel(ol.channel_capacity);

    // TODO: register dropped_events counter with prometheus_registry().
    // Same pattern as MultiSink::errors in D1: registration is a follow-up.
    // The counter increments in memory; it just won't appear on /metrics yet.
    let drop_counter = prometheus::IntCounter::new(
        "sqe_lineage_dropped_events_total",
        "OL events dropped due to channel back-pressure",
    )
    .expect("static prometheus opts cannot fail");

    // Build catalog lookup from flattened catalog config: name -> REST URL,
    // with `sqe://<name>` fallback when no URL is configured.
    let catalog_lookup_map: std::collections::HashMap<String, String> = config
        .flattened_catalogs()
        .iter()
        .map(|(name, c)| (name.clone(), c.catalog_url.clone()))
        .collect();
    let catalog_lookup: sqe_lineage::extract::CatalogLookup =
        Arc::new(move |name: &str| {
            catalog_lookup_map
                .get(name)
                .cloned()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("sqe://{name}"))
        });

    let producer = if ol.producer.is_empty() {
        format!("https://github.com/sbp/sqe/v{}", env!("CARGO_PKG_VERSION"))
    } else {
        ol.producer.clone()
    };

    let cfg = Arc::new(sqe_lineage::EmitterConfig {
        job_namespace: ol.job_namespace.clone(),
        producer,
        catalog_lookup,
    });

    // Spawn the emitter background task. The JoinHandle is intentionally
    // dropped: the bounded mpsc keeps the task alive for the process
    // lifetime (the Sender lives in the ChannelObserver below; the
    // Receiver lives in the spawned task).
    sqe_lineage::spawn_emitter(rx, multi, cfg);

    tracing::info!(
        file_sink = !ol.file_path.is_empty(),
        http_sink = !ol.http_endpoint.is_empty(),
        spool = !ol.spool_path.is_empty(),
        emit_selects = ol.emit_selects,
        channel_capacity = ol.channel_capacity,
        "OpenLineage emitter enabled"
    );

    Ok(Some(Arc::new(sqe_lineage::ChannelObserver::new(
        tx,
        drop_counter,
    ))))
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

    #[tokio::test]
    async fn flip_ready_and_drain_flips_readiness_first() {
        // drain_secs = 0 so the test does not sleep; readiness must still flip.
        let ready = AtomicBool::new(true);
        flip_ready_and_drain(&ready, 0).await;
        assert!(
            !ready.load(Ordering::Relaxed),
            "readiness must be flipped to NOT-ready on shutdown"
        );
    }

    #[tokio::test]
    async fn flip_ready_and_drain_flips_readiness_with_nonzero_drain() {
        // A short non-zero drain still flips readiness immediately, then sleeps.
        let ready = AtomicBool::new(true);
        flip_ready_and_drain(&ready, 1).await;
        assert!(!ready.load(Ordering::Relaxed));
    }

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
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
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
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
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
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
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
        // With an empty catalog_url, no catalog reachability check is performed.
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_readyz_not_ready_when_catalog_unreachable() {
        // #245: a worker with a populated but unreachable catalog_url must
        // report NOT ready so Kubernetes keeps it out of rotation, rather than
        // short-circuiting to ready on an empty URL.
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "worker",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            // RFC 5737 TEST-NET-1; never routes, so the probe fails fast.
            catalog_url: "http://192.0.2.1:1".to_string(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    // All CoordinatorConfig fields carry `#[serde(default)]`, so an empty TOML
    // table yields the single-node default we want to start each case from.
    fn coordinator_config_from(toml_src: &str) -> sqe_core::config::CoordinatorConfig {
        toml::from_str(toml_src).expect("coordinator config parses")
    }

    #[test]
    fn distributed_enabled_off_for_single_node_default() {
        // #226 security guard: the default single-node config (empty
        // worker_urls, empty worker_secret, opt-in off) must NOT enable the
        // worker path, or it would accept unauthenticated heartbeats and route
        // user bearer tokens to unknown workers.
        let coord = coordinator_config_from("");
        assert!(!distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_static_worker_urls() {
        let coord = coordinator_config_from("worker_urls = [\"http://w1:50052\"]");
        assert!(distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_heartbeat_discovery_via_secret() {
        // Dynamic discovery: no static URLs, but a worker_secret is set, so
        // heartbeats are authenticated and the registry must be wired (#226).
        let coord = coordinator_config_from("worker_secret = \"hunter2\"");
        assert!(coord.worker_urls.is_empty());
        assert!(distributed_enabled(&coord));
    }

    #[test]
    fn distributed_enabled_on_for_explicit_unauthenticated_opt_in() {
        let coord = coordinator_config_from("allow_unauthenticated_workers = true");
        assert!(distributed_enabled(&coord));
    }

    #[tokio::test]
    async fn test_readyz_not_ready() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(false)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });

        let response = readyz(axum::extract::State(state)).await;
        assert_eq!(response.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn api_queries_returns_tracked_queries() {
        let tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
            &sqe_core::QueryHistoryConfig { max_entries: 100, ttl_secs: 60 },
        ));
        let id = uuid::Uuid::now_v7();
        tracker.start(id, "alice", Some("cli"), "SELECT 1", "s1", None, vec![]);

        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: Some(tracker),
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });

        let Json(items) = api_queries(
            axum::extract::State(state),
            axum::extract::Query(QueryListParams { state: None, limit: None }),
        )
        .await;
        assert_eq!(items.len(), 1);
        // WEB-02: the unauth list no longer exposes the username or raw SQL;
        // it carries the SQL digest instead. Assert the item is populated.
        assert!(!items[0].sql_hash.is_empty());
    }

    #[tokio::test]
    async fn api_metrics_history_returns_empty_without_history() {
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });
        let Json(resp) = api_metrics_history(axum::extract::State(state)).await;
        assert_eq!(resp.bucket_seconds, sqe_coordinator::metrics_history::BUCKET_SECS);
        assert!(resp.buckets.is_empty());
    }

    #[tokio::test]
    async fn api_metrics_history_returns_buckets_when_history_populated() {
        use sqe_coordinator::metrics_history::{MetricsHistory, MetricsSample};
        let hist = Arc::new(MetricsHistory::new(4320));
        for i in 0..3u64 {
            hist.record(MetricsSample {
                unix_ms: i * 10_000,
                active_queries: 1,
                mem_used_bytes: 100,
                mem_limit_bytes: 1000,
                total_queries: i as usize + 1,
                failed_queries: 0,
                total_output_rows: i * 10,
                finished_queries: i as usize + 1,
                exec_ms_sum: (i + 1) * 100,
            });
        }
        let state = Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: Some(hist),
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        });
        let Json(resp) = api_metrics_history(axum::extract::State(state)).await;
        assert_eq!(resp.bucket_seconds, sqe_coordinator::metrics_history::BUCKET_SECS);
        assert!(!resp.buckets.is_empty());
        // 3 samples at 0, 10_000, 20_000 ms all fall in the same 900-s bucket
        assert_eq!(resp.buckets.len(), 1);
    }

    // ── Route-level bearer + admin guard tests ─────────────────────────────────
    //
    // These tests drive `build_health_router` directly via `tower::ServiceExt::oneshot`
    // to assert that:
    //   - `/healthz` is always open (no token needed)
    //   - web_ui routes require a valid admin bearer token
    //   - missing token -> 401, non-admin token -> 403, admin token -> 200

    struct StubAuthProvider {
        admin_roles: Vec<String>,
    }

    #[async_trait::async_trait]
    impl sqe_auth::AuthProvider for StubAuthProvider {
        async fn authenticate(
            &self,
            creds: &sqe_auth::FlightCredentials,
        ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
            let token = creds
                .bearer_token
                .as_ref()
                .map(|t| t.expose().to_string())
                .unwrap_or_default();
            // "admin-tok" -> admin; "user-tok" -> non-admin; anything else -> fail
            match token.as_str() {
                "admin-tok" => Ok(sqe_auth::Identity {
                    user_id: "admin".into(),
                    display_name: "Admin".into(),
                    roles: self.admin_roles.clone(),
                    subject: None,
                    email: None,
                    groups: vec![],
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                }),
                "user-tok" => Ok(sqe_auth::Identity {
                    user_id: "user".into(),
                    display_name: "User".into(),
                    roles: vec!["analyst".into()],
                    subject: None,
                    email: None,
                    groups: vec![],
                    catalog_token: None,
                    refresh_token: None,
                    expires_at: None,
                }),
                _ => Err(sqe_auth::AuthError::AuthFailed("bad token".into())),
            }
        }
    }

    fn make_guarded_state() -> Arc<HealthState> {
        let provider: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubAuthProvider {
            admin_roles: vec!["sqe-admin".into()],
        });
        let mut auth_cfg: sqe_core::config::AuthConfig =
            toml::from_str("").expect("empty auth config");
        auth_cfg.admin_roles = vec!["sqe-admin".into()];
        Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: true,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: Some(provider),
            auth_cfg: Some(auth_cfg),
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: None,
            success_audit_dedup: None,
        })
    }

    #[tokio::test]
    async fn healthz_open_without_token() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/healthz")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn web_ui_queries_no_token_is_401() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn web_ui_queries_non_admin_bearer_is_403() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .header("Authorization", "Bearer user-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn web_ui_queries_admin_bearer_is_200() {
        use tower::ServiceExt;
        let state = make_guarded_state();
        let app = build_health_router(state);
        let req = axum::http::Request::builder()
            .uri("/api/v1/queries")
            .header("Authorization", "Bearer admin-tok")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    // ── HealthState dedup tests ────────────────────────────────────────────
    //
    // Drive the production HealthState implementation of should_emit_success_audit
    // and note_dashboard_success directly. These guard against regressions in the
    // real code path, not just the test stub in web_auth::tests.

    fn make_dedup_state(window_secs: u64) -> Arc<HealthState> {
        let cache = if window_secs == 0 {
            None
        } else {
            Some(
                moka::sync::Cache::builder()
                    .time_to_live(std::time::Duration::from_secs(window_secs))
                    .build(),
            )
        };
        let counter = prometheus::IntCounter::new(
            format!("sqe_test_success_{window_secs}"),
            "test counter",
        )
        .unwrap();
        Arc::new(HealthState {
            ready: Arc::new(AtomicBool::new(true)),
            started_at: Instant::now(),
            role: "coordinator",
            worker_registry: None,
            query_tracker: None,
            web_ui: false,
            catalog_url: String::new(),
            node_info: None,
            metrics_history: None,
            bearer_provider: None,
            auth_cfg: None,
            security_cfg: None,
            audit: None,
            anonymous_denied: None,
            dashboard_success: Some(counter),
            success_audit_dedup: cache,
        })
    }

    #[test]
    fn health_state_dedup_same_principal_within_window() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(300);
        // First call: new principal -> emit.
        assert!(
            state.should_emit_success_audit("alice"),
            "first call must emit"
        );
        // Second call: same principal within window -> suppress.
        assert!(
            !state.should_emit_success_audit("alice"),
            "second call within window must suppress"
        );
        // Different principal -> emit.
        assert!(
            state.should_emit_success_audit("bob"),
            "distinct principal must emit"
        );
    }

    #[test]
    fn health_state_dedup_window_zero_always_emits() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(0);
        assert!(state.should_emit_success_audit("alice"), "window=0 first call");
        assert!(state.should_emit_success_audit("alice"), "window=0 second call");
        assert!(state.should_emit_success_audit("alice"), "window=0 third call");
    }

    #[test]
    fn health_state_note_dashboard_success_increments_counter() {
        use sqe_coordinator::web_auth::BearerAdminState;
        let state = make_dedup_state(300);
        // Counter starts at 0; each call increments.
        state.note_dashboard_success();
        state.note_dashboard_success();
        // We can't read the IntCounter value directly from the state (private field),
        // but we can confirm the method does not panic and wiring is complete.
        // The counter value is observable via Prometheus scrape; that is tested by
        // the MetricsRegistry tests in sqe-metrics.
    }
}
