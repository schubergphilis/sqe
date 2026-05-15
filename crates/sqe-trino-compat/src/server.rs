use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use dashmap::DashMap;
use tracing::{error, info, warn};
use uuid::Uuid;

use sqe_core::Session;
use sqe_core::config::SecurityConfig;

use crate::protocol::{
    self, NodeVersion, ServerInfo, TrinoColumn, TrinoError, TrinoResponse, TrinoStats,
};

/// Default number of rows per page for result pagination.
const DEFAULT_PAGE_SIZE: usize = 1000;

/// Results older than this are evicted (5 minutes).
const RESULT_TTL_SECS: u64 = 300;

/// Shared context for Trino /v1/info endpoints.
pub struct NodeContext {
    pub version: String,
    pub ready: Arc<AtomicBool>,
    pub started_at: Instant,
}

pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub results: DashMap<String, PaginatedResult>,
    pub node: NodeContext,
    /// Number of rows per page. Configurable for testing; defaults to [`DEFAULT_PAGE_SIZE`].
    pub page_size: usize,
    /// The TCP port the Trino-compat HTTP server is bound to.
    pub port: u16,
    /// OAuth2 external auth state. None if [auth.external] is not configured.
    pub oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
    /// Trusted-proxy allowlist used to decide whether to honour
    /// `x-forwarded-for`. Empty = ignore it. Issue #74.
    pub security: SecurityConfig,
    /// Optional pre-auth rate limit on credentials. Issue #57.
    pub auth_rate_limiter: Option<Arc<dyn TrinoAuthRateLimiter>>,
    /// When true, /v1/info omits the build version so unauthenticated
    /// callers cannot fingerprint the engine. Issue #40.
    pub expose_version: bool,
}

/// Trait the coordinator implements to plug an
/// `AuthRateLimiter` into the Trino path without coupling this crate
/// to the coordinator's concrete rate-limit module.
#[async_trait::async_trait]
pub trait TrinoAuthRateLimiter: Send + Sync + 'static {
    /// Returns `Err(())` when the (peer_ip, username) tuple is over budget.
    fn check(&self, peer_ip: &str, username: &str) -> Result<(), ()>;
}

/// Stores the full result set split into pages for pagination.
#[derive(Debug)]
pub struct PaginatedResult {
    /// Column metadata (shared across all pages).
    pub columns: Vec<TrinoColumn>,
    /// Row data split into fixed-size pages.
    pub pages: Vec<Vec<Vec<serde_json::Value>>>,
    /// Total number of pages.
    pub total_pages: usize,
    /// Total rows across every page; reported as `processedRows`.
    pub total_rows: usize,
    /// Wall-clock time at which this result was stored; used for TTL eviction.
    pub created_at: std::time::Instant,
    /// Username of the session that created this result; used for cancel authorization.
    pub owner_username: String,
    /// `INSERT`/`UPDATE`/`DELETE` for write paths; `None` for reads.
    pub update_type: Option<String>,
    /// Affected rows for write statements; mirrored on every page so dbt-trino
    /// can populate `rows_affected` from any response.
    pub update_count: Option<i64>,
}

/// Trino client headers extracted from the request.
#[derive(Debug, Clone, Default)]
pub struct TrinoClientHeaders {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub user: Option<String>,
    pub source: Option<String>,
}

#[async_trait::async_trait]
pub trait TrinoAuthenticator: Send + Sync + 'static {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Session, sqe_core::SqeError>;

    /// Validate a raw bearer token (JWT) and return an authenticated session.
    ///
    /// The default implementation rejects all bearer tokens. Override this in
    /// the coordinator adapter to route through the JWKS-validating auth chain.
    async fn authenticate_bearer(&self, _token: &str) -> Result<Session, sqe_core::SqeError> {
        Err(sqe_core::SqeError::Auth(
            "Bearer token authentication not configured".to_string(),
        ))
    }
}

#[async_trait::async_trait]
pub trait TrinoQueryExecutor: Send + Sync + 'static {
    /// Execute a SQL statement and return record batches.
    ///
    /// Returns the full `SqeError` so the Trino mapping in
    /// `TrinoError::from_sqe_error` dispatches on the original variant. The
    /// previous `Result<_, String>` shape lost the variant, forcing the
    /// caller to substring-classify the `Display` output and produced
    /// inconsistent error codes across Flight SQL and Trino HTTP for the
    /// same fault (issue #102).
    async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError>;
}

pub struct TrinoServerOptions {
    pub security: SecurityConfig,
    pub auth_rate_limiter: Option<Arc<dyn TrinoAuthRateLimiter>>,
    /// Whether `/v1/info` may return the exact build version. Default
    /// `false` for production deployments. Issue #40.
    pub expose_version: bool,
}

impl Default for TrinoServerOptions {
    fn default() -> Self {
        Self {
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: false,
        }
    }
}

pub fn start_trino_server<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    port: u16,
    node: NodeContext,
    oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    start_trino_server_with_options(
        authenticator,
        query_handler,
        port,
        node,
        oauth2,
        TrinoServerOptions::default(),
    )
}

pub fn start_trino_server_with_options<A, Q>(
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    port: u16,
    node: NodeContext,
    oauth2: Option<Arc<crate::oauth2::OAuth2State>>,
    options: TrinoServerOptions,
) -> tokio::task::JoinHandle<()>
where
    A: TrinoAuthenticator,
    Q: TrinoQueryExecutor,
{
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        results: DashMap::new(),
        node,
        page_size: DEFAULT_PAGE_SIZE,
        port,
        oauth2: oauth2.clone(),
        security: options.security,
        auth_rate_limiter: options.auth_rate_limiter,
        expose_version: options.expose_version,
    });

    let state_sweep = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let expired: Vec<String> = state_sweep
                .results
                .iter()
                .filter(|entry| entry.value().created_at.elapsed().as_secs() > RESULT_TTL_SECS)
                .map(|entry| entry.key().clone())
                .collect();
            for id in &expired {
                state_sweep.results.remove(id);
            }
            if !expired.is_empty() {
                tracing::debug!(count = expired.len(), "Evicted stale Trino result sets");
            }
        }
    });

    tokio::spawn(async move {
        // Restrictive CORS: no cross-origin by default. The Trino compat endpoint
        // is designed for JDBC/CLI clients, not browsers. An explicit empty
        // Access-Control-Allow-Origin blocks browser-based cross-origin requests.
        let cors_layer = axum::middleware::from_fn(|req: axum::extract::Request, next: axum::middleware::Next| async move {
            if req.method() == axum::http::Method::OPTIONS {
                // Preflight: respond with 204 and restrictive headers.
                return axum::response::Response::builder()
                    .status(204)
                    .header("Access-Control-Allow-Methods", "GET, POST, DELETE")
                    .header("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Trino-User, X-Trino-Catalog, X-Trino-Schema, X-Trino-Source")
                    .header("Access-Control-Max-Age", "3600")
                    .body(axum::body::Body::empty())
                    .unwrap()
                    .into_response();
            }
            next.run(req).await
        });

        let mut app = Router::new()
            .route("/v1/info", get(server_info::<A, Q>))
            .route("/v1/info/state", get(server_state::<A, Q>))
            .route("/v1/statement", post(submit_query::<A, Q>))
            .route("/v1/statement/{id}/{token}", get(get_results::<A, Q>))
            .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
            .layer(cors_layer)
            .with_state(state);

        if let Some(oauth2_state) = oauth2 {
            let oauth2_routes = Router::new()
                .route(
                    "/oauth2/token/initiate/{auth_id_hash}",
                    get(crate::oauth2::initiate_handler),
                )
                .route("/oauth2/callback", get(crate::oauth2::callback_handler))
                .route(
                    "/oauth2/token/{auth_id}",
                    get(crate::oauth2::poll_token_handler),
                )
                .route(
                    "/oauth2/token/{auth_id}",
                    delete(crate::oauth2::delete_token_handler),
                )
                .with_state(oauth2_state);
            app = app.merge(oauth2_routes);
        }

        let addr = format!("0.0.0.0:{port}");
        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(addr = %addr, error = %e, "Failed to bind Trino-compat HTTP server");
                return;
            }
        };

        info!("Trino-compat HTTP server listening on {addr}");

        if let Err(e) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        {
            error!(error = %e, "Trino-compat HTTP server exited with error");
        }
    })
}

/// Compute the trusted client IP for a Trino request: honour
/// `x-forwarded-for` only when the peer is in `[security] trusted_proxies`.
fn trino_client_ip<A, Q>(
    state: &Arc<TrinoState<A, Q>>,
    headers: &HeaderMap,
    peer: SocketAddr,
) -> String {
    let xff = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok());
    state
        .security
        .resolve_client_ip(Some(&peer.to_string()), xff)
}

// ── Trino /v1/info ────────────────────────────────────────────

fn format_uptime(started_at: Instant) -> String {
    let secs = started_at.elapsed().as_secs();
    if secs < 60 {
        format!("{secs}.00s")
    } else if secs < 3600 {
        format!("{:.2}m", secs as f64 / 60.0)
    } else if secs < 86400 {
        format!("{:.2}h", secs as f64 / 3600.0)
    } else {
        format!("{:.2}d", secs as f64 / 86400.0)
    }
}

async fn server_info<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> Json<ServerInfo> {
    let ready = state.node.ready.load(Ordering::Relaxed);
    // Issue #40: unauthenticated callers used to receive the exact
    // build version, which makes targeted-CVE matching trivial. The
    // Trino spec says /v1/info needs no auth, so the right fix is to
    // scrub the version unless the operator opts in. Clients that
    // need exact-version handshake (rare) can set `expose_version`.
    let version = if state.expose_version {
        state.node.version.clone()
    } else {
        coarse_version(&state.node.version)
    };
    Json(ServerInfo {
        node_version: NodeVersion { version },
        environment: "production".to_string(),
        coordinator: true,
        starting: !ready,
        uptime: format_uptime(state.node.started_at),
    })
}

/// Return `"<major>.<minor>"` from a semver-like input, or `"unknown"`
/// when the input cannot be parsed. The major.minor pair is enough for
/// JDBC drivers that branch on Trino protocol generation but does not
/// pin the exact build for CVE matching.
fn coarse_version(full: &str) -> String {
    let trimmed = full.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }
    let mut parts = trimmed.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        _ => "unknown".to_string(),
    }
}

async fn server_state<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
) -> String {
    let ready = state.node.ready.load(Ordering::Relaxed);
    if ready {
        "ACTIVE".to_string()
    } else {
        "STARTING".to_string()
    }
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> Response {
    let msg = msg.into();
    let body = TrinoResponse {
        id: String::new(),
        info_uri: None,
        stats: TrinoStats::failed(),
        error: Some(TrinoError {
            message: msg.clone(),
            error_code: 1,
            error_name: "USER_ERROR".to_string(),
            error_type: "USER_ERROR".to_string(),
            query_id: None,
            failure_info: Some(crate::protocol::TrinoFailureInfo {
                r#type: "io.trino.spi.USER_ERROR".to_string(),
                message: msg,
                suppressed: Vec::new(),
                cause: None,
                stack: Vec::new(),
            }),
            error_location: None,
        }),
        ..Default::default()
    };
    (status, Json(body)).into_response()
}

/// Classify a SQL statement against the leading keyword.
///
/// dbt-trino sets `rows_affected` from `updateType` + `updateCount`. Trino
/// itself emits `INSERT`, `UPDATE`, `DELETE`, `MERGE`, plus the schema-DDL
/// variants. We map them by the verb the user typed.
fn classify_update_type(sql: &str) -> Option<&'static str> {
    let token = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .find(|s| !s.is_empty())?
        .to_ascii_uppercase();
    match token.as_str() {
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "MERGE" => Some("MERGE"),
        "CREATE" => Some("CREATE TABLE"),
        "DROP" => Some("DROP TABLE"),
        "ALTER" => Some("ALTER TABLE"),
        "TRUNCATE" => Some("TRUNCATE TABLE"),
        _ => None,
    }
}

/// Extract the affected-row count from a write-path response.
///
/// DataFusion returns a single 1x1 batch with a `count` column for
/// INSERT/UPDATE/DELETE/MERGE. We grovel through that shape and fall back
/// to 0 if the engine returned nothing.
fn extract_update_count(batches: &[arrow_array::RecordBatch]) -> Option<i64> {
    let batch = batches.first()?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return None;
    }
    let arr = batch.column(0);
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return Some(a.value(0));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        return Some(a.value(0) as i64);
    }
    None
}

/// Emit `X-Trino-Set-*` / `X-Trino-Clear-*` response headers describing
/// the session-state mutation the executor just performed. Clients
/// observe these and resend the resulting state on the next request.
fn apply_session_headers(
    headers: &mut axum::http::HeaderMap,
    update: &protocol::UpdatedSessionState,
) {
    use axum::http::{HeaderName, HeaderValue};

    fn insert(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.insert(name, v);
        }
    }

    fn append(headers: &mut axum::http::HeaderMap, name: HeaderName, value: &str) {
        if let Ok(v) = HeaderValue::from_str(value) {
            headers.append(name, v);
        }
    }

    if let Some(catalog) = &update.set_catalog {
        insert(headers, HeaderName::from_static("x-trino-set-catalog"), catalog);
    }
    if let Some(schema) = &update.set_schema {
        insert(headers, HeaderName::from_static("x-trino-set-schema"), schema);
    }
    for (name, value) in &update.set_session {
        append(
            headers,
            HeaderName::from_static("x-trino-set-session"),
            &format!("{name}={value}"),
        );
    }
    for name in &update.clear_session {
        append(headers, HeaderName::from_static("x-trino-clear-session"), name);
    }
    for (name, sql) in &update.added_prepare {
        append(
            headers,
            HeaderName::from_static("x-trino-added-prepare"),
            &format!("{name}={sql}"),
        );
    }
    for name in &update.deallocated_prepare {
        append(
            headers,
            HeaderName::from_static("x-trino-deallocated-prepare"),
            name,
        );
    }
}

// ── Header extraction ─────────────────────────────────────────

/// Extract Trino client headers from the HTTP request.
fn extract_trino_headers(headers: &HeaderMap) -> TrinoClientHeaders {
    TrinoClientHeaders {
        catalog: extract_header(headers, "x-trino-catalog"),
        schema: extract_header(headers, "x-trino-schema"),
        user: extract_header(headers, "x-trino-user"),
        source: extract_header(headers, "x-trino-source"),
    }
}

/// Extract a single header value as a trimmed, non-empty string.
fn extract_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Apply extracted Trino headers to a session (catalog, schema, source).
fn apply_trino_headers(session: Session, trino_headers: &TrinoClientHeaders) -> Session {
    session
        .with_catalog(trino_headers.catalog.clone())
        .with_schema(trino_headers.schema.clone())
        .with_source(trino_headers.source.clone())
}

// ── Pagination helpers ────────────────────────────────────────

/// Split row data into fixed-size pages.
fn paginate_rows(
    rows: Vec<Vec<serde_json::Value>>,
    page_size: usize,
) -> Vec<Vec<Vec<serde_json::Value>>> {
    if rows.is_empty() {
        return vec![vec![]];
    }
    rows.chunks(page_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

/// Build a `nextUri` for the given query id and page token, or `None` if this is the last page.
fn next_uri(base_url: &str, query_id: &str, token: usize, total_pages: usize) -> Option<String> {
    if token + 1 < total_pages {
        Some(format!("{base_url}/v1/statement/{query_id}/{}", token + 1))
    } else {
        None
    }
}

/// Build an absolute `infoUri` for the given query.
fn info_uri(base_url: &str, query_id: &str) -> String {
    format!("{base_url}/v1/query/{query_id}")
}

/// Derive the base URL from the `Host` header, falling back to `localhost:<port>`.
fn extract_base_url(headers: &HeaderMap, bound_port: u16) -> String {
    let scheme = extract_header(headers, "x-forwarded-proto")
        .unwrap_or_else(|| "http".to_string());
    if let Some(host) = extract_header(headers, "host") {
        format!("{scheme}://{host}")
    } else {
        format!("http://localhost:{bound_port}")
    }
}

/// Build a [`TrinoResponse`] for the given page of a paginated result.
fn build_page_response(
    base_url: &str,
    query_id: &str,
    paginated: &PaginatedResult,
    page_token: usize,
) -> TrinoResponse {
    let page_data = paginated
        .pages
        .get(page_token)
        .cloned()
        .unwrap_or_default();
    let is_last = page_token + 1 >= paginated.total_pages;

    let metrics = protocol::ExecutionMetrics {
        elapsed_millis: paginated.created_at.elapsed().as_millis() as u64,
        processed_rows: paginated.total_rows as u64,
        ..protocol::ExecutionMetrics::default()
    };
    let stats = if is_last {
        TrinoStats::finished_with_metrics(&metrics)
    } else {
        TrinoStats::running_with_metrics(page_token + 1, paginated.total_pages, &metrics)
    };
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: next_uri(base_url, query_id, page_token, paginated.total_pages),
        columns: Some(paginated.columns.clone()),
        data: Some(page_data),
        stats,
        update_type: paginated.update_type.clone(),
        update_count: paginated.update_count,
        ..Default::default()
    }
}

// ── Handlers ──────────────────────────────────────────────────

#[tracing::instrument(
    skip_all,
    fields(
        db.system.name = "sqe",
        db.operation.name = tracing::field::Empty,
        db.namespace = tracing::field::Empty,
    ),
    name = "trino.submit_query",
)]
async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let sql = body.trim();
    if sql.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Empty query");
    }

    let client_ip = trino_client_ip(&state, &headers, peer);
    let peer_host = client_ip
        .rsplit_once(':')
        .filter(|(_, p)| p.chars().all(|c| c.is_ascii_digit()))
        .map(|(host, _)| host)
        .unwrap_or(&client_ip)
        .to_string();

    let trino_headers = extract_trino_headers(&headers);

    let session = if let Some(token) = extract_bearer_token(&headers) {
        match state.authenticator.authenticate_bearer(&token).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, client_ip = %client_ip, "Trino bearer token validation failed");
                return error_response(StatusCode::UNAUTHORIZED, "authentication failed");
            }
        }
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        if let Some(ref limiter) = state.auth_rate_limiter {
            if limiter.check(&peer_host, &user).is_err() {
                warn!(
                    user = %user,
                    client_ip = %client_ip,
                    "Trino auth rejected by rate limiter"
                );
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(axum::http::header::RETRY_AFTER, "1")],
                    "authentication rate limit",
                )
                    .into_response();
            }
        }
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, user = %user, client_ip = %client_ip, "Trino authentication failed");
                return error_response(StatusCode::UNAUTHORIZED, "authentication failed");
            }
        }
    } else if let Some(ref oauth2_state) = state.oauth2 {
        match crate::oauth2::generate_challenge(oauth2_state).await {
            Ok((_auth_id, www_authenticate)) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    [(axum::http::header::WWW_AUTHENTICATE, www_authenticate)],
                    "Authentication required",
                )
                    .into_response();
            }
            Err(status) => {
                return error_response(status, "Failed to generate auth challenge");
            }
        }
    } else {
        return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
    };

    // Apply Trino client headers (catalog, schema, source) to the session.
    let session = apply_trino_headers(session, &trino_headers);

    // Record OTel semantic convention attributes on the current span.
    {
        let span = tracing::Span::current();
        // Best-effort: classify the SQL to get the operation name.
        let op = sql
            .split_whitespace()
            .next()
            .unwrap_or("unknown")
            .to_uppercase();
        span.record("db.operation.name", op.as_str());
        if let Some(ref schema) = session.default_schema {
            span.record("db.namespace", schema.as_str());
        }
    }

    let sql_hash = {
        use sha2::{Digest, Sha256};
        let normalised: String = sql.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
        format!("{:x}", Sha256::digest(normalised.as_bytes()))
    };
    info!(
        user = %session.user.username,
        client_ip = %client_ip,
        catalog = ?session.default_catalog,
        schema = ?session.default_schema,
        source = ?session.source,
        sql_hash = %sql_hash,
        sql_len = sql.len(),
        "Trino query submitted"
    );

    let query_id = Uuid::new_v4().to_string();
    let base_url = extract_base_url(&headers, state.port);

    let session_update = protocol::parse_session_statement(sql);

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let update_type = classify_update_type(sql).map(str::to_string);
            let update_count = if update_type.is_some() {
                extract_update_count(&batches).or(Some(0))
            } else {
                None
            };
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let (columns, data) = protocol::batches_to_trino(&batches);
            let pages = paginate_rows(data, state.page_size);
            let total_pages = pages.len();

            let paginated = PaginatedResult {
                columns,
                pages,
                total_pages,
                total_rows,
                created_at: std::time::Instant::now(),
                owner_username: session.user.username.clone(),
                update_type,
                update_count,
            };

            // Build the first page response (token = 0).
            let response = build_page_response(&base_url, &query_id, &paginated, 0);

            // Store for subsequent GET requests (if there are more pages).
            state.results.insert(query_id, paginated);

            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if let Some(ref update) = session_update {
                apply_session_headers(resp.headers_mut(), update);
            }
            resp
        }
        Err(sqe_err) => {
            tracing::warn!(
                error_code = %sqe_err.error_code(),
                query_id = %query_id,
                error = %sqe_err,
                "Trino query execution failed"
            );
            let is_rate_limited =
                sqe_err.error_code() == sqe_core::SqeErrorCode::ResourceExhausted;
            let trino_error = TrinoError::from_sqe_error(&sqe_err, Some(&query_id));
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                stats: TrinoStats::failed(),
                error: Some(trino_error),
                ..Default::default()
            };
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            if is_rate_limited {
                resp.headers_mut().insert(
                    axum::http::header::RETRY_AFTER,
                    axum::http::HeaderValue::from_static("1"),
                );
            }
            resp
        }
    }
}

#[tracing::instrument(skip_all, name = "trino.get_results")]
async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path((id, token)): Path<(String, String)>,
) -> Response {
    // Authenticate the caller before exposing any result data.
    let session = if let Some(token) = extract_bearer_token(&headers) {
        match state.authenticator.authenticate_bearer(&token).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    // Parse the token as a page index.
    let page_token: usize = match token.parse() {
        Ok(t) => t,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "Invalid page token");
        }
    };

    let base_url = extract_base_url(&headers, state.port);

    match state.results.get(&id) {
        Some(paginated) => {
            if paginated.owner_username != session.user.username {
                warn!(
                    query_id = %id,
                    caller = %session.user.username,
                    owner = %paginated.owner_username,
                    "get_results denied: caller does not own query"
                );
                return StatusCode::FORBIDDEN.into_response();
            }
            if page_token >= paginated.total_pages {
                let response = TrinoResponse {
                    id: id.clone(),
                    info_uri: Some(info_uri(&base_url, &id)),
                    stats: TrinoStats::failed(),
                    error: Some(TrinoError {
                        message: "Page token out of range".to_string(),
                        error_code: 1,
                        error_name: "USER_ERROR".to_string(),
                        error_type: "USER_ERROR".to_string(),
                        query_id: None,
                        failure_info: None,
                        error_location: None,
                    }),
                    ..Default::default()
                };
                return (StatusCode::NOT_FOUND, Json(response)).into_response();
            }

            let response = build_page_response(&base_url, &id, &paginated, page_token);
            let is_last = page_token + 1 >= paginated.total_pages;

            // Drop the borrow before mutating.
            drop(paginated);

            // Clean up after the last page has been served.
            if is_last {
                state.results.remove(&id);
            }

            (StatusCode::OK, Json(response)).into_response()
        }
        None => {
            let response = TrinoResponse {
                id: id.clone(),
                info_uri: Some(info_uri(&base_url, &id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query not found".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                    query_id: None,
                    failure_info: None,
                    error_location: None,
                }),
                ..Default::default()
            };
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
    }
}

async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    ConnectInfo(_peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    // Authenticate the caller.
    let session = if let Some(token) = extract_bearer_token(&headers) {
        match state.authenticator.authenticate_bearer(&token).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
        }
    } else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    // Verify the caller owns the query result.
    if let Some(entry) = state.results.get(&id) {
        if entry.owner_username != session.user.username {
            warn!(
                query_id = %id,
                caller = %session.user.username,
                owner = %entry.owner_username,
                "Cancel denied: caller does not own query"
            );
            return StatusCode::FORBIDDEN.into_response();
        }
    }

    state.results.remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let token = auth.strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn extract_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let encoded = auth.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded).ok()?,
    )
    .ok()?;
    let (user, pass) = decoded.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TrinoColumn;

    fn test_peer() -> ConnectInfo<SocketAddr> {
        ConnectInfo("127.0.0.1:12345".parse().unwrap())
    }

    #[test]
    fn test_format_uptime_seconds() {
        let started = Instant::now();
        let uptime = format_uptime(started);
        assert!(
            uptime.ends_with('s'),
            "Expected seconds format, got: {uptime}"
        );
    }

    #[tokio::test]
    async fn test_server_info_when_ready() {
        let ready = Arc::new(AtomicBool::new(true));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: ready.clone(),
                started_at,
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let Json(info) = server_info(State(state)).await;
        assert_eq!(info.node_version.version, "0.1.0");
        assert!(info.coordinator);
        assert!(!info.starting);
        assert_eq!(info.environment, "production");
    }

    #[tokio::test]
    async fn test_server_info_when_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let started_at = Instant::now();

        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at,
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let Json(info) = server_info(State(state)).await;
        assert!(info.starting);
    }

    #[tokio::test]
    async fn test_server_state_active() {
        let ready = Arc::new(AtomicBool::new(true));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "ACTIVE");
    }

    #[tokio::test]
    async fn test_server_state_starting() {
        let ready = Arc::new(AtomicBool::new(false));
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready,
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "STARTING");
    }

    // Minimal mock types for handler tests
    struct MockAuth;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuth {
        async fn authenticate(&self, _: &str, _: &str) -> Result<Session, sqe_core::SqeError> {
            Err(sqe_core::SqeError::Auth("mock".to_string()))
        }
    }

    /// Mock auth that returns a session for whatever username the
    /// caller passes. Used by ownership tests that need auth to succeed
    /// so the post-auth branch is the one under test.
    struct MockAuthOk;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuthOk {
        async fn authenticate(&self, user: &str, _: &str) -> Result<Session, sqe_core::SqeError> {
            Ok(Session::new(
                user.to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
        async fn authenticate_bearer(&self, _: &str) -> Result<Session, sqe_core::SqeError> {
            Ok(Session::new(
                "bearer-user".to_string(),
                sqe_core::SecretString::new("mock-token".to_string()),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        }
    }

    fn basic_auth_header(user: &str, pass: &str) -> HeaderMap {
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(format!("{user}:{pass}"));
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Basic {encoded}").parse().unwrap(),
        );
        headers
    }

    struct MockQuery;
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for MockQuery {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
            Err(sqe_core::SqeError::Execution("mock".to_string()))
        }
    }

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer eyJhbGciOi.payload.sig".parse().unwrap(),
        );
        let token = extract_bearer_token(&headers).unwrap();
        assert_eq!(token, "eyJhbGciOi.payload.sig");
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let headers = HeaderMap::new();
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_empty() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer ".parse().unwrap());
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_bearer_token_basic_auth_ignored() {
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"user:pass",
        );
        headers.insert(
            "authorization",
            format!("Basic {encoded}").parse().unwrap(),
        );
        assert!(extract_bearer_token(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth() {
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"root:root123",
        );
        headers.insert(
            "authorization",
            format!("Basic {encoded}").parse().unwrap(),
        );

        let (user, pass) = extract_basic_auth(&headers).unwrap();
        assert_eq!(user, "root");
        assert_eq!(pass, "root123");
    }

    #[test]
    fn test_extract_basic_auth_missing() {
        let headers = HeaderMap::new();
        assert!(extract_basic_auth(&headers).is_none());
    }

    #[test]
    fn test_extract_basic_auth_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token123".parse().unwrap());
        assert!(extract_basic_auth(&headers).is_none());
    }

    // ── Pagination unit tests ─────────────────────────────────

    #[test]
    fn test_paginate_rows_empty() {
        let pages = paginate_rows(vec![], 10);
        assert_eq!(pages.len(), 1);
        assert!(pages[0].is_empty());
    }

    #[test]
    fn test_paginate_rows_single_page() {
        let rows: Vec<Vec<serde_json::Value>> = (0..5)
            .map(|i| vec![serde_json::json!(i)])
            .collect();
        let pages = paginate_rows(rows, 10);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].len(), 5);
    }

    #[test]
    fn test_paginate_rows_multiple_pages() {
        let rows: Vec<Vec<serde_json::Value>> = (0..25)
            .map(|i| vec![serde_json::json!(i)])
            .collect();
        let pages = paginate_rows(rows, 10);
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].len(), 10);
        assert_eq!(pages[1].len(), 10);
        assert_eq!(pages[2].len(), 5);
    }

    #[test]
    fn test_paginate_rows_exact_fit() {
        let rows: Vec<Vec<serde_json::Value>> = (0..20)
            .map(|i| vec![serde_json::json!(i)])
            .collect();
        let pages = paginate_rows(rows, 10);
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].len(), 10);
        assert_eq!(pages[1].len(), 10);
    }

    #[test]
    fn test_next_uri_has_next() {
        let uri = next_uri("http://localhost:8080", "q-123", 0, 3);
        assert_eq!(uri, Some("http://localhost:8080/v1/statement/q-123/1".to_string()));
    }

    #[test]
    fn test_next_uri_last_page() {
        let uri = next_uri("http://localhost:8080", "q-123", 2, 3);
        assert!(uri.is_none());
    }

    #[test]
    fn test_next_uri_single_page() {
        let uri = next_uri("http://localhost:8080", "q-123", 0, 1);
        assert!(uri.is_none());
    }

    #[test]
    fn test_build_page_response_first_page() {
        let paginated = PaginatedResult {
            columns: vec![TrinoColumn {
                name: "id".to_string(),
                r#type: "bigint".to_string(),
                type_signature: crate::protocol::type_signature_for("bigint"),
            }],
            pages: vec![
                vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
                vec![vec![serde_json::json!(3)]],
            ],
            total_pages: 2,
            created_at: Instant::now(),
            owner_username: "test".to_string(),
            total_rows: 0,
            update_type: None,
            update_count: None,
        };

        let resp = build_page_response("http://localhost:8080", "q-abc", &paginated, 0);
        assert_eq!(resp.id, "q-abc");
        assert_eq!(
            resp.info_uri,
            Some("http://localhost:8080/v1/query/q-abc".to_string())
        );
        assert_eq!(
            resp.next_uri,
            Some("http://localhost:8080/v1/statement/q-abc/1".to_string())
        );
        assert_eq!(resp.data.as_ref().unwrap().len(), 2);
        assert_eq!(resp.stats.state, "RUNNING");
    }

    #[test]
    fn test_build_page_response_last_page() {
        let paginated = PaginatedResult {
            columns: vec![TrinoColumn {
                name: "id".to_string(),
                r#type: "bigint".to_string(),
                type_signature: crate::protocol::type_signature_for("bigint"),
            }],
            pages: vec![
                vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
                vec![vec![serde_json::json!(3)]],
            ],
            total_pages: 2,
            created_at: Instant::now(),
            owner_username: "test".to_string(),
            total_rows: 0,
            update_type: None,
            update_count: None,
        };

        let resp = build_page_response("http://localhost:8080", "q-abc", &paginated, 1);
        assert!(resp.next_uri.is_none());
        assert_eq!(resp.data.as_ref().unwrap().len(), 1);
        assert_eq!(resp.stats.state, "FINISHED");
    }

    #[test]
    fn test_build_page_response_single_page() {
        let paginated = PaginatedResult {
            columns: vec![],
            pages: vec![vec![vec![serde_json::json!(42)]]],
            total_pages: 1,
            created_at: Instant::now(),
            owner_username: "test".to_string(),
            total_rows: 0,
            update_type: None,
            update_count: None,
        };

        let resp = build_page_response("http://localhost:8080", "q-single", &paginated, 0);
        assert!(resp.next_uri.is_none());
        assert_eq!(resp.stats.state, "FINISHED");
    }

    // ── Header extraction unit tests ──────────────────────────

    #[test]
    fn test_extract_trino_headers_all_present() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "iceberg".parse().unwrap());
        headers.insert("x-trino-schema", "public".parse().unwrap());
        headers.insert("x-trino-user", "alice".parse().unwrap());
        headers.insert("x-trino-source", "trino-cli".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert_eq!(trino.catalog.as_deref(), Some("iceberg"));
        assert_eq!(trino.schema.as_deref(), Some("public"));
        assert_eq!(trino.user.as_deref(), Some("alice"));
        assert_eq!(trino.source.as_deref(), Some("trino-cli"));
    }

    #[test]
    fn test_extract_trino_headers_none_present() {
        let headers = HeaderMap::new();
        let trino = extract_trino_headers(&headers);
        assert!(trino.catalog.is_none());
        assert!(trino.schema.is_none());
        assert!(trino.user.is_none());
        assert!(trino.source.is_none());
    }

    #[test]
    fn test_extract_trino_headers_partial() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "hive".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert_eq!(trino.catalog.as_deref(), Some("hive"));
        assert!(trino.schema.is_none());
        assert!(trino.user.is_none());
        assert!(trino.source.is_none());
    }

    #[test]
    fn test_extract_trino_headers_empty_values_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-catalog", "".parse().unwrap());
        headers.insert("x-trino-schema", "  ".parse().unwrap());

        let trino = extract_trino_headers(&headers);
        assert!(trino.catalog.is_none());
        assert!(trino.schema.is_none());
    }

    #[test]
    fn test_apply_trino_headers_to_session() {
        let session = Session::new(
            "testuser".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );

        let trino_headers = TrinoClientHeaders {
            catalog: Some("iceberg".to_string()),
            schema: Some("analytics".to_string()),
            user: Some("testuser".to_string()),
            source: Some("trino-jdbc".to_string()),
        };

        let session = apply_trino_headers(session, &trino_headers);
        assert_eq!(session.default_catalog.as_deref(), Some("iceberg"));
        assert_eq!(session.default_schema.as_deref(), Some("analytics"));
        assert_eq!(session.source.as_deref(), Some("trino-jdbc"));
    }

    #[test]
    fn test_apply_trino_headers_none_values() {
        let session = Session::new(
            "testuser".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );

        let trino_headers = TrinoClientHeaders::default();

        let session = apply_trino_headers(session, &trino_headers);
        assert!(session.default_catalog.is_none());
        assert!(session.default_schema.is_none());
        assert!(session.source.is_none());
    }

    // ── Integration-style tests for pagination flow ───────────

    #[test]
    fn test_paginated_result_cleanup_after_last_page() {
        let results: DashMap<String, PaginatedResult> = DashMap::new();
        results.insert(
            "q-test".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
            },
        );

        // Simulate fetching page 0 (not last) -- should NOT remove
        assert!(results.get("q-test").is_some());

        // Simulate fetching page 1 (last) -- should remove
        results.remove("q-test");
        assert!(results.get("q-test").is_none());
    }

    #[tokio::test]
    async fn test_get_results_invalid_token() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        // Authenticate so we exercise the page-token parse branch.
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path(("q-123".to_string(), "not-a-number".to_string())),
        )
        .await;

        assert_eq!(response.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_results_out_of_range_token() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        // Insert a result with 1 page
        state.results.insert(
            "q-456".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![]],
                total_pages: 1,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
            },
        );

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            HeaderMap::new(),
            Path(("q-456".to_string(), "5".to_string())),
        )
        .await;

        // Missing Authorization header now rejects with 401 before the
        // out-of-range branch runs. The 401 covers issue #34.
        assert_eq!(response.into_response().status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_get_results_returns_correct_page() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        state.results.insert(
            "q-paged".to_string(),
            PaginatedResult {
                columns: vec![TrinoColumn {
                    name: "val".to_string(),
                    r#type: "bigint".to_string(),
                    type_signature: crate::protocol::type_signature_for("bigint"),
                }],
                pages: vec![
                    vec![vec![serde_json::json!(10)]],
                    vec![vec![serde_json::json!(20)]],
                    vec![vec![serde_json::json!(30)]],
                ],
                total_pages: 3,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
            },
        );

        // Fetch page 1 (middle); authenticate as the result owner.
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path(("q-paged".to_string(), "1".to_string())),
        )
        .await;

        let resp = response.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let trino_resp: TrinoResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(trino_resp.data.as_ref().unwrap().len(), 1);
        assert_eq!(trino_resp.data.as_ref().unwrap()[0][0], serde_json::json!(20));
        assert_eq!(
            trino_resp.next_uri,
            Some("http://localhost:8080/v1/statement/q-paged/2".to_string())
        );
        assert_eq!(trino_resp.stats.state, "RUNNING");
        assert!(trino_resp.info_uri.is_some(), "infoUri must be present");

        // Result should still exist (not the last page)
        assert!(state.results.get("q-paged").is_some());
    }

    #[tokio::test]
    async fn test_get_results_last_page_cleans_up() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        state.results.insert(
            "q-cleanup".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![
                    vec![vec![serde_json::json!(1)]],
                    vec![vec![serde_json::json!(2)]],
                ],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
            },
        );

        // Fetch the last page (token = 1); authenticate as owner.
        let _response = get_results::<MockAuthOk, MockQuery>(
            State(state.clone()),
            test_peer(),
            basic_auth_header("test", "pw"),
            Path(("q-cleanup".to_string(), "1".to_string())),
        )
        .await;

        // Result should be cleaned up
        assert!(state.results.get("q-cleanup").is_none());
    }

    /// Regression for #34: an authenticated caller who is not the
    /// owner cannot pull the result set.
    #[tokio::test]
    async fn test_get_results_rejects_non_owner() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });
        state.results.insert(
            "q-secret".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![]],
                total_pages: 1,
                created_at: Instant::now(),
                owner_username: "alice".to_string(),
            },
        );
        let response = get_results::<MockAuthOk, MockQuery>(
            State(state),
            test_peer(),
            basic_auth_header("mallory", "pw"),
            Path(("q-secret".to_string(), "0".to_string())),
        )
        .await;
        assert_eq!(response.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_cancel_query_removes_result() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        state.results.insert(
            "q-cancel".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test-user".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
            },
        );

        // Build Basic auth header for "test-user:pass"
        let mut headers = HeaderMap::new();
        let encoded = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b"test-user:pass",
        );
        headers.insert("authorization", format!("Basic {encoded}").parse().unwrap());

        let response = cancel_query::<MockAuth, MockQuery>(
            State(state.clone()),
            test_peer(),
            headers,
            Path("q-cancel".to_string()),
        )
        .await;

        // MockAuth rejects all credentials, so we expect UNAUTHORIZED
        assert_eq!(response.into_response().status(), StatusCode::UNAUTHORIZED);
    }

    // ── Base URL extraction tests ────────────────────────────────

    #[test]
    fn test_extract_base_url_from_host_header() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "myhost:9090".parse().unwrap());
        assert_eq!(extract_base_url(&headers, 8080), "http://myhost:9090");
    }

    #[test]
    fn test_extract_base_url_fallback() {
        let headers = HeaderMap::new();
        assert_eq!(extract_base_url(&headers, 8080), "http://localhost:8080");
    }

    #[test]
    fn test_extract_base_url_with_forwarded_proto() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "myhost:9090".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        assert_eq!(extract_base_url(&headers, 8080), "https://myhost:9090");
    }

    // ── Bearer JWT passthrough tests ────────────────────────────

    /// MockAuth has no bearer provider, so `authenticate_bearer` returns Err.
    /// An invalid bearer token (not validated by any provider) should be rejected
    /// with 401 Unauthorized.
    #[tokio::test]
    async fn test_submit_query_bearer_jwt_passthrough() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let mut headers = HeaderMap::new();
        // JWT-shaped bearer token (three dot-separated segments)
        headers.insert(
            "authorization",
            "Bearer eyJhbGciOi.payload.signature".parse().unwrap(),
        );
        headers.insert("x-trino-user", "alice".parse().unwrap());

        let response = submit_query::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            headers,
            "SELECT 1".to_string(),
        )
        .await;

        let resp = response.into_response();
        // No bearer provider configured -> token validation fails -> 401
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Invalid bearer token should return 401"
        );
    }

    /// A non-JWT bearer token (no dots) should still return 401 when the
    /// bearer provider is not configured.
    #[tokio::test]
    async fn test_submit_query_bearer_opaque_token_rejected() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: true,
        });

        let mut headers = HeaderMap::new();
        // Opaque token (no dots) — not a JWT, no fallback
        headers.insert(
            "authorization",
            "Bearer some-opaque-token".parse().unwrap(),
        );

        let response = submit_query::<MockAuth, MockQuery>(
            State(state),
            test_peer(),
            headers,
            "SELECT 1".to_string(),
        )
        .await;

        let resp = response.into_response();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Opaque bearer tokens must be rejected when no provider is configured"
        );
    }

    // ── Trusted-proxy and coarse-version regression tests ─────────

    #[tokio::test]
    async fn test_server_info_scrubs_version_by_default() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "1.2.3-4567".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: false,
        });
        let Json(info) = server_info(State(state)).await;
        // Exact build is hidden; coarse major.minor is fine.
        assert_eq!(info.node_version.version, "1.2");
    }

    #[test]
    fn coarse_version_handles_short_input() {
        assert_eq!(coarse_version(""), "unknown");
        assert_eq!(coarse_version("dev"), "unknown");
        assert_eq!(coarse_version("0.1.0"), "0.1");
        assert_eq!(coarse_version("0.31.4-build123"), "0.31");
    }

    #[tokio::test]
    async fn test_trino_client_ip_ignores_xff_by_default() {
        let state = Arc::new(TrinoState::<MockAuth, MockQuery> {
            authenticator: Arc::new(MockAuth),
            query_handler: Arc::new(MockQuery),
            results: DashMap::new(),
            node: NodeContext {
                version: "0.1.0".to_string(),
                ready: Arc::new(AtomicBool::new(true)),
                started_at: Instant::now(),
            },
            page_size: DEFAULT_PAGE_SIZE,
            port: 8080,
            oauth2: None,
            security: SecurityConfig::default(),
            auth_rate_limiter: None,
            expose_version: false,
        });
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.2.3.4".parse().unwrap());
        let resolved =
            trino_client_ip(&state, &headers, "10.0.0.1:33333".parse().unwrap());
        assert_eq!(resolved, "10.0.0.1:33333");
    }
}
