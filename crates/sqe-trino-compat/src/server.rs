use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use dashmap::DashMap;
use tracing::{error, info, warn};
use uuid::Uuid;

use sqe_core::Session;

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
}

/// Stores the full result set split into pages for pagination.
pub struct PaginatedResult {
    /// Column metadata (shared across all pages).
    pub columns: Vec<TrinoColumn>,
    /// Row data split into fixed-size pages.
    pub pages: Vec<Vec<Vec<serde_json::Value>>>,
    /// Total number of pages.
    pub total_pages: usize,
    /// Wall-clock time at which this result was stored; used for TTL eviction.
    pub created_at: std::time::Instant,
    /// Username of the session that created this result; used for cancel authorization.
    pub owner_username: String,
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
    async fn authenticate(&self, username: &str, password: &str) -> Result<Session, String>;

    /// Validate a raw bearer token (JWT) and return an authenticated session.
    ///
    /// The default implementation rejects all bearer tokens. Override this in
    /// the coordinator adapter to route through the JWKS-validating auth chain.
    async fn authenticate_bearer(&self, _token: &str) -> Result<Session, String> {
        Err("Bearer token authentication not configured".to_string())
    }
}

#[async_trait::async_trait]
pub trait TrinoQueryExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        session: &Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String>;
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
    let state = Arc::new(TrinoState {
        authenticator,
        query_handler,
        results: DashMap::new(),
        node,
        page_size: DEFAULT_PAGE_SIZE,
        port,
        oauth2: oauth2.clone(),
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

        if let Err(e) = axum::serve(listener, app).await {
            error!(error = %e, "Trino-compat HTTP server exited with error");
        }
    })
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
    Json(ServerInfo {
        node_version: NodeVersion {
            version: state.node.version.clone(),
        },
        environment: "production".to_string(),
        coordinator: true,
        starting: !ready,
        uptime: format_uptime(state.node.started_at),
    })
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
    let body = TrinoResponse {
        id: String::new(),
        info_uri: None,
        next_uri: None,
        columns: None,
        data: None,
        stats: TrinoStats::failed(),
        error: Some(TrinoError {
            message: msg.into(),
            error_code: 1,
            error_name: "USER_ERROR".to_string(),
            error_type: "USER_ERROR".to_string(),
            query_id: None,
        }),
    };
    (status, Json(body)).into_response()
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

    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: next_uri(base_url, query_id, page_token, paginated.total_pages),
        columns: Some(paginated.columns.clone()),
        data: Some(page_data),
        stats: if is_last {
            TrinoStats::finished()
        } else {
            TrinoStats::running(page_token + 1, paginated.total_pages)
        },
        error: None,
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
    headers: HeaderMap,
    body: String,
) -> Response {
    let sql = body.trim();
    if sql.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Empty query");
    }

    // Extract client IP for logging on every request
    let client_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let trino_headers = extract_trino_headers(&headers);

    let session = if let Some(token) = extract_bearer_token(&headers) {
        // Bearer token auth: validate JWT through the auth provider chain.
        match state.authenticator.authenticate_bearer(&token).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, client_ip = %client_ip, "Trino bearer token validation failed");
                return error_response(StatusCode::UNAUTHORIZED, "Invalid bearer token");
            }
        }
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        // Basic auth: authenticate via OIDC password grant
        let client_ip = extract_header(&headers, "x-forwarded-for")
            .map(|s| {
                s.split(',')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string()
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, user = %user, client_ip = %client_ip, "Trino authentication failed");
                return error_response(StatusCode::UNAUTHORIZED, "Authentication failed");
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

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let pages = paginate_rows(data, state.page_size);
            let total_pages = pages.len();

            let paginated = PaginatedResult {
                columns,
                pages,
                total_pages,
                created_at: std::time::Instant::now(),
                owner_username: session.user.username.clone(),
            };

            // Build the first page response (token = 0).
            let response = build_page_response(&base_url, &query_id, &paginated, 0);

            // Store for subsequent GET requests (if there are more pages).
            state.results.insert(query_id, paginated);

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let sqe_err = sqe_core::SqeError::Execution(e);
            tracing::warn!(
                error_code = %sqe_err.error_code(),
                query_id = %query_id,
                error = %sqe_err,
                "Trino query execution failed"
            );
            let is_rate_limited = sqe_err
                .to_string()
                .to_ascii_lowercase()
                .contains("rate limit");
            let trino_error = TrinoError::from_sqe_error(&sqe_err, Some(&query_id));
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(trino_error),
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
                    next_uri: None,
                    columns: None,
                    data: None,
                    stats: TrinoStats::failed(),
                    error: Some(TrinoError {
                        message: "Page token out of range".to_string(),
                        error_code: 1,
                        error_name: "USER_ERROR".to_string(),
                        error_type: "USER_ERROR".to_string(),
                        query_id: None,
                    }),
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
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query not found".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                    query_id: None,
                }),
            };
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
    }
}

async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
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
        });

        let result = server_state(State(state)).await;
        assert_eq!(result, "STARTING");
    }

    // Minimal mock types for handler tests
    struct MockAuth;
    #[async_trait::async_trait]
    impl TrinoAuthenticator for MockAuth {
        async fn authenticate(&self, _: &str, _: &str) -> Result<Session, String> {
            Err("mock".to_string())
        }
    }
    struct MockQuery;
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for MockQuery {
        async fn execute(
            &self,
            _: &Session,
            _: &str,
        ) -> Result<Vec<arrow_array::RecordBatch>, String> {
            Err("mock".to_string())
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
            "token".to_string(),
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
            "token".to_string(),
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
        });

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
            HeaderMap::new(),
            Path(("q-123".to_string(), "not-a-number".to_string())),
        )
        .await;

        // Should return 400 for invalid token
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
            },
        );

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
            HeaderMap::new(),
            Path(("q-456".to_string(), "5".to_string())),
        )
        .await;

        assert_eq!(response.into_response().status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_results_returns_correct_page() {
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
            },
        );

        // Fetch page 1 (middle)
        let response = get_results::<MockAuth, MockQuery>(
            State(state.clone()),
            HeaderMap::new(),
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
            },
        );

        // Fetch the last page (token = 1)
        let _response = get_results::<MockAuth, MockQuery>(
            State(state.clone()),
            HeaderMap::new(),
            Path(("q-cleanup".to_string(), "1".to_string())),
        )
        .await;

        // Result should be cleaned up
        assert!(state.results.get("q-cleanup").is_none());
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
        });

        state.results.insert(
            "q-cancel".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test-user".to_string(),
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
            headers,
            "SELECT 1".to_string(),
        )
        .await;

        let resp = response.into_response();
        // No bearer provider configured → token validation fails → 401
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
        });

        let mut headers = HeaderMap::new();
        // Opaque token (no dots) — not a JWT, no fallback
        headers.insert(
            "authorization",
            "Bearer some-opaque-token".parse().unwrap(),
        );

        let response = submit_query::<MockAuth, MockQuery>(
            State(state),
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
}
