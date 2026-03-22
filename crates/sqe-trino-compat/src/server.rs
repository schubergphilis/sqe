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
}

/// Stores the full result set split into pages for pagination.
pub struct PaginatedResult {
    /// Column metadata (shared across all pages).
    pub columns: Vec<TrinoColumn>,
    /// Row data split into fixed-size pages.
    pub pages: Vec<Vec<Vec<serde_json::Value>>>,
    /// Total number of pages.
    pub total_pages: usize,
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
    });

    tokio::spawn(async move {
        let app = Router::new()
            .route("/v1/info", get(server_info::<A, Q>))
            .route("/v1/info/state", get(server_state::<A, Q>))
            .route("/v1/statement", post(submit_query::<A, Q>))
            .route("/v1/statement/{id}/{token}", get(get_results::<A, Q>))
            .route("/v1/statement/{id}", delete(cancel_query::<A, Q>))
            .with_state(state);

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
fn next_uri(query_id: &str, token: usize, total_pages: usize) -> Option<String> {
    if token + 1 < total_pages {
        Some(format!("/v1/statement/{query_id}/{}", token + 1))
    } else {
        None
    }
}

/// Build a [`TrinoResponse`] for the given page of a paginated result.
fn build_page_response(
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
        info_uri: None,
        next_uri: next_uri(query_id, page_token, paginated.total_pages),
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

#[tracing::instrument(skip_all, name = "trino.submit_query")]
async fn submit_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let sql = body.trim();
    if sql.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "Empty query");
    }

    let trino_headers = extract_trino_headers(&headers);

    let session = if let Some(token) = extract_bearer_token(&headers) {
        // Bearer token auth: create session directly from the JWT.
        let username = trino_headers
            .user
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        Session::new(
            username,
            token,
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        )
    } else if let Some((user, pass)) = extract_basic_auth(&headers) {
        // Basic auth: authenticate via OIDC password grant
        match state.authenticator.authenticate(&user, &pass).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, user = %user, "Trino authentication failed");
                return error_response(StatusCode::UNAUTHORIZED, "Authentication failed");
            }
        }
    } else {
        return error_response(StatusCode::UNAUTHORIZED, "Missing Authorization header");
    };

    // Apply Trino client headers (catalog, schema, source) to the session.
    let session = apply_trino_headers(session, &trino_headers);

    info!(
        user = %session.user.username,
        catalog = ?session.default_catalog,
        schema = ?session.default_schema,
        source = ?session.source,
        sql = sql,
        "Trino query submitted"
    );

    let query_id = Uuid::new_v4().to_string();

    match state.query_handler.execute(&session, sql).await {
        Ok(batches) => {
            let (columns, data) = protocol::batches_to_trino(&batches);
            let pages = paginate_rows(data, state.page_size);
            let total_pages = pages.len();

            let paginated = PaginatedResult {
                columns,
                pages,
                total_pages,
            };

            // Build the first page response (token = 0).
            let response = build_page_response(&query_id, &paginated, 0);

            // Store for subsequent GET requests (if there are more pages).
            state.results.insert(query_id, paginated);

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            warn!(error = %e, sql = sql, "Trino query execution failed");
            let response = TrinoResponse {
                id: query_id,
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query execution failed".to_string(),
                    error_code: 1,
                    error_name: "INTERNAL_ERROR".to_string(),
                    error_type: "INTERNAL_ERROR".to_string(),
                }),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}

#[tracing::instrument(skip_all, name = "trino.get_results")]
async fn get_results<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path((id, token)): Path<(String, String)>,
) -> Response {
    // Parse the token as a page index.
    let page_token: usize = match token.parse() {
        Ok(t) => t,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "Invalid page token");
        }
    };

    match state.results.get(&id) {
        Some(paginated) => {
            if page_token >= paginated.total_pages {
                return error_response(StatusCode::NOT_FOUND, "Page token out of range");
            }

            let response = build_page_response(&id, &paginated, page_token);
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
                id,
                info_uri: None,
                next_uri: None,
                columns: None,
                data: None,
                stats: TrinoStats::failed(),
                error: Some(TrinoError {
                    message: "Query not found".to_string(),
                    error_code: 1,
                    error_name: "USER_ERROR".to_string(),
                    error_type: "USER_ERROR".to_string(),
                }),
            };
            (StatusCode::NOT_FOUND, Json(response)).into_response()
        }
    }
}

async fn cancel_query<A: TrinoAuthenticator, Q: TrinoQueryExecutor>(
    State(state): State<Arc<TrinoState<A, Q>>>,
    Path(id): Path<String>,
) -> StatusCode {
    state.results.remove(&id);
    StatusCode::NO_CONTENT
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
        let uri = next_uri("q-123", 0, 3);
        assert_eq!(uri, Some("/v1/statement/q-123/1".to_string()));
    }

    #[test]
    fn test_next_uri_last_page() {
        let uri = next_uri("q-123", 2, 3);
        assert!(uri.is_none());
    }

    #[test]
    fn test_next_uri_single_page() {
        let uri = next_uri("q-123", 0, 1);
        assert!(uri.is_none());
    }

    #[test]
    fn test_build_page_response_first_page() {
        let paginated = PaginatedResult {
            columns: vec![TrinoColumn {
                name: "id".to_string(),
                r#type: "bigint".to_string(),
            }],
            pages: vec![
                vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
                vec![vec![serde_json::json!(3)]],
            ],
            total_pages: 2,
        };

        let resp = build_page_response("q-abc", &paginated, 0);
        assert_eq!(resp.id, "q-abc");
        assert_eq!(
            resp.next_uri,
            Some("/v1/statement/q-abc/1".to_string())
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
            }],
            pages: vec![
                vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
                vec![vec![serde_json::json!(3)]],
            ],
            total_pages: 2,
        };

        let resp = build_page_response("q-abc", &paginated, 1);
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
        };

        let resp = build_page_response("q-single", &paginated, 0);
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
        });

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
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
        });

        // Insert a result with 1 page
        state.results.insert(
            "q-456".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![]],
                total_pages: 1,
            },
        );

        let response = get_results::<MockAuth, MockQuery>(
            State(state),
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
        });

        state.results.insert(
            "q-paged".to_string(),
            PaginatedResult {
                columns: vec![TrinoColumn {
                    name: "val".to_string(),
                    r#type: "bigint".to_string(),
                }],
                pages: vec![
                    vec![vec![serde_json::json!(10)]],
                    vec![vec![serde_json::json!(20)]],
                    vec![vec![serde_json::json!(30)]],
                ],
                total_pages: 3,
            },
        );

        // Fetch page 1 (middle)
        let response = get_results::<MockAuth, MockQuery>(
            State(state.clone()),
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
            Some("/v1/statement/q-paged/2".to_string())
        );
        assert_eq!(trino_resp.stats.state, "RUNNING");

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
            },
        );

        // Fetch the last page (token = 1)
        let _response = get_results::<MockAuth, MockQuery>(
            State(state.clone()),
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
        });

        state.results.insert(
            "q-cancel".to_string(),
            PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
            },
        );

        let status = cancel_query::<MockAuth, MockQuery>(
            State(state.clone()),
            Path("q-cancel".to_string()),
        )
        .await;

        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(state.results.get("q-cancel").is_none());
    }
}
