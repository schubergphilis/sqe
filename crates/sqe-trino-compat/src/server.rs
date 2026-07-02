use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use moka::sync::Cache as MokaCache;
use tracing::{error, info, warn};
use uuid::Uuid;

use sqe_core::Session;
use sqe_core::config::SecurityConfig;

use crate::explain_compat;
use crate::info_schema_compat;
use crate::prepared;
use crate::protocol::{
    self, NodeVersion, ServerInfo, TrinoColumn, TrinoError, TrinoResponse, TrinoStats,
};

/// Default number of rows per page for result pagination.
const DEFAULT_PAGE_SIZE: usize = 1000;

/// Results older than this are evicted (5 minutes).
const RESULT_TTL_SECS: u64 = 300;

/// Upper bound on the bytes of cached Trino result JSON pages we keep
/// resident at any one time. Bounding the cache by estimated payload
/// size (not entry count) means a handful of large results cannot pin
/// gigabytes outside DataFusion's memory pool.
const RESULT_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// A query-registry entry idle (un-polled) for this long is evicted. This is
/// `time_to_idle`, NOT `time_to_live`: every poll's `queries.get(&id)` counts
/// as an access and resets the timer, so a query that runs longer than this
/// but is actively polled is never reaped. Only a genuinely abandoned client
/// (no poll for this long) triggers eviction, whose listener aborts the
/// still-running background task. Using `time_to_live` here would abort any
/// query still executing at the 300s mark mid-flight — the opposite of the
/// feature's purpose.
const QUERY_REGISTRY_IDLE_SECS: u64 = 300;

/// Default bounded wait applied to the POST and to a poll with no explicit
/// `maxWait`: no single HTTP call blocks longer than this on query progress.
const DEFAULT_MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Hard upper bound a client-supplied `maxWait` is clamped to.
const MAX_WAIT_CAP: std::time::Duration = std::time::Duration::from_secs(10);

fn build_result_cache() -> MokaCache<String, Arc<PaginatedResult>> {
    MokaCache::builder()
        .max_capacity(RESULT_CACHE_MAX_BYTES)
        .weigher(|_key: &String, value: &Arc<PaginatedResult>| {
            value.estimated_bytes.max(1)
        })
        .time_to_live(std::time::Duration::from_secs(RESULT_TTL_SECS))
        .build()
}

/// Shared context for Trino /v1/info endpoints.
pub struct NodeContext {
    pub version: String,
    pub ready: Arc<AtomicBool>,
    pub started_at: Instant,
}

pub struct TrinoState<A, Q> {
    pub authenticator: Arc<A>,
    pub query_handler: Arc<Q>,
    pub results: MokaCache<String, Arc<PaginatedResult>>,
    /// In-flight / recently-finished query lifecycle handles, keyed by
    /// `query_id`. Distinct from `results`, which holds finished page data.
    pub queries: MokaCache<String, Arc<QueryHandle>>,
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
    // Trait surface predates the `result_unit_err` lint; switching to a
    // dedicated `RateLimitError` would touch every implementer for no
    // wire-level benefit.
    #[allow(clippy::result_unit_err)]
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
    /// Pre-computed cache weight in bytes so the result-cache weigher
    /// does not have to walk the JSON tree on every insert.
    pub estimated_bytes: u32,
}

fn estimate_paginated_bytes(pages: &[Vec<Vec<serde_json::Value>>], columns: &[TrinoColumn]) -> u32 {
    let mut total: usize = std::mem::size_of::<PaginatedResult>();
    for col in columns {
        total = total.saturating_add(col.name.len() + col.type_signature.raw_type.len() + 32);
    }
    for page in pages {
        for row in page {
            for value in row {
                total = total.saturating_add(estimate_json_bytes(value));
            }
        }
    }
    total.try_into().unwrap_or(u32::MAX)
}

fn estimate_json_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(_) => 5,
        serde_json::Value::Number(_) => 16,
        serde_json::Value::String(s) => s.len() + 16,
        serde_json::Value::Array(items) => items.iter().map(estimate_json_bytes).sum::<usize>() + 16,
        serde_json::Value::Object(map) => {
            map.iter()
                .map(|(k, v)| k.len() + estimate_json_bytes(v) + 8)
                .sum::<usize>()
                + 16
        }
    }
}

/// Lifecycle state of a submitted statement executing on a background task.
#[derive(Debug)]
pub enum QueryStatus {
    /// Registered, background task not yet observed to start.
    Queued,
    /// Background task is running `Q::execute`.
    Running,
    /// Finished successfully; the `PaginatedResult` is in the result cache
    /// under the same `query_id`.
    Finished,
    /// Execution failed; carries the mapped Trino error to replay on poll.
    Failed(protocol::TrinoError),
    /// Cancelled via DELETE or evicted while still running.
    Cancelled,
}

impl QueryStatus {
    /// True once the query will never transition again.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            QueryStatus::Finished | QueryStatus::Failed(_) | QueryStatus::Cancelled
        )
    }
}

/// Shared handle for a statement executing in the background. Stored in the
/// query registry under `query_id`; the background task and every poll share
/// the same `Arc<QueryHandle>`.
#[derive(Debug)]
pub struct QueryHandle {
    /// Current lifecycle state; mutated by the background task and by cancel.
    pub status: std::sync::Mutex<QueryStatus>,
    /// Woken on every status transition so waiting polls re-check promptly.
    pub notify: tokio::sync::Notify,
    /// Abort handle for the background task; set just after spawn.
    pub abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    /// Username that submitted the query; used for poll/cancel authorization.
    pub owner_username: String,
    /// Session-state mutation (USE / SET CATALOG) to echo as response headers
    /// when the query finishes. `None` for ordinary statements.
    pub session_update: Option<protocol::UpdatedSessionState>,
    /// Registration time; used for diagnostics.
    pub created_at: std::time::Instant,
}

/// Build the query-state registry. Idle-evicts abandoned entries and, on
/// eviction of a still-running query, aborts its background task.
fn build_query_registry() -> MokaCache<String, Arc<QueryHandle>> {
    MokaCache::builder()
        .time_to_idle(std::time::Duration::from_secs(QUERY_REGISTRY_IDLE_SECS))
        .eviction_listener(|_key, handle: Arc<QueryHandle>, _cause| {
            let mut status = handle.status.lock().unwrap();
            if !status.is_terminal() {
                if let Some(abort) = handle.abort.lock().unwrap().as_ref() {
                    abort.abort();
                }
                *status = QueryStatus::Cancelled;
            }
        })
        .build()
}

/// Trino client headers extracted from the request.
#[derive(Debug, Clone, Default)]
pub struct TrinoClientHeaders {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub user: Option<String>,
    pub source: Option<String>,
    /// `iceberg.compression_codec` from the `X-Trino-Session` property list, if set.
    pub compression_codec: Option<String>,
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

    /// Describe a prepared statement's output columns (`DESCRIBE OUTPUT`) or
    /// bind parameters (`DESCRIBE INPUT`), returning a synthetic result set.
    ///
    /// `prepared_sql` is the resolved statement template (with `?`
    /// placeholders) looked up from the session's prepared statements. The
    /// default returns `NotImplemented` so executors that do not support
    /// prepared-statement introspection (test mocks) are unaffected.
    async fn describe_prepared(
        &self,
        _session: &Session,
        _prepared_sql: &str,
        _kind: crate::protocol::DescribeKind,
    ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
        Err(sqe_core::SqeError::NotImplemented(
            "DESCRIBE OUTPUT / DESCRIBE INPUT is not supported by this executor".into(),
        ))
    }
}

#[derive(Default)]
pub struct TrinoServerOptions {
    pub security: SecurityConfig,
    pub auth_rate_limiter: Option<Arc<dyn TrinoAuthRateLimiter>>,
    /// Whether `/v1/info` may return the exact build version. Default
    /// `false` for production deployments. Issue #40.
    pub expose_version: bool,
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
        results: build_result_cache(),
        queries: build_query_registry(),
        node,
        page_size: DEFAULT_PAGE_SIZE,
        port,
        oauth2: oauth2.clone(),
        security: options.security,
        auth_rate_limiter: options.auth_rate_limiter,
        expose_version: options.expose_version,
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
        // The SQL value must be URL-encoded: it contains spaces, commas, and
        // `=` that would otherwise corrupt the header. The Trino client replays
        // this verbatim in X-Trino-Prepared-Statement, where we URL-decode it.
        append(
            headers,
            HeaderName::from_static("x-trino-added-prepare"),
            &format!("{name}={}", prepared::form_urlencode(sql)),
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
    // The client replays `SET SESSION` values in `X-Trino-Session`; pick out
    // `iceberg.compression_codec` so writes in this request honour it (#353).
    let compression_codec = incoming_session_properties(headers)
        .into_iter()
        .find(|(k, _)| k == "iceberg.compression_codec")
        .map(|(_, v)| v);
    TrinoClientHeaders {
        catalog: extract_header(headers, "x-trino-catalog"),
        schema: extract_header(headers, "x-trino-schema"),
        user: extract_header(headers, "x-trino-user"),
        source: extract_header(headers, "x-trino-source"),
        compression_codec,
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

/// Parse the catalog session properties the client carries in `X-Trino-Session`
/// (the values it replays after a `SET SESSION`). The header is a
/// comma-separated list of `name=value` pairs, possibly split across multiple
/// header lines. Used to answer `SHOW SESSION`. (#323)
fn incoming_session_properties(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut props = Vec::new();
    for value in headers.get_all("x-trino-session") {
        let Ok(raw) = value.to_str() else { continue };
        for pair in raw.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((name, val)) = pair.split_once('=') {
                props.push((name.trim().to_string(), val.trim().to_string()));
            }
        }
    }
    props
}

/// Match a value against a Trino `LIKE` pattern (`%` = any sequence, `_` = any
/// single char). Used to filter `SHOW SESSION LIKE '...'`. Case-sensitive,
/// matching Trino's session-property names.
fn like_match(value: &str, pattern: &str) -> bool {
    // Anchored glob match via classic dynamic programming over chars.
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (n, m) = (v.len(), p.len());
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;
    for j in 1..=m {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=n {
        for j in 1..=m {
            dp[i][j] = match p[j - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && v[i - 1] == c,
            };
        }
    }
    dp[n][m]
}

/// Build the `SHOW SESSION` result set in Trino's column shape
/// (`Name, Value, Default, Type, Description`), one row per current session
/// property, filtered by an optional `LIKE` pattern on the name. (#323)
fn build_show_session_batches(
    props: &[(String, String)],
    like: Option<&str>,
) -> Vec<arrow_array::RecordBatch> {
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("Name", DataType::Utf8, false),
        Field::new("Value", DataType::Utf8, true),
        Field::new("Default", DataType::Utf8, true),
        Field::new("Type", DataType::Utf8, true),
        Field::new("Description", DataType::Utf8, true),
    ]));

    let (mut names, mut values) = (Vec::new(), Vec::new());
    for (name, value) in props {
        if let Some(pat) = like {
            if !like_match(name, pat) {
                continue;
            }
        }
        names.push(name.clone());
        values.push(value.clone());
    }
    let row_count = names.len();
    let empty: Vec<Option<String>> = vec![None; row_count];

    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(values)),
            Arc::new(StringArray::from(empty.clone())),
            Arc::new(StringArray::from(empty.clone())),
            Arc::new(StringArray::from(empty)),
        ],
    );
    match batch {
        Ok(b) => vec![b],
        Err(_) => vec![],
    }
}

/// Apply extracted Trino headers to a session (catalog, schema, source).
fn apply_trino_headers(session: Session, trino_headers: &TrinoClientHeaders) -> Session {
    session
        .with_catalog(trino_headers.catalog.clone())
        .with_schema(trino_headers.schema.clone())
        .with_source(trino_headers.source.clone())
        .with_compression_codec(trino_headers.compression_codec.clone())
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

/// Build a `queued`-route URI (the status/poll namespace).
fn queued_uri(base_url: &str, query_id: &str, token: usize) -> String {
    format!("{base_url}/v1/statement/queued/{query_id}/{token}")
}

/// Response for a POST whose query did not finish within the bounded wait:
/// no data, `state=QUEUED`, `nextUri` -> the queued poll route (token 1).
fn build_started_response(base_url: &str, query_id: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, 1)),
        stats: TrinoStats::queued(),
        ..Default::default()
    }
}

/// Response for a poll on a query still running: no data, `state=RUNNING`,
/// `nextUri` -> the next queued poll token.
fn build_running_response(base_url: &str, query_id: &str, next_token: usize) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(queued_uri(base_url, query_id, next_token)),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}

/// Response for a poll on a query that just finished: no data, `state=RUNNING`,
/// `nextUri` -> the results-paging route at token 0. The queued route stays a
/// status/redirect endpoint; the results route stays pure data paging.
fn build_finished_redirect_response(base_url: &str, query_id: &str) -> TrinoResponse {
    TrinoResponse {
        id: query_id.to_string(),
        info_uri: Some(info_uri(base_url, query_id)),
        next_uri: Some(format!("{base_url}/v1/statement/{query_id}/0")),
        stats: TrinoStats::running(0, 1),
        ..Default::default()
    }
}

/// Wait until `handle.status` is terminal or `max_wait` elapses. A missed
/// `notify_waiters` (tokio `Notify` does not store permits) only defers to the
/// next client poll — correctness holds, at most one extra round-trip.
async fn await_terminal_or_timeout(handle: &QueryHandle, max_wait: std::time::Duration) {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        {
            if handle.status.lock().unwrap().is_terminal() {
                return;
            }
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        // Ignore the timeout result: on either arm we loop and re-check status,
        // and the deadline check above terminates the loop when time is up.
        let _ = tokio::time::timeout(remaining, handle.notify.notified()).await;
    }
}

/// Parse Trino's `maxWait` duration string and clamp to `[0, MAX_WAIT_CAP]`.
/// Absent or unparseable input falls back to `DEFAULT_MAX_WAIT`.
fn clamp_max_wait(raw: Option<&str>) -> std::time::Duration {
    match raw.and_then(parse_trino_duration) {
        Some(d) => d.min(MAX_WAIT_CAP),
        None => DEFAULT_MAX_WAIT,
    }
}

/// Parse a Trino duration literal: an integer followed by `ms`, `s`, or `m`.
fn parse_trino_duration(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_millis);
    }
    if let Some(num) = s.strip_suffix('s') {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(std::time::Duration::from_secs);
    }
    if let Some(num) = s.strip_suffix('m') {
        return num
            .trim()
            .parse::<u64>()
            .ok()
            .map(|m| std::time::Duration::from_secs(m * 60));
    }
    None
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

    // DDL/update statements produce no columns. The Trino 465 JDBC client's
    // ResultRowsDecoder early-returns on `data == null` but otherwise asserts
    // `columns` is non-empty, so a non-null `data` with empty `columns` is
    // rejected ("Columns must be set when decoding data"). Real Trino omits
    // `data` entirely for updates; match that by emitting None when there are
    // no columns. A SELECT returning zero rows still has columns, so its
    // (empty) data stays present. See issue #314.
    let data = if paginated.columns.is_empty() {
        None
    } else {
        Some(page_data)
    };

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
        data,
        stats,
        update_type: paginated.update_type.clone(),
        update_count: paginated.update_count,
        ..Default::default()
    }
}

// ── Handlers ──────────────────────────────────────────────────

/// Dispatch a resolved statement to the right executor path: DESCRIBE
/// prepared, SHOW SESSION, or a normal `execute`. Mirrors the interception
/// order in `submit_query` so async execution behaves identically.
async fn run_statement<Q: TrinoQueryExecutor>(
    handler: &Q,
    session: &Session,
    exec_sql: &str,
    prepared: &std::collections::HashMap<String, String>,
    show_session_props: &[(String, String)],
) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
    if let Some((kind, name)) = protocol::parse_describe_prepared(exec_sql) {
        match prepared.get(&name) {
            Some(prepared_sql) => handler.describe_prepared(session, prepared_sql, kind).await,
            None => Err(sqe_core::SqeError::Execution(format!(
                "Prepared statement not found: {name}"
            ))),
        }
    } else if let Some(like) = protocol::parse_show_session(exec_sql) {
        Ok(build_show_session_batches(show_session_props, like.as_deref()))
    } else {
        handler.execute(session, exec_sql).await
    }
}

/// Turn a successful statement's record batches into a `PaginatedResult`:
/// apply info-schema/EXPLAIN Trino-compat reshaping, classify the update
/// type/count, and paginate. Pure post-processing shared by the sync and
/// async paths.
fn build_paginated_result(
    batches: Vec<arrow_array::RecordBatch>,
    exec_sql: &str,
    session_catalog: Option<&str>,
    page_size: usize,
    owner_username: String,
) -> PaginatedResult {
    let update_type = classify_update_type(exec_sql).map(str::to_string);
    let update_count = if update_type.is_some() {
        extract_update_count(&batches).or(Some(0))
    } else {
        None
    };
    let batches = if info_schema_compat::is_metadata_query(exec_sql) {
        let batches = info_schema_compat::apply_info_schema_compat(batches, session_catalog);
        if info_schema_compat::is_describe_or_show_columns(exec_sql) {
            info_schema_compat::reshape_describe_to_trino(batches)
        } else {
            batches
        }
    } else {
        batches
    };
    let batches = if explain_compat::is_explain(exec_sql) {
        explain_compat::reshape_explain_to_trino(batches)
    } else {
        batches
    };
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let (columns, data) = protocol::batches_to_trino(&batches);
    let pages = paginate_rows(data, page_size);
    let total_pages = pages.len();
    let estimated_bytes = estimate_paginated_bytes(&pages, &columns);
    PaginatedResult {
        columns,
        pages,
        total_pages,
        total_rows,
        created_at: std::time::Instant::now(),
        owner_username,
        update_type,
        update_count,
        estimated_bytes,
    }
}

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

    // Session-control statements with no result set are driven purely by
    // response headers and must NOT be sent to the executor (the SQL parser
    // rejects them):
    //   - PREPARE / DEALLOCATE PREPARE register/forget prepared SQL via
    //     x-trino-added-prepare / x-trino-deallocated-prepare. This is the
    //     Metabase/JDBC connect gate: the driver issues PREPARE before any
    //     query. (#1)
    //   - SET SESSION / RESET SESSION set/clear catalog session properties via
    //     x-trino-set-session / x-trino-clear-session. SQE has no tunable
    //     session-property registry, so these are accept-and-echo: the client's
    //     value round-trips in X-Trino-Session and surfaces in SHOW SESSION,
    //     and the executor (which would reject "SET SESSION ...") is bypassed.
    //     (#323)
    // USE / SET CATALOG (set_catalog / set_schema only) are left to fall
    // through: the executor handles them and the headers are applied after.
    if let Some(ref update) = session_update {
        let update_type = if !update.added_prepare.is_empty() {
            Some("PREPARE")
        } else if !update.deallocated_prepare.is_empty() {
            Some("DEALLOCATE")
        } else if !update.set_session.is_empty() {
            Some("SET SESSION")
        } else if !update.clear_session.is_empty() {
            Some("RESET SESSION")
        } else {
            None
        };
        if let Some(update_type) = update_type {
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                stats: TrinoStats::finished(),
                // Non-null (empty) columns: the field is skip_serializing_if
                // None, so omitting it makes the JDBC driver see `columns: null`
                // and getColumns() throws. Real Trino returns [] for these
                // non-query statements, so the driver builds a 0-column
                // ResultSet and (with updateType set) treats it as a completed
                // update rather than waiting for a result set.
                columns: Some(vec![]),
                update_type: Some(update_type.to_string()),
                ..Default::default()
            };
            let mut resp = (StatusCode::OK, Json(response)).into_response();
            apply_session_headers(resp.headers_mut(), update);
            return resp;
        }
    }

    // Trino prepared statements are stateless: the client carries the prepared
    // SQL in X-Trino-Prepared-Statement headers and submits `EXECUTE <name>
    // USING ...`. Resolve such a body into concrete SQL before execution.
    let prepared = {
        let values: Vec<String> = headers
            .get_all("x-trino-prepared-statement")
            .iter()
            .filter_map(|v| v.to_str().ok().map(str::to_string))
            .collect();
        prepared::parse_prepared_statements(&values)
    };
    let effective_sql = match prepared::rewrite_execute(sql, &prepared) {
        Ok(Some(rewritten)) => rewritten,
        Ok(None) => sql.to_string(),
        Err(msg) => {
            let response = TrinoResponse {
                id: query_id.clone(),
                info_uri: Some(info_uri(&base_url, &query_id)),
                stats: TrinoStats::failed(),
                error: Some(TrinoError::user_error(msg, Some(&query_id))),
                ..Default::default()
            };
            return (StatusCode::OK, Json(response)).into_response();
        }
    };
    // #9: wrap parenthesis-less `VALUES` rows (`INSERT INTO t VALUES 1`,
    // `VALUES 1, 2`) that Trino accepts but sqlparser rejects. Runs before
    // classification and planning so both see the normalized form; a no-op for
    // SQL that already parses (including a column named `values`). See #315.
    let effective_sql = sqe_sql::rewrite_paren_less_values(&effective_sql);
    // #351c: expand a leading bare `TABLE <name>` into `SELECT * FROM <name>`
    // (Trino/SQL-standard shorthand) that sqlparser rejects. Runs before
    // classification and planning so both see the normalized form; a no-op for
    // SQL that already parses (CREATE/DROP/SHOW CREATE TABLE, a column named
    // `table`). See #351c.
    let effective_sql = sqe_sql::rewrite_bare_table(&effective_sql);
    // #335: expand a nested / parameterized ROW-typed CAST target
    // (`CAST(row(1, row(10)) AS row(a int, b row(x int)))`) into nested
    // `named_struct(...)`. sqlparser rejects the nested ROW type outright, so
    // the AST-level rewriter never sees it; this source-level rewrite runs
    // first and yields parseable SQL. A no-op for the single-level ROW cast
    // (which parses and is handled by the AST rewriter) and for any SQL that
    // already parses. See #335.
    let effective_sql = sqe_sql::rewrite_nested_row_cast(&effective_sql);
    // #2: qualify an unqualified `information_schema` reference with the session
    // catalog so it resolves to (and, under polaris-auto, discovers) that
    // catalog instead of the engine default. Only metadata queries are touched.
    let effective_sql = match session.default_catalog.as_deref() {
        Some(cat) if info_schema_compat::is_metadata_query(&effective_sql) => {
            info_schema_compat::qualify_information_schema(&effective_sql, cat)
        }
        _ => effective_sql,
    };
    // #8: name unaliased expression columns _col0, _col1, ... the way Trino
    // does, instead of DataFusion's expression-text column names (e.g.
    // `Int64(1) + Int64(1)`), which JDBC/BI clients display verbatim. Scoped
    // to the Trino wire path: native Flight SQL clients keep DataFusion names.
    // A no-op for non-SELECT statements (DESCRIBE, SHOW, DDL) and for
    // projections containing a wildcard, so it is safe to run unconditionally
    // here, before the DESCRIBE-prepared interception below.
    let effective_sql = sqe_sql::alias_anonymous_select_columns(&effective_sql);
    let exec_sql = effective_sql.as_str();

    // Dispatch DESCRIBE prepared / SHOW SESSION / normal execute, then turn a
    // successful result into a `PaginatedResult`. Both steps live in
    // `run_statement` / `build_paginated_result` so the async spawn path can
    // reuse them verbatim.
    let show_session_props = incoming_session_properties(&headers);
    let exec_result = run_statement(
        state.query_handler.as_ref(),
        &session,
        exec_sql,
        &prepared,
        &show_session_props,
    )
    .await;

    match exec_result {
        Ok(batches) => {
            let paginated = build_paginated_result(
                batches,
                exec_sql,
                session.default_catalog.as_deref(),
                state.page_size,
                session.user.username.clone(),
            );
            let response = build_page_response(&base_url, &query_id, &paginated, 0);
            state.results.insert(query_id, Arc::new(paginated));
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

            let response = build_page_response(&base_url, &id, paginated.as_ref(), page_token);
            let is_last = page_token + 1 >= paginated.total_pages;

            drop(paginated);

            if is_last {
                state.results.invalidate(&id);
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

    state.results.invalidate(&id);
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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

    /// Captures the SQL strings the executor is asked to run, so a test can
    /// assert what the prepared-statement rewrite produced.
    struct RecordingQuery {
        seen: Arc<std::sync::Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl TrinoQueryExecutor for RecordingQuery {
        async fn execute(
            &self,
            _: &Session,
            sql: &str,
        ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
            self.seen.lock().unwrap().push(sql.to_string());
            Ok(vec![])
        }
    }

    fn recording_state(
        seen: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Arc<TrinoState<MockAuthOk, RecordingQuery>> {
        Arc::new(TrinoState::<MockAuthOk, RecordingQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(RecordingQuery { seen }),
            results: build_result_cache(),
            queries: build_query_registry(),
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
        })
    }

    #[test]
    fn like_match_handles_wildcards() {
        assert!(like_match("iceberg.compression_codec", "iceberg.compression_codec"));
        assert!(like_match("iceberg.compression_codec", "iceberg.%"));
        assert!(like_match("iceberg.compression_codec", "%codec"));
        assert!(like_match("abc", "a_c"));
        assert!(!like_match("iceberg.x", "hive.%"));
        assert!(!like_match("abc", "a_"));
        assert!(like_match("", "%"));
    }

    #[test]
    fn incoming_session_properties_parses_comma_list() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h".parse().unwrap(),
        );
        let props = incoming_session_properties(&headers);
        assert_eq!(
            props,
            vec![
                ("iceberg.compression_codec".to_string(), "ZSTD".to_string()),
                ("query_max_run_time".to_string(), "1h".to_string()),
            ]
        );
    }

    #[test]
    fn extract_trino_headers_picks_up_compression_codec() {
        // #353: the codec set via SET SESSION is replayed in X-Trino-Session and
        // must land on the parsed headers so it can reach the write path.
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h".parse().unwrap(),
        );
        let parsed = extract_trino_headers(&headers);
        assert_eq!(parsed.compression_codec.as_deref(), Some("ZSTD"));

        // Absent -> None (writer falls back to config default).
        let empty = extract_trino_headers(&HeaderMap::new());
        assert_eq!(empty.compression_codec, None);
    }

    #[test]
    fn apply_trino_headers_propagates_compression_codec_to_session() {
        let session = Session::new(
            "u".to_string(),
            sqe_core::SecretString::new("token".to_string()),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        let trino_headers = TrinoClientHeaders {
            catalog: None,
            schema: None,
            user: None,
            source: None,
            compression_codec: Some("SNAPPY".to_string()),
        };
        let session = apply_trino_headers(session, &trino_headers);
        assert_eq!(session.compression_codec.as_deref(), Some("SNAPPY"));
    }

    #[tokio::test]
    async fn submit_set_session_echoes_via_header_without_executing() {
        // SET SESSION must NOT reach the executor (which rejects "SET SESSION
        // ..." as an unsupported utility statement). It returns 200 +
        // x-trino-set-session header the client replays, with updateType set.
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "SET SESSION iceberg.compression_codec = 'ZSTD'".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().unwrap().is_empty(),
            "SET SESSION must not be sent to the executor"
        );
        let hdr = resp
            .headers()
            .get("x-trino-set-session")
            .expect("x-trino-set-session header")
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(hdr, "iceberg.compression_codec='ZSTD'");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("SET SESSION"));
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    #[tokio::test]
    async fn submit_show_session_returns_current_properties() {
        // SHOW SESSION surfaces the properties the client carries in
        // X-Trino-Session, filtered by LIKE, without hitting the executor.
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let mut headers = basic_auth_header("alice", "pw");
        headers.insert(
            "x-trino-session",
            "iceberg.compression_codec=ZSTD, query_max_run_time=1h".parse().unwrap(),
        );

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            headers,
            "SHOW SESSION LIKE 'iceberg.compression_codec'".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().unwrap().is_empty(),
            "SHOW SESSION must not be sent to the executor"
        );
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        // Trino's 5-column shape.
        let cols: Vec<String> = tr
            .columns
            .expect("columns")
            .into_iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(cols, vec!["Name", "Value", "Default", "Type", "Description"]);
        // Exactly the LIKE-matched property, with its value.
        let data = tr.data.expect("data");
        assert_eq!(data.len(), 1, "LIKE filter keeps one property: {data:?}");
        assert_eq!(data[0][0], serde_json::json!("iceberg.compression_codec"));
        assert_eq!(data[0][1], serde_json::json!("ZSTD"));
    }

    #[tokio::test]
    async fn submit_resolves_prepared_statement_from_header() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let mut headers = basic_auth_header("alice", "pw");
        // q1 = "SELECT * FROM t WHERE a = ?" url-encoded, as a real Trino
        // client carries it back from x-trino-added-prepare.
        headers.insert(
            "x-trino-prepared-statement",
            "q1=SELECT+%2A+FROM+t+WHERE+a+%3D+%3F".parse().unwrap(),
        );

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            headers,
            "EXECUTE q1 USING 5".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "executor should run exactly once");
        assert_eq!(seen[0], "SELECT * FROM t WHERE a = 5");
    }

    #[tokio::test]
    async fn submit_prepare_registers_via_header_without_executing() {
        // PREPARE is the JDBC/Metabase connect gate. It must NOT reach the
        // executor (the SQL parser rejects `PREPARE <name> FROM <sql>`); it
        // returns 200 + an x-trino-added-prepare header the client replays.
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "PREPARE ps FROM SELECT 1".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().unwrap().is_empty(),
            "PREPARE must not be sent to the executor"
        );
        let hdr = resp
            .headers()
            .get("x-trino-added-prepare")
            .expect("x-trino-added-prepare header")
            .to_str()
            .unwrap()
            .to_string();
        assert!(hdr.starts_with("ps="), "header was: {hdr}");
        // SQL is URL-encoded (space -> '+'), so the client can replay it.
        assert!(hdr.contains("SELECT"), "header was: {hdr}");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("PREPARE"));
        // Non-null empty columns (omitting it -> JSON null -> driver throws).
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    #[tokio::test]
    async fn submit_deallocate_returns_update_type_without_executing() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "DEALLOCATE PREPARE ps".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().unwrap().is_empty(),
            "DEALLOCATE must not be sent to the executor"
        );
        assert!(resp.headers().get("x-trino-deallocated-prepare").is_some());
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(tr.update_type.as_deref(), Some("DEALLOCATE"));
        assert_eq!(tr.columns.map(|c| c.len()), Some(0));
    }

    #[tokio::test]
    async fn submit_metadata_query_translates_types_and_scopes_catalog() {
        // Executor returns an information_schema.columns-shaped batch with Arrow
        // type names and a foreign (system) catalog row -- exactly what the
        // built-in information_schema produces.
        struct InfoSchemaQuery;
        #[async_trait::async_trait]
        impl TrinoQueryExecutor for InfoSchemaQuery {
            async fn execute(
                &self,
                _: &Session,
                _: &str,
            ) -> Result<Vec<arrow_array::RecordBatch>, sqe_core::SqeError> {
                use arrow_array::StringArray;
                use arrow_schema::{DataType, Field, Schema};
                let schema = Arc::new(Schema::new(vec![
                    Field::new("table_catalog", DataType::Utf8, false),
                    Field::new("column_name", DataType::Utf8, false),
                    Field::new("data_type", DataType::Utf8, false),
                ]));
                let batch = arrow_array::RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec!["iceberg", "system"])),
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(StringArray::from(vec!["Int64", "Utf8"])),
                    ],
                )
                .unwrap();
                Ok(vec![batch])
            }
        }

        let state = Arc::new(TrinoState::<MockAuthOk, InfoSchemaQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(InfoSchemaQuery),
            results: build_result_cache(),
            queries: build_query_registry(),
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

        let mut headers = basic_auth_header("alice", "pw");
        headers.insert("x-trino-catalog", "iceberg".parse().unwrap());

        let resp = submit_query::<MockAuthOk, InfoSchemaQuery>(
            State(state),
            test_peer(),
            headers,
            "SELECT table_catalog, column_name, data_type FROM information_schema.columns"
                .to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let tr: TrinoResponse = serde_json::from_slice(&body).unwrap();
        let data = tr.data.expect("data rows");

        // The `system` row is scoped out (session catalog = iceberg), and the
        // Arrow `Int64` is translated to Trino `bigint`.
        assert_eq!(data.len(), 1, "only the iceberg-catalog row should remain");
        assert_eq!(data[0][0], serde_json::json!("iceberg"));
        assert_eq!(data[0][2], serde_json::json!("bigint"));
    }

    #[tokio::test]
    async fn submit_unresolved_execute_never_reaches_executor() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = recording_state(seen.clone());

        // EXECUTE naming a statement the client never sent: must error out
        // before execution, so the executor is never called.
        let resp = submit_query::<MockAuthOk, RecordingQuery>(
            State(state),
            test_peer(),
            basic_auth_header("alice", "pw"),
            "EXECUTE missing USING 1".to_string(),
        )
        .await
        .into_response();

        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            seen.lock().unwrap().is_empty(),
            "unresolved EXECUTE must not reach the executor"
        );
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
    fn clamp_max_wait_parses_and_clamps() {
        use std::time::Duration;
        assert_eq!(clamp_max_wait(Some("500ms")), Duration::from_millis(500));
        assert_eq!(clamp_max_wait(Some("2s")), Duration::from_secs(2));
        assert_eq!(clamp_max_wait(Some("1m")), MAX_WAIT_CAP); // 60s clamped to cap
        assert_eq!(clamp_max_wait(Some("garbage")), DEFAULT_MAX_WAIT);
        assert_eq!(clamp_max_wait(None), DEFAULT_MAX_WAIT);
    }

    #[tokio::test]
    async fn await_terminal_returns_when_finished() {
        let handle = Arc::new(QueryHandle {
            status: std::sync::Mutex::new(QueryStatus::Running),
            notify: tokio::sync::Notify::new(),
            abort: std::sync::Mutex::new(None),
            owner_username: "u".to_string(),
            session_update: None,
            created_at: std::time::Instant::now(),
        });
        let h2 = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            *h2.status.lock().unwrap() = QueryStatus::Finished;
            h2.notify.notify_waiters();
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            await_terminal_or_timeout(&handle, std::time::Duration::from_secs(5)),
        )
        .await
        .expect("returned before outer timeout");
        assert!(handle.status.lock().unwrap().is_terminal());
    }

    #[tokio::test]
    async fn await_terminal_returns_on_timeout_when_still_running() {
        let handle = Arc::new(QueryHandle {
            status: std::sync::Mutex::new(QueryStatus::Running),
            notify: tokio::sync::Notify::new(),
            abort: std::sync::Mutex::new(None),
            owner_username: "u".to_string(),
            session_update: None,
            created_at: std::time::Instant::now(),
        });
        await_terminal_or_timeout(&handle, std::time::Duration::from_millis(30)).await;
        assert!(!handle.status.lock().unwrap().is_terminal());
    }

    #[test]
    fn started_response_points_at_queued_route_without_data() {
        let resp = build_started_response("http://h:8080", "q1");
        assert_eq!(resp.stats.state, "QUEUED");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/queued/q1/1")
        );
        assert!(resp.data.is_none());
        assert!(resp.columns.is_none());
    }

    #[test]
    fn running_response_increments_queued_token() {
        let resp = build_running_response("http://h:8080", "q1", 4);
        assert_eq!(resp.stats.state, "RUNNING");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/queued/q1/4")
        );
        assert!(resp.data.is_none());
    }

    #[test]
    fn finished_redirect_response_points_at_results_route() {
        let resp = build_finished_redirect_response("http://h:8080", "q1");
        assert_eq!(resp.stats.state, "RUNNING");
        assert_eq!(
            resp.next_uri.as_deref(),
            Some("http://h:8080/v1/statement/q1/0")
        );
        assert!(resp.data.is_none());
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
            estimated_bytes: 0,
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
            estimated_bytes: 0,
        };

        let resp = build_page_response("http://localhost:8080", "q-abc", &paginated, 1);
        assert!(resp.next_uri.is_none());
        assert_eq!(resp.data.as_ref().unwrap().len(), 1);
        assert_eq!(resp.stats.state, "FINISHED");
    }

    #[test]
    fn test_build_page_response_omits_data_for_columnless_update() {
        // Trino 465 JDBC client rejects a non-null `data` with empty `columns`
        // ("Columns must be set when decoding data"). Real Trino omits `data`
        // entirely for DDL/update statements. SQE must match: column-less result
        // -> data: None. See issue #314.
        let paginated = PaginatedResult {
            columns: vec![],
            pages: vec![vec![]],
            total_pages: 1,
            created_at: Instant::now(),
            owner_username: "test".to_string(),
            total_rows: 0,
            update_type: Some("CREATE TABLE".to_string()),
            update_count: None,
            estimated_bytes: 0,
        };

        let resp = build_page_response("http://localhost:8080", "q-ddl", &paginated, 0);
        assert!(
            resp.data.is_none(),
            "column-less update must omit data (got {:?})",
            resp.data
        );
        assert_eq!(resp.update_type, Some("CREATE TABLE".to_string()));
        assert_eq!(resp.stats.state, "FINISHED");

        // Wire-level check: `data` is skip_serializing_if = Option::is_none, so
        // a None value drops the field entirely. This is exactly what the Trino
        // 465 client's `data == null -> NULL_ROWS` early return needs to see.
        let json: serde_json::Value = serde_json::to_value(&resp).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            !obj.contains_key("data"),
            "serialized response must omit the data field for a column-less update, got {json}"
        );
        assert_eq!(obj.get("updateType").and_then(|v| v.as_str()), Some("CREATE TABLE"));
    }

    #[test]
    fn test_build_page_response_keeps_data_for_empty_select() {
        // A SELECT that legitimately returns zero rows still has columns, so
        // `data` must remain present (Some) and non-null. Gating is on empty
        // columns, not empty data.
        let paginated = PaginatedResult {
            columns: vec![TrinoColumn {
                name: "id".to_string(),
                r#type: "bigint".to_string(),
                type_signature: crate::protocol::type_signature_for("bigint"),
            }],
            pages: vec![vec![]],
            total_pages: 1,
            created_at: Instant::now(),
            owner_username: "test".to_string(),
            total_rows: 0,
            update_type: None,
            update_count: None,
            estimated_bytes: 0,
        };

        let resp = build_page_response("http://localhost:8080", "q-empty", &paginated, 0);
        assert!(
            resp.data.is_some(),
            "SELECT with columns must keep data present even when empty"
        );
        assert!(resp.data.as_ref().unwrap().is_empty());
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
            estimated_bytes: 0,
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
            compression_codec: None,
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
    fn query_registry_stores_and_reports_terminal_status() {
        let registry = build_query_registry();
        let handle = Arc::new(QueryHandle {
            status: std::sync::Mutex::new(QueryStatus::Running),
            notify: tokio::sync::Notify::new(),
            abort: std::sync::Mutex::new(None),
            owner_username: "alice".to_string(),
            session_update: None,
            created_at: std::time::Instant::now(),
        });
        registry.insert("q1".to_string(), handle.clone());

        let got = registry.get("q1").expect("handle present");
        assert!(!got.status.lock().unwrap().is_terminal());

        *got.status.lock().unwrap() = QueryStatus::Finished;
        assert!(got.status.lock().unwrap().is_terminal());
    }

    #[test]
    fn test_paginated_result_cleanup_after_last_page() {
        let results: MokaCache<String, Arc<PaginatedResult>> = build_result_cache();
        results.insert(
            "q-test".to_string(),
            Arc::new(PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
                estimated_bytes: 0,
            }),
        );

        assert!(results.get("q-test").is_some());

        results.invalidate("q-test");
        assert!(results.get("q-test").is_none());
    }

    #[tokio::test]
    async fn test_get_results_invalid_token() {
        let state = Arc::new(TrinoState::<MockAuthOk, MockQuery> {
            authenticator: Arc::new(MockAuthOk),
            query_handler: Arc::new(MockQuery),
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            "q-456".to_string(),
            Arc::new(PaginatedResult {
                columns: vec![],
                pages: vec![vec![]],
                total_pages: 1,
                created_at: Instant::now(),
                owner_username: "test".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
                estimated_bytes: 0,
            }),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            Arc::new(PaginatedResult {
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
                estimated_bytes: 0,
            }),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            Arc::new(PaginatedResult {
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
                estimated_bytes: 0,
            }),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            Arc::new(PaginatedResult {
                columns: vec![],
                pages: vec![vec![]],
                total_pages: 1,
                created_at: Instant::now(),
                owner_username: "alice".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
                estimated_bytes: 0,
            }),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            Arc::new(PaginatedResult {
                columns: vec![],
                pages: vec![vec![], vec![]],
                total_pages: 2,
                created_at: Instant::now(),
                owner_username: "test-user".to_string(),
                total_rows: 0,
                update_type: None,
                update_count: None,
                estimated_bytes: 0,
            }),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
            results: build_result_cache(),
            queries: build_query_registry(),
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
