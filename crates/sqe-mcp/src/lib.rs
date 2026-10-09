//! SQE Model Context Protocol (MCP) Streamable HTTP adapter.
//!
//! This crate is deliberately a resource server, not an OAuth proxy. Every
//! request carries the user's bearer token; SQE validates it and places the
//! same token in the query `Session`, preserving user -> MCP -> SQE -> source
//! identity and authorization.

use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use tracing::Instrument;
use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sqe_auth::{AuthProvider, FlightCredentials};
use sqe_core::{
    config::{McpAuthMode, McpConfig, McpPrincipalConfig},
    SecretString, Session, SqeError, SqeErrorCode,
};
use sqe_sql::StatementKind;

pub const PROTOCOL_VERSION: &str = "2025-11-25";
/// How long a client may serve `tools/list` from its cache (one hour).
pub const TOOLS_LIST_TTL_MS: u64 = 3_600_000;

#[async_trait]
pub trait QueryExecutor: Send + Sync + 'static {
    /// A statement the caller asked for (`execute_sql_readonly`, `sample_rows`,
    /// the change tool). The coordinator charges it to the per-user query
    /// limiter.
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError>;

    /// A `SHOW` / `EXPLAIN` a metadata tool issues on the caller's behalf,
    /// possibly hundreds per call (`search_tables` walks one `SHOW TABLES`
    /// per schema). Not charged per statement: the tool call was admitted
    /// once by [`Self::admit_metadata_call`]. Defaults to `execute` for
    /// executors without a limiter.
    async fn execute_metadata(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        self.execute(session, sql).await
    }

    /// One charge per metadata tool call (`rate_limit.metadata_per_user_per_minute`
    /// in the coordinator). `Err` is the denial the tool reports.
    fn admit_metadata_call(&self, session: &Session) -> Result<(), SqeError> {
        let _ = session;
        Ok(())
    }
}

/// Tools whose statements are metadata reads on the caller's behalf: admitted
/// once per call, their `SHOW`s never charged to the query limiter. Everything
/// else (`execute_sql_readonly`, `sample_rows`, the change tool,
/// `refresh_metadata`) is a statement the caller asked for.
fn is_metadata_tool(name: &str) -> bool {
    matches!(
        name,
        "list_catalogs"
            | "list_schemas"
            | "list_tables"
            | "describe_table"
            | "describe_schema"
            | "get_table_ddl"
            | "get_table_stats"
            | "search_tables"
            | "explain_query"
    )
}

/// The `Mcp-Session-Id` header (MCP Streamable HTTP, 2025-11-25).
pub const SESSION_ID_HEADER: &str = "mcp-session-id";

/// Longest client label kept from `clientInfo` or `User-Agent`.
const CLIENT_LABEL_MAX: usize = 64;
/// Client sessions the registry keeps before evicting the least recently
/// seen one. Each entry is a few dozen bytes; this is a bound, not a budget.
const CLIENT_SESSIONS_MAX: usize = 4096;
/// A client session that has been quiet this long is dropped on the next
/// eviction pass. Agents that keep talking are refreshed on every call.
const CLIENT_SESSION_IDLE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub struct McpServerState {
    auth: Arc<dyn AuthProvider>,
    executor: Arc<dyn QueryExecutor>,
    config: McpConfig,
    /// Static-mode principals with their tokens already resolved from
    /// `token_env` / `token_b64`. Resolution happens once, here, rather than
    /// per request. A principal whose token cannot be resolved is DROPPED
    /// (fail closed) and logged; config validation refuses that case at
    /// startup, so the server path never sees it.
    principals: Arc<Vec<StaticPrincipal>>,
    /// `Mcp-Session-Id` -> the client that introduced itself on `initialize`.
    /// Advisory only: an unknown or missing id never fails a request, it just
    /// leaves the audit label at the `User-Agent` fallback. Every other part
    /// of the server stays stateless.
    clients: Arc<ClientRegistry>,
    /// `[[hive.roots]]` catalog names. A change statement targeting one of
    /// these runs on the engine's storage credentials, not the caller's, so
    /// it needs the catalog named in `write_catalogs` explicitly.
    hive_roots: Arc<Vec<String>>,
    /// `refresh_metadata` cooldown: user + scope -> last refresh (issue
    /// #666). A model that loops on "table missing, refresh, table missing"
    /// must not turn into a metastore flood; inside the window the tool
    /// answers "recently refreshed" without touching a cache.
    refreshes: Arc<Mutex<HashMap<String, Instant>>>,
    /// Resolves the engine's object-store credentials once and reports the
    /// outcome, for `list_catalogs` diagnostics. `None` when the embedding
    /// has no storage to probe (tests, a catalog with no S3 behind it).
    storage_probe: Option<StorageProbe>,
    /// `Retry-After` for a pre-auth denial: the pre-auth limiter's answer
    /// carries no wait time (its error is the same for every adapter), so
    /// this is the bucket's refill interval, `60 / auth_attempts_per_minute`
    /// rounded up, which is the longest a client can have to wait.
    auth_retry_after: Duration,
}

/// What a storage-credential probe reports. Mirrors
/// `sqe_catalog::aws_config::StorageCredentialReport` without a dependency
/// on that crate: `sqe-mcp` is SQL-only, the coordinator wires the probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCredentialReport {
    /// `ok`, `auth_error`, or `unknown`.
    pub status: &'static str,
    /// `aws-sdk-default-chain`, `static-keys`, `static-temporary-keys`.
    pub provider: &'static str,
    /// RFC 3339, when the provider knows when the credentials lapse.
    pub expires_at: Option<String>,
    /// The failure text on `auth_error`.
    pub detail: Option<String>,
}

/// A probe of the engine's object-store credentials.
pub type StorageProbe =
    Arc<dyn Fn() -> futures::future::BoxFuture<'static, StorageCredentialReport> + Send + Sync>;

/// Least time between two `refresh_metadata` calls for the same user and
/// scope.
const REFRESH_COOLDOWN: Duration = Duration::from_secs(10);

/// What a client said about itself on `initialize`, bound to the identity
/// that said it AND the address it said it from. The binding stops one
/// caller from relabelling another caller's audit trail by replaying a
/// session id it observed; the address matters in the modes where every
/// caller is the same configured user (#680).
struct ClientSession {
    user: String,
    peer: String,
    label: String,
    seen: Instant,
}

#[derive(Default)]
struct ClientRegistry {
    inner: Mutex<HashMap<String, ClientSession>>,
}

impl ClientRegistry {
    fn insert(&self, user: &str, peer: &str, label: String) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.len() >= CLIENT_SESSIONS_MAX {
            let now = Instant::now();
            inner.retain(|_, session| now.duration_since(session.seen) < CLIENT_SESSION_IDLE);
            if inner.len() >= CLIENT_SESSIONS_MAX {
                if let Some(oldest) = inner
                    .iter()
                    .min_by_key(|(_, session)| session.seen)
                    .map(|(id, _)| id.clone())
                {
                    inner.remove(&oldest);
                }
            }
        }
        inner.insert(
            id.clone(),
            ClientSession {
                user: user.to_string(),
                peer: peer.to_string(),
                label,
                seen: Instant::now(),
            },
        );
        id
    }

    /// The label for `id`, only if `user` at `peer` is who registered it.
    fn label(&self, id: &str, user: &str, peer: &str) -> Option<String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = inner.get_mut(id)?;
        if session.user != user || session.peer != peer {
            return None;
        }
        session.seen = Instant::now();
        Some(session.label.clone())
    }

    fn remove(&self, id: &str, user: &str, peer: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner
            .get(id)
            .is_some_and(|session| session.user == user && session.peer == peer)
        {
            inner.remove(id);
        }
    }
}

/// Reduce a client-supplied string to something safe to put in a log line and
/// an audit record: printable ASCII minus quotes and backslashes, at most
/// [`CLIENT_LABEL_MAX`] bytes, or `None` when nothing survives.
fn sanitize_label(raw: &str) -> Option<String> {
    let label: String = raw
        .chars()
        .filter(|c| c.is_ascii_graphic() || *c == ' ')
        .filter(|c| !matches!(c, '"' | '\'' | '\\'))
        .take(CLIENT_LABEL_MAX)
        .collect();
    let label = label.trim().to_string();
    (!label.is_empty()).then_some(label)
}

/// `name/version` from an `initialize` request's `clientInfo`, sanitized.
fn client_label_from_client_info(params: &Value) -> Option<String> {
    let info = params.get("clientInfo")?;
    let name = info.get("name").and_then(Value::as_str)?;
    let version = info.get("version").and_then(Value::as_str);
    let raw = match version {
        Some(version) if !version.trim().is_empty() => format!("{name}/{version}"),
        _ => name.to_string(),
    };
    sanitize_label(&raw)
}

/// The `Session.source` value for one tool call: `mcp:<tool>` plus the client
/// label when one is known. This is what the audit trail and `system.runtime`
/// show as the initiating application, so an operator can tell an AI agent's
/// queries from a JDBC client's, and one agent from another.
fn source_label(tool: &str, client: Option<&str>) -> String {
    match client {
        Some(client) => format!("mcp:{tool}:{client}"),
        None => format!("mcp:{tool}"),
    }
}

/// One static-mode principal: an opaque bearer plus the identity it grants.
struct StaticPrincipal {
    token: SecretString,
    user: String,
    roles: Vec<String>,
    default_catalog: Option<String>,
    default_schema: Option<String>,
}

impl StaticPrincipal {
    fn resolve(config: &McpPrincipalConfig) -> Option<Self> {
        match config.resolve_token() {
            Ok(token) => Some(Self {
                token,
                user: config.user.clone(),
                roles: config.roles.clone(),
                default_catalog: config.default_catalog.clone(),
                default_schema: config.default_schema.clone(),
            }),
            Err(message) => {
                tracing::error!(
                    "MCP static principal disabled: {message} (config validation should have \
                     refused this at startup)"
                );
                None
            }
        }
    }
}

impl McpServerState {
    pub fn new(
        auth: Arc<dyn AuthProvider>,
        executor: Arc<dyn QueryExecutor>,
        config: McpConfig,
    ) -> Self {
        let principals: Arc<Vec<StaticPrincipal>> = Arc::new(if config.auth.mode == McpAuthMode::Static {
            config
                .auth
                .principals
                .iter()
                .filter_map(StaticPrincipal::resolve)
                .collect()
        } else {
            Vec::new()
        });
        Self {
            auth,
            executor,
            config,
            principals,
            clients: Arc::new(ClientRegistry::default()),
            hive_roots: Arc::new(Vec::new()),
            refreshes: Arc::new(Mutex::new(HashMap::new())),
            storage_probe: None,
            auth_retry_after: Duration::from_secs(6),
        }
    }

    /// Attach the engine's storage-credential probe; `list_catalogs`
    /// reports its result as an `object_store` diagnostics entry.
    pub fn with_storage_probe(mut self, probe: StorageProbe) -> Self {
        self.storage_probe = Some(probe);
        self
    }

    /// Record a `refresh_metadata` call for `key` unless one happened less
    /// than [`REFRESH_COOLDOWN`] ago; returns how long ago that was.
    fn refresh_cooldown_hit(&self, key: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut map = self
            .refreshes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(last) = map.get(key) {
            let since = now.duration_since(*last);
            if since < REFRESH_COOLDOWN {
                return Some(since);
            }
        }
        // Keep the map bounded: entries older than the window are useless.
        map.retain(|_, at| now.duration_since(*at) < REFRESH_COOLDOWN);
        map.insert(key.to_string(), now);
        None
    }

    /// The `[[hive.roots]]` catalog names; see `McpConfig::write_catalogs`.
    pub fn with_hive_roots(mut self, roots: Vec<String>) -> Self {
        self.hive_roots = Arc::new(roots);
        self
    }

    /// `Retry-After` for a pre-auth denial, from `rate_limit.auth_attempts_per_minute`.
    pub fn with_auth_rate(mut self, attempts_per_minute: u32) -> Self {
        self.auth_retry_after = Duration::from_secs(60u64.div_ceil(u64::from(attempts_per_minute.max(1))));
        self
    }
}

/// Apply the endpoint-wide default catalog and schema, overridden per static
/// principal. Set on the session, so the same defaults reach the engine's
/// front door (`information_schema` qualification), name resolution, and the
/// change tool's target check.
fn with_defaults(
    session: Session,
    config: &McpConfig,
    principal: Option<&StaticPrincipal>,
) -> Session {
    let catalog = principal
        .and_then(|p| p.default_catalog.clone())
        .or_else(|| config.default_catalog.clone());
    let schema = principal
        .and_then(|p| p.default_schema.clone())
        .or_else(|| config.default_schema.clone());
    session.with_catalog(catalog).with_schema(schema)
}

pub fn router(state: McpServerState) -> Router {
    let mut router = Router::new().route("/mcp", post(mcp_post).get(mcp_get).delete(mcp_delete));
    // Protected-resource metadata describes an authorization server. Static and
    // anonymous modes have none, and metadata pointing at a server that does
    // not exist sends clients chasing it.
    if state.config.auth.mode == McpAuthMode::Oauth {
        router = router
            .route(
                "/.well-known/oauth-protected-resource",
                get(resource_metadata),
            )
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(resource_metadata),
            );
    }
    router
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(state)
}

async fn resource_metadata(State(state): State<McpServerState>) -> impl IntoResponse {
    Json(json!({
        "resource": state.config.resource_uri,
        "authorization_servers": state.config.authorization_servers,
        "bearer_methods_supported": ["header"],
        "scopes_supported": supported_scopes(&state.config),
        "resource_documentation": "https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization"
    }))
}

fn supported_scopes(config: &McpConfig) -> Vec<&str> {
    let mut scopes = vec![config.read_scope.as_str()];
    if config.allow_writes {
        scopes.push(config.write_scope.as_str());
    }
    scopes
}

async fn mcp_get(
    State(state): State<McpServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if !origin_allowed(&state.config, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    match authenticate(&state, &headers, peer.ip().to_string()).await {
        Ok(_) => (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "POST, DELETE")],
            "This SQE MCP server is stateless and does not expose a server event stream",
        )
            .into_response(),
        Err(response) => response,
    }
}

async fn mcp_delete(
    State(state): State<McpServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    if !origin_allowed(&state.config, &headers) {
        return StatusCode::FORBIDDEN.into_response();
    }
    match authenticate(&state, &headers, peer.ip().to_string()).await {
        Ok(auth) => {
            if let Some(id) = session_id(&headers) {
                state
                    .clients
                    .remove(id, &auth.session.user.username, &peer.ip().to_string());
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(response) => response,
    }
}

fn session_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(SESSION_ID_HEADER)?
        .to_str()
        .ok()
        .filter(|id| !id.is_empty())
}

/// The client label for this request: what the client said on `initialize`
/// if it echoes the `Mcp-Session-Id` we issued and is still the same identity,
/// else the sanitized `User-Agent`, else nothing.
fn client_label(
    state: &McpServerState,
    headers: &HeaderMap,
    user: &str,
    peer: &str,
) -> Option<String> {
    session_id(headers)
        .and_then(|id| state.clients.label(id, user, peer))
        .or_else(|| {
            headers
                .get(header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .and_then(sanitize_label)
        })
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Clone)]
struct AuthContext {
    session: Session,
    /// OAuth scopes from the caller's token, or `None` in the modes that have
    /// no external issuer. `None` means "gate on roles alone", never
    /// "everything is permitted".
    scopes: Option<BTreeSet<String>>,
}

async fn mcp_post(
    State(state): State<McpServerState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if !origin_allowed(&state.config, &headers) {
        tracing::warn!("Rejected MCP request from an unapproved Origin");
        return StatusCode::FORBIDDEN.into_response();
    }
    let auth = match authenticate(&state, &headers, peer.ip().to_string()).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let request: JsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => return rpc_error(None, -32700, format!("Parse error: {error}")),
    };
    if request.method == "tools/call"
        && request.params.get("name").and_then(Value::as_str) == Some("execute_sql_change")
        && auth
            .scopes
            .as_ref()
            .is_some_and(|scopes| !scopes.contains(&state.config.write_scope))
    {
        return auth_error_for_scope(
            &state.config,
            StatusCode::FORBIDDEN,
            Some("insufficient_scope"),
            &format!("{} {}", state.config.read_scope, state.config.write_scope),
        );
    }
    if request.method == "notifications/initialized" {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request.id.clone();
    let user = auth.session.user.username.as_str();
    let mut issued_session_id = None;
    // One span per request, child of the caller's `traceparent` when it sent
    // one: the statement span below it stamps the same trace id on the audit
    // record, and the response carries it back, so an agent's question and
    // the engine's record of what ran can be correlated.
    let span = tracing::info_span!(
        "mcp.request",
        method = %request.method,
        user = %user,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
    );
    sqe_metrics_core::propagation::adopt_trace_context_from_headers(&span, &headers);
    let (result, trace_id) = async {
        let result = dispatch(
            &state,
            &auth,
            &headers,
            &request,
            &peer.ip().to_string(),
            &mut issued_session_id,
        )
        .await;
        (result, sqe_metrics_core::propagation::current_trace_id())
    }
    .instrument(span)
    .await;
    // Without an OTel layer no span context is valid; a fresh id still lets
    // the client refer to this exchange, it just correlates with nothing.
    let trace_id = trace_id.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
    let mut response = match result {
        Ok(value) => rpc_result(id, with_trace_meta(value, &trace_id)),
        Err((code, message)) => rpc_error(id, code, message),
    };
    if let Some(value) = issued_session_id
        .as_deref()
        .and_then(|id| HeaderValue::from_str(id).ok())
    {
        response.headers_mut().insert(SESSION_ID_HEADER, value);
    }
    response
}

/// `result._meta.trace_id`: the W3C trace id of this exchange (MCP reserves
/// `_meta` on results for exactly this kind of side channel).
fn with_trace_meta(mut value: Value, trace_id: &str) -> Value {
    if let Some(object) = value.as_object_mut() {
        let meta = object.entry("_meta").or_insert_with(|| json!({}));
        if let Some(meta) = meta.as_object_mut() {
            meta.insert("trace_id".to_string(), json!(trace_id));
        }
    }
    value
}

async fn dispatch(
    state: &McpServerState,
    auth: &AuthContext,
    headers: &HeaderMap,
    request: &JsonRpcRequest,
    peer_ip: &str,
    issued_session_id: &mut Option<String>,
) -> Result<Value, (i32, String)> {
    let user = auth.session.user.username.as_str();
    match request.method.as_str() {
        "initialize" => {
            // Remember who is talking so every later call can be attributed
            // to this client in the audit trail. Nothing else hangs off the id.
            let label = client_label_from_client_info(&request.params)
                .or_else(|| client_label(state, headers, user, peer_ip))
                .unwrap_or_else(|| "unknown".to_string());
            tracing::info!(user, client = %label, "MCP client initialized");
            *issued_session_id = Some(state.clients.insert(user, peer_ip, label));
            Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "sqe", "version": sqe_core::VERSION },
                "instructions": SERVER_INSTRUCTIONS
            }))
        }
        "ping" => Ok(json!({})),
        // The tool set is fixed per process (config, not caller), so the
        // spec's cache hints (SEP-2549, 2026-07-28) tell a client to stop
        // re-listing on every turn: an hour, cacheable by anyone in between.
        // Older clients ignore the two fields.
        "tools/list" => Ok(json!({
            "tools": tools(&state.config),
            "ttlMs": TOOLS_LIST_TTL_MS,
            "cacheScope": "public",
        })),
        "tools/call" => {
            let client = client_label(state, headers, user, peer_ip);
            call_tool(state, auth, client.as_deref(), &request.params).await
        }
        _ => Err((-32601, format!("Method not found: {}", request.method))),
    }
}

fn origin_allowed(config: &McpConfig, headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    config
        .allowed_origins
        .iter()
        .any(|allowed| allowed == origin)
}

// The Err variant is a ready-to-send axum Response, which is large by
// construction; boxing it here would only move the allocation.
#[allow(clippy::result_large_err)]
async fn authenticate(
    state: &McpServerState,
    headers: &HeaderMap,
    peer_ip: String,
) -> Result<AuthContext, Response> {
    // The pre-auth rate limit (#562) lives in the provider chain: the
    // coordinator wraps it in a limiter keyed on `(peer_ip, bearer
    // fingerprint)`, so OAuth mode carries the peer address in the
    // credentials and maps the denial to 429 below. Static and anonymous
    // modes never reach a provider: the static compare is constant-time
    // against a local list and costs no IdP round trip, which is what the
    // limiter exists to cap.
    let auth = match state.config.auth.mode {
        McpAuthMode::Oauth => authenticate_oauth(state, headers, &peer_ip).await,
        McpAuthMode::Static => authenticate_static(state, headers),
        McpAuthMode::Anonymous => Ok(authenticate_anonymous(state)),
    }?;
    // The coordinator keys the query rate limit on `(peer, user)` from this
    // (#680): in static and anonymous mode every caller is one user.
    Ok(AuthContext {
        session: auth.session.with_peer_ip(Some(peer_ip)),
        ..auth
    })
}

/// Match the caller's bearer against the configured static principals.
///
/// The comparison lives here rather than in the auth chain on purpose: a
/// deployment with no IdP typically ends its chain in the `anonymous`
/// provider, which accepts any credential, so delegating a static token to the
/// chain would let a WRONG token authenticate.
// The Err variant is a ready-to-send axum Response, which is large by
// construction; boxing it here would only move the allocation.
#[allow(clippy::result_large_err)]
fn authenticate_static(
    state: &McpServerState,
    headers: &HeaderMap,
) -> Result<AuthContext, Response> {
    let Some(token) = bearer(headers) else {
        return Err(auth_error(&state.config, StatusCode::UNAUTHORIZED, None));
    };
    let matched = state.principals.iter().find(|principal| {
        let expected = principal.token.expose().as_bytes();
        let presented = token.as_bytes();
        expected.len() == presented.len()
            && bool::from(subtle::ConstantTimeEq::ct_eq(expected, presented))
    });
    let Some(principal) = matched else {
        tracing::warn!("MCP static bearer did not match any configured principal");
        return Err(auth_error(
            &state.config,
            StatusCode::UNAUTHORIZED,
            Some("invalid_token"),
        ));
    };
    Ok(AuthContext {
        session: with_defaults(
            local_session(principal.user.clone(), principal.roles.clone()),
            &state.config,
            Some(principal),
        ),
        scopes: None,
    })
}

/// Accept every caller as the one configured identity.
fn authenticate_anonymous(state: &McpServerState) -> AuthContext {
    let anonymous = &state.config.auth.anonymous;
    AuthContext {
        session: with_defaults(
            local_session(anonymous.user.clone(), anonymous.roles.clone()),
            &state.config,
            None,
        ),
        scopes: None,
    }
}

/// A session for an identity SQE itself resolved, with no upstream token.
///
/// The catalog token is deliberately empty: these modes exist because the
/// source is reached with engine credentials, and the MCP bearer is a
/// credential for SQE, not something to forward upstream. Matches what the
/// coordinator already builds for the anonymous SQL path.
fn local_session(user: String, roles: Vec<String>) -> Session {
    Session::new(
        user,
        SecretString::new(String::new()),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        roles,
    )
}

// The Err variant is a ready-to-send axum Response, which is large by
// construction; boxing it here would only move the allocation.
#[allow(clippy::result_large_err)]
async fn authenticate_oauth(
    state: &McpServerState,
    headers: &HeaderMap,
    peer_ip: &str,
) -> Result<AuthContext, Response> {
    let Some(token) = bearer(headers) else {
        return Err(auth_error(&state.config, StatusCode::UNAUTHORIZED, None));
    };
    let credentials = FlightCredentials {
        bearer_token: Some(SecretString::new(token.to_string())),
        peer_ip: Some(peer_ip.to_string()),
        ..Default::default()
    };
    let identity = state
        .auth
        .authenticate_oauth(&credentials)
        .await
        .map_err(|error| match error {
            // #562: denied by the pre-auth limiter before any provider ran.
            sqe_auth::AuthError::RateLimited => {
                let secs = state.auth_retry_after.as_secs().max(1);
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(header::RETRY_AFTER, secs.to_string())],
                    Json(json!({
                        "error": "rate_limited",
                        "limiter": "auth",
                        "window_seconds": 60,
                        "retry_after_seconds": secs,
                        "knobs": ["rate_limit.auth_attempts_per_minute", "rate_limit.auth_burst"],
                        "detail": "too many credential presentations from this peer; wait and resend the same request",
                    })),
                )
                    .into_response()
            }
            error => {
                tracing::warn!(error = %error, "MCP bearer authentication failed");
                auth_error(
                    &state.config,
                    StatusCode::UNAUTHORIZED,
                    Some("invalid_token"),
                )
            }
        })?;
    // #676: the validated provider knows the scopes (JWT claims, introspection).
    // Only decode the payload ourselves when the provider left them empty.
    let scopes = if identity.scopes.is_empty() {
        token_scopes(token)
    } else {
        Some(identity.scopes.iter().cloned().collect())
    };
    let scopes = scopes.ok_or_else(|| {
        tracing::warn!("MCP bearer has no readable OAuth scope/scp claim");
        auth_error(
            &state.config,
            StatusCode::FORBIDDEN,
            Some("insufficient_scope"),
        )
    })?;
    if !scopes.contains(&state.config.read_scope) {
        return Err(auth_error(
            &state.config,
            StatusCode::FORBIDDEN,
            Some("insufficient_scope"),
        ));
    }
    // Shared identity-to-session mapping (finding 204); the raw MCP bearer
    // is the catalog token when the provider vended none.
    let session = with_defaults(identity.into_session(Some(token)), &state.config, None);
    Ok(AuthContext {
        session,
        scopes: Some(scopes),
    })
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
}

fn token_scopes(token: &str) -> Option<BTreeSet<String>> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let mut out = BTreeSet::new();
    for name in ["scope", "scp"] {
        match claims.get(name) {
            Some(Value::String(value)) => out.extend(value.split_whitespace().map(str::to_string)),
            Some(Value::Array(values)) => {
                out.extend(values.iter().filter_map(Value::as_str).map(str::to_string))
            }
            _ => {}
        }
    }
    (!out.is_empty()).then_some(out)
}

fn auth_error(config: &McpConfig, status: StatusCode, error: Option<&str>) -> Response {
    auth_error_for_scope(config, status, error, &config.read_scope)
}

fn auth_error_for_scope(
    config: &McpConfig,
    status: StatusCode,
    error: Option<&str>,
    scope: &str,
) -> Response {
    let mut challenge = if config.auth.mode == McpAuthMode::Oauth {
        let metadata = format!(
            "{}/.well-known/oauth-protected-resource/mcp",
            config
                .resource_uri
                .trim_end_matches("/mcp")
                .trim_end_matches('/')
        );
        format!("Bearer resource_metadata=\"{metadata}\", scope=\"{scope}\"")
    } else {
        // No authorization server to discover, and no OAuth scope to ask for:
        // the caller either holds a configured token or does not.
        "Bearer".to_string()
    };
    if let Some(error) = error {
        challenge.push_str(&format!(", error=\"{error}\""));
    }
    let mut response = (
        status,
        Json(json!({ "error": error.unwrap_or("unauthorized") })),
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&challenge) {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, value);
    }
    response
}

/// What `initialize` tells an agent about this server. Under four sentences
/// on purpose: the first live agent made 21 `describe_table` calls where 3
/// `describe_schema` calls would have done, because nothing pointed it there.
const SERVER_INSTRUCTIONS: &str = "Governed SQL access as the authenticated user: source grants, row filters and column masks apply to every statement, and file table functions (read_parquet, read_csv, read_json) are refused. \
To find tables use search_tables, then describe_schema: it returns every table of a schema with columns, table_type and queryable in one paged call; describe_table is for one table you already chose. \
Rows of list_tables and search_tables carry table_type (table or view). \
The change tool is separately gated by OAuth scope, SQE role and source-data authorization.";

fn tools(config: &McpConfig) -> Vec<Value> {
    let mut tools = vec![
        tool("execute_sql_readonly", "Execute exactly one read-only SQL statement against catalog tables. SQE parses and classifies the statement before execution; source grants, row filters, and column masks apply as the authenticated user. File table functions (read_parquet, read_csv, read_json) are refused: they bypass the catalog. A query without its own LIMIT is capped at the server's max_result_rows; when `truncated` is true, total_rows means 'more than the cap', so aggregate or add LIMIT instead of paging. Do not scan information_schema across a whole catalog: over a Glue catalog it enumerates every database first (135 databases took 12 s cold for one call and client timeouts for columns). Use describe_schema per schema and search_tables for names instead.", json!({"type":"object","properties":{"sql":{"type":"string"}},"required":["sql"],"additionalProperties":false}), true, false),
        tool("list_catalogs", "List catalogs visible to the authenticated user. `diagnostics` carries one entry per Hive root (Glue, sidecar) with `status` reachable | auth_error | timeout | unreachable and a `detail`: auth_error means the engine's own AWS credentials are missing or expired, which no retry from the client side can fix; the detail carries an error_id the operator can find in the server log.", empty_schema(), true, false),
        tool("list_schemas", "List schemas visible to the authenticated user in a catalog.", one_string_schema("catalog"), true, false),
        tool("list_tables", "List tables and views visible to the authenticated user in a catalog and schema; each row carries table_type (table or view). To see columns for all of them at once use describe_schema.", two_string_schema("catalog", "schema"), true, false),
        tool("describe_table", "Return columns for one table you already chose. To explore a schema use describe_schema, which returns the columns of every table in one paged call.", three_string_schema(), true, false),
        tool("describe_schema", "Describe every table and view in one schema in a single call, for catalog crawlers: per table the columns describe_table would return (same policy filtering), the table kind (table or view) and source (iceberg, hive_external, view), optionally the metastore's row count and size (`include_stats`: Iceberg snapshot summary or Glue/Hive table parameters, never a scan) and whether the table resolves and plans (`include_queryability`: `EXPLAIN SELECT * ... LIMIT 0`, table resolution and planning only, no scan and no storage listing; storage reachability is the `object_store` diagnostics entry of list_catalogs). Paged by table name: `page_size` defaults to 200 (at most 500); pass back `next_page_token` for the next page. One table failing does not fail the call; its `error`, `stats_error` or `queryability_error` says why.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"include_stats":{"type":"boolean"},"include_queryability":{"type":"boolean"},"page_token":{"type":"string"},"page_size":{"type":"integer","minimum":1,"maximum":500}},"required":["catalog","schema"],"additionalProperties":false}), true, false),
        tool("get_table_ddl", "Return SHOW CREATE TABLE output for a visible table.", three_string_schema(), true, false),
        tool("get_table_stats", "Return Iceberg table statistics visible to the authenticated user. Hive external tables have no snapshot statistics and return an error; use describe_table for them.", three_string_schema(), true, false),
        tool("search_tables", "Find tables by name across the schemas of one catalog (SHOW SCHEMAS, then SHOW TABLES per schema, matched server-side). `pattern` is a SQL LIKE pattern (% and _ are wildcards; a bare word matches as a substring, case-insensitive). `catalog` defaults to the session's default catalog. Only tables visible to the authenticated user are returned; each row carries table_type (table or view). Paged by (schema, table): `page_size` defaults to 200 (at most 500); pass back `next_page_token` for the next page. `skipped_schemas` names every schema that could not be listed and why; nothing is dropped silently. Follow up with describe_schema on the schemas that matched.", json!({"type":"object","properties":{"pattern":{"type":"string"},"catalog":{"type":"string"},"page_token":{"type":"string"},"page_size":{"type":"integer","minimum":1,"maximum":500}},"required":["pattern"],"additionalProperties":false}), true, false),
        tool("sample_rows", "Return up to `limit` rows (default 10, at most 100) of a table, as the authenticated user: row filters and column masks apply. Prefer this over SELECT * for a first look at a table.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"table":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["catalog","schema","table"],"additionalProperties":false}), true, false),
        tool("explain_query", "Explain a read-only SQL query without allowing a write statement to be hidden behind EXPLAIN.", one_string_schema("sql"), true, false),
        tool("whoami", "Show the SQE identity, roles, groups, and OAuth scopes for this request; never returns tokens.", empty_schema(), true, false),
        tool("refresh_metadata", "Drop cached catalog metadata so the next call re-reads it from the source. Use ONLY when a table or column you expect is missing, or a schema looks stale after a change outside SQE; routine queries never need it. Scope it as narrowly as you can: `table` (with `catalog` and `schema`) refreshes one table, `schema` (with `catalog`) one schema, nothing refreshes this session's catalog view. Rate-limited per scope; a repeat inside the window is a no-op that says so.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"table":{"type":"string"}},"additionalProperties":false}), false, false),
    ];
    if config.allow_writes {
        tools.push(tool("execute_sql_change", "Execute one data or DDL statement: INSERT, UPDATE, DELETE, MERGE, CREATE/DROP/ALTER TABLE, SCHEMA or VIEW, TRUNCATE, ANALYZE. Grants, policies, secrets, catalogs, session settings and procedures are refused here and go through Flight SQL or Trino HTTP. Requires the configured OAuth write scope and SQE writer role; the target catalog must be one the operator allowed for MCP writes; source catalog and storage authorization still apply as the authenticated user.", one_string_schema("sql"), false, true));
    }
    tools
}

fn tool(
    name: &str,
    description: &str,
    input_schema: Value,
    read_only: bool,
    destructive: bool,
) -> Value {
    json!({
        "name": name,
        "title": name.replace('_', " "),
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": destructive,
            "idempotentHint": read_only,
            "openWorldHint": false
        }
    })
}

fn empty_schema() -> Value {
    json!({"type":"object","properties":{},"additionalProperties":false})
}
fn one_string_schema(name: &str) -> Value {
    json!({"type":"object","properties":{name:{"type":"string"}},"required":[name],"additionalProperties":false})
}
fn two_string_schema(a: &str, b: &str) -> Value {
    json!({"type":"object","properties":{a:{"type":"string"},b:{"type":"string"}},"required":[a,b],"additionalProperties":false})
}
fn three_string_schema() -> Value {
    json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"table":{"type":"string"}},"required":["catalog","schema","table"],"additionalProperties":false})
}

async fn call_tool(
    state: &McpServerState,
    auth: &AuthContext,
    client: Option<&str>,
    params: &Value,
) -> Result<Value, (i32, String)> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| (-32602, "Missing tool name".into()))?;
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if name == "whoami" {
        return Ok(tool_success(json!({
            "user": auth.session.user.username,
            "roles": auth.session.user.roles,
            "subject": auth.session.user.subject,
            "email": auth.session.user.email,
            "groups": auth.session.user.groups,
            "oauth_scopes": auth.scopes,
            "client": client,
        })));
    }
    if is_metadata_tool(name) {
        if let Err(error) = state.executor.admit_metadata_call(&auth.session) {
            return Ok(tool_failure(error.to_string()));
        }
    }
    let session_catalog = auth.session.default_catalog.as_deref();
    if name == "list_catalogs" {
        let session = auth
            .session
            .clone()
            .with_source(Some(source_label(name, client)));
        return list_catalogs_with_diagnostics(state, &session).await;
    }
    if name == "search_tables" {
        let session = auth
            .session
            .clone()
            .with_source(Some(source_label(name, client)));
        return search_tables(state, &session, &args).await.map(tool_success);
    }
    if name == "list_tables" {
        let session = auth
            .session
            .clone()
            .with_source(Some(source_label(name, client)));
        return Ok(match list_tables(state, &session, &args).await {
            Ok(value) => tool_success(value),
            Err((-32602, message)) => return Err((-32602, message)),
            Err((_, message)) => tool_failure(with_refresh_hint(name, message)),
        });
    }
    if name == "describe_schema" {
        let session = auth
            .session
            .clone()
            .with_source(Some(source_label(name, client)));
        return describe_schema(state, &session, &args)
            .await
            .map(tool_success);
    }
    let mut show_catalogs = false;
    let sql = match name {
        "execute_sql_readonly" => {
            let sql = required(&args, "sql")?;
            let kind = ensure_read_only(sql, session_catalog).map_err(|e| (-32602, e))?;
            show_catalogs = matches!(kind, StatementKind::ShowCatalogs);
            with_row_cap(sql, &kind, state.config.max_result_rows)
        }
        "execute_sql_change" => {
            authorize_write(state, auth)?;
            let sql = required(&args, "sql")?;
            let kind = ensure_change(sql, session_catalog).map_err(|e| (-32602, e))?;
            authorize_write_target(state, &auth.session, &kind)?;
            sql.to_string()
        }
        "list_schemas" => format!("SHOW SCHEMAS FROM {}", ident(required(&args, "catalog")?)),
        "describe_table" => format!("SHOW COLUMNS FROM {}", table_ref(&args)?),
        "get_table_ddl" => format!("SHOW CREATE TABLE {}", table_ref(&args)?),
        "get_table_stats" => format!("SHOW STATS FOR {}", table_ref(&args)?),
        "sample_rows" => format!(
            "SELECT * FROM {} LIMIT {}",
            table_ref(&args)?,
            sample_limit(&args)?
        ),
        "explain_query" => {
            let inner = required(&args, "sql")?;
            ensure_read_only(inner, session_catalog).map_err(|e| (-32602, e))?;
            format!("EXPLAIN {inner}")
        }
        "refresh_metadata" => {
            // Narrowest scope the arguments allow (issue #666). `catalog`
            // falls back to the session default so `schema` alone works the
            // way `search_tables` does; a table without a schema is an
            // argument error, not a guess.
            let optional = |key: &str| {
                args.get(key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
            };
            let catalog = optional("catalog").or(session_catalog);
            let (sql, scope) = match (optional("schema"), optional("table")) {
                (Some(schema), Some(table)) => {
                    let Some(catalog) = catalog else {
                        return Err((-32602, "refresh_metadata with `table` needs `catalog` (or a session default catalog)".to_string()));
                    };
                    let qualified = format!("{catalog}.{schema}.{table}");
                    (
                        format!(
                            "CALL system.refresh_catalog_cache(table => '{}')",
                            qualified.replace('\'', "''")
                        ),
                        format!("table:{qualified}"),
                    )
                }
                (None, Some(_)) => {
                    return Err((-32602, "refresh_metadata with `table` also needs `schema`".to_string()));
                }
                (Some(schema), None) => {
                    let Some(catalog) = catalog else {
                        return Err((-32602, "refresh_metadata with `schema` needs `catalog` (or a session default catalog)".to_string()));
                    };
                    let qualified = format!("{catalog}.{schema}");
                    (
                        format!(
                            "CALL system.refresh_catalog_cache(schema => '{}')",
                            qualified.replace('\'', "''")
                        ),
                        format!("schema:{qualified}"),
                    )
                }
                (None, None) => (
                    "CALL system.refresh_catalog_cache()".to_string(),
                    "session".to_string(),
                ),
            };
            let key = format!("{}|{scope}", auth.session.user.username);
            if let Some(since) = state.refresh_cooldown_hit(&key) {
                return Ok(tool_success(json!({
                    "scope": scope,
                    "status": "skipped",
                    "reason": format!(
                        "refreshed {}s ago; wait {}s before refreshing this scope again",
                        since.as_secs(),
                        REFRESH_COOLDOWN.as_secs()
                    ),
                })));
            }
            sql
        }
        _ => return Err((-32602, format!("Unknown tool: {name}"))),
    };
    // Tag the session with the tool and client so the audit trail and
    // `system.runtime` show WHICH agent ran WHAT through MCP, not just the user.
    let session = auth
        .session
        .clone()
        .with_source(Some(source_label(name, client)));
    let result = if is_metadata_tool(name) {
        state.executor.execute_metadata(&session, &sql).await
    } else {
        state.executor.execute(&session, &sql).await
    };
    match result {
        Ok(batches) => {
            let batches = if show_catalogs {
                strip_phantom_catalogs(batches, &state.hive_roots)
            } else {
                batches
            };
            Ok(tool_success(batches_json(
                &batches,
                state.config.max_result_rows,
                state.config.max_response_bytes,
            )))
        }
        Err(error) => Ok(tool_failure(with_decimal_hint(with_refresh_hint(
            name,
            client_error_text(&error),
        )))),
    }
}

/// What the model is told when the engine fails (#675). A user error (bad
/// SQL, a name that does not exist, a denial) is about the caller's own
/// statement and goes through verbatim, as it does on Flight and Trino. A
/// system error (object store, catalog, execution) carries bucket names,
/// object keys, hostnames and region or account hints, and an agent
/// transcript is a leaky place for those: the client gets the code's
/// generic sentence and an `error_id`, and the full text goes to the server
/// log at WARN under that id. Two system texts stay verbatim because their
/// hints are the fix: the Arrow decimal overflow (value and precision only,
/// see `with_decimal_hint`) and a rate-limit denial (retry-after).
fn client_error_text(error: &SqeError) -> String {
    let full = error.to_string();
    let code = error.error_code();
    let lower = full.to_ascii_lowercase();
    if code.is_user_error()
        || code == SqeErrorCode::ResourceExhausted
        || lower.starts_with("rate limit")
        || lower.contains("too large to store in a decimal")
    {
        return full;
    }
    let error_id = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
    tracing::warn!(
        error_id,
        error_code = code.name(),
        error = %full,
        "MCP tool call failed; detail withheld from the client"
    );
    format!(
        "{} [{}, error_id {error_id}]. The server log has the detail under that id.",
        code.generic_message(),
        code.name()
    )
}

/// A Parquet value that does not fit the decimal the metastore declares for
/// its column surfaces as Arrow's bare "1029.23 is too large to store in a
/// Decimal128 of precision 5. Max is 999.99", which names neither the table
/// nor the column and reads like an engine bug. It is a metastore
/// definition narrower than the data (Athena refuses the same row). The
/// hint says so, names the declared precision so the column can be found
/// with `describe_table`, and points at the fix, which is not on the client.
fn with_decimal_hint(message: String) -> String {
    let lower = message.to_ascii_lowercase();
    let Some(at) = lower.find("too large to store in a decimal") else {
        return message;
    };
    let precision = lower[at..]
        .split("precision ")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .filter(|p| !p.is_empty())
        .map(|p| format!(" (declared precision {p})"))
        .unwrap_or_default();
    format!(
        "{message}\nThe table's metastore definition declares a decimal column{precision} \
         narrower than the values its data files hold. This is a definition error, not a \
         query error: widen the column's decimal type in the metastore (Glue: ALTER TABLE ... \
         CHANGE COLUMN). describe_table shows which column carries that precision."
    )
}

/// How long `list_catalogs` waits on each Hive root's probe before calling
/// it `timeout`. Bounded because a metastore that hangs must not hang the
/// one tool an agent calls first.
const CATALOG_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// `list_catalogs`: `SHOW CATALOGS`, plus one `diagnostics` entry per Hive
/// root the caller can see. Each root is probed with `SHOW SCHEMAS FROM
/// <root>` (one metastore call, run concurrently, bounded by
/// [`CATALOG_PROBE_TIMEOUT`]) and reported as `reachable`, `auth_error`
/// (the engine's AWS credentials are missing or expired), `timeout`, or
/// `unreachable`, with the redacted error text of `client_error_text` as
/// `detail` (#675). An Iceberg catalog is listed without a probe: its
/// authorization is per user and `list_schemas` answers for it. Without
/// this, an expired SSO session surfaced as `dispatch failure` on the second
/// tool call and as empty listings after that, and a sidecar had no way to
/// show "the catalog is fine, the engine's credentials are not".
async fn list_catalogs_with_diagnostics(
    state: &McpServerState,
    session: &Session,
) -> Result<Value, (i32, String)> {
    let batches = match state.executor.execute_metadata(session, "SHOW CATALOGS").await {
        Ok(batches) => strip_phantom_catalogs(batches, &state.hive_roots),
        Err(error) => return Ok(tool_failure(error.to_string())),
    };
    let visible = first_column_strings(&batches);
    let roots: Vec<String> = state
        .hive_roots
        .iter()
        .filter(|root| visible.iter().any(|v| v.eq_ignore_ascii_case(root)))
        .cloned()
        .collect();
    let diagnostics: Vec<Value> = {
        use futures::StreamExt;
        futures::stream::iter(roots)
            .map(|root| async move {
                let sql = format!("SHOW SCHEMAS FROM {}", ident(&root));
                let probe = tokio::time::timeout(
                    CATALOG_PROBE_TIMEOUT,
                    state.executor.execute_metadata(session, &sql),
                )
                .await;
                let (status, detail) = match probe {
                    Ok(Ok(_)) => ("reachable", None),
                    Ok(Err(error)) => {
                        // Classified on the full text, reported with the
                        // redacted one (#675): the region and the SSO detail
                        // are in the server log under the error_id.
                        let status = if error
                            .to_string()
                            .contains("AWS credentials missing or expired")
                        {
                            "auth_error"
                        } else {
                            "unreachable"
                        };
                        (status, Some(client_error_text(&error)))
                    }
                    Err(_) => (
                        "timeout",
                        Some(format!(
                            "no answer from the metastore within {}s",
                            CATALOG_PROBE_TIMEOUT.as_secs()
                        )),
                    ),
                };
                json!({
                    "catalog": root,
                    "kind": "hive_external",
                    "status": status,
                    "detail": detail,
                })
            })
            .buffered(4)
            .collect()
            .await
    };
    let mut diagnostics = diagnostics;
    // The engine's own object-store credentials, the one failure that takes
    // every scan down while `SELECT 1` and the metadata tools keep working.
    // Bounded like the root probes; a chain that hangs reports `timeout`.
    if let Some(probe) = &state.storage_probe {
        let entry = match tokio::time::timeout(CATALOG_PROBE_TIMEOUT, probe()).await {
            // Status and a reason only (#765): the provider kind and the
            // expiry timestamp describe the engine's credential setup, which
            // an agent transcript has no use for.
            Ok(report) => json!({
                "kind": "object_store",
                "status": report.status,
                "detail": report.detail.or_else(|| {
                    report.expires_at.is_some().then(|| {
                        "temporary credentials; the SDK renews them while the session lasts"
                            .to_string()
                    })
                }),
            }),
            Err(_) => json!({
                "kind": "object_store",
                "status": "timeout",
                "detail": format!(
                    "no answer from the credential provider within {}s",
                    CATALOG_PROBE_TIMEOUT.as_secs()
                ),
            }),
        };
        diagnostics.push(entry);
    }
    let mut body = batches_json(
        &batches,
        state.config.max_result_rows,
        state.config.max_response_bytes,
    );
    if let Value::Object(map) = &mut body {
        map.insert("diagnostics".to_string(), Value::Array(diagnostics));
    }
    Ok(tool_success(body))
}

/// Catalog names `SHOW CATALOGS` lists that no statement can resolve (#706):
/// the engine's placeholder for an unnamed warehouse and DataFusion's own
/// defaults. `list_schemas` on them fails with "unknown catalog", so an
/// agent that walks the list trips on the first entry. A Hive root with one
/// of these names is real and kept.
// ponytail: a REST catalog literally named `default` is dropped too; pass
// the configured names into the state if a deployment ever does that.
const PHANTOM_CATALOGS: [&str; 3] = ["default", "datafusion", "system"];

fn strip_phantom_catalogs(batches: Vec<RecordBatch>, hive_roots: &[String]) -> Vec<RecordBatch> {
    let is_phantom = |name: &str| {
        PHANTOM_CATALOGS
            .iter()
            .any(|p| name.eq_ignore_ascii_case(p))
            && !hive_roots.iter().any(|r| name.eq_ignore_ascii_case(r))
    };
    batches
        .into_iter()
        .filter_map(|batch| {
            let names = first_column_strings(std::slice::from_ref(&batch));
            if names.len() != batch.num_rows() || !names.iter().any(|n| is_phantom(n)) {
                return Some(batch);
            }
            let kept: Vec<&str> = names
                .iter()
                .map(String::as_str)
                .filter(|n| !is_phantom(n))
                .collect();
            let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
            columns[0] = Arc::new(arrow_array::StringArray::from(kept));
            RecordBatch::try_new(batch.schema(), columns).ok()
        })
        .collect()
}

/// A not-found answer from a lookup tool gets one line pointing at
/// `refresh_metadata` (issue #666): a table created or altered outside SQE
/// can sit behind a cached listing until its TTL, and the model would
/// otherwise retry the same lookup. Only the lookup tools, only for
/// not-found, so the hint never appears on a syntax error or a denial.
fn with_refresh_hint(tool: &str, message: String) -> String {
    let lookup = matches!(
        tool,
        "list_tables" | "describe_table" | "get_table_ddl" | "sample_rows"
    );
    let not_found = {
        let m = message.to_ascii_lowercase();
        m.contains("not found") || m.contains("does not exist")
    };
    if lookup && not_found {
        format!(
            "{message}\nIf this table or schema was created or changed recently, call \
             refresh_metadata for that scope and retry once."
        )
    } else {
        message
    }
}

/// Most rows `sample_rows` returns, whatever the caller asks for.
const SAMPLE_ROWS_MAX: u64 = 100;
const SAMPLE_ROWS_DEFAULT: u64 = 10;

/// `limit` for `sample_rows`: absent means the default, anything else is
/// clamped to `1..=SAMPLE_ROWS_MAX`. Accepts a JSON number or a numeric
/// string, since MCP clients differ on how they send integers.
fn sample_limit(args: &Value) -> Result<u64, (i32, String)> {
    let Some(value) = args.get("limit") else {
        return Ok(SAMPLE_ROWS_DEFAULT);
    };
    let limit = match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim().parse::<u64>().ok(),
        Value::Null => Some(SAMPLE_ROWS_DEFAULT),
        _ => None,
    }
    .ok_or_else(|| (-32602, "limit must be a positive integer".to_string()))?;
    if limit == 0 {
        return Err((-32602, "limit must be a positive integer".to_string()));
    }
    Ok(limit.min(SAMPLE_ROWS_MAX))
}

/// Most schemas `search_tables` walks in one catalog.
const SEARCH_SCHEMAS_MAX: usize = 200;
/// `SHOW TABLES` statements `search_tables` keeps in flight at once.
const SEARCH_SCHEMAS_CONCURRENCY: usize = 8;

/// Case-insensitive SQL `LIKE`: `%` matches any run, `_` one character, and
/// there is no escape. A pattern with neither wildcard matches as a
/// substring, which is what a caller typing a word means.
fn like_matches(pattern: &str, name: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let bare = !pattern.contains('%') && !pattern.contains('_');
    let p: Vec<char> = if bare {
        format!("%{pattern}%").chars().collect()
    } else {
        pattern.chars().collect()
    };
    let s: Vec<char> = name.to_lowercase().chars().collect();
    // Two pointers with backtrack to the last `%` (#739): the recursive
    // version tried every split per `%`, depth = pattern length.
    let (mut pi, mut si) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        match p.get(pi) {
            Some('%') => {
                star = Some((pi, si));
                pi += 1;
            }
            Some(c) if *c == '_' || *c == s[si] => {
                pi += 1;
                si += 1;
            }
            _ => match star {
                Some((sp, ss)) => {
                    pi = sp + 1;
                    si = ss + 1;
                    star = Some((sp, ss + 1));
                }
                None => return false,
            },
        }
    }
    p[pi..].iter().all(|c| *c == '%')
}

/// Longest `search_tables` pattern accepted; longer is an argument error.
const SEARCH_PATTERN_MAX_CHARS: usize = 256;

/// The first column of a `SHOW` result as strings, in row order.
fn first_column_strings(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            if let Some(value) =
                sqe_trino_protocol::encode::arrow_value_to_json(column.as_ref(), row).as_str()
            {
                out.push(value.to_string());
            }
        }
    }
    out
}

/// `list_tables`: `SHOW TABLES` plus `SHOW VIEWS` for one schema, so every row
/// says whether it is a table or a view without a second call per name. The
/// same two listings `describe_schema` runs; a catalog that cannot answer
/// `SHOW VIEWS` lists everything as a table (`table_kinds` is best effort).
async fn list_tables(
    state: &McpServerState,
    session: &Session,
    args: &Value,
) -> Result<Value, (i32, String)> {
    let catalog = required(args, "catalog")?;
    let schema = required(args, "schema")?;
    let sql = format!("SHOW TABLES FROM {}.{}", ident(catalog), ident(schema));
    let names = state.executor.execute_metadata(session, &sql)
        .await
        .map_err(|e| (-32000, e.to_string()))?;
    let kinds = table_kinds(state, session, catalog, schema).await;
    let rows: Vec<Value> = first_column_strings(&names)
        .into_iter()
        .take(state.config.max_result_rows)
        .map(|name| {
            json!({
                "table_name": name,
                "table_type": table_type_of(&kinds, &name),
            })
        })
        .collect();
    Ok(json!({
        "columns": [
            {"name": "table_name", "type": "Utf8"},
            {"name": "table_type", "type": "Utf8"},
        ],
        "rows": rows,
    }))
}

/// `"view"` when `kinds` (from `SHOW VIEWS`) lists the name, else `"table"`.
fn table_type_of(kinds: &HashMap<String, String>, name: &str) -> &'static str {
    if kinds.contains_key(&name.to_ascii_lowercase()) {
        "view"
    } else {
        "table"
    }
}

/// `search_tables`: walk `SHOW SCHEMAS` and `SHOW TABLES` for one catalog and
/// match names in the server.
///
/// Not `information_schema.tables`: a catalog-qualified
/// `<catalog>.information_schema.tables` is answered by DataFusion's built-in
/// listing, which knows the system schemas and whatever Iceberg tables the
/// session already loaded, and nothing else (measured against the Ranger
/// stack: 40 `information_schema` / `runtime` rows, no `ac.orders`). The
/// `SHOW` statements go to the catalog and through the same visibility
/// filtering as every other metadata read, so a table the caller may not see
/// is not in the answer. A schema the caller may not list is skipped, not an
/// error: it is not theirs to know about.
async fn search_tables(
    state: &McpServerState,
    session: &Session,
    args: &Value,
) -> Result<Value, (i32, String)> {
    let pattern = required(args, "pattern")?;
    if pattern.chars().count() > SEARCH_PATTERN_MAX_CHARS {
        return Err((
            -32602,
            format!("pattern is longer than {SEARCH_PATTERN_MAX_CHARS} characters"),
        ));
    }
    let catalog = args
        .get("catalog")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .or_else(|| session.default_catalog.clone())
        .ok_or_else(|| {
            (
                -32602,
                "search_tables needs a catalog: pass `catalog`, or configure mcp.default_catalog"
                    .to_string(),
            )
        })?;
    let schemas = state.executor.execute_metadata(session, &format!("SHOW SCHEMAS FROM {}", ident(&catalog)))
        .await
        .map_err(|e| (-32000, format!("listing schemas of {catalog}: {e}")))?;
    // One `SHOW TABLES` per schema, a bounded number in flight at once
    // (`buffered`, so results come back in schema order and the response is
    // deterministic). Serially, a Glue catalog of 135 databases took 17 s
    // for one search; each listing is a metastore round trip and nothing
    // here depends on the previous one.
    let page_size = page_size_arg(args)?.min(state.config.max_result_rows.max(1));
    let page_token = args
        .get("page_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(decode_search_token)
        .transpose()?;
    let mut all_schemas: Vec<String> = first_column_strings(&schemas)
        .into_iter()
        .filter(|s| !s.eq_ignore_ascii_case("information_schema"))
        .collect();
    all_schemas.sort();
    let mut skipped: Vec<Value> = all_schemas
        .iter()
        .skip(SEARCH_SCHEMAS_MAX)
        .map(|s| json!({"schema": s, "reason": format!("beyond the {SEARCH_SCHEMAS_MAX}-schema walk cap")}))
        .collect();
    let schema_names: Vec<String> = all_schemas.into_iter().take(SEARCH_SCHEMAS_MAX).collect();
    let listings: Vec<(String, Result<Vec<String>, String>)> = {
        use futures::StreamExt;
        futures::stream::iter(schema_names)
            .map(|schema| {
                let catalog = catalog.clone();
                async move {
                    let sql = format!("SHOW TABLES FROM {}.{}", ident(&catalog), ident(&schema));
                    let tables = match state.executor.execute_metadata(session, &sql).await {
                        Ok(batches) => Ok(first_column_strings(&batches)),
                        Err(error) => {
                            tracing::debug!(%catalog, %schema, %error, "search_tables: schema skipped");
                            Err(error.to_string())
                        }
                    };
                    (schema, tables)
                }
            })
            .buffered(SEARCH_SCHEMAS_CONCURRENCY)
            .collect()
            .await
    };
    // Every match, ordered by (schema, table): the page token is the last
    // pair of the previous page, so a table created between pages lands
    // where it sorts and is never skipped or repeated.
    let mut matches: Vec<(String, String)> = Vec::new();
    for (schema, tables) in listings {
        match tables {
            Ok(tables) => {
                let mut names: Vec<String> = tables.into_iter().filter(|t| like_matches(pattern, t)).collect();
                names.sort();
                names.dedup();
                matches.extend(names.into_iter().map(|t| (schema.clone(), t)));
            }
            Err(reason) => skipped.push(json!({"schema": schema, "reason": reason})),
        }
    }
    let total = matches.len();
    let start = page_token
        .as_ref()
        .map(|token| matches.partition_point(|m| m <= token))
        .unwrap_or(0);
    let page: Vec<(String, String)> = matches[start..].iter().take(page_size).cloned().collect();
    let more_after_page = start + page.len() < total;

    // One `SHOW VIEWS` per schema on this page, not per schema walked: a
    // search usually lands in a few schemas, and the kind is what lets an
    // agent tell a view from a table without a call per name.
    let mut kinds_by_schema: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut rows = Vec::with_capacity(page.len());
    for (schema, table) in &page {
        if !kinds_by_schema.contains_key(schema) {
            let kinds = table_kinds(state, session, &catalog, schema).await;
            kinds_by_schema.insert(schema.clone(), kinds);
        }
        rows.push(json!({
            "table_catalog": catalog,
            "table_schema": schema,
            "table_name": table,
            "table_type": table_type_of(&kinds_by_schema[schema], table),
        }));
    }
    let mut out = json!({
        "columns": [
            {"name": "table_catalog", "type": "Utf8"},
            {"name": "table_schema", "type": "Utf8"},
            {"name": "table_name", "type": "Utf8"},
            {"name": "table_type", "type": "Utf8"},
        ],
        "rows": rows,
        "total_rows": total,
        "returned_rows": page.len(),
        "truncated": more_after_page,
        "skipped_schemas": skipped,
    });
    if more_after_page {
        if let Some((schema, table)) = page.last() {
            out["next_page_token"] = json!(encode_search_token(schema, table));
        }
    }
    Ok(out)
}

/// `search_tables` page token: the `(schema, table)` pair the page ended on.
/// Opaque to the caller; the separator cannot occur in an identifier.
const SEARCH_TOKEN_SEPARATOR: char = '\u{1f}';

fn encode_search_token(schema: &str, table: &str) -> String {
    format!("{schema}{SEARCH_TOKEN_SEPARATOR}{table}")
}

fn decode_search_token(token: &str) -> Result<(String, String), (i32, String)> {
    token
        .split_once(SEARCH_TOKEN_SEPARATOR)
        .map(|(s, t)| (s.to_string(), t.to_string()))
        .ok_or_else(|| (-32602, "page_token is not one search_tables returned".to_string()))
}

/// `describe_schema` page bounds. A page is what one crawler request can
/// digest; the byte cap below still applies inside a page.
const DESCRIBE_SCHEMA_PAGE_DEFAULT: usize = 200;
const DESCRIBE_SCHEMA_PAGE_MAX: usize = 500;
/// Per-table statements in flight at once. Each is a metadata read against a
/// cache the coordinator already warmed with the listing, so this bounds
/// concurrent planner work, not network fan-out.
const DESCRIBE_SCHEMA_CONCURRENCY: usize = 8;
/// Deadline for one per-table statement inside `describe_schema` (columns,
/// stats, plan). Generous for a cold Glue table (a listing plus a manifest,
/// one to three seconds) and far below the 90 s a caller gives the whole
/// page; a statement that has not answered by then is not slow, it is stuck.
const DESCRIBE_SCHEMA_PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// `describe_schema`: one schema's tables and views, each described the way
/// the single-table tools would, in one call.
///
/// Composed of the SAME statements those tools issue, so nothing here is
/// looser than `describe_table`: the listing is `SHOW TABLES` (visibility
/// filtered), the columns are `SHOW COLUMNS FROM` per table (the policy
/// path that drops restricted columns on a Hive table), statistics are
/// `SHOW STATS FOR` (snapshot or metastore numbers, never a scan), and
/// queryability is `EXPLAIN SELECT * FROM t LIMIT 0` (table resolution and
/// planning only; the zero limit is eliminated before any scan, so no
/// object-store listing happens). The
/// table kind comes from one `SHOW VIEWS FROM <catalog>.<schema>` per call
/// and is best effort: a catalog that cannot answer it leaves every entry a
/// `table`. Per-table statements run with bounded concurrency; a statement
/// that fails is recorded on its table and the call goes on.
async fn describe_schema(
    state: &McpServerState,
    session: &Session,
    args: &Value,
) -> Result<Value, (i32, String)> {
    use futures::StreamExt;

    let catalog = required(args, "catalog")?.to_string();
    let schema = required(args, "schema")?.to_string();
    let include_stats = bool_arg(args, "include_stats")?;
    let include_queryability = bool_arg(args, "include_queryability")?;
    let page_size = page_size_arg(args)?;
    let page_token = args
        .get("page_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty());

    let listing = state.executor.execute_metadata(
            session,
            &format!("SHOW TABLES FROM {}.{}", ident(&catalog), ident(&schema)),
        )
        .await
        .map_err(|e| (-32000, format!("listing tables of {catalog}.{schema}: {e}")))?;
    let mut names = first_column_strings(&listing);
    names.sort();
    names.dedup();
    let total = names.len();
    // Pagination by name: the token is the last name of the previous page,
    // so a table created between pages lands where its name sorts and is
    // never skipped or repeated.
    let start = page_token
        .map(|token| names.partition_point(|n| n.as_str() <= token))
        .unwrap_or(0);
    let page: Vec<String> = names[start..].iter().take(page_size).cloned().collect();
    let more_after_page = start + page.len() < total;

    let kinds = table_kinds(state, session, &catalog, &schema).await;
    let source = if state
        .hive_roots
        .iter()
        .any(|r| r.eq_ignore_ascii_case(&catalog))
    {
        "hive_external"
    } else {
        "iceberg"
    };

    let mut entries: Vec<Value> = futures::stream::iter(page)
        .map(|name| {
            let kind = kinds
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
                .unwrap_or("table");
            describe_one_table(
                state,
                session,
                &catalog,
                &schema,
                name,
                kind,
                source,
                include_stats,
                include_queryability,
            )
        })
        .buffer_unordered(DESCRIBE_SCHEMA_CONCURRENCY)
        .collect()
        .await;
    entries.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));

    // The response cap applies inside a page too: drop tables from the end
    // until it fits, and point the next page at the first one dropped.
    let mut truncated = false;
    let fits = |entries: &[Value]| {
        serde_json::to_vec(&json!({"tables": entries}))
            .map(|v| v.len() <= state.config.max_response_bytes)
            .unwrap_or(false)
    };
    while entries.len() > 1 && !fits(&entries) {
        entries.pop();
        truncated = true;
    }
    let next_page_token = if more_after_page || truncated {
        entries.last().and_then(|e| e["name"].as_str()).map(str::to_string)
    } else {
        None
    };

    let mut out = json!({
        "catalog": catalog,
        "schema": schema,
        "tables": entries,
        "total_tables": total,
        "truncated": truncated || more_after_page,
    });
    if let Some(token) = next_page_token {
        out["next_page_token"] = json!(token);
    }
    Ok(out)
}

/// One table's entry. Every statement that fails lands in a field on the
/// entry rather than failing the call: a crawler wants the other 199 tables.
#[allow(clippy::too_many_arguments)]
async fn describe_one_table(
    state: &McpServerState,
    session: &Session,
    catalog: &str,
    schema: &str,
    name: String,
    kind: &str,
    source: &str,
    include_stats: bool,
    include_queryability: bool,
) -> Value {
    let table = format!("{}.{}.{}", ident(catalog), ident(schema), ident(&name));
    let is_view = kind == "view";
    let mut entry = json!({
        "name": name,
        "table_type": if is_view { "view" } else { "table" },
        "source": if is_view { "view" } else { source },
        "columns": [],
    });

    match probe(state, session, &format!("SHOW COLUMNS FROM {table}")).await {
        Ok(batches) => {
            let columns: Vec<Value> = rows_json(&batches)
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    let mut column = json!({
                        "name": row.get("column_name").cloned().unwrap_or(Value::Null),
                        "data_type": row.get("data_type").cloned().unwrap_or(Value::Null),
                        "nullable": row
                            .get("is_nullable")
                            .and_then(Value::as_str)
                            .map(|v| !v.eq_ignore_ascii_case("NO"))
                            .unwrap_or(true),
                        "ordinal": index + 1,
                    });
                    if let Some(comment) = row.get("comment").filter(|c| !c.is_null()) {
                        column["comment"] = comment.clone();
                    }
                    column
                })
                .collect();
            entry["columns"] = Value::Array(columns);
        }
        Err(error) => {
            // Already the client text: `probe` redacts (#675).
            entry["error"] = json!(error);
        }
    }

    // Each probe has its own deadline. A Hive table's stats and plan both
    // touch object storage (a listing, a manifest), and with the engine's
    // credentials expired those calls did not fail, they waited: one
    // describe_schema over 25 tables hung indefinitely with either flag on
    // while the flag-less call answered in 0.4 s. A probe past its deadline
    // records the timeout on its own field and the page goes on.
    if include_stats {
        entry["row_count"] = Value::Null;
        entry["size_bytes"] = Value::Null;
        entry["snapshot_id"] = Value::Null;
        match probe(state, session, &format!("SHOW STATS FOR {table}")).await {
            Ok(batches) => {
                // The summary row is the one with no column name.
                if let Some(summary) = rows_json(&batches)
                    .into_iter()
                    .find(|row| row.get("column_name").is_none_or(Value::is_null))
                {
                    entry["row_count"] = whole_number(summary.get("row_count"));
                    entry["size_bytes"] = whole_number(summary.get("data_size"));
                }
            }
            Err(error) => {
                entry["stats_error"] = json!(error);
            }
        }
    }

    if include_queryability {
        // LIMIT 0, not LIMIT 1: DataFusion resolves the table (which is
        // where a Hive table's format, SerDe, location and column types are
        // checked, and where a view's SQL is planned) and then eliminates the
        // zero limit before physical planning, so no scan is built and no
        // object-store listing happens. With LIMIT 1 every table paid an S3
        // LIST per call, half a second warm and a hang with expired
        // credentials; "queryable" means "SQE can resolve and plan it", and
        // storage reachability is the object_store entry of list_catalogs.
        match probe(state, session, &format!("EXPLAIN SELECT * FROM {table} LIMIT 0")).await {
            Ok(_) => entry["queryable"] = json!(true),
            Err(error) => {
                entry["queryable"] = json!(false);
                entry["queryability_error"] = json!(error);
            }
        }
    }
    entry
}

/// One per-table statement of `describe_schema`, bounded by
/// [`DESCRIBE_SCHEMA_PROBE_TIMEOUT`]. The error is the statement's own text,
/// or the deadline's, so the caller can put it on the table's field.
async fn probe(
    state: &McpServerState,
    session: &Session,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    match tokio::time::timeout(DESCRIBE_SCHEMA_PROBE_TIMEOUT, state.executor.execute_metadata(session, sql))
        .await
    {
        Ok(Ok(batches)) => Ok(batches),
        Ok(Err(error)) => Err(client_error_text(&error)),
        Err(_) => Err(format!(
            "probe timed out after {}s (the statement did not fail, it waited: with the \
             engine's storage credentials expired an object-store call can hang; check \
             list_catalogs diagnostics)",
            DESCRIBE_SCHEMA_PROBE_TIMEOUT.as_secs()
        )),
    }
}

/// `table_name -> "view"` for the views of one schema, lower-cased names,
/// from `SHOW VIEWS FROM <catalog>.<schema>`. Best effort: an empty map when
/// the catalog cannot answer, which the caller reads as "everything is a
/// table".
async fn table_kinds(
    state: &McpServerState,
    session: &Session,
    catalog: &str,
    schema: &str,
) -> HashMap<String, String> {
    // One listing of one schema. `information_schema.tables` answered the
    // same question by enumerating every schema of the catalog, which on a
    // Glue catalog of a hundred databases was 12 s cold for one call.
    let sql = format!("SHOW VIEWS FROM {}.{}", ident(catalog), ident(schema));
    match state.executor.execute_metadata(session, &sql).await {
        Ok(batches) => first_column_strings(&batches)
            .into_iter()
            .map(|name| (name.to_ascii_lowercase(), "view".to_string()))
            .collect(),
        Err(error) => {
            tracing::debug!(%catalog, %schema, %error, "describe_schema: table kinds unavailable");
            HashMap::new()
        }
    }
}

/// Rows as JSON objects keyed by column name, in batch order.
fn rows_json(batches: &[RecordBatch]) -> Vec<Map<String, Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut object = Map::new();
            for (index, field) in batch.schema().fields().iter().enumerate() {
                object.insert(
                    field.name().clone(),
                    sqe_trino_protocol::encode::arrow_value_to_json(batch.column(index).as_ref(), row),
                );
            }
            rows.push(object);
        }
    }
    rows
}

/// A statistic as a JSON integer when it is a whole number, else null:
/// `SHOW STATS` reports Float64, a crawler wants a count.
fn whole_number(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Number(n)) => n
            .as_f64()
            .filter(|f| f.is_finite() && *f >= 0.0)
            .map(|f| json!(f.round() as u64))
            .unwrap_or(Value::Null),
        Some(Value::String(s)) => s
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite() && *f >= 0.0)
            .map(|f| json!(f.round() as u64))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// An optional boolean argument; accepts JSON booleans and the strings
/// `true` / `false`, since MCP clients differ on how they send them.
fn bool_arg(args: &Value, name: &str) -> Result<bool, (i32, String)> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("true") => Ok(true),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => Err((-32602, format!("{name} must be a boolean"))),
    }
}

/// `page_size` for `describe_schema` and `search_tables`: absent means the default, anything
/// else is clamped to `1..=DESCRIBE_SCHEMA_PAGE_MAX`.
fn page_size_arg(args: &Value) -> Result<usize, (i32, String)> {
    let Some(value) = args.get("page_size") else {
        return Ok(DESCRIBE_SCHEMA_PAGE_DEFAULT);
    };
    let size = match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim().parse::<u64>().ok(),
        Value::Null => Some(DESCRIBE_SCHEMA_PAGE_DEFAULT as u64),
        _ => None,
    }
    .ok_or_else(|| (-32602, "page_size must be a positive integer".to_string()))?;
    if size == 0 {
        return Err((-32602, "page_size must be a positive integer".to_string()));
    }
    Ok((size as usize).min(DESCRIBE_SCHEMA_PAGE_MAX))
}

fn required<'a>(args: &'a Value, name: &str) -> Result<&'a str, (i32, String)> {
    args.get(name)
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| (-32602, format!("Missing non-empty argument: {name}")))
}

fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn table_ref(args: &Value) -> Result<String, (i32, String)> {
    if required(args, "catalog")?.eq_ignore_ascii_case("system") {
        return Err((-32602, "the system catalog is not an MCP surface".to_string()));
    }
    Ok(format!(
        "{}.{}.{}",
        ident(required(args, "catalog")?),
        ident(required(args, "schema")?),
        ident(required(args, "table")?)
    ))
}

/// Classify through the same chain the engine uses (finding 700): the
/// pre-parse pipeline strips the SQE clauses sqlparser cannot see, the front
/// door parses once with the source-level fallbacks and AST rewrites, and the
/// classifier reads that AST. The `session_catalog` lets a two-part
/// `information_schema.<table>` resolve the way it will at execution. The
/// ORIGINAL text is what the executor gets: the engine runs its own door on it,
/// and the pre-parse pipeline's output is classification input, not SQL to run
/// (it has the time-travel clauses stripped).
fn classify(sql: &str, session_catalog: Option<&str>) -> Result<StatementKind, String> {
    ensure_single_statement(sql)?;
    let classifiable =
        sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql)).map_err(|e| e.to_string())?;
    let normalized = sqe_sql::SqlFrontDoor::new(sqe_sql::WireDialect::Mcp)
        .with_session_catalog(session_catalog)
        .normalize(classifiable.as_str())
        .map_err(|e| e.to_string())?;
    sqe_sql::classify_normalized(normalized).map_err(|e| e.to_string())
}

/// One statement in the raw text, before the pre-parse pipeline rewrites it.
/// Text the engine's dialect reads differently from this one (#683) is
/// refused by the front door in `classify`, as on every other door.
fn ensure_single_statement(sql: &str) -> Result<(), String> {
    use sqlparser::{
        dialect::GenericDialect,
        tokenizer::{Token, Tokenizer},
    };
    let dialect = GenericDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql)
        .tokenize()
        .map_err(|error| format!("SQL tokenization failed: {error}"))?;
    tokens.retain(|token| !matches!(token, Token::Whitespace(_)));
    if matches!(tokens.last(), Some(Token::SemiColon)) {
        tokens.pop();
    }
    if tokens.iter().any(|token| matches!(token, Token::SemiColon)) {
        return Err("Exactly one SQL statement is allowed per MCP tool call".to_string());
    }
    Ok(())
}

/// Classify and require a read-only kind; returns the kind for the caller.
fn ensure_read_only(sql: &str, session_catalog: Option<&str>) -> Result<StatementKind, String> {
    // `classify` already refuses a Query hiding a write (SELECT INTO,
    // WITH-INSERT; `sqe_sql::hidden_write`).
    let kind = classify(sql, session_catalog)?;
    if !kind.is_read_only() {
        return Err(format!("Read-only tool rejected {} statement", kind.name()));
    }
    // FROM read_parquet(...) is a Query too, and skips catalog/Ranger
    // (#703 #704).
    if let Some(reason) = walk_query_shape(&kind).denied {
        return Err(format!("Read-only tool rejected {reason}"));
    }
    // SHOW CREATE names its target outside any TableFactor, so the visitor
    // above never sees it (#774): run the relation denylist on it directly.
    if let StatementKind::ShowCreateTable(stmt) = &kind {
        if let sqlparser::ast::Statement::ShowCreate { obj_name, .. } = stmt.as_ref() {
            if let Some(reason) = denied_relation(obj_name) {
                return Err(format!("Read-only tool rejected {reason}"));
            }
        }
    }
    // A trailing comment would swallow the appended LIMIT of `with_row_cap`,
    // so the engine collected the whole result (#775). Refuse it instead of
    // running uncapped.
    if matches!(kind, StatementKind::Query(_)) && ends_with_comment(sql) {
        return Err("Read-only tool rejected a statement ending in a comment; \
                    remove the trailing comment"
            .to_string());
    }
    Ok(kind)
}

/// The table functions a tool statement may call: the catalog-scoped Iceberg
/// metadata TVFs (`crates/sqe-catalog/src/iceberg_metadata_tvf.rs`), which
/// resolve through the catalog. Everything else with `(...)` in FROM is
/// refused (#766): `read_*`, `*_scan`, `quack_query` and whatever gets
/// registered next read paths with the engine's credentials.
const CATALOG_TABLE_FUNCTIONS: [&str; 6] = [
    "table_snapshots",
    "table_manifests",
    "table_history",
    "table_files",
    "table_partitions",
    "table_refs",
];

/// Scalar-position names that are still file readers (`SELECT read_csv('x')`).
fn is_file_reader_name(name: &str) -> bool {
    matches!(name, "read_parquet" | "read_csv" | "read_json" | "quack_query")
        || name.starts_with("read_")
        || name.ends_with("_scan")
}

fn object_name_last(name: &sqlparser::ast::ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
}

/// A table function call outside [`CATALOG_TABLE_FUNCTIONS`]: the reason text.
fn denied_table_function(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let last = object_name_last(name).unwrap_or_default();
    (!CATALOG_TABLE_FUNCTIONS.contains(&last.as_str())).then(|| {
        format!(
            "table function {last}; MCP queries catalog tables, not object-store paths. \
             Use sample_rows or SELECT from a table name"
        )
    })
}

/// A plain relation the tools must not read: the `system` catalog (#705:
/// `system.runtime.queries` carries every client's SQL), DataFusion's
/// `information_schema.df_settings` (#765: runtime settings, the spill path
/// among them) and a quoted path used as a table (`FROM '/tmp/x.parquet'`).
fn denied_relation(name: &sqlparser::ast::ObjectName) -> Option<String> {
    let first = name.0.first()?.as_ident()?;
    if first.quote_style == Some('\'') {
        return Some(format!(
            "path {:?} in FROM; MCP queries catalog tables, not object-store paths",
            first.value
        ));
    }
    if name.0.len() > 1 && first.value.eq_ignore_ascii_case("system") {
        return Some(format!(
            "system catalog table {name}; the runtime views are an operator surface"
        ));
    }
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|i| i.value.to_ascii_lowercase())
        .collect();
    matches!(
        parts.as_slice(),
        [.., schema, table] if schema == "information_schema" && table == "df_settings"
    )
    .then(|| format!("{name}; the engine's runtime settings are an operator surface"))
}

struct QueryShapeFinder {
    denied: Option<String>,
}

impl sqlparser::ast::Visitor for QueryShapeFinder {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        factor: &sqlparser::ast::TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        use sqlparser::ast::{Expr, TableFactor};
        self.denied = match factor {
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, .. } => denied_table_function(name),
            TableFactor::Table { name, .. } => denied_relation(name),
            TableFactor::TableFunction {
                expr: Expr::Function(func),
                ..
            } => denied_table_function(&func.name),
            TableFactor::TableFunction { .. } => Some("TABLE(<expression>) in FROM".to_string()),
            _ => None,
        };
        if self.denied.is_some() {
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &sqlparser::ast::Expr) -> std::ops::ControlFlow<Self::Break> {
        if let sqlparser::ast::Expr::Function(func) = expr {
            if let Some(name) = object_name_last(&func.name).filter(|n| is_file_reader_name(n)) {
                self.denied = Some(format!("file function {name} in an expression"));
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// The parsed statement of the kinds whose sources can hide a table
/// function or a `system` read: queries and the data statements the change
/// tool runs (`INSERT ... SELECT FROM read_parquet(...)`).
fn statement_of(kind: &StatementKind) -> Option<&sqlparser::ast::Statement> {
    match kind {
        StatementKind::Query(stmt)
        | StatementKind::CreateTable(stmt)
        | StatementKind::Ctas(stmt)
        | StatementKind::Insert(stmt)
        | StatementKind::Merge(stmt)
        | StatementKind::Delete(stmt)
        | StatementKind::Update(stmt)
        | StatementKind::CreateView(stmt) => Some(stmt.as_ref()),
        _ => None,
    }
}

fn walk_query_shape(kind: &StatementKind) -> QueryShapeFinder {
    use sqlparser::ast::Visit;
    let mut finder = QueryShapeFinder { denied: None };
    if let Some(stmt) = statement_of(kind) {
        let _ = stmt.visit(&mut finder);
    }
    finder
}

/// Append `LIMIT max_rows + 1` to a plain query that has no row bound of its
/// own (issue #670). The response keeps at most `max_rows` rows, so without
/// this the coordinator collected the whole result (up to
/// `query.max_result_rows`, a million rows by default) for a thousand-row
/// answer. One extra row keeps `truncated` honest: `total_rows` then reads
/// `max_rows + 1`, meaning "more than the cap", not the true count.
///
/// Text append, not an AST print: the executor must see the ORIGINAL text
/// (time-travel clauses are stripped from the classification copy). A
/// statement that already carries LIMIT, FETCH, or a FOR clause is passed
/// through unchanged; every non-query kind (SHOW ...) is too. A trailing
/// comment would swallow the suffix; `ensure_read_only` rejects those (#775).
fn with_row_cap(sql: &str, kind: &StatementKind, max_rows: usize) -> String {
    let StatementKind::Query(stmt) = kind else {
        return sql.to_string();
    };
    let sqlparser::ast::Statement::Query(query) = stmt.as_ref() else {
        return sql.to_string();
    };
    if query.limit_clause.is_some()
        || query.fetch.is_some()
        || query.for_clause.is_some()
        || !query.pipe_operators.is_empty()
        || max_rows == 0
    {
        return sql.to_string();
    }
    let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
    format!("{trimmed} LIMIT {}", max_rows.saturating_add(1))
}

/// Whether the last token of `sql` is a line comment, in which case anything
/// appended to the text would become part of it. A closed block comment is
/// safe to append after.
fn ends_with_comment(sql: &str) -> bool {
    use sqlparser::{
        dialect::GenericDialect,
        tokenizer::{Token, Tokenizer, Whitespace},
    };
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return true;
    };
    tokens
        .iter()
        .rev()
        .find(|token| {
            !matches!(
                token,
                Token::Whitespace(Whitespace::Space | Whitespace::Newline | Whitespace::Tab)
                    | Token::SemiColon
            )
        })
        .is_some_and(|token| matches!(token, Token::Whitespace(Whitespace::SingleLineComment { .. })))
}

/// Classify and require a change kind; returns the kind so the target gate
/// can inspect it without parsing twice.
fn ensure_change(sql: &str, session_catalog: Option<&str>) -> Result<StatementKind, String> {
    let kind = classify(sql, session_catalog)?;
    if kind.is_read_only() {
        return Err(format!(
            "Use a read-only tool for {} statements",
            kind.name()
        ));
    }
    if !is_change_surface(&kind) {
        return Err(format!(
            "{} is outside execute_sql_change's surface, which is data statements and \
             table, schema and view DDL; grants, policies, secrets, catalogs, session \
             settings and procedures go through Flight SQL or Trino HTTP",
            kind.name()
        ));
    }
    if let Some(reason) = walk_query_shape(&kind).denied {
        return Err(format!("execute_sql_change rejected {reason}"));
    }
    Ok(kind)
}

/// What `execute_sql_change` executes (#677): data statements and the DDL
/// of tables, schemas and views. Everything else that is not read-only
/// (GRANT/REVOKE/DENY, policy and tag DDL, secrets, ATTACH/DETACH, session
/// settings, transactions, procedures, CALL) is administration, which the
/// tool's name and description do not announce and an operator enabling
/// `allow_writes` does not expect. The engine still authorizes what passes;
/// this is the tool's own surface, fail closed on kinds it does not list.
fn is_change_surface(kind: &StatementKind) -> bool {
    matches!(
        kind,
        StatementKind::CreateTable(_)
            | StatementKind::CreateExternalTable(_)
            | StatementKind::Ctas(_)
            | StatementKind::Insert(_)
            | StatementKind::Merge(_)
            | StatementKind::Delete(_)
            | StatementKind::Update(_)
            | StatementKind::Drop(_)
            | StatementKind::Rename(_)
            | StatementKind::AlterSchema(_)
            | StatementKind::AlterTableProps(_)
            | StatementKind::Comment(_)
            | StatementKind::DropNestedColumn { .. }
            | StatementKind::PartitionEvolution(_)
            | StatementKind::RefDdl(_)
            | StatementKind::HivePartitionDdl(_)
            | StatementKind::MsckRepairTable(_)
            | StatementKind::Analyze(_)
            | StatementKind::Truncate(_)
            | StatementKind::CreateView(_)
            | StatementKind::DropView(_)
            | StatementKind::CreateSchema(_)
            | StatementKind::DropSchema(_)
    )
}

/// Where a change statement writes, as the parser sees it.
enum WriteTarget {
    /// `[catalog.][schema.]table`
    Table(Vec<String>),
    /// `[catalog.]schema`
    Schema(Vec<String>),
}

fn name_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .unwrap_or_default()
        })
        .collect()
}

fn relation_name(factor: &sqlparser::ast::TableFactor) -> Option<Vec<String>> {
    match factor {
        sqlparser::ast::TableFactor::Table { name, .. } => Some(name_parts(name)),
        _ => None,
    }
}

/// The objects a change statement writes to. `None` when the statement kind
/// has no table target the parser exposes (procedures, grants, session
/// statements, policy DDL): the gate then fails closed rather than guessing.
/// Sources (`INSERT ... SELECT FROM other.t`) are reads and are not targets;
/// policy enforcement covers them.
fn write_targets(kind: &StatementKind) -> Option<Vec<WriteTarget>> {
    use sqlparser::ast::{FromTable, ObjectType, SchemaName, Statement, TableObject};
    let table = |parts: Vec<String>| WriteTarget::Table(parts);
    let targets = match kind {
        StatementKind::Insert(stmt) => match stmt.as_ref() {
            Statement::Insert(insert) => match &insert.table {
                TableObject::TableName(name) => vec![table(name_parts(name))],
                _ => return None,
            },
            _ => return None,
        },
        StatementKind::Ctas(stmt) | StatementKind::CreateTable(stmt) => match stmt.as_ref() {
            Statement::CreateTable(create) => vec![table(name_parts(&create.name))],
            _ => return None,
        },
        StatementKind::CreateExternalTable(spec) => {
            let mut parts = Vec::new();
            if let Some(catalog) = &spec.catalog {
                parts.push(catalog.clone());
            }
            if let Some(database) = &spec.database {
                parts.push(database.clone());
            } else if spec.catalog.is_some() {
                // A catalog without a database cannot resolve as three parts.
                return None;
            }
            parts.push(spec.name.clone());
            vec![table(parts)]
        }
        StatementKind::Merge(stmt) => match stmt.as_ref() {
            Statement::Merge(merge) => vec![table(relation_name(&merge.table)?)],
            _ => return None,
        },
        StatementKind::Delete(stmt) => match stmt.as_ref() {
            Statement::Delete(delete) => {
                if !delete.tables.is_empty() {
                    delete.tables.iter().map(|n| table(name_parts(n))).collect()
                } else {
                    let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) =
                        &delete.from;
                    from.iter()
                        .map(|twj| relation_name(&twj.relation).map(table))
                        .collect::<Option<Vec<_>>>()?
                }
            }
            _ => return None,
        },
        StatementKind::Update(stmt) => match stmt.as_ref() {
            Statement::Update(update) => vec![table(relation_name(&update.table.relation)?)],
            _ => return None,
        },
        StatementKind::Drop(stmt) | StatementKind::DropView(stmt) | StatementKind::DropSchema(stmt) => {
            match stmt.as_ref() {
                Statement::Drop {
                    object_type: ObjectType::Schema,
                    names,
                    ..
                } => names.iter().map(|n| WriteTarget::Schema(name_parts(n))).collect(),
                Statement::Drop { names, .. } => names.iter().map(|n| table(name_parts(n))).collect(),
                _ => return None,
            }
        }
        StatementKind::Rename(stmt) | StatementKind::AlterTableProps(stmt) => match stmt.as_ref() {
            Statement::AlterTable(alter) => vec![table(name_parts(&alter.name))],
            _ => return None,
        },
        StatementKind::CreateView(stmt) => match stmt.as_ref() {
            Statement::CreateView(view) => vec![table(name_parts(&view.name))],
            _ => return None,
        },
        StatementKind::CreateSchema(stmt) => match stmt.as_ref() {
            Statement::CreateSchema { schema_name, .. } => match schema_name {
                SchemaName::Simple(name) | SchemaName::NamedAuthorization(name, _) => {
                    vec![WriteTarget::Schema(name_parts(name))]
                }
                SchemaName::UnnamedAuthorization(_) => return None,
            },
            _ => return None,
        },
        StatementKind::Comment(stmt) => match stmt.as_ref() {
            Statement::Comment { object_name, .. } => vec![table(name_parts(object_name))],
            _ => return None,
        },
        StatementKind::Truncate(name) => {
            vec![table(name.split('.').map(|p| p.trim_matches('"').to_string()).collect())]
        }
        _ => return None,
    };
    Some(targets)
}

/// The catalog a target resolves to, given the session default.
fn target_catalog(target: &WriteTarget, default_catalog: Option<&str>) -> Option<String> {
    let (parts, qualified_len) = match target {
        WriteTarget::Table(parts) => (parts, 3),
        WriteTarget::Schema(parts) => (parts, 2),
    };
    if parts.iter().any(String::is_empty) {
        return None;
    }
    if parts.len() >= qualified_len {
        parts.first().cloned()
    } else {
        default_catalog.map(str::to_string)
    }
}

/// The catalog gate for the change tool. Inactive when nothing is configured
/// to gate on; otherwise every target must resolve to a catalog that passes:
/// a Hive root must be named in `write_catalogs` (the write runs on the
/// engine's credentials), and any catalog must be named when the list is
/// non-empty. Unresolvable targets fail closed.
fn authorize_write_target(
    state: &McpServerState,
    session: &Session,
    kind: &StatementKind,
) -> Result<(), (i32, String)> {
    let allow = &state.config.write_catalogs;
    let hive = &state.hive_roots;
    if allow.is_empty() && hive.is_empty() {
        return Ok(());
    }
    let listed = |catalog: &str| allow.iter().any(|c| c.eq_ignore_ascii_case(catalog));
    let is_hive = |catalog: &str| hive.iter().any(|c| c.eq_ignore_ascii_case(catalog));
    let targets = write_targets(kind).ok_or_else(|| {
        (
            -32003,
            format!(
                "The MCP change tool cannot determine the target catalog of a {} statement, and \
                 mcp.write_catalogs or [[hive.roots]] are configured; only statements with a \
                 table or schema target are allowed here",
                kind.name()
            ),
        )
    })?;
    for target in &targets {
        let catalog = target_catalog(target, session.default_catalog.as_deref()).ok_or_else(|| {
            (
                -32003,
                "Qualify the target with its catalog: this session has no default catalog and \
                 mcp.write_catalogs or [[hive.roots]] are configured"
                    .to_string(),
            )
        })?;
        if is_hive(&catalog) && !listed(&catalog) {
            return Err((
                -32003,
                format!(
                    "Catalog '{catalog}' is a Hive root: MCP change statements there run on the \
                     engine's storage credentials, not yours, and need the catalog named in \
                     mcp.write_catalogs"
                ),
            ));
        }
        if !allow.is_empty() && !listed(&catalog) {
            return Err((
                -32003,
                format!("Catalog '{catalog}' is not in mcp.write_catalogs"),
            ));
        }
    }
    Ok(())
}

fn authorize_write(state: &McpServerState, auth: &AuthContext) -> Result<(), (i32, String)> {
    if !state.config.allow_writes {
        return Err((-32602, "MCP writes are disabled".into()));
    }
    if let Some(scopes) = &auth.scopes {
        if !scopes.contains(&state.config.write_scope) {
            return Err((
                -32003,
                format!("OAuth scope '{}' is required", state.config.write_scope),
            ));
        }
    }
    if !auth
        .session
        .user
        .roles
        .iter()
        .any(|role| state.config.write_roles.contains(role))
    {
        return Err((
            -32003,
            "The authenticated user has no configured MCP writer role".into(),
        ));
    }
    Ok(())
}

fn batches_json(batches: &[RecordBatch], max_rows: usize, max_bytes: usize) -> Value {
    let columns: Vec<Value> = batches
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| json!({"name":field.name(),"type":field.data_type().to_string()}))
                .collect()
        })
        .unwrap_or_default();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    // Byte accounting is incremental (issue #670): each row is encoded once
    // and its length added to a running total, against the fixed cost of the
    // column list and the object framing. The old probe re-serialized the
    // whole accumulated array after every row, quadratic in the row cap.
    let framing = serde_json::to_vec(&json!({"columns": columns, "rows": []}))
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    let mut used = framing;
    let mut rows = Vec::new();
    // One wide cell must not be the whole budget (#707): `repeat('x', 1e6)`
    // is one row under the row cap and a megabyte to the model.
    let cell_budget = max_bytes.min(CELL_BYTES_MAX);
    let mut cell_truncated = false;
    'batches: for batch in batches {
        for row in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                break 'batches;
            }
            let mut object = Map::new();
            for (index, field) in batch.schema().fields().iter().enumerate() {
                let mut value =
                    sqe_trino_protocol::encode::arrow_value_to_json(batch.column(index).as_ref(), row);
                // Arrays, maps and rows arrive as JSON structure (#818); an
                // oversized one is cut as its JSON text instead.
                let oversized = match &value {
                    Value::String(text) => (text.len() > cell_budget).then(|| text.clone()),
                    Value::Array(_) | Value::Object(_) => {
                        Some(value.to_string()).filter(|text| text.len() > cell_budget)
                    }
                    _ => None,
                };
                if let Some(mut text) = oversized {
                    truncate_cell(&mut text, cell_budget);
                    value = Value::String(text);
                    cell_truncated = true;
                }
                object.insert(field.name().clone(), value);
            }
            let object = Value::Object(object);
            // The row's bytes plus the separating comma.
            let cost = serde_json::to_vec(&object)
                .map(|v| v.len() + 1)
                .unwrap_or(usize::MAX);
            if used.saturating_add(cost) > max_bytes {
                break 'batches;
            }
            used += cost;
            rows.push(object);
        }
    }
    let returned_rows = rows.len();
    json!({
        "columns": columns,
        "rows": rows,
        "returned_rows": returned_rows,
        "total_rows": total_rows,
        "truncated": returned_rows < total_rows || cell_truncated
    })
}

/// Cuts `text` to `budget` bytes on a char boundary and notes what was dropped.
fn truncate_cell(text: &mut String, budget: usize) {
    let mut end = budget;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let dropped = text.len() - end;
    text.truncate(end);
    text.push_str(&format!("...[truncated {dropped} bytes]"));
}

/// Longest cell serialized into a tool response (#707).
const CELL_BYTES_MAX: usize = 64 * 1024;

fn tool_success(value: Value) -> Value {
    json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value,"isError":false})
}
fn tool_failure(message: String) -> Value {
    if let Some(secs) = retry_after_in(&message) {
        // A limiter denial (`RateLimitDenial::into_error` text): structured so
        // a client can wait exactly that long instead of giving up on a
        // string. The limiter name and knob are in the text.
        return json!({
            "content":[{"type":"text","text":message}],
            "isError":true,
            "structuredContent":{"error":"rate_limited","retry_after_seconds":secs,"detail":message},
        });
    }
    json!({"content":[{"type":"text","text":message}],"isError":true})
}

/// The `retry after {n}s` a limiter denial ends with, if this is one.
fn retry_after_in(message: &str) -> Option<u64> {
    let rest = message.rsplit_once("retry after ")?.1;
    rest.strip_suffix('s')?.trim().parse().ok()
}
fn rpc_result(id: Option<Value>, result: Value) -> Response {
    (
        StatusCode::OK,
        Json(json!({"jsonrpc":"2.0","id":id.unwrap_or(Value::Null),"result":result})),
    )
        .into_response()
}
fn rpc_error(id: Option<Value>, code: i32, message: String) -> Response {
    (StatusCode::OK, Json(json!({"jsonrpc":"2.0","id":id.unwrap_or(Value::Null),"error":{"code":code,"message":message}}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use axum::body::Body;
    use axum::http::Request;
    use sqe_auth::{AuthError, Identity};
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct MockAuth;

    #[async_trait]
    impl AuthProvider for MockAuth {
        async fn authenticate_oauth(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
            self.authenticate(credentials).await
        }

        async fn authenticate(
            &self,
            _credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            Ok(Identity {
                user_id: "alice".to_string(),
                display_name: "Alice".to_string(),
                roles: vec!["reader".to_string()],
                subject: Some("subject-alice".to_string()),
                email: Some("alice@example.com".to_string()),
                groups: vec!["analysts".to_string()],
                catalog_token: None,
                refresh_token: None,
                expires_at: Some(chrono::Utc::now() + chrono::Duration::minutes(5)),
                scopes: vec![],
            })
        }
    }

    /// Records, per call: the catalog token, `(username, roles)`, the
    /// `Session.source` label the tool call stamped, the session's
    /// `(default_catalog, default_schema)`, and the SQL text handed over.
    #[derive(Default)]
    struct RecordingExecutor(
        Mutex<Option<String>>,
        Mutex<Option<(String, Vec<String>)>>,
        Mutex<Option<String>>,
        Mutex<Option<(Option<String>, Option<String>)>>,
        Mutex<Option<String>>,
        /// `current_trace_id()` at execute time: what the statement span and
        /// the audit record see.
        Mutex<Option<Option<String>>>,
    );

    #[async_trait]
    impl QueryExecutor for RecordingExecutor {
        async fn execute(
            &self,
            session: &Session,
            sql: &str,
        ) -> Result<Vec<RecordBatch>, SqeError> {
            *self.0.lock().unwrap() = Some(session.access_token().expose().to_string());
            *self.1.lock().unwrap() = Some((
                session.user.username.clone(),
                session.user.roles.clone(),
            ));
            *self.2.lock().unwrap() = session.source.clone();
            *self.3.lock().unwrap() = Some((
                session.default_catalog.clone(),
                session.default_schema.clone(),
            ));
            *self.4.lock().unwrap() = Some(sql.to_string());
            *self.5.lock().unwrap() = Some(sqe_metrics_core::propagation::current_trace_id());
            Ok(vec![])
        }
    }

    /// `traceparent` on the request becomes the trace the statement runs in
    /// (what the audit record stamps) and comes back as `_meta.trace_id`; a
    /// request without one gets a fresh id.
    #[tokio::test(flavor = "current_thread")]
    async fn traceparent_correlates_the_request_the_statement_and_the_response() {
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tracing_subscriber::layer::SubscriberExt;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder().build();
        let tracer = provider.tracer("mcp-trace-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        // Process-wide, not thread-local: the request runs across tokio
        // worker threads under the harness, and a thread-local default
        // leaves the OTel layer behind on the test's own thread, so the
        // adopted context reads as invalid and a fresh id comes back.
        // Nothing else in this suite installs a subscriber.
        let _ = tracing::subscriber::set_global_default(subscriber);

        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            test_config(),
        ));
        let token = scoped_token("sqe:mcp:read");
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        let traceparent = format!("00-{trace}-00f067aa0ba902b7-01");
        let response = send(&app, &token, &[("traceparent", traceparent.as_str())], tool_call("execute_sql_readonly")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        assert_eq!(body["result"]["_meta"]["trace_id"], trace, "{body}");
        assert_eq!(
            executor.5.lock().unwrap().clone().flatten().as_deref(),
            Some(trace),
            "the statement ran inside the caller's trace"
        );

        let response = send(&app, &token, &[], tool_call("execute_sql_readonly")).await;
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap();
        let fresh = body["result"]["_meta"]["trace_id"].as_str().unwrap();
        assert_eq!(fresh.len(), 32, "{fresh}");
        assert_ne!(fresh, trace);
        assert_eq!(
            executor.5.lock().unwrap().clone().flatten().as_deref(),
            Some(fresh),
            "statement and response share the fresh trace"
        );
    }

    fn test_config() -> McpConfig {
        McpConfig {
            resource_uri: "https://sqe.example.com/mcp".to_string(),
            authorization_servers: vec!["https://idp.example.com".to_string()],
            ..Default::default()
        }
    }

    fn scoped_token(scopes: &str) -> String {
        let claims = json!({"scope": scopes, "sub": "subject-alice"});
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&claims).unwrap());
        format!("header.{payload}.signature")
    }

    #[test]
    fn read_only_guard_uses_sqe_classifier() {
        for sql in [
            "SELECT 1",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SHOW CATALOGS",
            "SHOW STATS FOR iceberg.ns.t",
        ] {
            ensure_read_only(sql, None).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CALL system.rewrite_data_files(table => 'ns.t')",
        ] {
            assert!(ensure_read_only(sql, None).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn read_only_guard_rejects_file_tvfs_and_query_shaped_writes() {
        for sql in [
            "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
            "SELECT * FROM read_csv('/tmp/x.csv')",
            "SELECT * FROM read_json('hf://datasets/x')",
            "WITH t AS (SELECT * FROM read_parquet('s3://b/x')) SELECT * FROM t",
            "SELECT * FROM u JOIN read_parquet('s3://b/x') v ON true",
        ] {
            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(
                err.contains("Read-only tool rejected"),
                "{sql}: {err}"
            );
        }
        for sql in [
            "SELECT 1 INTO t",
            "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM x",
        ] {
            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(err.contains("inside a query is not supported"), "{sql}: {err}");
        }
        ensure_read_only("SELECT 'read_parquet' AS x", None).expect("literal is not a TVF");
        ensure_read_only("SELECT * FROM read_parquet", None)
            .expect("bare name without args is a table, not a TVF");
    }

    /// #766: every table function outside the catalog-scoped Iceberg metadata
    /// set is refused, as is a quoted path in FROM and a file reader in scalar
    /// position. `read_/**/csv('x')` does NOT become `read_csv`: the comment
    /// is whitespace to the tokenizer, so it parses as table `read_` aliased
    /// `csv (x)`, a plain catalog relation with no file access.
    #[test]
    fn read_only_guard_fails_closed_on_unknown_table_functions_and_paths() {
        for sql in [
            "SELECT * FROM parquet_scan('x')",
            "SELECT read_csv('x')",
            "SELECT * FROM t WHERE read_parquet('x') IS NOT NULL",
            "SELECT * FROM '/tmp/x.parquet'",
            "SELECT * FROM quack_query('a','b')",
            "SELECT * FROM some_new_tvf('x')",
            "SELECT * FROM TABLE(read_parquet('x'))",
        ] {
            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(err.contains("Read-only tool rejected"), "{sql}: {err}");
        }
        let kind = ensure_read_only("SELECT * FROM read_/**/csv('x')", None).expect("plain table read_");
        assert!(format!("{kind:?}").contains("value: \"read_\""), "{kind:?}");
        ensure_read_only("SELECT * FROM table_snapshots('s','t')", None).expect("catalog TVF stays");
        ensure_read_only("SELECT * FROM iceberg.table_files('s','t')", None).expect("qualified catalog TVF stays");
        ensure_read_only("SELECT upper(name) FROM t WHERE length(name) > 1", None)
            .expect("ordinary scalar functions stay");
        let err = ensure_change("INSERT INTO t SELECT * FROM read_parquet('x')", None)
            .expect_err("change tool sources are gated too");
        assert!(err.contains("execute_sql_change rejected"), "{err}");
    }

    /// #705 #763: `system.runtime.queries` carries every client's SQL text.
    #[test]
    fn read_only_guard_refuses_the_system_catalog() {
        for sql in [
            "SELECT query FROM system.runtime.queries",
            "SELECT * FROM SYSTEM.runtime.nodes",
            "WITH q AS (SELECT * FROM system.runtime.tasks) SELECT * FROM q",
            "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM system.runtime.queries)",
        ] {
            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(err.contains("Read-only tool rejected system catalog"), "{sql}: {err}");
        }
        ensure_read_only("SELECT * FROM system", None).expect("a table named system is not the catalog");
        ensure_read_only("SELECT * FROM iceberg.system.t", None).expect("a schema named system is fine");
        let err = table_ref(&json!({"catalog": "System", "schema": "runtime", "table": "queries"}))
            .expect_err("sample_rows shares the gate");
        assert_eq!(err.0, -32602);
    }

    #[test]
    fn like_matcher_is_iterative_and_patterns_are_capped() {
        // Depth = pattern length before #739; this would overflow the stack.
        let long = "%".repeat(1 << 20) + "z";
        assert!(!like_matches(&long, &"a".repeat(2_000)));
        assert!(like_matches(&"%".repeat(500), "anything"));
        assert!(like_matches("%a%b%c", "xaxbxc"));
        assert!(!like_matches("%a%b%c", "xaxbxcx"));
        assert!(like_matches("a%", "a"));
        assert!(!like_matches("a_", "a"));
    }

    #[test]
    fn a_wide_cell_is_cut_to_the_cell_budget_and_reported() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let wide = "x".repeat(100 * 1024);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![wide.as_str()]))])
            .unwrap();
        let value = batches_json(&[batch], 10, 1024 * 1024);
        let cell = value["rows"][0]["v"].as_str().unwrap();
        assert!(cell.len() < CELL_BYTES_MAX + 64, "{}", cell.len());
        assert!(cell.ends_with("bytes]"), "{}", &cell[cell.len() - 40..]);
        assert_eq!(value["returned_rows"], 1);
        assert_eq!(value["truncated"], true);
    }

    /// A nested cell is JSON structure, not a string (#818), so the
    /// string-only cut missed it and one wide array emptied the page.
    #[test]
    fn a_wide_nested_cell_is_cut_as_json_text_and_small_ones_stay_structured() {
        use arrow_array::{types::Int64Type, Array, ListArray};
        let wide = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some((0..20_000).map(|i| Some(1_000_000 + i)).collect::<Vec<_>>()),
            Some(vec![Some(1), None]),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new("v", wide.data_type().clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(wide)]).unwrap();
        let value = batches_json(&[batch], 10, 1024 * 1024);
        assert_eq!(value["returned_rows"], 2);
        assert_eq!(value["truncated"], true);
        let cell = value["rows"][0]["v"]
            .as_str()
            .expect("an oversized array becomes text");
        assert!(cell.starts_with("[1000000,1000001,"), "{}", &cell[..40]);
        assert!(cell.len() < CELL_BYTES_MAX + 64, "{}", cell.len());
        assert!(cell.ends_with("bytes]"), "{}", &cell[cell.len() - 40..]);
        assert_eq!(value["rows"][1]["v"], json!([1, null]));
    }

    /// #723 #749: the change tool applies the same denylist, and the
    /// outbound `quack_query` TVF is on it for both tools.
    #[test]
    fn change_guard_rejects_denied_table_functions() {
        for sql in [
            "CREATE TABLE t AS SELECT * FROM read_parquet('s3://b/x.parquet')",
            "INSERT INTO t SELECT * FROM read_csv('/tmp/x.csv')",
            "CREATE VIEW v AS SELECT * FROM read_json('hf://datasets/x')",
            "INSERT INTO t SELECT * FROM quack_query('quack:h:9495', 'SELECT 1')",
            "CREATE TABLE t AS WITH s AS (SELECT * FROM read_parquet('s3://b/x')) SELECT * FROM s",
        ] {
            let err = ensure_change(sql, None).expect_err(sql);
            assert!(
                err.contains("rejected table function"),
                "{sql}: {err}"
            );
        }
        ensure_change("CREATE TABLE t AS SELECT * FROM src", None).expect("plain CTAS passes");
        let err = ensure_read_only(
            "SELECT * FROM quack_query('quack:h:9495', 'SELECT 1')",
            None,
        )
        .expect_err("quack_query");
        assert!(err.contains("quack_query"), "{err}");
    }

    #[test]
    fn rejects_multiple_statements_but_not_semicolon_in_literal() {
        assert!(ensure_read_only("SELECT 1; DROP TABLE t", None).is_err());
        assert!(ensure_read_only("SELECT ';' AS value;", None).is_ok());
    }

    /// #683: the gates classify with `GenericDialect`, the engine plans the
    /// text it is sent under DataFusion's DuckDB dialect. Generic nests block
    /// comments and reads Oracle `q'[...]'` strings; DuckDB does neither, so
    /// the same text was one SELECT to the gate and a write, or two
    /// statements, to the planner.
    #[test]
    fn refuses_text_the_engine_dialect_reads_differently() {
        use sqlparser::{
            ast::Statement,
            dialect::{DuckDbDialect, GenericDialect},
            parser::Parser,
        };
        type EngineReads = fn(&[Statement]) -> bool;
        let cases: &[(&str, EngineReads)] = &[
            (
                "/* /* */ INSERT INTO t SELECT * FROM t; -- */ SELECT * FROM t",
                |s| matches!(s, [Statement::Insert(_)]),
            ),
            ("/* /* */ DELETE FROM t; -- */ SELECT * FROM t", |s| {
                matches!(s, [Statement::Delete(_)])
            }),
            (
                "/* /* */ COPY (SELECT * FROM t) TO 'out.parquet'; -- */ SELECT * FROM t",
                |s| matches!(s, [Statement::Copy { .. }]),
            ),
            ("SELECT q'[', 1 FROM t; DROP TABLE t; -- ]' FROM t", |s| {
                s.len() == 2
            }),
        ];
        for (sql, engine_reads) in cases {
            // The premise: one query to the gate, something else to the engine.
            let gate = Parser::parse_sql(&GenericDialect {}, sql).expect(sql);
            assert!(
                matches!(gate.as_slice(), [Statement::Query(_)]),
                "{sql}: {gate:?}"
            );
            let engine = Parser::parse_sql(&DuckDbDialect {}, sql).expect(sql);
            assert!(engine_reads(&engine), "{sql}: {engine:?}");

            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(err.contains("dialect"), "{sql}: {err}");
        }
    }

    /// The check and the engine agree on ordinary comments and quoting: what
    /// the gate accepts, the engine dialect parses to the same one statement,
    /// with the row cap appended as well.
    #[test]
    fn accepted_text_parses_the_same_under_the_engine_dialect() {
        use sqlparser::{dialect::DuckDbDialect, parser::Parser};
        for sql in [
            "SELECT * FROM t",
            "SELECT * FROM t /* note */",
            "-- leading\nSELECT * FROM t WHERE a = '/*'",
            "SELECT '--;' AS a, \"x;y\" FROM t;",
            "SELECT * FROM t WHERE a = $$;$$",
            "/* one */ /* two */ SELECT * FROM t",
        ] {
            let kind = ensure_read_only(sql, None).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let capped = with_row_cap(sql, &kind, 10);
            let engine = Parser::parse_sql(&DuckDbDialect {}, &capped).expect(&capped);
            assert!(
                matches!(engine.as_slice(), [sqlparser::ast::Statement::Query(q)] if q.limit_clause.is_some()),
                "{capped}: {engine:?}"
            );
        }
    }

    #[test]
    fn write_tool_is_not_advertised_by_default() {
        let config = McpConfig::default();
        assert!(!tools(&config)
            .iter()
            .any(|tool| tool["name"] == "execute_sql_change"));
        let mut enabled = config;
        enabled.allow_writes = true;
        assert!(tools(&enabled)
            .iter()
            .any(|tool| tool["name"] == "execute_sql_change"));
    }

    // Issue #666: `refresh_metadata` is a read tool in effect (it drops
    // caches, never data) but not read-only by annotation, so a client that
    // auto-approves read-only tools still asks before a refresh.
    #[test]
    fn refresh_metadata_is_advertised_and_not_marked_read_only() {
        let tool = tools(&McpConfig::default())
            .into_iter()
            .find(|tool| tool["name"] == "refresh_metadata")
            .expect("refresh_metadata advertised");
        assert_eq!(tool["annotations"]["readOnlyHint"], false);
        assert_eq!(tool["annotations"]["destructiveHint"], false);
        let props = &tool["inputSchema"]["properties"];
        for key in ["catalog", "schema", "table"] {
            assert!(props.get(key).is_some(), "{key} argument");
        }
        assert!(tool["inputSchema"].get("required").is_none(), "every argument is optional");
    }

    #[test]
    fn not_found_from_a_lookup_tool_points_at_refresh_metadata() {
        let hinted = with_refresh_hint("describe_table", "table 'glue.db.t' not found".to_string());
        assert!(hinted.contains("refresh_metadata"), "{hinted}");
        let plain = with_refresh_hint("execute_sql_readonly", "table 'x' not found".to_string());
        assert!(!plain.contains("refresh_metadata"), "only lookup tools get the hint");
        let denied = with_refresh_hint("describe_table", "permission denied".to_string());
        assert!(!denied.contains("refresh_metadata"), "only not-found gets the hint");
    }

    #[test]
    fn refresh_cooldown_holds_per_user_and_scope() {
        let state = test_state_for_cooldown();
        assert!(state.refresh_cooldown_hit("alice|table:c.s.t").is_none(), "first call runs");
        assert!(
            state.refresh_cooldown_hit("alice|table:c.s.t").is_some(),
            "a repeat inside the window is skipped"
        );
        assert!(
            state.refresh_cooldown_hit("alice|schema:c.s").is_none(),
            "another scope is independent"
        );
        assert!(
            state.refresh_cooldown_hit("bob|table:c.s.t").is_none(),
            "another user is independent"
        );
    }

    fn test_state_for_cooldown() -> McpServerState {
        McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(RecordingExecutor::default()),
            McpConfig::default(),
        )
    }

    /// #676: an opaque (non-JWT) bearer passes when the provider already
    /// reported the scopes on the `Identity`.
    #[tokio::test]
    async fn provider_scopes_win_over_payload_decoding_for_opaque_tokens() {
        struct ScopedAuth;
        #[async_trait]
        impl AuthProvider for ScopedAuth {
            async fn authenticate_oauth(
                &self,
                c: &FlightCredentials,
            ) -> Result<Identity, AuthError> {
                self.authenticate(c).await
            }
            async fn authenticate(&self, _c: &FlightCredentials) -> Result<Identity, AuthError> {
                let mut identity = MockAuth.authenticate(&FlightCredentials::default()).await?;
                identity.scopes = vec!["sqe:mcp:read".to_string()];
                Ok(identity)
            }
        }
        let state = McpServerState::new(
            Arc::new(ScopedAuth),
            Arc::new(RecordingExecutor::default()),
            McpConfig::default(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer opaque-token-no-dots".parse().unwrap());
        let ctx = authenticate_oauth(&state, &headers, "127.0.0.1")
            .await
            .unwrap_or_else(|r| panic!("opaque token rejected: {}", r.status()));
        assert!(ctx.scopes.expect("scopes").contains("sqe:mcp:read"));
        // Without provider scopes the same opaque token still has no readable scope.
        let state = McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(RecordingExecutor::default()),
            McpConfig::default(),
        );
        let denied = authenticate_oauth(&state, &headers, "127.0.0.1").await.err().expect("403");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn reads_standard_scope_and_scp_claims() {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"scope":"openid sqe:mcp:read","scp":["extra"]}"#);
        let token = format!("e30.{payload}.signature");
        let scopes = token_scopes(&token).expect("scope claims");
        assert!(scopes.contains("sqe:mcp:read"));
        assert!(scopes.contains("extra"));
    }

    #[test]
    fn result_serialization_is_row_bounded_and_reports_truncation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let value = batches_json(&[batch], 2, 1024 * 1024);
        assert_eq!(value["total_rows"], 3);
        assert_eq!(value["returned_rows"], 2);
        assert_eq!(value["truncated"], true);
    }

    #[test]
    fn result_serialization_is_byte_bounded_and_the_body_fits_the_cap() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let values: Vec<String> = (0..50).map(|i| format!("row-{i:0>40}")).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(
                values.iter().map(String::as_str).collect::<Vec<_>>(),
            ))],
        )
        .unwrap();
        let cap = 600;
        let value = batches_json(&[batch], 1_000, cap);
        let returned = value["returned_rows"].as_u64().unwrap();
        assert!(returned > 0 && returned < 50, "returned {returned}");
        assert_eq!(value["truncated"], true);
        let body = serde_json::to_vec(&json!({"columns": value["columns"], "rows": value["rows"]}))
            .unwrap();
        assert!(body.len() <= cap, "{} > {cap}", body.len());
        // A cap below the framing returns no rows rather than failing.
        let none = batches_json(&[], 1_000, 1);
        assert_eq!(none["returned_rows"], 0);
    }

    #[test]
    fn plain_queries_get_a_row_cap_and_bounded_or_non_query_statements_do_not() {
        let cap = |sql: &str| {
            let kind = ensure_read_only(sql, None).unwrap();
            with_row_cap(sql, &kind, 1_000)
        };
        assert_eq!(cap("SELECT * FROM t"), "SELECT * FROM t LIMIT 1001");
        assert_eq!(cap("SELECT * FROM t;  "), "SELECT * FROM t LIMIT 1001");
        assert_eq!(
            cap("WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT 2"),
            "WITH c AS (SELECT 1 AS x) SELECT x FROM c UNION ALL SELECT 2 LIMIT 1001"
        );
        assert_eq!(
            cap("SELECT * FROM t FOR VERSION AS OF 42"),
            "SELECT * FROM t FOR VERSION AS OF 42 LIMIT 1001"
        );
        assert_eq!(cap("SELECT * FROM t LIMIT 5"), "SELECT * FROM t LIMIT 5");
        assert_eq!(
            cap("SELECT * FROM t ORDER BY 1 LIMIT 5 OFFSET 2"),
            "SELECT * FROM t ORDER BY 1 LIMIT 5 OFFSET 2"
        );
        assert_eq!(
            cap("SELECT * FROM t FETCH FIRST 3 ROWS ONLY"),
            "SELECT * FROM t FETCH FIRST 3 ROWS ONLY"
        );
        assert_eq!(cap("SELECT 1 /* note */"), "SELECT 1 /* note */ LIMIT 1001");
        assert_eq!(cap("SHOW TABLES FROM c.s"), "SHOW TABLES FROM c.s");
        // #775: a trailing comment would swallow the LIMIT; it is refused,
        // not run uncapped.
        for sql in ["SELECT 1 -- trailing", "SELECT * FROM t --", "SELECT 1; --"] {
            let err = ensure_read_only(sql, None).expect_err(sql);
            assert!(err.contains("ending in a comment"), "{sql}: {err}");
        }
        let kind = ensure_read_only("SELECT 1", None).unwrap();
        assert_eq!(with_row_cap("SELECT 1", &kind, 0), "SELECT 1");
    }

    #[tokio::test]
    async fn execute_sql_readonly_sends_the_capped_statement_to_the_engine() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            test_config(),
        ));
        let token = scoped_token("sqe:mcp:read");
        let request = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "method":"tools/call",
                    "params":{"name":"execute_sql_readonly","arguments":{"sql":"SELECT * FROM t"}}
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let expected = format!("SELECT * FROM t LIMIT {}", test_config().max_result_rows + 1);
        assert_eq!(executor.4.lock().unwrap().as_deref(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn request_bearer_is_forwarded_to_the_sqe_session() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            test_config(),
        ));
        let token = scoped_token("sqe:mcp:read");
        let request = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({
                    "jsonrpc":"2.0",
                    "id":1,
                    "method":"tools/call",
                    "params":{"name":"execute_sql_readonly","arguments":{"sql":"SELECT 1"}}
                })
                .to_string(),
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(executor.0.lock().unwrap().as_deref(), Some(token.as_str()));
    }

    #[tokio::test]
    async fn unauthenticated_request_returns_rfc9728_challenge() {
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(RecordingExecutor::default()),
            test_config(),
        ));
        let response = app
            .oneshot(
                Request::post("/mcp")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response.headers()[header::WWW_AUTHENTICATE]
            .to_str()
            .unwrap();
        assert!(challenge.contains("resource_metadata="));
        assert!(challenge.contains("scope=\"sqe:mcp:read\""));
    }

    #[tokio::test]
    async fn browser_origin_is_fail_closed() {
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(RecordingExecutor::default()),
            test_config(),
        ));
        let response = app
            .oneshot(
                Request::post("/mcp")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .header(header::ORIGIN, "https://evil.example")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn router_can_be_merged_into_existing_web_listener() {
        let existing = Router::new().route("/health", get(|| async { "OK" }));
        let mcp = router(McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(RecordingExecutor::default()),
            test_config(),
        ));
        let _combined = existing.merge(mcp);
    }
    // -----------------------------------------------------------------------
    // [mcp.auth] mode = "static" / "anonymous": deployments with no IdP
    // -----------------------------------------------------------------------

    use sqe_core::config::{McpAnonymousConfig, McpAuthConfig};

    const READ_TOKEN: &str = "reader-token-long-enough";
    const WRITE_TOKEN: &str = "writer-token-long-enough";

    fn b64(value: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(value)
    }

    fn static_config(allow_writes: bool) -> McpConfig {
        McpConfig {
            resource_uri: "http://localhost:9091/mcp".to_string(),
            allow_writes,
            auth: McpAuthConfig {
                mode: McpAuthMode::Static,
                principals: vec![
                    McpPrincipalConfig {
                        user: "mcp-reader".to_string(),
                        roles: vec!["reader".to_string()],
                        token_env: String::new(),
                        token_b64: b64(READ_TOKEN),
                        default_catalog: None,
                        default_schema: None,
                    },
                    McpPrincipalConfig {
                        user: "mcp-writer".to_string(),
                        roles: vec!["data_writer".to_string()],
                        token_env: String::new(),
                        token_b64: b64(WRITE_TOKEN),
                        default_catalog: Some("iceberg".to_string()),
                        default_schema: Some("ns".to_string()),
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn anonymous_config() -> McpConfig {
        McpConfig {
            resource_uri: "http://localhost:9091/mcp".to_string(),
            auth: McpAuthConfig {
                mode: McpAuthMode::Anonymous,
                anonymous: McpAnonymousConfig {
                    user: "kiosk".to_string(),
                    roles: vec!["reader".to_string()],
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn tool_call(name: &str) -> String {
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"tools/call",
            "params":{"name":name,"arguments":{"sql":"SELECT 1"}}
        })
        .to_string()
    }

    async fn post(
        config: McpConfig,
        executor: Arc<RecordingExecutor>,
        bearer: Option<&str>,
        body: String,
    ) -> Response<Body> {
        let app = router(McpServerState::new(Arc::new(MockAuth), executor, config));
        let mut request = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = bearer {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        app.oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap()
    }

    /// A request against a shared router, with arbitrary extra headers, so
    /// two calls can share the server-side client registry.
    async fn send(
        app: &Router,
        bearer: &str,
        extra_headers: &[(&str, &str)],
        body: String,
    ) -> Response<Body> {
        let mut request = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }
        app.clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap()
    }

    fn initialize(client_name: &str, client_version: &str) -> String {
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"initialize",
            "params":{
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": client_name, "version": client_version}
            }
        })
        .to_string()
    }

    fn session_id_of(response: &Response<Body>) -> String {
        response
            .headers()
            .get(SESSION_ID_HEADER)
            .expect("initialize issues an Mcp-Session-Id")
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn tool_calls_stamp_tool_and_client_into_the_session_source() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            static_config(false),
        ));
        let init = send(&app, READ_TOKEN, &[], initialize("claude-code", "2.1.0")).await;
        assert_eq!(init.status(), StatusCode::OK);
        let session_id = session_id_of(&init);

        // The session id wins over User-Agent: clientInfo is the client's own
        // name for itself, the UA is whatever HTTP library it happens to use.
        let call = send(
            &app,
            READ_TOKEN,
            &[(SESSION_ID_HEADER, &session_id), ("user-agent", "node")],
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(call.status(), StatusCode::OK);
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:execute_sql_readonly:claude-code/2.1.0")
        );

        // DELETE ends the client session; the label falls back afterwards.
        let delete = app
            .clone()
            .oneshot(
                Request::delete("/mcp")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .header(header::AUTHORIZATION, format!("Bearer {READ_TOKEN}"))
                    .header(SESSION_ID_HEADER, &session_id)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(delete.status(), StatusCode::NO_CONTENT);
        send(
            &app,
            READ_TOKEN,
            &[(SESSION_ID_HEADER, &session_id)],
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:execute_sql_readonly")
        );
    }

    #[tokio::test]
    async fn another_principal_cannot_relabel_its_calls_with_a_borrowed_session_id() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            static_config(true),
        ));
        let init = send(&app, READ_TOKEN, &[], initialize("claude-code", "2.1.0")).await;
        let readers_session_id = session_id_of(&init);

        // The writer replays the reader's id. The binding to the identity that
        // registered it means the label is not applied; the writer's own
        // User-Agent is used instead, so its audit trail stays its own.
        let call = send(
            &app,
            WRITE_TOKEN,
            &[
                (SESSION_ID_HEADER, &readers_session_id),
                ("user-agent", "codex-cli/1.2"),
            ],
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(call.status(), StatusCode::OK);
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:execute_sql_readonly:codex-cli/1.2")
        );
    }

    /// #680: in static and anonymous mode every caller is the same user, so
    /// the binding also covers the address. The same principal from another
    /// host replaying the id gets its own label, and its session carries
    /// its own peer for the rate-limit key.
    #[tokio::test]
    async fn the_same_principal_from_another_address_cannot_borrow_a_session_id() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            static_config(false),
        ));
        let init = send(&app, READ_TOKEN, &[], initialize("claude-code", "2.1.0")).await;
        let laptop_session_id = session_id_of(&init);

        let request = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([10, 0, 0, 9], 4242))))
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {READ_TOKEN}"))
            .header(SESSION_ID_HEADER, &laptop_session_id)
            .header("user-agent", "codex-cli/1.2")
            .body(Body::from(tool_call("execute_sql_readonly")))
            .unwrap();
        let call = app.clone().oneshot(request).await.unwrap();
        assert_eq!(call.status(), StatusCode::OK);
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:execute_sql_readonly:codex-cli/1.2"),
            "the label registered from 127.0.0.1 is not applied to a call from 10.0.0.9"
        );
    }

    #[tokio::test]
    async fn sessions_carry_the_peer_address_for_the_rate_limit_key() {
        let peer_seen = Arc::new(Mutex::new(None));
        struct PeerExecutor(Arc<Mutex<Option<String>>>);
        #[async_trait]
        impl QueryExecutor for PeerExecutor {
            async fn execute(&self, session: &Session, _sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
                *self.0.lock().unwrap() = session.peer_ip.clone();
                Ok(Vec::new())
            }
        }
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            Arc::new(PeerExecutor(peer_seen.clone())),
            static_config(false),
        ));
        let call = send(&app, READ_TOKEN, &[], tool_call("execute_sql_readonly")).await;
        assert_eq!(call.status(), StatusCode::OK);
        assert_eq!(peer_seen.lock().unwrap().as_deref(), Some("127.0.0.1"));
    }

    #[tokio::test]
    async fn unknown_session_id_and_no_user_agent_leave_the_bare_tool_label() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            static_config(false),
        ));
        // An id this server never issued (or one from before a restart) is
        // ignored, never an error: the id is advisory.
        let call = send(
            &app,
            READ_TOKEN,
            &[(SESSION_ID_HEADER, "not-issued-by-this-server")],
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(call.status(), StatusCode::OK);
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:execute_sql_readonly")
        );
    }

    #[test]
    fn client_labels_are_printable_ascii_and_bounded() {
        // Newlines and quotes cannot reach a log line or an audit record.
        assert_eq!(
            sanitize_label("claude-code/2.1\n\"; DROP").as_deref(),
            Some("claude-code/2.1; DROP")
        );
        assert_eq!(sanitize_label("\t\n\u{1b}"), None);
        assert_eq!(
            sanitize_label(&"a".repeat(500)).map(|s| s.len()),
            Some(CLIENT_LABEL_MAX)
        );
        // clientInfo without a version is just the name; without a name it is
        // nothing, not "unknown/1.0".
        assert_eq!(
            client_label_from_client_info(&json!({"clientInfo":{"name":"codex","version":""}}))
                .as_deref(),
            Some("codex")
        );
        assert_eq!(
            client_label_from_client_info(&json!({"clientInfo":{"version":"1.0"}})),
            None
        );
        assert_eq!(source_label("list_tables", None), "mcp:list_tables");
        assert_eq!(
            source_label("list_tables", Some("codex/1.0")),
            "mcp:list_tables:codex/1.0"
        );
    }

    /// #562: OAuth mode hands the provider chain the peer address (the
    /// chain-level limiter keys on it) and maps the limiter's denial to 429.
    /// The provider here records what it was given and denies the second
    /// call the way the wrapper does.
    #[tokio::test]
    async fn oauth_carries_the_peer_address_and_maps_rate_limit_to_429() {
        struct PeerRecordingAuth {
            seen: Mutex<Vec<Option<String>>>,
        }
        #[async_trait]
        impl AuthProvider for PeerRecordingAuth {
            async fn authenticate_oauth(
                &self,
                credentials: &FlightCredentials,
            ) -> Result<Identity, AuthError> {
                let calls = {
                    let mut seen = self.seen.lock().unwrap();
                    seen.push(credentials.peer_ip.clone());
                    seen.len()
                };
                if calls > 1 {
                    return Err(AuthError::RateLimited);
                }
                MockAuth.authenticate(credentials).await
            }
            async fn authenticate(
                &self,
                credentials: &FlightCredentials,
            ) -> Result<Identity, AuthError> {
                MockAuth.authenticate(credentials).await
            }
        }
        let auth = Arc::new(PeerRecordingAuth {
            seen: Mutex::new(Vec::new()),
        });
        let app = router(McpServerState::new(
            auth.clone(),
            Arc::new(RecordingExecutor::default()),
            test_config(),
        ));
        let token = scoped_token("sqe:mcp:read");
        let first = send(&app, &token, &[], tool_call("execute_sql_readonly")).await;
        assert_eq!(first.status(), StatusCode::OK);
        let second = send(&app, &token, &[], tool_call("execute_sql_readonly")).await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            second.headers().get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some("6"),
            "60 / the default 10 attempts per minute"
        );
        let bytes = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "rate_limited");
        assert_eq!(body["limiter"], "auth");
        assert_eq!(body["window_seconds"], 60);
        assert_eq!(body["retry_after_seconds"], 6);
        assert!(body["knobs"].as_array().unwrap().iter().any(|k| k == "rate_limit.auth_burst"));

        // A limiter denial inside a tool call is structured the same way.
        let failure = tool_failure(
            "Rate limit exceeded: per-user limit reached for user 'u' (rate_limit.per_user_queries_per_minute, per minute); retry after 3s".to_string(),
        );
        assert_eq!(failure["isError"], true);
        assert_eq!(failure["structuredContent"]["error"], "rate_limited");
        assert_eq!(failure["structuredContent"]["retry_after_seconds"], 3);
        assert!(tool_failure("table not found".to_string()).get("structuredContent").is_none());

        let seen = auth.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            seen.iter().all(|p| p.as_deref() == Some("127.0.0.1")),
            "the peer address travels with the credentials: {seen:?}"
        );
    }

    fn change_call(sql: &str) -> String {
        json!({
            "jsonrpc":"2.0",
            "id":1,
            "method":"tools/call",
            "params":{"name":"execute_sql_change","arguments":{"sql":sql}}
        })
        .to_string()
    }

    /// One change-tool call against a fresh server: the JSON-RPC body and
    /// the executor, so a test can assert both the refusal and that nothing
    /// reached the engine.
    async fn change(
        config: McpConfig,
        hive_roots: &[&str],
        token: &str,
        sql: &str,
    ) -> (Value, Arc<RecordingExecutor>) {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(
            McpServerState::new(Arc::new(MockAuth), executor.clone(), config)
                .with_hive_roots(hive_roots.iter().map(|s| s.to_string()).collect()),
        );
        let response = send(&app, token, &[], change_call(sql)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (serde_json::from_slice(&bytes).unwrap(), executor)
    }

    fn executed(executor: &RecordingExecutor) -> Option<String> {
        executor.4.lock().unwrap().clone()
    }

    /// The tool set is fixed per process, so `tools/list` tells the client
    /// to cache it (SEP-2549) and returns the same list every time.
    #[tokio::test]
    async fn tools_list_carries_cache_hints_and_is_stable() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor,
            static_config(false),
        ));
        let list = || async {
            let response = send(
                &app,
                READ_TOKEN,
                &[],
                r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_string(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
        };
        let first = list().await;
        assert_eq!(first["ttlMs"], json!(TOOLS_LIST_TTL_MS));
        assert_eq!(first["cacheScope"], json!("public"));
        assert!(!first["tools"].as_array().unwrap().is_empty());
        // `_meta.trace_id` is per request by design; the list itself is not.
        let without_meta = |mut v: Value| {
            v.as_object_mut().unwrap().remove("_meta");
            v
        };
        assert_eq!(
            without_meta(list().await),
            without_meta(first),
            "the list a client caches is the list it would get again"
        );
    }

    #[tokio::test]
    async fn change_tool_refuses_a_catalog_outside_write_catalogs() {
        let mut config = static_config(true);
        config.write_catalogs = vec!["Iceberg".to_string()];

        let (body, executor) =
            change(config.clone(), &[], WRITE_TOKEN, "INSERT INTO iceberg.ns.t VALUES (1)").await;
        assert!(body.get("error").is_none(), "{body}");
        assert!(executed(&executor).is_some());

        let (body, executor) =
            change(config.clone(), &[], WRITE_TOKEN, "INSERT INTO other.ns.t VALUES (1)").await;
        assert_eq!(body["error"]["code"], -32003, "{body}");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not in mcp.write_catalogs"));
        assert!(executed(&executor).is_none(), "refused statements never reach the engine");

        // The writer principal's default catalog is iceberg, so an unqualified
        // target resolves there and passes.
        let (body, executor) = change(config, &[], WRITE_TOKEN, "INSERT INTO t VALUES (1)").await;
        assert!(body.get("error").is_none(), "{body}");
        assert!(executed(&executor).is_some());
    }

    #[tokio::test]
    async fn hive_roots_need_an_explicit_write_catalogs_entry() {
        // No allowlist at all: Iceberg writes pass as before, the Hive root
        // does not, because that write would run on the engine's credentials.
        let config = static_config(true);
        let (body, executor) =
            change(config.clone(), &["lake"], WRITE_TOKEN, "INSERT INTO lake.db.t VALUES (1)").await;
        assert_eq!(body["error"]["code"], -32003, "{body}");
        assert!(body["error"]["message"].as_str().unwrap().contains("Hive root"));
        assert!(executed(&executor).is_none());

        let (body, _) =
            change(config.clone(), &["lake"], WRITE_TOKEN, "INSERT INTO iceberg.ns.t VALUES (1)").await;
        assert!(body.get("error").is_none(), "{body}");

        // Naming the root is the opt-in.
        let mut opted_in = static_config(true);
        opted_in.write_catalogs = vec!["lake".to_string()];
        let (body, executor) =
            change(opted_in, &["lake"], WRITE_TOKEN, "INSERT INTO lake.db.t VALUES (1)").await;
        assert!(body.get("error").is_none(), "{body}");
        assert!(executed(&executor).is_some());
    }

    /// A procedure has no parser-visible target, which used to make it the
    /// fail-closed case of the catalog gate. Since #677 it never reaches the
    /// gate: procedures are outside the tool's surface, gated or not.
    #[tokio::test]
    async fn change_tool_refuses_a_procedure_before_the_catalog_gate() {
        for (config, hive_roots) in [
            (
                {
                    let mut c = static_config(true);
                    c.write_catalogs = vec!["iceberg".to_string()];
                    c
                },
                vec![],
            ),
            (static_config(true), vec![]),
        ] {
            let (body, executor) = change(
                config,
                &hive_roots,
                WRITE_TOKEN,
                "CALL system.rewrite_data_files(table => 'ns.t')",
            )
            .await;
            assert_eq!(body["error"]["code"], -32602, "{body}");
            assert!(body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("outside execute_sql_change"));
            assert!(executed(&executor).is_none());
        }
    }

    #[tokio::test]
    async fn unqualified_target_without_a_default_catalog_is_refused_while_gated() {
        let mut config = static_config(true);
        config.write_catalogs = vec!["iceberg".to_string()];
        // The writer principal carries defaults; strip them so resolution
        // has nothing to fall back on.
        for principal in &mut config.auth.principals {
            principal.default_catalog = None;
            principal.default_schema = None;
        }
        let (body, executor) = change(config, &[], WRITE_TOKEN, "INSERT INTO t VALUES (1)").await;
        assert_eq!(body["error"]["code"], -32003, "{body}");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Qualify the target"));
        assert!(executed(&executor).is_none());
    }

    #[test]
    fn write_targets_resolve_the_catalog_of_each_change_kind() {
        let catalog_of = |sql: &str| {
            let kind = classify(sql, Some("dflt")).unwrap_or_else(|e| panic!("{sql}: {e}"));
            let targets = write_targets(&kind).unwrap_or_else(|| panic!("{sql}: no targets"));
            targets
                .iter()
                .map(|t| target_catalog(t, Some("dflt")).unwrap_or_else(|| panic!("{sql}: unresolved")))
                .collect::<Vec<_>>()
        };
        assert_eq!(catalog_of("INSERT INTO a.b.t VALUES (1)"), ["a"]);
        assert_eq!(catalog_of("INSERT INTO b.t VALUES (1)"), ["dflt"]);
        assert_eq!(catalog_of("UPDATE a.b.t SET x = 1"), ["a"]);
        assert_eq!(catalog_of("DELETE FROM a.b.t WHERE x = 1"), ["a"]);
        assert_eq!(
            catalog_of("MERGE INTO a.b.t USING a.b.s ON t.id = s.id WHEN MATCHED THEN DELETE"),
            ["a"]
        );
        assert_eq!(catalog_of("CREATE TABLE a.b.t (x INT)"), ["a"]);
        // The source is a read, not a target: only the created table counts.
        assert_eq!(
            catalog_of("CREATE TABLE a.b.t AS SELECT * FROM other.c.d"),
            ["a"]
        );
        assert_eq!(catalog_of("DROP TABLE a.b.t"), ["a"]);
        assert_eq!(catalog_of("DROP SCHEMA a.b"), ["a"]);
        assert_eq!(catalog_of("CREATE SCHEMA b"), ["dflt"]);
        assert_eq!(catalog_of("ALTER TABLE a.b.t RENAME TO u"), ["a"]);
        assert_eq!(catalog_of("TRUNCATE TABLE a.b.t"), ["a"]);

        let call = classify("CALL system.rewrite_data_files(table => 'ns.t')", None).unwrap();
        assert!(write_targets(&call).is_none(), "procedures have no parser-visible target");
    }

    #[tokio::test]
    async fn session_defaults_come_from_the_principal_then_the_endpoint() {
        let mut config = static_config(true);
        config.default_catalog = Some("warehouse".to_string());
        config.default_schema = Some("public".to_string());
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            config,
        ));

        send(&app, READ_TOKEN, &[], tool_call("execute_sql_readonly")).await;
        assert_eq!(
            executor.3.lock().unwrap().clone(),
            Some((Some("warehouse".to_string()), Some("public".to_string())))
        );

        // The writer principal declares its own defaults, which win.
        send(&app, WRITE_TOKEN, &[], tool_call("execute_sql_readonly")).await;
        assert_eq!(
            executor.3.lock().unwrap().clone(),
            Some((Some("iceberg".to_string()), Some("ns".to_string())))
        );
    }

    /// #677: the change tool is data and table/schema/view DDL. Administration
    /// that is not read-only is refused by the tool itself, before the engine.
    #[test]
    fn change_tool_refuses_administration_statements() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t WHERE a = 1",
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t RENAME TO u",
            "TRUNCATE TABLE t",
            "CREATE SCHEMA s",
            "DROP VIEW v",
        ] {
            ensure_change(sql, Some("dflt")).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in [
            "GRANT SELECT ON t TO ROLE analyst",
            "REVOKE SELECT ON t FROM ROLE analyst",
            "CALL system.rewrite_data_files(table => 'ns.t')",
            "ATTACH 's3://b/wh/' AS lake (TYPE hive_external)",
            "DROP SECRET partner",
            "SET SESSION iceberg.compression_codec = 'ZSTD'",
            "BEGIN",
        ] {
            let err = ensure_change(sql, Some("dflt")).expect_err(sql);
            assert!(err.contains("outside execute_sql_change"), "{sql}: {err}");
        }
    }

    #[test]
    fn classification_goes_through_the_engine_front_door() {
        // The door accepts what the engine accepts; the original text is what
        // the executor receives, so nothing here re-serializes.
        ensure_read_only("SELECT * FROM information_schema.tables", Some("iceberg"))
            .expect("information_schema with a session catalog");
        ensure_read_only("SELECT 1 + 1", None).expect("anonymous column");
        ensure_read_only("SELECT * FROM t FOR VERSION AS OF 5", None)
            .expect("SQE time-travel clause survives the pre-parse pipeline");
        assert!(ensure_read_only("SELEC 1", None).is_err());
    }

    #[test]
    fn like_matching_is_case_insensitive_and_treats_a_bare_word_as_a_substring() {
        assert!(like_matches("ORD", "orders"));
        assert!(like_matches("orders", "ORDERS"));
        assert!(!like_matches("orderz", "orders"));
        // Explicit wildcards are the caller's own: anchored at both ends.
        assert!(like_matches("ord%", "orders"));
        assert!(!like_matches("rd%", "orders"));
        assert!(like_matches("o_ders", "orders"));
        assert!(!like_matches("o_ers", "orders"));
        assert!(like_matches("%", ""));
        // A quote is a character in the pattern, never syntax.
        assert!(!like_matches("x' OR '1'='1", "orders"));
    }

    /// Answers `SHOW SCHEMAS` / `SHOW TABLES` from a fixed map, recording
    /// every statement it was asked to run.
    struct ScriptedExecutor {
        answers: HashMap<&'static str, Vec<&'static str>>,
        asked: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl QueryExecutor for ScriptedExecutor {
        async fn execute(&self, _session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
            self.asked.lock().unwrap().push(sql.to_string());
            let Some(values) = self.answers.get(sql) else {
                return Err(SqeError::Auth(format!("not authorized: {sql}")));
            };
            let schema = Arc::new(Schema::new(vec![Field::new("Name", DataType::Utf8, true)]));
            let column = StringArray::from(values.clone());
            Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()])
        }
    }

    /// Answers `SHOW CATALOGS` with a fixed list and every `SHOW SCHEMAS`
    /// with a scripted error, so the diagnostics classification is under
    /// test, not the catalog.
    struct ProbeExecutor {
        catalogs: Vec<&'static str>,
        schema_errors: HashMap<&'static str, SqeError>,
    }

    #[async_trait]
    impl QueryExecutor for ProbeExecutor {
        async fn execute(&self, _session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
            if sql == "SHOW CATALOGS" {
                let schema = Arc::new(Schema::new(vec![Field::new("Catalog", DataType::Utf8, true)]));
                let column = StringArray::from(self.catalogs.clone());
                return Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()]);
            }
            match self.schema_errors.get(sql) {
                Some(SqeError::Execution(m)) => Err(SqeError::Execution(m.clone())),
                Some(other) => Err(SqeError::Execution(other.to_string())),
                None => {
                    let schema = Arc::new(Schema::new(vec![Field::new("Schema", DataType::Utf8, true)]));
                    let column = StringArray::from(vec!["a"]);
                    Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap()])
                }
            }
        }
    }

    #[test]
    fn a_decimal_overflow_gets_the_metastore_hint_with_the_declared_precision() {
        let raw = "Query execution failed: Arrow error: Invalid argument error: 1029.23 is too \
                   large to store in a Decimal128 of precision 5. Max is 999.99";
        let hinted = with_decimal_hint(raw.to_string());
        assert!(hinted.starts_with(raw), "{hinted}");
        assert!(hinted.contains("(declared precision 5)"), "{hinted}");
        assert!(hinted.contains("widen the column's decimal type in the metastore"), "{hinted}");
        assert!(hinted.contains("describe_table"), "{hinted}");
        // Anything else passes through untouched.
        let other = "Schema error: No field named tag".to_string();
        assert_eq!(with_decimal_hint(other.clone()), other);
    }

    #[tokio::test]
    async fn list_catalogs_reports_the_engines_storage_credentials() {
        let executor = Arc::new(ProbeExecutor {
            catalogs: vec!["glue"],
            schema_errors: HashMap::new(),
        });
        let probe: StorageProbe = Arc::new(|| {
            Box::pin(async {
                StorageCredentialReport {
                    status: "auth_error",
                    provider: "aws-sdk-default-chain",
                    expires_at: None,
                    detail: Some(
                        "storage credentials expired or unavailable (aws sdk default chain): \
                         the SSO session has expired"
                            .to_string(),
                    ),
                }
            })
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            .with_hive_roots(vec!["glue".into()])
            .with_storage_probe(probe);
        let session = local_session("mcp-reader".into(), vec![]);

        let result = list_catalogs_with_diagnostics(&state, &session).await.unwrap();
        let diagnostics = result["structuredContent"]["diagnostics"].as_array().unwrap().clone();
        let storage = diagnostics
            .iter()
            .find(|d| d["kind"] == "object_store")
            .expect("an object_store entry");
        assert_eq!(storage["status"], "auth_error", "{storage}");
        // #765: the provider kind and the expiry never reach the client.
        assert!(
            storage.get("provider").is_none() && storage.get("expires_at").is_none(),
            "{storage}"
        );
        assert!(storage["detail"].as_str().unwrap().contains("SSO session has expired"));
        // The Hive root itself answered, so the two are told apart.
        let glue = diagnostics.iter().find(|d| d["catalog"] == "glue").unwrap();
        assert_eq!(glue["status"], "reachable", "{glue}");
    }

    #[tokio::test]
    async fn list_catalogs_reports_each_hive_roots_credential_state() {
        let executor = Arc::new(ProbeExecutor {
            catalogs: vec!["default", "glue", "lake", "iceberg"],
            schema_errors: HashMap::from([
                (
                    "SHOW SCHEMAS FROM \"glue\"",
                    SqeError::Execution(
                        "hive_external: AWS credentials missing or expired for the Glue catalog \
                         (region=eu-central-1, op=get_databases: dispatch failure: user error: \
                         the SSO session has expired)"
                            .to_string(),
                    ),
                ),
                (
                    "SHOW SCHEMAS FROM \"lake\"",
                    SqeError::Execution("hive_external: object store error: connection refused".to_string()),
                ),
            ]),
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            // `hidden` is a Hive root the caller cannot see: no probe, no entry.
            .with_hive_roots(vec!["glue".into(), "lake".into(), "hidden".into()]);
        let session = local_session("mcp-reader".into(), vec![]);

        let result = list_catalogs_with_diagnostics(&state, &session).await.unwrap();
        let body = &result["structuredContent"];
        // #706: the engine's `default` placeholder is not a catalog anyone can name.
        assert_eq!(body["rows"].as_array().unwrap().len(), 3, "{body}");
        assert_eq!(body["total_rows"], 3, "{body}");
        let diagnostics = body["diagnostics"].as_array().unwrap();
        assert_eq!(diagnostics.len(), 2, "one per visible Hive root: {diagnostics:?}");
        let by_name = |n: &str| diagnostics.iter().find(|d| d["catalog"] == n).cloned().unwrap();
        let glue = by_name("glue");
        assert_eq!(glue["status"], "auth_error", "{glue}");
        assert_eq!(glue["kind"], "hive_external");
        // #675: classified on the full text, but the region and the SSO
        // detail stay in the server log; the client gets the id to quote.
        let detail = glue["detail"].as_str().unwrap();
        assert!(!detail.contains("eu-central-1") && detail.contains("error_id"), "{glue}");
        let lake = by_name("lake");
        assert_eq!(lake["status"], "unreachable", "{lake}");
        let detail = lake["detail"].as_str().unwrap();
        assert!(!detail.contains("connection refused") && detail.contains("error_id"), "{lake}");
        // The Iceberg catalog is listed and not probed.
        assert!(diagnostics.iter().all(|d| d["catalog"] != "iceberg"));
    }

    /// #706: `default`, `datafusion` and `system` are not catalogs a
    /// statement can name, unless one is a configured Hive root.
    #[tokio::test]
    async fn list_catalogs_drops_phantom_catalog_names_unless_they_are_hive_roots() {
        let executor = Arc::new(ProbeExecutor {
            catalogs: vec!["default", "glue", "datafusion", "system"],
            schema_errors: HashMap::new(),
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            .with_hive_roots(vec!["glue".into()]);
        let session = local_session("mcp-reader".into(), vec![]);
        let body = list_catalogs_with_diagnostics(&state, &session)
            .await
            .unwrap()["structuredContent"]
            .clone();
        let names: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["Catalog"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["glue"], "{body}");

        // A Hive root that happens to be called `default` is real and stays.
        let executor = Arc::new(ProbeExecutor {
            catalogs: vec!["default", "glue"],
            schema_errors: HashMap::new(),
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            .with_hive_roots(vec!["default".into()]);
        let body = list_catalogs_with_diagnostics(&state, &session)
            .await
            .unwrap()["structuredContent"]
            .clone();
        assert_eq!(body["rows"].as_array().unwrap().len(), 2, "{body}");
    }

    /// #765: an expiring SDK credential is reported as temporary, never with its timestamp.
    #[tokio::test]
    async fn list_catalogs_diagnostics_never_carry_the_credential_expiry() {
        let executor = Arc::new(ProbeExecutor {
            catalogs: vec!["glue"],
            schema_errors: HashMap::new(),
        });
        let probe: StorageProbe = Arc::new(|| {
            Box::pin(async {
                StorageCredentialReport {
                    status: "ok",
                    provider: "aws-sdk-default-chain",
                    expires_at: Some("2026-09-21T12:00:00Z".to_string()),
                    detail: None,
                }
            })
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            .with_storage_probe(probe);
        let session = local_session("mcp-reader".into(), vec![]);
        let result = list_catalogs_with_diagnostics(&state, &session)
            .await
            .unwrap();
        let text = result["structuredContent"].to_string();
        assert!(
            !text.contains("2026-09-21")
                && !text.contains("expires_at")
                && !text.contains("aws-sdk"),
            "{text}"
        );
        let storage = result["structuredContent"]["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["kind"] == "object_store")
            .unwrap()
            .clone();
        assert_eq!(storage["status"], "ok");
        assert!(
            storage["detail"]
                .as_str()
                .unwrap()
                .contains("temporary credentials"),
            "{storage}"
        );
    }

    /// #765: `df_settings` lists the engine's runtime settings (spill path
    /// included); the rest of `information_schema` stays readable.
    #[test]
    fn read_only_refuses_df_settings_but_not_information_schema_tables() {
        for sql in [
            "SELECT * FROM information_schema.df_settings",
            "SELECT * FROM iceberg.information_schema.df_settings",
            "SELECT name FROM INFORMATION_SCHEMA.DF_SETTINGS WHERE name LIKE '%spill%'",
        ] {
            let err = ensure_read_only(sql, Some("iceberg")).expect_err(sql);
            assert!(
                err.to_ascii_lowercase().contains("df_settings") && err.contains("operator surface"),
                "{sql}: {err}"
            );
        }
        ensure_read_only("SELECT * FROM information_schema.tables", Some("iceberg"))
            .expect("tables");
        ensure_read_only("SELECT * FROM iceberg.information_schema.columns", Some("iceberg"))
            .expect("columns");
    }

    /// #774: `SHOW CREATE` names its target outside a TableFactor; the
    /// denylist must still see it.
    #[test]
    fn read_only_refuses_show_create_of_denied_relations() {
        for sql in [
            "SHOW CREATE TABLE system.runtime.queries",
            "SHOW CREATE VIEW system.runtime.queries",
            "SHOW CREATE TABLE \"SYSTEM\".runtime.queries",
            "SHOW CREATE TABLE information_schema.df_settings",
            "SHOW CREATE TABLE iceberg.INFORMATION_SCHEMA.\"df_settings\"",
        ] {
            let err = ensure_read_only(sql, Some("iceberg")).expect_err(sql);
            assert!(err.contains("operator surface"), "{sql}: {err}");
        }
        ensure_read_only("SHOW CREATE TABLE iceberg.sales.orders", Some("iceberg")).expect("ddl");
        ensure_read_only("SHOW CREATE TABLE information_schema.tables", Some("iceberg"))
            .expect("info schema tables");
    }

    /// #675: a system error's text (bucket, key, host, region) stays in the
    /// server log; a user error, the decimal overflow and a rate-limit
    /// denial go through verbatim because the caller can act on them.
    #[test]
    fn system_error_text_is_withheld_from_the_client_and_user_errors_are_not() {
        let storage = SqeError::Execution(
            "hive_external: object store error: GET s3://acme-prod-lake/dt=2026/part-0.parquet \
             from https://s3.eu-central-1.amazonaws.com: 403"
                .to_string(),
        );
        let text = client_error_text(&storage);
        for leak in ["acme-prod-lake", "part-0", "amazonaws", "eu-central-1"] {
            assert!(!text.contains(leak), "{text}");
        }
        assert!(text.contains("error_id"), "{text}");

        let missing = SqeError::with_code(
            SqeErrorCode::TableNotFound,
            "table 'sales.orderz' not found; did you mean 'sales.orders'?",
        );
        assert_eq!(client_error_text(&missing), missing.to_string());

        let decimal = SqeError::Execution(
            "1029.23 is too large to store in a Decimal128 of precision 5. Max is 999.99".to_string(),
        );
        assert!(client_error_text(&decimal).contains("precision 5"));

        let limited = SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            "rate limit: 120 queries per minute; retry after 12s",
        );
        assert!(client_error_text(&limited).contains("retry after 12s"));
    }

    #[tokio::test]
    async fn search_tables_walks_show_statements_and_skips_unlistable_schemas() {
        let executor = Arc::new(ScriptedExecutor {
            answers: HashMap::from([
                (
                    "SHOW SCHEMAS FROM \"sales_wh\"",
                    vec!["information_schema", "ac", "ops", "locked"],
                ),
                ("SHOW TABLES FROM \"sales_wh\".\"ac\"", vec!["orders", "customers"]),
                ("SHOW TABLES FROM \"sales_wh\".\"ops\"", vec!["order_events", "audit"]),
                ("SHOW VIEWS FROM \"sales_wh\".\"ops\"", vec!["order_events"]),
                // "locked" has no answer: the executor refuses it, as a catalog
                // refuses a schema the caller may not list.
            ]),
            asked: Mutex::new(Vec::new()),
        });
        let mut config = static_config(false);
        config.max_result_rows = 1;
        let state = McpServerState::new(Arc::new(MockAuth), executor.clone(), config);
        let session = local_session("mcp-reader".into(), vec![]);

        let result = search_tables(
            &state,
            &session,
            &json!({"pattern": "ord", "catalog": "sales_wh"}),
        )
        .await
        .unwrap();
        // Two matches across two schemas; the response cap keeps one, says so,
        // and hands out the token for the rest.
        assert_eq!(result["total_rows"], 2);
        assert_eq!(result["returned_rows"], 1);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["rows"][0]["table_schema"], "ac");
        assert_eq!(result["rows"][0]["table_name"], "orders");
        assert_eq!(result["rows"][0]["table_type"], "table", "no SHOW VIEWS answer for ac: a table");
        let token = result["next_page_token"].as_str().unwrap().to_string();
        // The schema the caller may not list is reported, never dropped.
        assert_eq!(result["skipped_schemas"][0]["schema"], "locked");
        assert!(result["skipped_schemas"][0]["reason"].as_str().unwrap().contains("not authorized"));
        let asked = executor.asked.lock().unwrap().clone();
        assert!(
            !asked.iter().any(|s| s == "SHOW VIEWS FROM \"sales_wh\".\"ops\""),
            "ops is not on this page; its kinds are not fetched: {asked:?}"
        );
        let next = search_tables(
            &state,
            &session,
            &json!({"pattern": "ord", "catalog": "sales_wh", "page_token": token}),
        )
        .await
        .unwrap();
        assert_eq!(next["rows"][0]["table_schema"], "ops");
        assert_eq!(next["rows"][0]["table_name"], "order_events");
        assert_eq!(next["rows"][0]["table_type"], "view");
        assert_eq!(next["truncated"], false);
        assert!(next.get("next_page_token").is_none());
        let bad = search_tables(&state, &session, &json!({"pattern": "ord", "catalog": "sales_wh", "page_token": "junk"}))
            .await
            .unwrap_err();
        assert_eq!(bad.0, -32602);
        assert!(
            !asked.iter().any(|s| s.contains("information_schema")),
            "information_schema is not a schema to search: {asked:?}"
        );
        assert!(asked.iter().any(|s| s == "SHOW TABLES FROM \"sales_wh\".\"locked\""));

        // No catalog anywhere: an argument error that says what to pass, and
        // nothing runs.
        let before = executor.asked.lock().unwrap().len();
        let err = search_tables(&state, &session, &json!({"pattern": "x"}))
            .await
            .unwrap_err();
        assert_eq!(err.0, -32602);
        assert!(err.1.contains("needs a catalog"));
        assert_eq!(executor.asked.lock().unwrap().len(), before);

        // The session default catalog fills in.
        let session = session.with_catalog(Some("sales_wh".into()));
        let result = search_tables(&state, &session, &json!({"pattern": "%audit"}))
            .await
            .unwrap();
        assert_eq!(result["rows"][0]["table_name"], "audit");
        assert_eq!(result["rows"][0]["table_type"], "table");
        let result = search_tables(&state, &session, &json!({"pattern": "events"}))
            .await
            .unwrap();
        assert_eq!(result["rows"][0]["table_type"], "view", "{result}");
    }

    /// A walk over more schemas than one call's query budget lists everything:
    /// the tool call is admitted once and its `SHOW`s take the metadata lane.
    #[tokio::test]
    async fn search_tables_over_135_schemas_is_one_admission_and_no_query_charge() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct LaneCounter {
            admitted: AtomicUsize,
            query_lane: AtomicUsize,
            metadata_lane: AtomicUsize,
        }
        #[async_trait]
        impl QueryExecutor for LaneCounter {
            async fn execute(&self, _s: &Session, _sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
                self.query_lane.fetch_add(1, Ordering::SeqCst);
                Err(SqeError::Execution("the query lane must not carry metadata".into()))
            }
            async fn execute_metadata(&self, _s: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
                self.metadata_lane.fetch_add(1, Ordering::SeqCst);
                let schema = Arc::new(Schema::new(vec![Field::new("Name", DataType::Utf8, true)]));
                let values: Vec<String> = if sql.starts_with("SHOW SCHEMAS") {
                    (0..135).map(|i| format!("db{i:03}")).collect()
                } else if sql.starts_with("SHOW TABLES") {
                    vec!["orders".to_string(), "audit".to_string()]
                } else {
                    Vec::new()
                };
                Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()])
            }
            fn admit_metadata_call(&self, _s: &Session) -> Result<(), SqeError> {
                self.admitted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let executor = Arc::new(LaneCounter {
            admitted: AtomicUsize::new(0),
            query_lane: AtomicUsize::new(0),
            metadata_lane: AtomicUsize::new(0),
        });
        let mut config = static_config(false);
        config.max_result_rows = 500;
        let state = McpServerState::new(Arc::new(MockAuth), executor.clone(), config);
        let auth = AuthContext { session: local_session("mcp-reader".into(), vec![]), scopes: None };
        let result = call_tool(
            &state,
            &auth,
            None,
            &json!({"name": "search_tables", "arguments": {"pattern": "ord", "catalog": "glue", "page_size": 500}}),
        )
        .await
        .unwrap();
        let body = &result["structuredContent"];
        assert_eq!(body["total_rows"], 135, "{body}");
        assert_eq!(body["returned_rows"], 135);
        assert_eq!(body["skipped_schemas"].as_array().unwrap().len(), 0);
        assert_eq!(executor.admitted.load(Ordering::SeqCst), 1, "one admission per tool call");
        assert_eq!(executor.query_lane.load(Ordering::SeqCst), 0);
        // SHOW SCHEMAS + 135 SHOW TABLES + one SHOW VIEWS per schema on the page.
        assert_eq!(executor.metadata_lane.load(Ordering::SeqCst), 1 + 135 + 135);
    }

    /// `list_tables` carries the kind of every name from one `SHOW VIEWS`, so an
    /// agent can skip views without a call per table.
    #[tokio::test]
    async fn list_tables_marks_views_from_one_show_views() {
        let executor = Arc::new(ScriptedExecutor {
            answers: HashMap::from([
                ("SHOW TABLES FROM \"glue\".\"bi\"", vec!["Sales_V", "facts"]),
                ("SHOW VIEWS FROM \"glue\".\"bi\"", vec!["sales_v"]),
                ("SHOW TABLES FROM \"glue\".\"raw\"", vec!["events"]),
            ]),
            asked: Mutex::new(Vec::new()),
        });
        let state = McpServerState::new(Arc::new(MockAuth), executor.clone(), static_config(false));
        let session = local_session("mcp-reader".into(), vec![]);

        let result = list_tables(&state, &session, &json!({"catalog": "glue", "schema": "bi"}))
            .await
            .unwrap();
        assert_eq!(result["rows"][0]["table_name"], "Sales_V");
        assert_eq!(result["rows"][0]["table_type"], "view", "kinds compare case-insensitively");
        assert_eq!(result["rows"][1]["table_type"], "table");
        assert_eq!(result["columns"][1]["name"], "table_type");

        // A schema whose views cannot be listed lists everything as a table.
        let result = list_tables(&state, &session, &json!({"catalog": "glue", "schema": "raw"}))
            .await
            .unwrap();
        assert_eq!(result["rows"][0]["table_type"], "table");
        assert_eq!(executor.asked.lock().unwrap().len(), 4, "two statements per call");

        // A schema the caller may not list is the executor's error, not a panic.
        let err = list_tables(&state, &session, &json!({"catalog": "glue", "schema": "locked"}))
            .await
            .unwrap_err();
        assert_eq!(err.0, -32000);
        assert!(err.1.contains("not authorized"), "{}", err.1);
    }

    /// The text an agent reads first steers it to `describe_schema`.
    #[test]
    fn instructions_and_descriptions_point_at_describe_schema() {
        assert!(SERVER_INSTRUCTIONS.contains("search_tables, then describe_schema"));
        assert!(SERVER_INSTRUCTIONS.matches(". ").count() < 4, "under four sentences: {SERVER_INSTRUCTIONS}");
        let tools = tools(&static_config(false));
        let desc = |name: &str| {
            tools
                .iter()
                .find(|t| t["name"] == name)
                .unwrap()["description"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert!(desc("describe_table").contains("one table you already chose"));
        assert!(desc("describe_table").contains("describe_schema"));
        assert!(desc("list_tables").contains("table_type"));
        assert!(desc("search_tables").contains("table_type"));
        assert!(desc("execute_sql_readonly").contains("Do not scan information_schema across a whole catalog"));
        assert!(desc("execute_sql_readonly").contains("describe_schema"));
    }

    // ── describe_schema ──────────────────────────────────────────────────

    /// What one statement answers in the `describe_schema` fixture.
    #[derive(Clone)]
    enum Answer {
        /// `SHOW TABLES` / `SHOW VIEWS`: one name column.
        Names(Vec<&'static str>),
        /// `SHOW COLUMNS`: `(column_name, data_type, is_nullable, comment)`.
        Columns(Vec<(&'static str, &'static str, &'static str, Option<&'static str>)>),
        /// `SHOW STATS`: the summary row's `(row_count, data_size)` after one
        /// column row.
        Stats(Option<f64>, Option<f64>),
        /// `EXPLAIN`: one plan row.
        Plan,
        Fail(&'static str),
        /// Never answers, as an object-store call with expired credentials
        /// did on 2026-09-16: the deadline is what turns it into an error.
        Hang,
    }

    /// Answers statements by exact SQL, records what was asked, and refuses
    /// anything unscripted, as a catalog refuses what the caller may not see.
    struct TableExecutor {
        answers: HashMap<&'static str, Answer>,
        asked: Mutex<Vec<String>>,
    }

    fn utf8(values: Vec<Option<&str>>) -> Arc<dyn arrow_array::Array> {
        Arc::new(StringArray::from(values))
    }

    #[async_trait]
    impl QueryExecutor for TableExecutor {
        async fn execute(&self, _session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
            self.asked.lock().unwrap().push(sql.to_string());
            let Some(answer) = self.answers.get(sql) else {
                return Err(SqeError::Auth(format!("not authorized: {sql}")));
            };
            let batch = match answer {
                Answer::Fail(message) => return Err(SqeError::Execution(message.to_string())),
                Answer::Hang => {
                    std::future::pending::<()>().await;
                    unreachable!("a pending future never resolves")
                }
                Answer::Names(names) => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("Table", DataType::Utf8, true)])),
                    vec![utf8(names.iter().map(|n| Some(*n)).collect())],
                ),
                Answer::Columns(columns) => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("column_name", DataType::Utf8, false),
                        Field::new("data_type", DataType::Utf8, false),
                        Field::new("is_nullable", DataType::Utf8, false),
                        Field::new("comment", DataType::Utf8, true),
                    ])),
                    vec![
                        utf8(columns.iter().map(|c| Some(c.0)).collect()),
                        utf8(columns.iter().map(|c| Some(c.1)).collect()),
                        utf8(columns.iter().map(|c| Some(c.2)).collect()),
                        utf8(columns.iter().map(|c| c.3).collect()),
                    ],
                ),
                Answer::Stats(row_count, data_size) => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("column_name", DataType::Utf8, true),
                        Field::new("data_size", DataType::Float64, true),
                        Field::new("row_count", DataType::Float64, true),
                    ])),
                    vec![
                        utf8(vec![Some("id"), None]),
                        Arc::new(arrow_array::Float64Array::from(vec![None, *data_size])),
                        Arc::new(arrow_array::Float64Array::from(vec![None, *row_count])),
                    ],
                ),
                Answer::Plan => RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("plan", DataType::Utf8, true)])),
                    vec![utf8(vec![Some("TableScan")])],
                ),
            }
            .unwrap();
            Ok(vec![batch])
        }
    }

    fn schema_fixture() -> Arc<TableExecutor> {
        let cols = |names: &[&'static str]| {
            Answer::Columns(names.iter().map(|n| (*n, "Utf8", "YES", None)).collect())
        };
        Arc::new(TableExecutor {
            answers: HashMap::from([
                (
                    "SHOW TABLES FROM \"glue\".\"bi\"",
                    Answer::Names(vec!["events", "orders", "customers", "broken", "sales_v"]),
                ),
                ("SHOW VIEWS FROM \"glue\".\"bi\"", Answer::Names(vec!["sales_v"])),
                ("SHOW COLUMNS FROM \"glue\".\"bi\".\"broken\"", Answer::Fail("hive_external: unreadable storage")),
                ("SHOW COLUMNS FROM \"glue\".\"bi\".\"customers\"", cols(&["id", "email"])),
                ("SHOW COLUMNS FROM \"glue\".\"bi\".\"events\"", cols(&["ts", "kind"])),
                (
                    "SHOW COLUMNS FROM \"glue\".\"bi\".\"orders\"",
                    // `ssn` is restricted for this caller: the coordinator's
                    // SHOW COLUMNS already left it out, and so must we.
                    Answer::Columns(vec![
                        ("id", "Int64", "NO", Some("order id")),
                        ("amount", "Float64", "YES", None),
                    ]),
                ),
                ("SHOW COLUMNS FROM \"glue\".\"bi\".\"sales_v\"", cols(&["month", "total"])),
                ("SHOW STATS FOR \"glue\".\"bi\".\"orders\"", Answer::Stats(Some(1200.0), Some(65536.0))),
                // A user-class refusal (NotSupported) goes through verbatim; a
                // storage text would not (#675, see `broken` above).
                ("SHOW STATS FOR \"glue\".\"bi\".\"sales_v\"", Answer::Fail("SHOW STATS is not supported for a view: views carry no statistics")),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"orders\" LIMIT 0", Answer::Plan),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"sales_v\" LIMIT 0", Answer::Fail("Schema error: No field named tag")),
            ]),
            asked: Mutex::new(Vec::new()),
        })
    }

    fn schema_state(executor: Arc<TableExecutor>) -> McpServerState {
        McpServerState::new(Arc::new(MockAuth), executor, static_config(false))
            .with_hive_roots(vec!["glue".to_string()])
    }

    fn names_of(result: &Value) -> Vec<&str> {
        result["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect()
    }

    /// A stats probe that never answers (an object-store call with expired
    /// credentials, 2026-09-16) must not hold the page: the table comes back
    /// with the timeout on `stats_error`, its other probes intact. The
    /// paused clock makes the 20 s deadline fire without waiting for it.
    #[tokio::test(start_paused = true)]
    async fn describe_schema_bounds_a_hanging_probe_and_returns_the_page() {
        let executor = Arc::new(TableExecutor {
            answers: HashMap::from([
                ("SHOW TABLES FROM \"glue\".\"bi\"", Answer::Names(vec!["stuck", "fine"])),
                ("SHOW VIEWS FROM \"glue\".\"bi\"", Answer::Names(vec![])),
                (
                    "SHOW COLUMNS FROM \"glue\".\"bi\".\"stuck\"",
                    Answer::Columns(vec![("id", "Int64", "NO", None)]),
                ),
                (
                    "SHOW COLUMNS FROM \"glue\".\"bi\".\"fine\"",
                    Answer::Columns(vec![("id", "Int64", "NO", None)]),
                ),
                ("SHOW STATS FOR \"glue\".\"bi\".\"stuck\"", Answer::Hang),
                ("SHOW STATS FOR \"glue\".\"bi\".\"fine\"", Answer::Stats(Some(7.0), Some(512.0))),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"stuck\" LIMIT 0", Answer::Plan),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"fine\" LIMIT 0", Answer::Hang),
            ]),
            asked: Mutex::new(Vec::new()),
        });
        let state = schema_state(executor);
        let session = local_session("mcp-reader".into(), vec![]);

        let page = describe_schema(
            &state,
            &session,
            &json!({"catalog": "glue", "schema": "bi", "include_stats": true, "include_queryability": true}),
        )
        .await
        .unwrap();
        let tables = page["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 2, "{page}");
        let by = |n: &str| tables.iter().find(|t| t["name"] == n).unwrap().clone();
        let stuck = by("stuck");
        assert!(
            stuck["stats_error"].as_str().unwrap().contains("probe timed out after 20s"),
            "{stuck}"
        );
        assert_eq!(stuck["queryable"], json!(true), "the plan probe still ran: {stuck}");
        assert_eq!(stuck["columns"].as_array().unwrap().len(), 1);
        let fine = by("fine");
        assert_eq!(fine["row_count"], json!(7), "{fine}");
        assert_eq!(fine["queryable"], json!(false), "{fine}");
        assert!(
            fine["queryability_error"].as_str().unwrap().contains("probe timed out"),
            "{fine}"
        );
    }

    #[tokio::test]
    async fn describe_schema_pages_by_table_name() {
        let executor = schema_fixture();
        let state = schema_state(executor.clone());
        let session = local_session("mcp-reader".into(), vec![]);

        let first = describe_schema(&state, &session, &json!({"catalog": "glue", "schema": "bi", "page_size": 2}))
            .await
            .unwrap();
        assert_eq!(names_of(&first), vec!["broken", "customers"], "sorted, first page: {first}");
        assert_eq!(first["total_tables"], 5);
        assert_eq!(first["truncated"], true);
        assert_eq!(first["next_page_token"], "customers");

        let second = describe_schema(
            &state,
            &session,
            &json!({"catalog": "glue", "schema": "bi", "page_size": 2, "page_token": "customers"}),
        )
        .await
        .unwrap();
        assert_eq!(names_of(&second), vec!["events", "orders"]);
        assert_eq!(second["next_page_token"], "orders");

        let third = describe_schema(
            &state,
            &session,
            &json!({"catalog": "glue", "schema": "bi", "page_size": 2, "page_token": "orders"}),
        )
        .await
        .unwrap();
        assert_eq!(names_of(&third), vec!["sales_v"]);
        assert_eq!(third["truncated"], false);
        assert!(third.get("next_page_token").is_none(), "{third}");

        // A page bigger than the cap is clamped, not refused; zero is an error.
        assert_eq!(page_size_arg(&json!({})).unwrap(), DESCRIBE_SCHEMA_PAGE_DEFAULT);
        assert_eq!(page_size_arg(&json!({"page_size": 100000})).unwrap(), DESCRIBE_SCHEMA_PAGE_MAX);
        assert!(page_size_arg(&json!({"page_size": 0})).is_err());

        // Without stats or queryability nothing but SHOW COLUMNS ran per table.
        let asked = executor.asked.lock().unwrap().clone();
        assert!(!asked.iter().any(|s| s.starts_with("SHOW STATS") || s.starts_with("EXPLAIN")), "{asked:?}");
    }

    #[tokio::test]
    async fn describe_schema_isolates_one_tables_failure_and_reports_kinds_stats_and_plans() {
        let executor = schema_fixture();
        let state = schema_state(executor.clone());
        let session = local_session("mcp-reader".into(), vec![]);

        let result = describe_schema(
            &state,
            &session,
            &json!({"catalog": "glue", "schema": "bi", "include_stats": true, "include_queryability": "true"}),
        )
        .await
        .unwrap();
        let tables = result["tables"].as_array().unwrap();
        let by_name = |name: &str| tables.iter().find(|t| t["name"] == name).unwrap().clone();

        // The unreadable table is an entry with an error, not a failed call.
        // The storage text itself stays in the server log (#675).
        let broken = by_name("broken");
        let error = broken["error"].as_str().unwrap();
        assert!(error.contains("error_id") && !error.contains("unreadable storage"), "{broken}");
        assert_eq!(broken["columns"], json!([]));
        assert_eq!(tables.len(), 5);

        // A regular Hive table: columns with ordinals and the Glue comment,
        // metastore numbers as integers, plans fine.
        let orders = by_name("orders");
        assert_eq!(orders["table_type"], "table");
        assert_eq!(orders["source"], "hive_external");
        assert_eq!(orders["columns"][0]["name"], "id");
        assert_eq!(orders["columns"][0]["nullable"], false);
        assert_eq!(orders["columns"][0]["ordinal"], 1);
        assert_eq!(orders["columns"][0]["comment"], "order id");
        assert_eq!(orders["columns"][1]["ordinal"], 2);
        assert!(orders["columns"][1].get("comment").is_none(), "no comment, no key: {orders}");
        assert_eq!(orders["row_count"], 1200);
        assert_eq!(orders["size_bytes"], 65536);
        assert_eq!(orders["snapshot_id"], Value::Null);
        assert_eq!(orders["queryable"], true);

        // A view: kind from information_schema, no stats (recorded, not
        // fatal), and the planning error a crawler marks it unqueryable with.
        let view = by_name("sales_v");
        assert_eq!(view["table_type"], "view");
        assert_eq!(view["source"], "view");
        assert_eq!(view["row_count"], Value::Null);
        assert!(view["stats_error"].as_str().unwrap().contains("no statistics"));
        assert_eq!(view["queryable"], false);
        assert!(view["queryability_error"].as_str().unwrap().contains("No field named tag"));

        // A table the kinds listing did not mention is a table; a table with
        // no scripted stats or plan records that too rather than failing.
        let events = by_name("events");
        assert_eq!(events["table_type"], "table");
        assert!(events["stats_error"].as_str().is_some());
        assert_eq!(events["queryable"], false);

        // Argument validation.
        let err = describe_schema(&state, &session, &json!({"catalog": "glue", "schema": "bi", "include_stats": 3}))
            .await
            .unwrap_err();
        assert_eq!(err.0, -32602);
        let err = describe_schema(&state, &session, &json!({"catalog": "glue"})).await.unwrap_err();
        assert_eq!(err.0, -32602);

        // A schema the caller may not list is the one error that fails the call.
        let err = describe_schema(&state, &session, &json!({"catalog": "glue", "schema": "locked"}))
            .await
            .unwrap_err();
        assert_eq!(err.0, -32000);
        assert!(err.1.contains("listing tables of glue.locked"), "{}", err.1);
    }

    #[tokio::test]
    async fn describe_schema_columns_are_exactly_what_describe_table_returns() {
        let executor = schema_fixture();
        let state = schema_state(executor.clone());
        let session = local_session("mcp-reader".into(), vec![]);

        // The single-table tool, through the same call path clients use.
        let single = call_tool(
            &state,
            &AuthContext {
                session: session.clone(),
                scopes: None,
            },
            None,
            &json!({"name": "describe_table", "arguments": {"catalog": "glue", "schema": "bi", "table": "orders"}}),
        )
        .await
        .unwrap();
        let single_names: Vec<&str> = single["structuredContent"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["column_name"].as_str().unwrap())
            .collect();
        assert_eq!(single_names, vec!["id", "amount"], "the restricted column is absent: {single}");

        let bulk = describe_schema(&state, &session, &json!({"catalog": "glue", "schema": "bi"}))
            .await
            .unwrap();
        let orders = bulk["tables"].as_array().unwrap().iter().find(|t| t["name"] == "orders").unwrap();
        let bulk_names: Vec<&str> = orders["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect();
        assert_eq!(bulk_names, single_names);

        // Same statement, so the same policy path: whatever SHOW COLUMNS
        // withholds from describe_table it withholds here.
        let asked = executor.asked.lock().unwrap().clone();
        let column_statements: Vec<&String> = asked
            .iter()
            .filter(|s| s.ends_with("\"orders\"") && s.starts_with("SHOW COLUMNS"))
            .collect();
        assert_eq!(column_statements.len(), 2);
        assert_eq!(column_statements[0], column_statements[1]);
    }

    #[test]
    fn describe_schema_is_advertised_as_a_read_tool() {
        let listed = tools(&static_config(false));
        let tool = listed.iter().find(|t| t["name"] == "describe_schema").expect("advertised");
        assert_eq!(tool["annotations"]["readOnlyHint"], true);
        assert_eq!(tool["inputSchema"]["required"], json!(["catalog", "schema"]));
        assert_eq!(tool["inputSchema"]["properties"]["page_size"]["maximum"], 500);
    }

    #[test]
    fn sample_rows_limit_is_defaulted_and_capped() {
        assert_eq!(sample_limit(&json!({})).unwrap(), SAMPLE_ROWS_DEFAULT);
        assert_eq!(sample_limit(&json!({"limit": 25})).unwrap(), 25);
        assert_eq!(sample_limit(&json!({"limit": "7"})).unwrap(), 7);
        assert_eq!(sample_limit(&json!({"limit": 100000})).unwrap(), SAMPLE_ROWS_MAX);
        assert!(sample_limit(&json!({"limit": 0})).is_err());
        assert!(sample_limit(&json!({"limit": -3})).is_err());
        assert!(sample_limit(&json!({"limit": "many"})).is_err());
    }

    #[tokio::test]
    async fn discovery_tools_run_as_the_caller_with_the_generated_sql() {
        let executor = Arc::new(RecordingExecutor::default());
        let app = router(McpServerState::new(
            Arc::new(MockAuth),
            executor.clone(),
            static_config(false),
        ));
        let body = json!({
            "jsonrpc":"2.0","id":1,"method":"tools/call",
            "params":{"name":"sample_rows","arguments":{"catalog":"sales_wh","schema":"ac","table":"orders","limit":3}}
        })
        .to_string();
        let response = send(&app, READ_TOKEN, &[], body).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            executed(&executor).as_deref(),
            Some("SELECT * FROM \"sales_wh\".\"ac\".\"orders\" LIMIT 3")
        );
        assert_eq!(
            executor.1.lock().unwrap().clone().map(|(user, _)| user).as_deref(),
            Some("mcp-reader"),
            "the sample runs as the caller, so filters and masks apply"
        );

        // The writer principal defaults to catalog iceberg: search needs no
        // catalog argument, and starts from that catalog's schemas.
        let body = json!({
            "jsonrpc":"2.0","id":1,"method":"tools/call",
            "params":{"name":"search_tables","arguments":{"pattern":"ord"}}
        })
        .to_string();
        send(&app, WRITE_TOKEN, &[], body).await;
        assert_eq!(
            executed(&executor).as_deref(),
            Some("SHOW SCHEMAS FROM \"iceberg\"")
        );
        assert_eq!(
            executor.2.lock().unwrap().as_deref(),
            Some("mcp:search_tables"),
            "the walk is attributed to the tool like any other call"
        );
        assert!(tools(&McpConfig::default())
            .iter()
            .filter(|t| t["name"] == "search_tables" || t["name"] == "sample_rows")
            .all(|t| t["annotations"]["readOnlyHint"] == json!(true)));
    }

    #[tokio::test]
    async fn static_mode_runs_the_query_as_the_matching_principal() {
        let executor = Arc::new(RecordingExecutor::default());
        let response = post(
            static_config(false),
            executor.clone(),
            Some(READ_TOKEN),
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let identity = executor.1.lock().unwrap().clone().expect("query ran");
        assert_eq!(identity.0, "mcp-reader");
        assert_eq!(identity.1, vec!["reader".to_string()]);
    }

    /// The load-bearing test for mode 2: the auth chain here accepts ANY
    /// credential (as the `anonymous` provider does in a Glue deployment), so
    /// delegating the static token to it would let a wrong token through.
    #[tokio::test]
    async fn static_mode_rejects_an_unknown_token_despite_a_permissive_auth_chain() {
        let executor = Arc::new(RecordingExecutor::default());
        let response = post(
            static_config(false),
            executor.clone(),
            Some("not-the-configured-token"),
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(executor.1.lock().unwrap().is_none(), "no query may run");
    }

    #[tokio::test]
    async fn static_mode_never_forwards_the_mcp_token_to_the_catalog() {
        let executor = Arc::new(RecordingExecutor::default());
        post(
            static_config(false),
            executor.clone(),
            Some(READ_TOKEN),
            tool_call("execute_sql_readonly"),
        )
        .await;
        let catalog_token = executor.0.lock().unwrap().clone().expect("query ran");
        assert!(
            catalog_token.is_empty(),
            "static MCP token must not become the catalog bearer, got: {catalog_token}"
        );
    }

    #[tokio::test]
    async fn static_mode_denies_the_change_tool_to_a_reader_principal() {
        let executor = Arc::new(RecordingExecutor::default());
        let response = post(
            static_config(true),
            executor.clone(),
            Some(READ_TOKEN),
            json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":"execute_sql_change",
                          "arguments":{"sql":"INSERT INTO t VALUES (1)"}}
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert!(value["error"].is_object(), "expected a JSON-RPC error: {value}");
        assert!(executor.1.lock().unwrap().is_none(), "no write may run");
    }

    #[tokio::test]
    async fn static_mode_allows_the_change_tool_for_a_writer_principal() {
        let executor = Arc::new(RecordingExecutor::default());
        let response = post(
            static_config(true),
            executor.clone(),
            Some(WRITE_TOKEN),
            json!({
                "jsonrpc":"2.0","id":1,"method":"tools/call",
                "params":{"name":"execute_sql_change",
                          "arguments":{"sql":"INSERT INTO t VALUES (1)"}}
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let identity = executor.1.lock().unwrap().clone().expect("write ran");
        assert_eq!(identity.0, "mcp-writer");
    }

    #[tokio::test]
    async fn anonymous_mode_serves_reads_with_no_bearer_at_all() {
        let executor = Arc::new(RecordingExecutor::default());
        let response = post(
            anonymous_config(),
            executor.clone(),
            None,
            tool_call("execute_sql_readonly"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let identity = executor.1.lock().unwrap().clone().expect("query ran");
        assert_eq!(identity.0, "kiosk");
        assert_eq!(identity.1, vec!["reader".to_string()]);
    }

    #[tokio::test]
    async fn oauth_discovery_is_not_advertised_without_an_authorization_server() {
        for config in [static_config(false), anonymous_config()] {
            let app = router(McpServerState::new(
                Arc::new(MockAuth),
                Arc::new(RecordingExecutor::default()),
                config,
            ));
            let response = app
                .oneshot(
                    Request::get("/.well-known/oauth-protected-resource")
                        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn static_mode_challenge_points_at_no_metadata() {
        let response = post(
            static_config(false),
            Arc::new(RecordingExecutor::default()),
            None,
            r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#.to_string(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response.headers()[header::WWW_AUTHENTICATE]
            .to_str()
            .unwrap();
        assert!(
            !challenge.contains("resource_metadata"),
            "no authorization server exists to discover, got: {challenge}"
        );
    }
}
