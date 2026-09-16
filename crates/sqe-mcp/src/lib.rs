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
    SecretString, Session, SqeError,
};
use sqe_sql::StatementKind;

pub const PROTOCOL_VERSION: &str = "2025-11-25";

#[async_trait]
pub trait QueryExecutor: Send + Sync + 'static {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError>;
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
}

/// Least time between two `refresh_metadata` calls for the same user and
/// scope.
const REFRESH_COOLDOWN: Duration = Duration::from_secs(10);

/// What a client said about itself on `initialize`, bound to the identity that
/// said it. The binding stops one caller from relabelling another caller's
/// audit trail by replaying a session id it observed.
struct ClientSession {
    user: String,
    label: String,
    seen: Instant,
}

#[derive(Default)]
struct ClientRegistry {
    inner: Mutex<HashMap<String, ClientSession>>,
}

impl ClientRegistry {
    fn insert(&self, user: &str, label: String) -> String {
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
                label,
                seen: Instant::now(),
            },
        );
        id
    }

    /// The label for `id`, only if `user` is the identity that registered it.
    fn label(&self, id: &str, user: &str) -> Option<String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let session = inner.get_mut(id)?;
        if session.user != user {
            return None;
        }
        session.seen = Instant::now();
        Some(session.label.clone())
    }

    fn remove(&self, id: &str, user: &str) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.get(id).is_some_and(|session| session.user == user) {
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
        }
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
                state.clients.remove(id, &auth.session.user.username);
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
fn client_label(state: &McpServerState, headers: &HeaderMap, user: &str) -> Option<String> {
    session_id(headers)
        .and_then(|id| state.clients.label(id, user))
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
    let id = request.id.clone();
    let user = auth.session.user.username.as_str();
    let mut issued_session_id = None;
    let result = match request.method.as_str() {
        "initialize" => {
            // Remember who is talking so every later call can be attributed
            // to this client in the audit trail. Nothing else hangs off the id.
            let label = client_label_from_client_info(&request.params)
                .or_else(|| client_label(&state, &headers, user))
                .unwrap_or_else(|| "unknown".to_string());
            tracing::info!(user, client = %label, "MCP client initialized");
            issued_session_id = Some(state.clients.insert(user, label));
            Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "sqe", "version": sqe_core::VERSION },
                "instructions": "Governed SQL access. Prefer read-only metadata and query tools. The change tool is separately gated by OAuth scope, SQE role, and source-data authorization."
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools(&state.config) })),
        "tools/call" => {
            let client = client_label(&state, &headers, user);
            call_tool(&state, &auth, client.as_deref(), &request.params).await
        }
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        _ => Err((-32601, format!("Method not found: {}", request.method))),
    };
    let mut response = match result {
        Ok(value) => rpc_result(id, value),
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
    match state.config.auth.mode {
        McpAuthMode::Oauth => authenticate_oauth(state, headers, &peer_ip).await,
        McpAuthMode::Static => authenticate_static(state, headers),
        McpAuthMode::Anonymous => Ok(authenticate_anonymous(state)),
    }
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
            sqe_auth::AuthError::RateLimited => (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error":"rate_limited"})),
            )
                .into_response(),
            error => {
                tracing::warn!(error = %error, "MCP bearer authentication failed");
                auth_error(
                    &state.config,
                    StatusCode::UNAUTHORIZED,
                    Some("invalid_token"),
                )
            }
        })?;
    let scopes = token_scopes(token).ok_or_else(|| {
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

fn tools(config: &McpConfig) -> Vec<Value> {
    let mut tools = vec![
        tool("execute_sql_readonly", "Execute exactly one read-only SQL statement. SQE parses and classifies the statement before execution; source grants, row filters, and column masks apply as the authenticated user.", json!({"type":"object","properties":{"sql":{"type":"string"}},"required":["sql"],"additionalProperties":false}), true, false),
        tool("list_catalogs", "List catalogs visible to the authenticated user. `diagnostics` carries one entry per Hive root (Glue, sidecar) with `status` reachable | auth_error | timeout | unreachable and a `detail`: auth_error means the engine's own AWS credentials are missing or expired (the detail names the region), which no retry from the client side can fix.", empty_schema(), true, false),
        tool("list_schemas", "List schemas visible to the authenticated user in a catalog.", one_string_schema("catalog"), true, false),
        tool("list_tables", "List tables visible to the authenticated user in a catalog and schema.", two_string_schema("catalog", "schema"), true, false),
        tool("describe_table", "Return columns for a table visible to the authenticated user.", three_string_schema(), true, false),
        tool("describe_schema", "Describe every table and view in one schema in a single call, for catalog crawlers: per table the columns describe_table would return (same policy filtering), the table kind (table or view) and source (iceberg, hive_external, view), optionally the metastore's row count and size (`include_stats`: Iceberg snapshot summary or Glue/Hive table parameters, never a scan) and whether `SELECT * ... LIMIT 1` plans (`include_queryability`: planning only, nothing runs). Paged by table name: `page_size` defaults to 200 (at most 500); pass back `next_page_token` for the next page. One table failing does not fail the call; its `error`, `stats_error` or `queryability_error` says why.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"include_stats":{"type":"boolean"},"include_queryability":{"type":"boolean"},"page_token":{"type":"string"},"page_size":{"type":"integer","minimum":1,"maximum":500}},"required":["catalog","schema"],"additionalProperties":false}), true, false),
        tool("get_table_ddl", "Return SHOW CREATE TABLE output for a visible table.", three_string_schema(), true, false),
        tool("get_table_stats", "Return Iceberg table statistics visible to the authenticated user. Hive external tables have no snapshot statistics and return an error; use describe_table for them.", three_string_schema(), true, false),
        tool("search_tables", "Find tables by name across the schemas of one catalog (SHOW SCHEMAS, then SHOW TABLES per schema, matched server-side). `pattern` is a SQL LIKE pattern (% and _ are wildcards; a bare word matches as a substring, case-insensitive). `catalog` defaults to the session's default catalog. Only tables visible to the authenticated user are returned.", json!({"type":"object","properties":{"pattern":{"type":"string"},"catalog":{"type":"string"}},"required":["pattern"],"additionalProperties":false}), true, false),
        tool("sample_rows", "Return up to `limit` rows (default 10, at most 100) of a table, as the authenticated user: row filters and column masks apply. Prefer this over SELECT * for a first look at a table.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"table":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100}},"required":["catalog","schema","table"],"additionalProperties":false}), true, false),
        tool("explain_query", "Explain a read-only SQL query without allowing a write statement to be hidden behind EXPLAIN.", one_string_schema("sql"), true, false),
        tool("whoami", "Show the SQE identity, roles, groups, and OAuth scopes for this request; never returns tokens.", empty_schema(), true, false),
        tool("refresh_metadata", "Drop cached catalog metadata so the next call re-reads it from the source. Use ONLY when a table or column you expect is missing, or a schema looks stale after a change outside SQE; routine queries never need it. Scope it as narrowly as you can: `table` (with `catalog` and `schema`) refreshes one table, `schema` (with `catalog`) one schema, nothing refreshes this session's catalog view. Rate-limited per scope; a repeat inside the window is a no-op that says so.", json!({"type":"object","properties":{"catalog":{"type":"string"},"schema":{"type":"string"},"table":{"type":"string"}},"additionalProperties":false}), false, false),
    ];
    if config.allow_writes {
        tools.push(tool("execute_sql_change", "Execute one change/write SQL statement. Requires the configured OAuth write scope and SQE writer role; the target catalog must be one the operator allowed for MCP writes; source catalog and storage authorization still apply as the authenticated user.", one_string_schema("sql"), false, true));
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
    if name == "describe_schema" {
        let session = auth
            .session
            .clone()
            .with_source(Some(source_label(name, client)));
        return describe_schema(state, &session, &args)
            .await
            .map(tool_success);
    }
    let sql = match name {
        "execute_sql_readonly" => {
            let sql = required(&args, "sql")?;
            ensure_read_only(sql, session_catalog).map_err(|e| (-32602, e))?;
            sql.to_string()
        }
        "execute_sql_change" => {
            authorize_write(state, auth)?;
            let sql = required(&args, "sql")?;
            let kind = ensure_change(sql, session_catalog).map_err(|e| (-32602, e))?;
            authorize_write_target(state, &auth.session, &kind)?;
            sql.to_string()
        }
        "list_schemas" => format!("SHOW SCHEMAS FROM {}", ident(required(&args, "catalog")?)),
        "list_tables" => format!(
            "SHOW TABLES FROM {}.{}",
            ident(required(&args, "catalog")?),
            ident(required(&args, "schema")?)
        ),
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
    match state.executor.execute(&session, &sql).await {
        Ok(batches) => Ok(tool_success(batches_json(
            &batches,
            state.config.max_result_rows,
            state.config.max_response_bytes,
        ))),
        Err(error) => Ok(tool_failure(with_refresh_hint(name, error.to_string()))),
    }
}

/// How long `list_catalogs` waits on each Hive root's probe before calling
/// it `timeout`. Bounded because a metastore that hangs must not hang the
/// one tool an agent calls first.
const CATALOG_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// `list_catalogs`: `SHOW CATALOGS`, plus one `diagnostics` entry per Hive
/// root the caller can see. Each root is probed with `SHOW SCHEMAS FROM
/// <root>` (one metastore call, run concurrently, bounded by
/// [`CATALOG_PROBE_TIMEOUT`]) and reported as `reachable`, `auth_error`
/// (the engine's AWS credentials are missing or expired; the coordinator
/// names the region in `detail`), `timeout`, or `unreachable` with the
/// error text. An Iceberg catalog is listed without a probe: its
/// authorization is per user and `list_schemas` answers for it. Without
/// this, an expired SSO session surfaced as `dispatch failure` on the second
/// tool call and as empty listings after that, and a sidecar had no way to
/// show "the catalog is fine, the engine's credentials are not".
async fn list_catalogs_with_diagnostics(
    state: &McpServerState,
    session: &Session,
) -> Result<Value, (i32, String)> {
    let batches = match state.executor.execute(session, "SHOW CATALOGS").await {
        Ok(batches) => batches,
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
                    state.executor.execute(session, &sql),
                )
                .await;
                let (status, detail) = match probe {
                    Ok(Ok(_)) => ("reachable", None),
                    Ok(Err(error)) => {
                        let text = error.to_string();
                        let status = if text.contains("AWS credentials missing or expired") {
                            "auth_error"
                        } else {
                            "unreachable"
                        };
                        (status, Some(text))
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
    fn go(p: &[char], s: &[char]) -> bool {
        match p.split_first() {
            None => s.is_empty(),
            Some(('%', rest)) => (0..=s.len()).any(|i| go(rest, &s[i..])),
            Some(('_', rest)) => !s.is_empty() && go(rest, &s[1..]),
            Some((c, rest)) => s.first().is_some_and(|d| d == c) && go(rest, &s[1..]),
        }
    }
    let pattern = pattern.to_lowercase();
    let bare = !pattern.contains('%') && !pattern.contains('_');
    let p: Vec<char> = if bare {
        format!("%{pattern}%").chars().collect()
    } else {
        pattern.chars().collect()
    };
    let s: Vec<char> = name.to_lowercase().chars().collect();
    go(&p, &s)
}

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
    let schemas = state
        .executor
        .execute(session, &format!("SHOW SCHEMAS FROM {}", ident(&catalog)))
        .await
        .map_err(|e| (-32000, format!("listing schemas of {catalog}: {e}")))?;
    // One `SHOW TABLES` per schema, a bounded number in flight at once
    // (`buffered`, so results come back in schema order and the response is
    // deterministic). Serially, a Glue catalog of 135 databases took 17 s
    // for one search; each listing is a metastore round trip and nothing
    // here depends on the previous one.
    let schema_names: Vec<String> = first_column_strings(&schemas)
        .into_iter()
        .filter(|s| !s.eq_ignore_ascii_case("information_schema"))
        .take(SEARCH_SCHEMAS_MAX)
        .collect();
    let listings: Vec<(String, Option<Vec<String>>)> = {
        use futures::StreamExt;
        futures::stream::iter(schema_names)
            .map(|schema| {
                let catalog = catalog.clone();
                async move {
                    let sql = format!("SHOW TABLES FROM {}.{}", ident(&catalog), ident(&schema));
                    let tables = match state.executor.execute(session, &sql).await {
                        Ok(batches) => Some(first_column_strings(&batches)),
                        Err(error) => {
                            tracing::debug!(%catalog, %schema, %error, "search_tables: schema skipped");
                            None
                        }
                    };
                    (schema, tables)
                }
            })
            .buffered(SEARCH_SCHEMAS_CONCURRENCY)
            .collect()
            .await
    };
    let mut rows = Vec::new();
    let mut total = 0usize;
    for (schema, tables) in listings {
        let Some(tables) = tables else { continue };
        for table in tables.into_iter().filter(|t| like_matches(pattern, t)) {
            total += 1;
            if rows.len() < state.config.max_result_rows {
                rows.push(json!({
                    "table_catalog": catalog,
                    "table_schema": schema,
                    "table_name": table,
                }));
            }
        }
    }
    let returned = rows.len();
    Ok(json!({
        "columns": [
            {"name": "table_catalog", "type": "Utf8"},
            {"name": "table_schema", "type": "Utf8"},
            {"name": "table_name", "type": "Utf8"},
        ],
        "rows": rows,
        "total_rows": total,
        "returned_rows": returned,
        "truncated": returned < total,
    }))
}

/// `describe_schema` page bounds. A page is what one crawler request can
/// digest; the byte cap below still applies inside a page.
const DESCRIBE_SCHEMA_PAGE_DEFAULT: usize = 200;
const DESCRIBE_SCHEMA_PAGE_MAX: usize = 500;
/// Per-table statements in flight at once. Each is a metadata read against a
/// cache the coordinator already warmed with the listing, so this bounds
/// concurrent planner work, not network fan-out.
const DESCRIBE_SCHEMA_CONCURRENCY: usize = 8;

/// `describe_schema`: one schema's tables and views, each described the way
/// the single-table tools would, in one call.
///
/// Composed of the SAME statements those tools issue, so nothing here is
/// looser than `describe_table`: the listing is `SHOW TABLES` (visibility
/// filtered), the columns are `SHOW COLUMNS FROM` per table (the policy
/// path that drops restricted columns on a Hive table), statistics are
/// `SHOW STATS FOR` (snapshot or metastore numbers, never a scan), and
/// queryability is `EXPLAIN SELECT * FROM t LIMIT 1` (planning only). The
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

    let listing = state
        .executor
        .execute(
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

    match state
        .executor
        .execute(session, &format!("SHOW COLUMNS FROM {table}"))
        .await
    {
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
            entry["error"] = json!(error.to_string());
        }
    }

    if include_stats {
        entry["row_count"] = Value::Null;
        entry["size_bytes"] = Value::Null;
        entry["snapshot_id"] = Value::Null;
        match state
            .executor
            .execute(session, &format!("SHOW STATS FOR {table}"))
            .await
        {
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
                entry["stats_error"] = json!(error.to_string());
            }
        }
    }

    if include_queryability {
        match state
            .executor
            .execute(session, &format!("EXPLAIN SELECT * FROM {table} LIMIT 1"))
            .await
        {
            Ok(_) => entry["queryable"] = json!(true),
            Err(error) => {
                entry["queryable"] = json!(false);
                entry["queryability_error"] = json!(error.to_string());
            }
        }
    }
    entry
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
    match state.executor.execute(session, &sql).await {
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

/// `page_size` for `describe_schema`: absent means the default, anything
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

fn is_read_only(kind: &StatementKind) -> bool {
    matches!(
        kind,
        StatementKind::Query(_)
            | StatementKind::ShowCatalogs
            | StatementKind::ShowSchemas(_)
            | StatementKind::ShowTables(_)
            | StatementKind::ShowViews(_)
            | StatementKind::ShowGrants(_)
            | StatementKind::ShowEffectiveGrants(_)
            | StatementKind::ShowEffectivePolicy(_)
            | StatementKind::ShowTags(_)
            | StatementKind::ShowMaskingPolicies(_)
            | StatementKind::ShowCreateTable(_)
            | StatementKind::ShowCreateSchema(_)
            | StatementKind::ShowStats(_)
            | StatementKind::ShowPartitions(_)
    )
}

/// Classify and require a read-only kind; returns the kind for the caller.
fn ensure_read_only(sql: &str, session_catalog: Option<&str>) -> Result<StatementKind, String> {
    let kind = classify(sql, session_catalog)?;
    if is_read_only(&kind) {
        Ok(kind)
    } else {
        Err(format!("Read-only tool rejected {} statement", kind.name()))
    }
}

/// Classify and require a change kind; returns the kind so the target gate
/// can inspect it without parsing twice.
fn ensure_change(sql: &str, session_catalog: Option<&str>) -> Result<StatementKind, String> {
    let kind = classify(sql, session_catalog)?;
    if is_read_only(&kind) {
        return Err(format!(
            "Use a read-only tool for {} statements",
            kind.name()
        ));
    }
    Ok(kind)
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
    let mut rows = Vec::new();
    'batches: for batch in batches {
        for row in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                break 'batches;
            }
            let mut object = Map::new();
            for (index, field) in batch.schema().fields().iter().enumerate() {
                object.insert(
                    field.name().clone(),
                    sqe_trino_protocol::encode::arrow_value_to_json(batch.column(index).as_ref(), row),
                );
            }
            rows.push(Value::Object(object));
            let probe = json!({"columns":columns,"rows":rows});
            if serde_json::to_vec(&probe)
                .map(|v| v.len() > max_bytes)
                .unwrap_or(true)
            {
                rows.pop();
                break 'batches;
            }
        }
    }
    let returned_rows = rows.len();
    json!({
        "columns": columns,
        "rows": rows,
        "returned_rows": returned_rows,
        "total_rows": total_rows,
        "truncated": returned_rows < total_rows
    })
}

fn tool_success(value: Value) -> Value {
    json!({"content":[{"type":"text","text":value.to_string()}],"structuredContent":value,"isError":false})
}
fn tool_failure(message: String) -> Value {
    json!({"content":[{"type":"text","text":message}],"isError":true})
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
            Ok(vec![])
        }
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
    fn rejects_multiple_statements_but_not_semicolon_in_literal() {
        assert!(ensure_read_only("SELECT 1; DROP TABLE t", None).is_err());
        assert!(ensure_read_only("SELECT ';' AS value;", None).is_ok());
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
        let bytes = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "rate_limited");

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

    #[tokio::test]
    async fn change_tool_fails_closed_on_statements_without_a_table_target() {
        let mut config = static_config(true);
        config.write_catalogs = vec!["iceberg".to_string()];
        let (body, executor) = change(
            config,
            &[],
            WRITE_TOKEN,
            "CALL system.rewrite_data_files(table => 'ns.t')",
        )
        .await;
        assert_eq!(body["error"]["code"], -32003, "{body}");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cannot determine the target catalog"));
        assert!(executed(&executor).is_none());

        // With nothing configured to gate on, the same call is unchanged.
        let (body, executor) = change(
            static_config(true),
            &[],
            WRITE_TOKEN,
            "CALL system.rewrite_data_files(table => 'ns.t')",
        )
        .await;
        assert!(body.get("error").is_none(), "{body}");
        assert!(executed(&executor).is_some());
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
        assert_eq!(body["rows"].as_array().unwrap().len(), 4, "{body}");
        let diagnostics = body["diagnostics"].as_array().unwrap();
        assert_eq!(diagnostics.len(), 2, "one per visible Hive root: {diagnostics:?}");
        let by_name = |n: &str| diagnostics.iter().find(|d| d["catalog"] == n).cloned().unwrap();
        let glue = by_name("glue");
        assert_eq!(glue["status"], "auth_error", "{glue}");
        assert_eq!(glue["kind"], "hive_external");
        assert!(glue["detail"].as_str().unwrap().contains("region=eu-central-1"), "{glue}");
        let lake = by_name("lake");
        assert_eq!(lake["status"], "unreachable", "{lake}");
        assert!(lake["detail"].as_str().unwrap().contains("connection refused"), "{lake}");
        // The Iceberg catalog is listed and not probed.
        assert!(diagnostics.iter().all(|d| d["catalog"] != "iceberg"));
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
        // Two matches across two schemas; the response cap keeps one and says so.
        assert_eq!(result["total_rows"], 2);
        assert_eq!(result["returned_rows"], 1);
        assert_eq!(result["truncated"], true);
        assert_eq!(result["rows"][0]["table_schema"], "ac");
        assert_eq!(result["rows"][0]["table_name"], "orders");
        let asked = executor.asked.lock().unwrap().clone();
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
                ("SHOW STATS FOR \"glue\".\"bi\".\"sales_v\"", Answer::Fail("views carry no statistics")),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"orders\" LIMIT 1", Answer::Plan),
                ("EXPLAIN SELECT * FROM \"glue\".\"bi\".\"sales_v\" LIMIT 1", Answer::Fail("Schema error: No field named tag")),
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
        let broken = by_name("broken");
        assert!(broken["error"].as_str().unwrap().contains("unreadable storage"), "{broken}");
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
