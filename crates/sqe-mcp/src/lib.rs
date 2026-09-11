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

pub trait AuthRateLimiter: Send + Sync + 'static {
    /// `credential` is a non-secret label for the presented credential (a
    /// SHA-256 fingerprint prefix of the bearer, or `no-bearer`), so the
    /// per-tuple budget is spent per token, not per address. Behind a proxy
    /// every agent shares one peer IP; without this key one garbage-bearer
    /// loop would exhaust the budget for all of them, and one attacker could
    /// rotate tokens for free.
    fn allow(&self, peer_ip: &str, credential: &str) -> bool;
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
    auth_rate_limiter: Option<Arc<dyn AuthRateLimiter>>,
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
}

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
}

impl StaticPrincipal {
    fn resolve(config: &McpPrincipalConfig) -> Option<Self> {
        match config.resolve_token() {
            Ok(token) => Some(Self {
                token,
                user: config.user.clone(),
                roles: config.roles.clone(),
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
            auth_rate_limiter: None,
            config,
            principals,
            clients: Arc::new(ClientRegistry::default()),
        }
    }

    pub fn with_auth_rate_limiter(mut self, limiter: Arc<dyn AuthRateLimiter>) -> Self {
        self.auth_rate_limiter = Some(limiter);
        self
    }
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
    // The credential half of the rate-limit key is a fingerprint of the
    // bearer, never the bearer: the limiter stores its keys.
    let credential = bearer(headers)
        .map(sqe_trino_compat::server::bearer_fingerprint)
        .unwrap_or_else(|| "no-bearer".to_string());
    if state
        .auth_rate_limiter
        .as_ref()
        .is_some_and(|limiter| !limiter.allow(&peer_ip, &credential))
    {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        )
            .into_response());
    }
    match state.config.auth.mode {
        McpAuthMode::Oauth => authenticate_oauth(state, headers).await,
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
        session: local_session(principal.user.clone(), principal.roles.clone()),
        scopes: None,
    })
}

/// Accept every caller as the one configured identity.
fn authenticate_anonymous(state: &McpServerState) -> AuthContext {
    let anonymous = &state.config.auth.anonymous;
    AuthContext {
        session: local_session(anonymous.user.clone(), anonymous.roles.clone()),
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
) -> Result<AuthContext, Response> {
    let Some(token) = bearer(headers) else {
        return Err(auth_error(&state.config, StatusCode::UNAUTHORIZED, None));
    };
    let credentials = FlightCredentials {
        bearer_token: Some(SecretString::new(token.to_string())),
        ..Default::default()
    };
    let identity = state
        .auth
        .authenticate_oauth(&credentials)
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "MCP bearer authentication failed");
            auth_error(
                &state.config,
                StatusCode::UNAUTHORIZED,
                Some("invalid_token"),
            )
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
    let session = identity.into_session(Some(token));
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
        tool("list_catalogs", "List catalogs visible to the authenticated user.", empty_schema(), true, false),
        tool("list_schemas", "List schemas visible to the authenticated user in a catalog.", one_string_schema("catalog"), true, false),
        tool("list_tables", "List tables visible to the authenticated user in a catalog and schema.", two_string_schema("catalog", "schema"), true, false),
        tool("describe_table", "Return columns for a table visible to the authenticated user.", three_string_schema(), true, false),
        tool("get_table_ddl", "Return SHOW CREATE TABLE output for a visible table.", three_string_schema(), true, false),
        tool("get_table_stats", "Return Iceberg table statistics visible to the authenticated user.", three_string_schema(), true, false),
        tool("explain_query", "Explain a read-only SQL query without allowing a write statement to be hidden behind EXPLAIN.", one_string_schema("sql"), true, false),
        tool("whoami", "Show the SQE identity, roles, groups, and OAuth scopes for this request; never returns tokens.", empty_schema(), true, false),
    ];
    if config.allow_writes {
        tools.push(tool("execute_sql_change", "Execute one change/write SQL statement. Requires the configured OAuth write scope and SQE writer role; source catalog and storage authorization still apply as the authenticated user.", one_string_schema("sql"), false, true));
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
    let sql = match name {
        "execute_sql_readonly" => {
            let sql = required(&args, "sql")?;
            ensure_read_only(sql).map_err(|e| (-32602, e))?;
            sql.to_string()
        }
        "execute_sql_change" => {
            authorize_write(state, auth)?;
            let sql = required(&args, "sql")?;
            ensure_change(sql).map_err(|e| (-32602, e))?;
            sql.to_string()
        }
        "list_catalogs" => "SHOW CATALOGS".to_string(),
        "list_schemas" => format!("SHOW SCHEMAS FROM {}", ident(required(&args, "catalog")?)),
        "list_tables" => format!(
            "SHOW TABLES FROM {}.{}",
            ident(required(&args, "catalog")?),
            ident(required(&args, "schema")?)
        ),
        "describe_table" => format!("SHOW COLUMNS FROM {}", table_ref(&args)?),
        "get_table_ddl" => format!("SHOW CREATE TABLE {}", table_ref(&args)?),
        "get_table_stats" => format!("SHOW STATS FOR {}", table_ref(&args)?),
        "explain_query" => {
            let inner = required(&args, "sql")?;
            ensure_read_only(inner).map_err(|e| (-32602, e))?;
            format!("EXPLAIN {inner}")
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
        Err(error) => Ok(tool_failure(error.to_string())),
    }
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

fn classify(sql: &str) -> Result<StatementKind, String> {
    ensure_single_statement(sql)?;
    let normalized =
        sqe_sql::pre_parse_pipeline(&sqe_sql::UserSql::from(sql)).map_err(|e| e.to_string())?;
    sqe_sql::parse_and_classify_typed(&normalized).map_err(|e| e.to_string())
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

fn ensure_read_only(sql: &str) -> Result<(), String> {
    let kind = classify(sql)?;
    let allowed = matches!(
        kind,
        StatementKind::Query(_)
            | StatementKind::ShowCatalogs
            | StatementKind::ShowSchemas(_)
            | StatementKind::ShowTables(_)
            | StatementKind::ShowGrants(_)
            | StatementKind::ShowEffectiveGrants(_)
            | StatementKind::ShowEffectivePolicy(_)
            | StatementKind::ShowTags(_)
            | StatementKind::ShowMaskingPolicies(_)
            | StatementKind::ShowCreateTable(_)
            | StatementKind::ShowCreateSchema(_)
            | StatementKind::ShowStats(_)
            | StatementKind::ShowPartitions(_)
    );
    if allowed {
        Ok(())
    } else {
        Err(format!("Read-only tool rejected {} statement", kind.name()))
    }
}

fn ensure_change(sql: &str) -> Result<(), String> {
    let kind = classify(sql)?;
    if ensure_read_only(sql).is_ok() {
        return Err(format!(
            "Use a read-only tool for {} statements",
            kind.name()
        ));
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
                    sqe_trino_compat::types::arrow_value_to_json(batch.column(index).as_ref(), row),
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

    /// Records, per call: the catalog token, `(username, roles)`, and the
    /// `Session.source` label the tool call stamped.
    #[derive(Default)]
    struct RecordingExecutor(
        Mutex<Option<String>>,
        Mutex<Option<(String, Vec<String>)>>,
        Mutex<Option<String>>,
    );

    #[async_trait]
    impl QueryExecutor for RecordingExecutor {
        async fn execute(
            &self,
            session: &Session,
            _sql: &str,
        ) -> Result<Vec<RecordBatch>, SqeError> {
            *self.0.lock().unwrap() = Some(session.access_token().expose().to_string());
            *self.1.lock().unwrap() = Some((
                session.user.username.clone(),
                session.user.roles.clone(),
            ));
            *self.2.lock().unwrap() = session.source.clone();
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
            ensure_read_only(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CALL system.rewrite_data_files(table => 'ns.t')",
        ] {
            assert!(ensure_read_only(sql).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn rejects_multiple_statements_but_not_semicolon_in_literal() {
        assert!(ensure_read_only("SELECT 1; DROP TABLE t").is_err());
        assert!(ensure_read_only("SELECT ';' AS value;").is_ok());
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
                    },
                    McpPrincipalConfig {
                        user: "mcp-writer".to_string(),
                        roles: vec!["data_writer".to_string()],
                        token_env: String::new(),
                        token_b64: b64(WRITE_TOKEN),
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

    #[tokio::test]
    async fn auth_rate_limit_is_keyed_by_bearer_fingerprint_not_peer_ip_alone() {
        struct RecordingLimiter(Mutex<Vec<(String, String)>>);
        impl AuthRateLimiter for RecordingLimiter {
            fn allow(&self, peer_ip: &str, credential: &str) -> bool {
                self.0
                    .lock()
                    .unwrap()
                    .push((peer_ip.to_string(), credential.to_string()));
                true
            }
        }
        let limiter = Arc::new(RecordingLimiter(Mutex::new(Vec::new())));
        let app = router(
            McpServerState::new(
                Arc::new(MockAuth),
                Arc::new(RecordingExecutor::default()),
                static_config(true),
            )
            .with_auth_rate_limiter(limiter.clone()),
        );
        send(&app, READ_TOKEN, &[], tool_call("execute_sql_readonly")).await;
        send(&app, WRITE_TOKEN, &[], tool_call("execute_sql_readonly")).await;
        let no_bearer = Request::post("/mcp")
            .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(tool_call("execute_sql_readonly")))
            .unwrap();
        app.clone().oneshot(no_bearer).await.unwrap();

        let keys = limiter.0.lock().unwrap();
        assert_eq!(keys.len(), 3);
        // Same peer, two credentials, two buckets.
        assert_eq!(keys[0].0, keys[1].0);
        assert!(keys[0].1.starts_with("bearer:"));
        assert!(keys[1].1.starts_with("bearer:"));
        assert_ne!(keys[0].1, keys[1].1);
        assert_eq!(keys[2].1, "no-bearer");
        // The key is a fingerprint: the limiter never sees the token itself.
        assert!(keys.iter().all(|(_, key)| !key.contains(READ_TOKEN) && !key.contains(WRITE_TOKEN)));
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
