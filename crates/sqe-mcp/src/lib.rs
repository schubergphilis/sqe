//! SQE Model Context Protocol (MCP) Streamable HTTP adapter.
//!
//! This crate is deliberately a resource server, not an OAuth proxy. Every
//! request carries the user's bearer token; SQE validates it and places the
//! same token in the query `Session`, preserving user -> MCP -> SQE -> source
//! identity and authorization.

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

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
use sqe_core::{config::McpConfig, SecretString, Session, SqeError};
use sqe_sql::StatementKind;

pub const PROTOCOL_VERSION: &str = "2025-11-25";

#[async_trait]
pub trait QueryExecutor: Send + Sync + 'static {
    async fn execute(&self, session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError>;
}

pub trait AuthRateLimiter: Send + Sync + 'static {
    fn allow(&self, peer_ip: &str) -> bool;
}

#[derive(Clone)]
pub struct McpServerState {
    auth: Arc<dyn AuthProvider>,
    executor: Arc<dyn QueryExecutor>,
    auth_rate_limiter: Option<Arc<dyn AuthRateLimiter>>,
    config: McpConfig,
}

impl McpServerState {
    pub fn new(
        auth: Arc<dyn AuthProvider>,
        executor: Arc<dyn QueryExecutor>,
        config: McpConfig,
    ) -> Self {
        Self {
            auth,
            executor,
            auth_rate_limiter: None,
            config,
        }
    }

    pub fn with_auth_rate_limiter(mut self, limiter: Arc<dyn AuthRateLimiter>) -> Self {
        self.auth_rate_limiter = Some(limiter);
        self
    }
}

pub fn router(state: McpServerState) -> Router {
    Router::new()
        .route("/mcp", post(mcp_post).get(mcp_get).delete(mcp_delete))
        .route(
            "/.well-known/oauth-protected-resource",
            get(resource_metadata),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(resource_metadata),
        )
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
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(response) => response,
    }
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
    scopes: BTreeSet<String>,
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
        && !auth.scopes.contains(&state.config.write_scope)
    {
        return auth_error_for_scope(
            &state.config,
            StatusCode::FORBIDDEN,
            Some("insufficient_scope"),
            &format!("{} {}", state.config.read_scope, state.config.write_scope),
        );
    }
    let id = request.id.clone();
    let result = match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": "sqe", "version": sqe_core::VERSION },
            "instructions": "Governed SQL access. Prefer read-only metadata and query tools. The change tool is separately gated by OAuth scope, SQE role, and source-data authorization."
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tools(&state.config) })),
        "tools/call" => call_tool(&state, &auth, &request.params).await,
        "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
        _ => Err((-32601, format!("Method not found: {}", request.method))),
    };
    match result {
        Ok(value) => rpc_result(id, value),
        Err((code, message)) => rpc_error(id, code, message),
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

async fn authenticate(
    state: &McpServerState,
    headers: &HeaderMap,
    peer_ip: String,
) -> Result<AuthContext, Response> {
    if state
        .auth_rate_limiter
        .as_ref()
        .is_some_and(|limiter| !limiter.allow(&peer_ip))
    {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate_limited"})),
        )
            .into_response());
    }
    let Some(token) = bearer(headers) else {
        return Err(auth_error(&state.config, StatusCode::UNAUTHORIZED, None));
    };
    let credentials = FlightCredentials {
        bearer_token: Some(SecretString::new(token.to_string())),
        ..Default::default()
    };
    let identity = state
        .auth
        .authenticate(&credentials)
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
    let expiry = identity
        .expires_at
        .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(1));
    let catalog_token = identity
        .catalog_token
        .clone()
        .unwrap_or_else(|| SecretString::new(token.to_string()));
    let session = Session::new(
        identity.user_id,
        catalog_token,
        identity.refresh_token,
        expiry,
        identity.roles,
    )
    .with_identity(identity.subject, identity.email, identity.groups);
    Ok(AuthContext { session, scopes })
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
    let metadata = format!(
        "{}/.well-known/oauth-protected-resource/mcp",
        config
            .resource_uri
            .trim_end_matches("/mcp")
            .trim_end_matches('/')
    );
    let mut challenge = format!(
        "Bearer resource_metadata=\"{metadata}\", scope=\"{}\"",
        scope
    );
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
    match state.executor.execute(&auth.session, &sql).await {
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
    if !auth.scopes.contains(&state.config.write_scope) {
        return Err((
            -32003,
            format!("OAuth scope '{}' is required", state.config.write_scope),
        ));
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

    #[derive(Default)]
    struct RecordingExecutor(Mutex<Option<String>>);

    #[async_trait]
    impl QueryExecutor for RecordingExecutor {
        async fn execute(
            &self,
            session: &Session,
            _sql: &str,
        ) -> Result<Vec<RecordBatch>, SqeError> {
            *self.0.lock().unwrap() = Some(session.access_token().expose().to_string());
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
}
