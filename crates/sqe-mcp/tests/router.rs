//! Router-level MCP tests (#673): every path here goes through
//! `sqe_mcp::router` with a fake `AuthProvider` and a fake `QueryExecutor`.
//! No Polaris, no network, no coordinator.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    Router,
};
use base64::Engine;
use serde_json::{json, Value};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::{
    config::{McpAuthConfig, McpAuthMode, McpConfig, McpPrincipalConfig},
    Session, SqeError,
};
use sqe_mcp::{router, McpServerState, QueryExecutor, SESSION_ID_HEADER};
use tower::ServiceExt;

/// Token = `<user>.<base64 claims>.sig`. The fake provider reads the user
/// from the first segment and rejects `bad` (an expired or wrong-audience
/// token from a real validator); the router reads the scopes from the claims.
fn token(user: &str, scopes: &str) -> String {
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&json!({"scope": scopes, "sub": user})).unwrap());
    format!("{user}.{claims}.sig")
}

struct FakeAuth;

#[async_trait]
impl AuthProvider for FakeAuth {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        self.authenticate_oauth(credentials).await
    }

    async fn authenticate_oauth(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        let bearer = credentials.bearer_token.as_ref().ok_or(AuthError::AuthFailed("no bearer".into()))?;
        let user = bearer.expose().split('.').next().unwrap_or_default().to_string();
        if user == "bad" {
            return Err(AuthError::AuthFailed("token rejected".into()));
        }
        Ok(Identity {
            user_id: user.clone(),
            display_name: user,
            roles: vec!["reader".to_string(), "data_writer".to_string()],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: Some(chrono::Utc::now() + chrono::Duration::minutes(5)),
            scopes: vec![],
        })
    }
}

/// Answers every statement with `rows` Int64 rows and records the SQL it saw.
#[derive(Default)]
struct FakeExecutor {
    rows: usize,
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl QueryExecutor for FakeExecutor {
    async fn execute(&self, _session: &Session, sql: &str) -> Result<Vec<RecordBatch>, SqeError> {
        self.calls.lock().unwrap().push(sql.to_string());
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let values = Int64Array::from((0..self.rows as i64).collect::<Vec<_>>());
        Ok(vec![RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()])
    }
}

fn oauth_config() -> McpConfig {
    McpConfig {
        resource_uri: "https://sqe.example.com/mcp".to_string(),
        authorization_servers: vec!["https://idp.example.com".to_string()],
        ..Default::default()
    }
}

const STATIC_TOKEN: &str = "static-token-with-enough-length";

fn static_config() -> McpConfig {
    McpConfig {
        auth: McpAuthConfig {
            mode: McpAuthMode::Static,
            principals: vec![McpPrincipalConfig {
                user: "kiosk".to_string(),
                roles: vec!["reader".to_string()],
                token_env: String::new(),
                token_b64: base64::engine::general_purpose::STANDARD.encode(STATIC_TOKEN),
                default_catalog: None,
                default_schema: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn app(config: McpConfig, executor: Arc<FakeExecutor>) -> Router {
    router(McpServerState::new(Arc::new(FakeAuth), executor, config))
}

fn app_rows(config: McpConfig, rows: usize) -> (Router, Arc<FakeExecutor>) {
    let executor = Arc::new(FakeExecutor { rows, ..Default::default() });
    (app(config, executor.clone()), executor)
}

fn rpc(method: &str, params: Value) -> String {
    json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string()
}

fn call(tool: &str, sql: &str) -> String {
    rpc("tools/call", json!({"name":tool,"arguments":{"sql":sql}}))
}

/// POST `/mcp` and return `(status, headers, json body)`; a non-JSON body
/// (bare status responses) comes back as `Value::Null`.
async fn post(
    app: &Router,
    bearer: Option<&str>,
    extra: &[(&str, &str)],
    body: String,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut request = Request::post("/mcp")
        .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(bearer) = bearer {
        request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    for (name, value) in extra {
        request = request.header(*name, *value);
    }
    let response = app.clone().oneshot(request.body(Body::from(body)).unwrap()).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, headers, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn read() -> String {
    token("alice", "sqe:mcp:read")
}

#[tokio::test]
async fn missing_bearer_is_401() {
    let (app, _) = app_rows(oauth_config(), 0);
    let (status, headers, _) = post(&app, None, &[], rpc("ping", json!({}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(headers.get(header::WWW_AUTHENTICATE).unwrap().to_str().unwrap().starts_with("Bearer"));
}

#[tokio::test]
async fn bearer_rejected_by_provider_is_401() {
    let (app, executor) = app_rows(oauth_config(), 0);
    let (status, headers, _) = post(&app, Some(&token("bad", "sqe:mcp:read")), &[], call("execute_sql_readonly", "SELECT 1")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(headers.get(header::WWW_AUTHENTICATE).unwrap().to_str().unwrap().contains("invalid_token"));
    assert!(executor.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn static_token_mismatch_is_401_and_match_is_200() {
    let (app, _) = app_rows(static_config(), 0);
    let (status, _, _) = post(&app, Some("static-token-with-enough-lengtX"), &[], rpc("ping", json!({}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, body) = post(&app, Some(STATIC_TOKEN), &[], rpc("ping", json!({}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("error").is_none(), "{body}");
}

#[tokio::test]
async fn token_without_read_scope_is_403() {
    let (app, executor) = app_rows(oauth_config(), 0);
    let (status, headers, _) = post(&app, Some(&token("alice", "openid")), &[], call("execute_sql_readonly", "SELECT 1")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(headers.get(header::WWW_AUTHENTICATE).unwrap().to_str().unwrap().contains("insufficient_scope"));
    assert!(executor.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn change_tool_without_write_scope_is_403_before_dispatch() {
    let mut config = oauth_config();
    config.allow_writes = true;
    let (app, executor) = app_rows(config, 0);
    let (status, _, _) = post(&app, Some(&read()), &[], call("execute_sql_change", "INSERT INTO t VALUES (1)")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(executor.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn body_over_one_mib_is_413() {
    let (app, _) = app_rows(oauth_config(), 0);
    let sql = "x".repeat(1024 * 1024 + 1);
    let (status, _, _) = post(&app, Some(&read()), &[], call("execute_sql_readonly", &sql)).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn origin_not_in_allow_list_is_403() {
    let mut config = oauth_config();
    config.allowed_origins = vec!["https://ok.example".to_string()];
    let (app, _) = app_rows(config, 0);
    let (status, _, _) = post(&app, Some(&read()), &[("origin", "https://evil.example")], rpc("ping", json!({}))).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _, _) = post(&app, Some(&read()), &[("origin", "https://ok.example")], rpc("ping", json!({}))).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn tools_list_advertises_change_tool_only_when_writes_enabled() {
    let names = |body: &Value| -> Vec<String> {
        body["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect()
    };
    let (app, _) = app_rows(oauth_config(), 0);
    let (_, _, body) = post(&app, Some(&read()), &[], rpc("tools/list", json!({}))).await;
    assert!(!names(&body).iter().any(|n| n == "execute_sql_change"), "{body}");
    assert!(names(&body).iter().any(|n| n == "execute_sql_readonly"), "{body}");

    let mut config = oauth_config();
    config.allow_writes = true;
    let (app, _) = app_rows(config, 0);
    let (_, _, body) = post(&app, Some(&read()), &[], rpc("tools/list", json!({}))).await;
    assert!(names(&body).iter().any(|n| n == "execute_sql_change"), "{body}");
}

#[tokio::test]
async fn change_tool_refuses_catalog_outside_write_catalogs() {
    let mut config = oauth_config();
    config.allow_writes = true;
    config.write_catalogs = vec!["iceberg".to_string()];
    let (app, executor) = app_rows(config, 0);
    let rw = token("alice", "sqe:mcp:read sqe:mcp:write");
    let (status, _, body) = post(&app, Some(&rw), &[], call("execute_sql_change", "INSERT INTO other.ns.t VALUES (1)")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32003, "{body}");
    assert!(body["error"]["message"].as_str().unwrap().contains("not in mcp.write_catalogs"));
    assert!(executor.calls.lock().unwrap().is_empty(), "refused statements never reach the engine");

    let (_, _, body) = post(&app, Some(&rw), &[], call("execute_sql_change", "INSERT INTO iceberg.ns.t VALUES (1)")).await;
    assert!(body.get("error").is_none(), "{body}");
    assert_eq!(executor.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn max_result_rows_truncates_and_flags_it() {
    let mut config = oauth_config();
    config.max_result_rows = 5;
    let (app, _) = app_rows(config, 20);
    let (_, _, body) = post(&app, Some(&read()), &[], call("execute_sql_readonly", "SELECT 1")).await;
    let result = &body["result"]["structuredContent"];
    assert_eq!(result["rows"].as_array().unwrap().len(), 5, "{body}");
    assert_eq!(result["returned_rows"], 5);
    assert_eq!(result["total_rows"], 20);
    assert_eq!(result["truncated"], true);
}

#[tokio::test]
async fn max_response_bytes_truncates_and_flags_it() {
    let mut config = oauth_config();
    // Framing `{"columns":[{"name":"n","type":"Int64"}],"rows":[]}` is 51
    // bytes; each `{"n":d}` row plus its comma is 8. 75 fits exactly 3 rows.
    config.max_response_bytes = 75;
    let (app, _) = app_rows(config, 10);
    let (_, _, body) = post(&app, Some(&read()), &[], call("execute_sql_readonly", "SELECT 1")).await;
    let result = &body["result"]["structuredContent"];
    assert_eq!(result["returned_rows"], 3, "{body}");
    assert_eq!(result["total_rows"], 10);
    assert_eq!(result["truncated"], true);
}

#[tokio::test]
async fn borrowed_session_id_does_not_relabel_another_principal() {
    let (app, _) = app_rows(oauth_config(), 0);
    let bob = token("bob", "sqe:mcp:read");
    let init = rpc("initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"claude-code","version":"1.0"}}));
    let (status, headers, _) = post(&app, Some(&bob), &[], init).await;
    assert_eq!(status, StatusCode::OK);
    let sid = headers.get(SESSION_ID_HEADER).expect("initialize issues a session id").to_str().unwrap().to_string();

    let whoami = rpc("tools/call", json!({"name":"whoami"}));
    // Bob echoing his own id gets his registered label.
    let (_, _, body) = post(&app, Some(&bob), &[(SESSION_ID_HEADER, &sid)], whoami.clone()).await;
    assert_eq!(body["result"]["structuredContent"]["client"], "claude-code/1.0", "{body}");
    // Alice replaying Bob's id is not relabelled: her own User-Agent applies.
    let (_, _, body) = post(&app, Some(&read()), &[(SESSION_ID_HEADER, &sid), ("user-agent", "codex-cli/1.2")], whoami).await;
    assert_eq!(body["result"]["structuredContent"]["client"], "codex-cli/1.2", "{body}");
}

#[tokio::test]
async fn readonly_tool_rejects_ddl_before_the_executor_runs() {
    let (app, executor) = app_rows(oauth_config(), 0);
    let (status, _, body) = post(&app, Some(&read()), &[], call("execute_sql_readonly", "DROP TABLE iceberg.ns.t")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32602, "{body}");
    assert!(executor.calls.lock().unwrap().is_empty(), "the executor was never invoked");
}
