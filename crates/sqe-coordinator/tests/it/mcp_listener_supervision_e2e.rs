//! The dedicated `/mcp` listener over a real TCP socket, idle past the
//! session-context cache TTL, then used again
//! (`make test-integration FILTER=mcp_listener`).
//!
//! What this proves: `start_mcp_server` binds an ephemeral port and reports
//! it, an `initialize` and a `tools/call` succeed over the wire, the listener
//! is still accepting after sitting idle longer than the coordinator's
//! session-context cache TTL (60 s; the cached `SessionContext` for the
//! caller is gone and rebuilt on the next call), and the liveness flag the
//! ops port reads is still set afterwards. The sidecar saw a live PID with a
//! dead `/mcp` twice after idle time; this is the shortest idle that crosses
//! a server-side expiry, on the real listener rather than a `tower::oneshot`.
//!
//! The MCP router runs in-process over a `QueryHandler` wired like the Flight
//! path. Authentication is a test `AuthProvider` that maps any bearer to the
//! root session the fixture authenticated.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use async_trait::async_trait;
use serde_json::{json, Value};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::{config::McpConfig, Session};

/// Every bearer is the fixture's root session.
struct RootAuth(Session);

impl RootAuth {
    fn identity(&self) -> Identity {
        let session = &self.0;
        Identity {
            user_id: session.user.username.clone(),
            display_name: session.user.username.clone(),
            roles: session.user.roles.clone(),
            subject: session.user.subject.clone(),
            email: session.user.email.clone(),
            groups: session.user.groups.clone(),
            catalog_token: Some(session.access_token().clone()),
            refresh_token: session.refresh_token().cloned(),
            expires_at: Some(session.token_expiry()),
        }
    }
}

#[async_trait]
impl AuthProvider for RootAuth {
    async fn authenticate_oauth(
        &self,
        _credentials: &FlightCredentials,
    ) -> Result<Identity, AuthError> {
        Ok(self.identity())
    }

    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Ok(self.identity())
    }
}

/// `header.<payload>.signature` carrying the read scope; `RootAuth` is the
/// verifier, so the signature is never checked.
fn bearer() -> String {
    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&json!({"sub": "root", "scope": "sqe:mcp:read"})).unwrap());
    format!("e30.{payload}.test")
}

/// One JSON-RPC request over TCP. Returns the status and the parsed body
/// (the server answers plain JSON to a client that accepts it).
async fn rpc(client: &reqwest::Client, url: &str, body: Value) -> (u16, Value) {
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("authorization", format!("Bearer {}", bearer()))
        .header("user-agent", "sqe-it/1")
        .body(body.to_string())
        .send()
        .await
        .expect("the listener accepted the connection");
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    let json = text
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .map(str::to_string)
        .unwrap_or(text);
    (status, serde_json::from_str(&json).unwrap_or(Value::Null))
}

fn initialize() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                   "clientInfo": {"name": "sqe-it", "version": "0"}}
    })
}

fn list_catalogs() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "list_catalogs", "arguments": {}}
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn mcp_listener_answers_again_after_idling_past_the_session_cache_ttl() {
    let (session, handler) = crate::common::setup_handler().await;
    let config = sqe_core::SqeConfig::load(&crate::common::test_config_path()).expect("config");

    let alive = Arc::new(AtomicBool::new(true));
    let (task, bound) = sqe_coordinator::mcp_server::start_mcp_server(
        Arc::new(RootAuth(session.clone())),
        Arc::new(handler),
        Arc::new(sqe_coordinator::rate_limiter::QueryRateLimiter::new(
            &config.rate_limit,
        )),
        Arc::new(sqe_coordinator::rate_limiter::MetadataRateLimiter::new(
            &config.rate_limit,
        )),
        McpConfig {
            enabled: true,
            // 0: an ephemeral port, reported back so the test cannot collide.
            port: 0,
            resource_uri: "https://sqe.test/mcp".to_string(),
            authorization_servers: vec!["https://idp.test/realms/it".to_string()],
            ..Default::default()
        },
        Vec::new(),
        &config.coordinator.tls,
        alive.clone(),
        &config.storage,
        sqe_coordinator::mcp_server::CancellationToken::new(),
    )
    .await
    .expect("listener bound");
    assert_ne!(bound.port(), 0, "the bound port is reported, not the requested 0");
    let url = format!("http://127.0.0.1:{}/mcp", bound.port());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();

    let (status, body) = rpc(&client, &url, initialize()).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["result"]["protocolVersion"], "2025-11-25", "{body}");
    let (status, body) = rpc(&client, &url, list_catalogs()).await;
    assert_eq!(status, 200, "{body}");
    assert_ne!(body["result"]["isError"], json!(true), "{body}");

    // Idle past the coordinator's session-context cache TTL (60 s default):
    // the caller's cached SessionContext expires and the next call rebuilds
    // it. Nothing else in the process should notice the silence.
    let idle = std::time::Duration::from_secs(65);
    eprintln!("mcp listener: idling {idle:?} on port {}", bound.port());
    tokio::time::sleep(idle).await;

    assert!(alive.load(Ordering::SeqCst), "the serve task must still be running");
    assert!(!task.is_finished(), "the serve task must still be running");
    let (status, body) = rpc(&client, &url, initialize()).await;
    assert_eq!(status, 200, "re-initialize after idle: {body}");
    let (status, body) = rpc(&client, &url, list_catalogs()).await;
    assert_eq!(status, 200, "tools/call after idle: {body}");
    assert_ne!(body["result"]["isError"], json!(true), "{body}");
    assert!(alive.load(Ordering::SeqCst));

    // Ending the serve task, however it ends, clears the flag /healthz reads.
    task.abort();
    let _ = task.await;
    assert!(
        !alive.load(Ordering::SeqCst),
        "a listener task that is gone must not report itself alive"
    );
    let (status, _) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        match client.post(&url).body("{}").send().await {
            Ok(r) => (r.status().as_u16(), Value::Null),
            Err(_) => (0, Value::Null),
        }
    })
    .await
    .unwrap_or((0, Value::Null));
    assert_eq!(status, 0, "the port is closed once the task is gone");
}
