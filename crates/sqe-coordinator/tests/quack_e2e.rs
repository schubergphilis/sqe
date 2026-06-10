//! End-to-end test for the DuckDB Quack RPC server.
//!
//! Brings up the full Quack stack in-process and against the lightweight test
//! stack (`docker-compose.test.yml`): real Polaris bearer for catalog access,
//! real `QueryHandler` for execution, real axum HTTP listener, real wire codec
//! talking to itself over `reqwest`.
//!
//! Run with: `./scripts/integration-test.sh quack_e2e`
//!
//! What's covered:
//! - `POST /quack` accepts a `ConnectionRequest`, validates the bearer through
//!   a `FixedIdentityProvider` (the Polaris realm we test against has no JWKS
//!   plumbing; the auth chain integration is exercised by other tests)
//! - The server assigns a fresh `connection_id`
//! - `PrepareRequest("SELECT 1")` routes through `CoordinatorExecutor` ->
//!   `QueryHandler` -> DataFusion -> back through `record_batch_to_data_chunk`
//!   -> `PrepareResponse`
//! - The decoded `PrepareResponse` carries the expected single INT64 column

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_quack_server::{router, QuackServerState};
use sqe_quack_wire::data_chunk::LogicalTypeId;
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, MessageHeader, MessageType, PrepareRequest,
    QuackMessage,
};

/// `AuthProvider` stub that maps any non-empty bearer to a fixed `Identity`
/// pre-loaded with a Polaris-issued catalog token from the test stack.
struct FixedIdentityProvider {
    identity: Identity,
}

#[async_trait]
impl AuthProvider for FixedIdentityProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        if credentials
            .bearer_token
            .as_ref()
            .map(|t| t.expose().is_empty())
            .unwrap_or(true)
        {
            return Err(AuthError::AuthFailed("empty bearer".to_string()));
        }
        Ok(self.identity.clone())
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh
async fn quack_select_one_round_trip() {
    common::init_tracing();

    // ── 1. Authenticate against the lightweight Polaris stack ─────────────
    let config = sqe_core::SqeConfig::load(&common::test_config_path()).expect("load test config");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("create authenticator");
    let session = authenticator
        .authenticate("root", "")
        .await
        .expect("Polaris client_credentials auth");
    let bearer = session.access_token().expose().to_string();
    assert!(!bearer.is_empty(), "Polaris should issue a non-empty token");

    // ── 2. Build a real `QueryHandler` against the live catalog ───────────
    let policy: Arc<dyn sqe_policy::PolicyEnforcer> = Arc::new(sqe_policy::PassthroughEnforcer);
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let query_handler = Arc::new(
        sqe_coordinator::QueryHandler::new(
            policy,
            None,
            config.clone(),
            None,
            None,
            None,
            None,
            query_tracker,
            None,
            None,
            None,
            sqe_coordinator::RuntimeCatalogRegistry::default(),
            sqe_core::SecretStore::default(),
        )
        .expect("build QueryHandler"),
    );

    // ── 3. Spawn a Quack server backed by the real QueryHandler ───────────
    let auth_provider: Arc<dyn AuthProvider> = Arc::new(FixedIdentityProvider {
        identity: Identity {
            user_id: session.user.username.clone(),
            display_name: session.user.username.clone(),
            roles: session.user.roles.clone(),
            // Catalog calls executed mid-query use the Polaris bearer so
            // they appear as the authenticated user. This is the same
            // forward-the-token convention Flight SQL uses.
            catalog_token: Some(sqe_core::SecretString::new(bearer.clone())),
            refresh_token: None,
            expires_at: None,
        },
    });
    let executor: Arc<dyn sqe_quack_server::QueryExecutor> =
        Arc::new(sqe_coordinator::CoordinatorExecutor::new(query_handler));
    let state = QuackServerState::new(auth_provider, executor);
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        // QUACK-08: the handler reads the peer SocketAddr via ConnectInfo for
        // the per-IP auth limiter, so serve with connect-info.
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("axum serve");
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let base = format!("http://{addr}");

    // ── 4. ConnectionRequest -> ConnectionResponse ────────────────────────
    let connect_header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let connect_body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: bearer.clone(),
        client_duckdb_version: "v1.5.2".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let connect_bytes = encode_message(&connect_header, &connect_body);
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(connect_bytes)
        .send()
        .await
        .expect("send connect");
    assert_eq!(resp.status(), 200);
    let resp_bytes = resp.bytes().await.expect("read body").to_vec();
    let (connect_resp_header, connect_resp_body) =
        decode_message(&resp_bytes).expect("decode connect response");
    assert_eq!(
        connect_resp_header.r#type,
        MessageType::ConnectionResponse,
        "expected ConnectionResponse, header was {:?}",
        connect_resp_header
    );
    let connection_id = connect_resp_header.connection_id.clone();
    assert!(
        !connection_id.is_empty(),
        "server should assign a connection_id"
    );
    match connect_resp_body {
        QuackMessage::ConnectionResponse(c) => assert_eq!(c.quack_version, 1),
        other => panic!("expected ConnectionResponse, got {other:?}"),
    }

    // ── 5. PrepareRequest("SELECT 1") -> PrepareResponse ──────────────────
    let prepare_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(1),
    };
    let prepare_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT 1".to_string(),
    });
    let prepare_bytes = encode_message(&prepare_header, &prepare_body);
    let resp = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb")
        .body(prepare_bytes)
        .send()
        .await
        .expect("send prepare");
    assert_eq!(resp.status(), 200);
    let resp_bytes = resp.bytes().await.expect("read body").to_vec();
    let (prepare_resp_header, prepare_resp_body) =
        decode_message(&resp_bytes).expect("decode prepare response");

    assert_eq!(prepare_resp_header.connection_id, connection_id);
    assert_eq!(prepare_resp_header.client_query_id, Some(1));

    let prepare_response = match prepare_resp_body {
        QuackMessage::PrepareResponse(r) => r,
        QuackMessage::ErrorResponse(e) => panic!(
            "SELECT 1 returned ErrorResponse: {} (this likely means the QueryHandler \
             could not execute the query — check the coordinator logs and that the \
             test stack is up: docker compose -f docker-compose.test.yml up -d)",
            e.message
        ),
        other => panic!("expected PrepareResponse, got {other:?}"),
    };

    assert_eq!(prepare_response.result_types.len(), 1, "one result column");
    assert!(
        // DataFusion's SELECT 1 typically yields Int64; accept any signed integer
        // family to keep the test stable across DF version bumps.
        matches!(
            prepare_response.result_types[0].id,
            LogicalTypeId::Integer | LogicalTypeId::BigInt | LogicalTypeId::SmallInt
        ),
        "expected an integer column type, got {:?}",
        prepare_response.result_types[0].id
    );
    assert!(!prepare_response.needs_more_fetch);
    assert_eq!(prepare_response.results.len(), 1);
    assert_eq!(prepare_response.results[0].row_count, 1);
    assert_eq!(prepare_response.results[0].columns.len(), 1);
}
