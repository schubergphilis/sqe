//! Test-only stub `AuthProvider` and `QueryExecutor` implementations plus
//! helpers shared across the integration tests.

// Each integration test file compiles its own copy of this module via
// `mod support;`. Items not used by a given file would otherwise trigger
// dead-code warnings.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::config::SecurityConfig;
use sqe_core::Session;
use sqe_quack_server::{
    router, PendingResults, QuackServerState, QueryError, QueryExecutor, QueryStream, SessionStore,
};
use sqe_quack_wire::data_chunk::{DataChunk, VectorData};
use sqe_quack_wire::message::{
    decode_message, encode_message, ConnectionRequest, FetchRequest, MessageHeader, MessageType,
    PrepareRequest, QuackMessage,
};

// -----------------------------------------------------------------------------
// AuthProvider stubs
// -----------------------------------------------------------------------------

pub struct AcceptProvider {
    pub user_id: String,
    /// Token expiry handed to every minted `Identity`. `None` means "no
    /// provider expiry", which the shared mapping turns into the default
    /// one-hour session lifetime.
    pub expires_at: Option<DateTime<Utc>>,
}

impl AcceptProvider {
    pub fn new(user_id: &str) -> Self {
        Self {
            user_id: user_id.to_string(),
            expires_at: None,
        }
    }

    pub fn expiring_in(user_id: &str, ttl: chrono::Duration) -> Self {
        Self {
            user_id: user_id.to_string(),
            expires_at: Some(Utc::now() + ttl),
        }
    }
}

#[async_trait]
impl AuthProvider for AcceptProvider {
    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Ok(Identity {
            user_id: self.user_id.clone(),
            display_name: self.user_id.clone(),
            roles: vec!["test-role".to_string()],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: self.expires_at,
            scopes: vec![],
        })
    }
}

pub struct RejectProvider {
    pub reason: String,
}

#[async_trait]
impl AuthProvider for RejectProvider {
    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Err(AuthError::AuthFailed(self.reason.clone()))
    }
}

pub struct SkipProvider;

#[async_trait]
impl AuthProvider for SkipProvider {
    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        Err(AuthError::NotMyCredentials)
    }
}

/// Stand-in for the coordinator's rate-limited chain (#562): refuses every
/// credential as `RateLimited` and records the `peer_ip` it was handed, so a
/// test can check that the server sends the resolved client address along.
pub struct RateLimitedProvider {
    pub seen_peer_ips: std::sync::Mutex<Vec<Option<String>>>,
}

impl RateLimitedProvider {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            seen_peer_ips: std::sync::Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl AuthProvider for RateLimitedProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        self.seen_peer_ips
            .lock()
            .unwrap()
            .push(credentials.peer_ip.clone());
        Err(AuthError::RateLimited)
    }
}

pub fn accept_provider() -> Arc<dyn AuthProvider> {
    Arc::new(AcceptProvider::new("test-user"))
}

// -----------------------------------------------------------------------------
// QueryExecutor stubs
// -----------------------------------------------------------------------------

/// Returns a fixed single-column INT32 batch named `x` with values [1, 2, 3].
pub struct FixedRowExecutor;

#[async_trait]
impl QueryExecutor for FixedRowExecutor {
    async fn execute(&self, _session: &Session, _sql: &str) -> Result<QueryStream, QueryError> {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let schema = int32_schema("x");
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![arr]).unwrap();
        Ok(QueryStream::from_batches(schema, vec![batch]))
    }
}

/// Returns the given `QueryError` regardless of input. Used to exercise the
/// error-mapping branches of the prepare handler.
pub struct ErroringExecutor {
    pub error: fn() -> QueryError,
}

#[async_trait]
impl QueryExecutor for ErroringExecutor {
    async fn execute(&self, _session: &Session, _sql: &str) -> Result<QueryStream, QueryError> {
        Err((self.error)())
    }
}

/// Emits `batches` batches of `rows_per_batch` consecutive Int32 values in one
/// column named `x`, so a test can assert the concatenation of every page
/// equals the input exactly.
pub struct CountingExecutor {
    pub batches: usize,
    pub rows_per_batch: i32,
}

impl CountingExecutor {
    /// The values the executor will produce, in order.
    pub fn expected_values(&self) -> Vec<i32> {
        (0..(self.batches as i32 * self.rows_per_batch)).collect()
    }
}

#[async_trait]
impl QueryExecutor for CountingExecutor {
    async fn execute(&self, _session: &Session, _sql: &str) -> Result<QueryStream, QueryError> {
        let schema = int32_schema("x");
        let mut out = Vec::with_capacity(self.batches);
        for b in 0..self.batches as i32 {
            let start = b * self.rows_per_batch;
            let arr: Arc<dyn Array> = Arc::new(Int32Array::from(
                (start..start + self.rows_per_batch).collect::<Vec<_>>(),
            ));
            out.push(RecordBatch::try_new(Arc::clone(&schema), vec![arr]).unwrap());
        }
        Ok(QueryStream::from_batches(schema, out))
    }
}

/// Yields no batches at all, but still declares a schema — the case that used
/// to lose `result_names`/`result_types` entirely because they were read off
/// the first batch.
pub struct EmptyResultExecutor;

#[async_trait]
impl QueryExecutor for EmptyResultExecutor {
    async fn execute(&self, _session: &Session, _sql: &str) -> Result<QueryStream, QueryError> {
        Ok(QueryStream::from_batches(int32_schema("x"), Vec::new()))
    }
}

pub fn int32_schema(name: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)]))
}

pub fn noop_executor() -> Arc<dyn QueryExecutor> {
    Arc::new(FixedRowExecutor)
}

// -----------------------------------------------------------------------------
// Server spawn helpers
// -----------------------------------------------------------------------------

pub async fn spawn_server_with(provider: Arc<dyn AuthProvider>) -> String {
    let (base, _sessions) = spawn_server_with_sessions(provider, Arc::new(FixedRowExecutor)).await;
    base
}

pub async fn spawn_server_with_executor(
    provider: Arc<dyn AuthProvider>,
    executor: Arc<dyn QueryExecutor>,
) -> String {
    let (base, _sessions) = spawn_server_with_sessions(provider, executor).await;
    base
}

pub async fn spawn_server_with_sessions(
    provider: Arc<dyn AuthProvider>,
    executor: Arc<dyn QueryExecutor>,
) -> (String, SessionStore) {
    let (base, sessions, _pending) = spawn_state(QuackServerState::new(provider, executor)).await;
    (base, sessions)
}

/// Serve an arbitrary pre-built state, handing back the base URL plus the two
/// stores so a test can assert on session / pending-result bookkeeping.
pub async fn spawn_state(state: QuackServerState) -> (String, SessionStore, PendingResults) {
    let sessions = state.sessions.clone();
    let pending = state.pending.clone();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // QUACK-08: the handler reads the peer SocketAddr via ConnectInfo for
        // the per-IP auth limiter, so serve with connect-info.
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (format!("http://{addr}"), sessions, pending)
}

// -----------------------------------------------------------------------------
// Wire helpers
// -----------------------------------------------------------------------------

/// POST one encoded message and decode the reply. `forwarded_for` sets the
/// `x-forwarded-for` header, which the server honours only when the peer is a
/// configured trusted proxy — that is how a test fakes two different clients
/// over one loopback socket.
pub async fn post_message(
    base: &str,
    header: &MessageHeader,
    body: &QuackMessage,
    forwarded_for: Option<&str>,
) -> (MessageHeader, QuackMessage) {
    let mut req = reqwest::Client::new()
        .post(format!("{base}/quack"))
        .header("content-type", "application/vnd.duckdb");
    if let Some(xff) = forwarded_for {
        req = req.header("x-forwarded-for", xff);
    }
    let resp = req.body(encode_message(header, body)).send().await.unwrap();
    decode_message(&resp.bytes().await.unwrap()).unwrap()
}

pub async fn connect_as(base: &str, forwarded_for: Option<&str>) -> String {
    let header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: "super_secret".to_string(),
        client_duckdb_version: "v1.5.2".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let (resp_header, resp_body) = post_message(base, &header, &body, forwarded_for).await;
    match resp_body {
        QuackMessage::ConnectionResponse(_) => resp_header.connection_id,
        other => panic!("expected ConnectionResponse, got {other:?}"),
    }
}

pub async fn send_prepare_as(
    base: &str,
    connection_id: &str,
    sql: &str,
    forwarded_for: Option<&str>,
) -> QuackMessage {
    let header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.to_string(),
        client_query_id: Some(1),
    };
    let body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: sql.to_string(),
    });
    post_message(base, &header, &body, forwarded_for).await.1
}

pub async fn send_fetch_as(
    base: &str,
    connection_id: &str,
    uuid: i128,
    forwarded_for: Option<&str>,
) -> QuackMessage {
    let header = MessageHeader {
        r#type: MessageType::FetchRequest,
        connection_id: connection_id.to_string(),
        client_query_id: Some(1),
    };
    let body = QuackMessage::FetchRequest(FetchRequest { uuid });
    post_message(base, &header, &body, forwarded_for).await.1
}

/// Decode the Int32 column of a `DataChunk` produced by [`CountingExecutor`].
pub fn int32_values(chunk: &DataChunk) -> Vec<i32> {
    match &chunk.columns[0].data {
        VectorData::Fixed(bytes) => bytes
            .chunks_exact(4)
            .map(|b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect(),
        other => panic!("expected Fixed VectorData, got {other:?}"),
    }
}

/// A `SecurityConfig` that trusts loopback as a proxy, so `x-forwarded-for`
/// decides the client key and one test process can present as two clients.
pub fn trusts_loopback_proxy() -> SecurityConfig {
    SecurityConfig {
        trusted_proxies: vec!["127.0.0.1".to_string()],
        ..SecurityConfig::default()
    }
}
