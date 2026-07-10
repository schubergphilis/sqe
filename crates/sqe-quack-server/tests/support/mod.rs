//! Test-only stub `AuthProvider` and `QueryExecutor` implementations plus
//! helpers shared across the integration tests.

// Each integration test file compiles its own copy of this module via
// `mod support;`. Items not used by a given file would otherwise trigger
// dead-code warnings.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_core::Session;
use sqe_quack_server::{router, QuackServerState, QueryError, QueryExecutor, SessionStore};

// -----------------------------------------------------------------------------
// AuthProvider stubs
// -----------------------------------------------------------------------------

pub struct AcceptProvider {
    pub user_id: String,
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
            expires_at: None,
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

pub fn accept_provider() -> Arc<dyn AuthProvider> {
    Arc::new(AcceptProvider {
        user_id: "test-user".to_string(),
    })
}

// -----------------------------------------------------------------------------
// QueryExecutor stubs
// -----------------------------------------------------------------------------

/// Returns a fixed single-column INT32 batch named `x` with values [1, 2, 3].
pub struct FixedRowExecutor;

#[async_trait]
impl QueryExecutor for FixedRowExecutor {
    async fn execute(
        &self,
        _session: &Session,
        _sql: &str,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        let arr: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        Ok(vec![batch])
    }
}

/// Returns the given `QueryError` regardless of input. Used to exercise the
/// error-mapping branches of the prepare handler.
pub struct ErroringExecutor {
    pub error: fn() -> QueryError,
}

#[async_trait]
impl QueryExecutor for ErroringExecutor {
    async fn execute(
        &self,
        _session: &Session,
        _sql: &str,
    ) -> Result<Vec<RecordBatch>, QueryError> {
        Err((self.error)())
    }
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
    let state = QuackServerState::new(provider, executor);
    let sessions = state.sessions.clone();
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
    (format!("http://{addr}"), sessions)
}
