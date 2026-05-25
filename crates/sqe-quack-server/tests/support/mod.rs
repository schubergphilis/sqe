//! Test-only stub `AuthProvider` implementations and helpers shared across the
//! integration tests.

// Each integration test file compiles its own copy of this module via
// `mod support;`. Items not used by a given file would otherwise trigger
// dead-code warnings.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sqe_auth::{AuthError, AuthProvider, FlightCredentials, Identity};
use sqe_quack_server::{router, QuackServerState, SessionStore};

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

pub async fn spawn_server_with(provider: Arc<dyn AuthProvider>) -> String {
    let (base, _sessions) = spawn_server_with_sessions(provider).await;
    base
}

pub async fn spawn_server_with_sessions(provider: Arc<dyn AuthProvider>) -> (String, SessionStore) {
    let state = QuackServerState::with_provider(provider);
    let sessions = state.sessions.clone();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    (format!("http://{addr}"), sessions)
}

pub fn accept_provider() -> Arc<dyn AuthProvider> {
    Arc::new(AcceptProvider {
        user_id: "test-user".to_string(),
    })
}
