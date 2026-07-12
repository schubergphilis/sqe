//! Shared types and in-memory store for interactive auth sessions.

use std::time::{Duration, Instant};

use moka::sync::Cache;

/// Token set returned by successful OIDC flows (device code, auth code).
#[derive(Debug, Clone)]
pub struct TokenSet {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
}

/// State of a pending interactive authentication session.
#[derive(Debug, Clone)]
pub enum PendingAuth {
    AwaitingCallback {
        code_verifier: String,
        state: String,
        created_at: Instant,
    },
    Complete(TokenSet),
    Failed(String),
}

/// In-memory store for interactive auth sessions with TTL-based expiry.
pub struct PendingAuthStore {
    store: Cache<String, PendingAuth>,
}

impl PendingAuthStore {
    pub fn new(challenge_timeout: Duration) -> Self {
        Self {
            store: Cache::builder()
                .time_to_live(challenge_timeout)
                .max_capacity(10_000)
                .build(),
        }
    }

    pub fn insert_pending(&self, auth_id: &str, code_verifier: String, state: String) {
        self.store.insert(
            auth_id.to_string(),
            PendingAuth::AwaitingCallback {
                code_verifier,
                state,
                created_at: Instant::now(),
            },
        );
    }

    pub fn complete(&self, auth_id: &str, tokens: TokenSet) {
        self.store
            .insert(auth_id.to_string(), PendingAuth::Complete(tokens));
    }

    pub fn fail(&self, auth_id: &str, error: String) {
        self.store
            .insert(auth_id.to_string(), PendingAuth::Failed(error));
    }

    pub fn poll(&self, auth_id: &str) -> Option<PendingAuth> {
        self.store.get(auth_id)
    }

    pub fn remove(&self, auth_id: &str) {
        self.store.invalidate(auth_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_poll_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "verifier".to_string(), "state-abc".to_string());
        let result = store.poll("auth-1");
        assert!(result.is_some());
        match result.unwrap() {
            PendingAuth::AwaitingCallback {
                code_verifier,
                state,
                ..
            } => {
                assert_eq!(code_verifier, "verifier");
                assert_eq!(state, "state-abc");
            }
            other => panic!("expected AwaitingCallback, got: {other:?}"),
        }
    }

    #[test]
    fn complete_overwrites_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.complete(
            "auth-1",
            TokenSet {
                access_token: "at".to_string(),
                id_token: Some("idt".to_string()),
                refresh_token: None,
                expires_in: 3600,
            },
        );
        match store.poll("auth-1").unwrap() {
            PendingAuth::Complete(ts) => {
                assert_eq!(ts.access_token, "at");
                assert_eq!(ts.id_token.as_deref(), Some("idt"));
            }
            other => panic!("expected Complete, got: {other:?}"),
        }
    }

    #[test]
    fn fail_overwrites_pending() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.fail("auth-1", "user denied".to_string());
        match store.poll("auth-1").unwrap() {
            PendingAuth::Failed(msg) => assert_eq!(msg, "user denied"),
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    #[test]
    fn remove_deletes_session() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string());
        store.remove("auth-1");
        assert!(store.poll("auth-1").is_none());
    }

    #[test]
    fn poll_missing_returns_none() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        assert!(store.poll("nonexistent").is_none());
    }
}
