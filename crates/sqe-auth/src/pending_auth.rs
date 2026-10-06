//! Shared types and in-memory store for interactive auth sessions.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use moka::sync::Cache;

/// Token set returned by successful OIDC flows (device code, auth code).
#[derive(Clone)]
pub struct TokenSet {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
}

impl std::fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenSet")
            .field("access_token", &"<redacted>")
            .field("has_id_token", &self.id_token.is_some())
            .field("has_refresh_token", &self.refresh_token.is_some())
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

/// State of a pending interactive authentication session.
#[derive(Clone)]
pub enum PendingAuth {
    AwaitingCallback {
        code_verifier: String,
        state: String,
        /// IdP authorization URL the browser is redirected to on `initiate`.
        authorization_url: String,
        /// Address of the client that requested the challenge, shown on the
        /// interstitial page so the browser user can spot a session minted
        /// by someone else.
        requester_ip: String,
        created_at: Instant,
    },
    /// Callback claimed; token exchange is in flight. Polling remains pending.
    Exchanging,
    /// Terminal tombstone on shared entries, so concurrent pollers cannot read twice.
    Consumed,
    Complete(TokenSet),
    Failed(String),
}

impl std::fmt::Debug for PendingAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AwaitingCallback { created_at, .. } => f
                .debug_struct("AwaitingCallback")
                .field("code_verifier", &"<redacted>")
                .field("state", &"<redacted>")
                .field("authorization_url", &"<redacted>")
                .field("created_at", created_at)
                .finish(),
            Self::Complete(tokens) => f.debug_tuple("Complete").field(tokens).finish(),
            Self::Failed(_) => f.write_str("Failed(<redacted>)"),
            Self::Exchanging => f.write_str("Exchanging"),
            Self::Consumed => f.write_str("Consumed"),
        }
    }
}

/// In-memory store for interactive auth sessions with TTL-based expiry.
pub struct PendingAuthStore {
    store: Cache<String, Arc<Mutex<PendingAuth>>>,
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

    pub fn insert_pending(
        &self,
        auth_id: &str,
        code_verifier: String,
        state: String,
        authorization_url: String,
        requester_ip: String,
    ) {
        self.store.insert(
            auth_id.to_string(),
            Arc::new(Mutex::new(PendingAuth::AwaitingCallback {
                code_verifier,
                state,
                authorization_url,
                requester_ip,
                created_at: Instant::now(),
            })),
        );
    }

    /// Update an existing live flow only. Mutating the shared entry preserves
    /// its original expiry; a late exchange cannot recreate a deleted/expired key.
    pub fn complete(&self, auth_id: &str, tokens: TokenSet) -> bool {
        self.finish(auth_id, PendingAuth::Complete(tokens))
    }

    pub fn fail(&self, auth_id: &str, error: String) -> bool {
        self.finish(auth_id, PendingAuth::Failed(error))
    }

    fn finish(&self, auth_id: &str, result: PendingAuth) -> bool {
        let Some(entry) = self.store.get(auth_id) else {
            return false;
        };
        let Ok(mut state) = entry.lock() else {
            return false;
        };
        if !matches!(
            *state,
            PendingAuth::AwaitingCallback { .. } | PendingAuth::Exchanging
        ) {
            return false;
        }
        *state = result;
        true
    }

    /// Atomically verify/claim the callback before contacting the IdP. A second
    /// callback cannot obtain the verifier or overwrite the first exchange.
    pub fn take_callback_verifier(&self, returned_state: &str) -> Result<String, &'static str> {
        let entry = self
            .store
            .get(returned_state)
            .ok_or("Invalid or expired state parameter")?;
        let mut state = entry.lock().map_err(|_| "Auth session unavailable")?;
        match &*state {
            PendingAuth::AwaitingCallback { state: stored, .. } => {
                let stored = sqe_core::SecretString::new(stored.clone());
                let returned = sqe_core::SecretString::new(returned_state.to_string());
                if !stored.ct_eq(&returned) {
                    return Err("State parameter does not match the stored value");
                }
            }
            _ => return Err("Invalid or expired state parameter"),
        }
        let PendingAuth::AwaitingCallback { code_verifier, .. } =
            std::mem::replace(&mut *state, PendingAuth::Exchanging)
        else {
            unreachable!()
        };
        Ok(code_verifier)
    }

    /// Non-consuming inspection for redirect and pending-flow state.
    pub fn poll(&self, auth_id: &str) -> Option<PendingAuth> {
        let entry = self.store.get(auth_id)?;
        let state = entry.lock().ok()?;
        (!matches!(*state, PendingAuth::Consumed)).then(|| state.clone())
    }

    /// Completed tokens have one winning reader. Every observer of the shared
    /// entry sees the tombstone, even if it fetched the Arc before invalidation.
    /// Pending and failed states remain repeatably pollable.
    pub fn poll_and_consume(&self, auth_id: &str) -> Option<PendingAuth> {
        let entry = self.store.get(auth_id)?;
        let mut state = entry.lock().ok()?;
        match &*state {
            PendingAuth::Complete(_) => {
                let result = std::mem::replace(&mut *state, PendingAuth::Consumed);
                self.store.invalidate(auth_id);
                Some(result)
            }
            PendingAuth::Consumed => None,
            _ => Some(state.clone()),
        }
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
        store.insert_pending(
            "auth-1",
            "verifier".to_string(),
            "state-abc".to_string(),
            String::new(),
            String::new(),
        );
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
        store.insert_pending("auth-1", "v".to_string(), "s".to_string(), String::new(), String::new());
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
        store.insert_pending("auth-1", "v".to_string(), "s".to_string(), String::new(), String::new());
        store.fail("auth-1", "user denied".to_string());
        match store.poll("auth-1").unwrap() {
            PendingAuth::Failed(msg) => assert_eq!(msg, "user denied"),
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    #[test]
    fn remove_deletes_session() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("auth-1", "v".to_string(), "s".to_string(), String::new(), String::new());
        store.remove("auth-1");
        assert!(store.poll("auth-1").is_none());
    }

    #[test]
    fn poll_missing_returns_none() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        assert!(store.poll("nonexistent").is_none());
    }
    fn tokens() -> TokenSet {
        TokenSet {
            access_token: "access-canary".into(),
            id_token: Some("id-canary".into()),
            refresh_token: Some("refresh-canary".into()),
            expires_in: 3600,
        }
    }

    #[test]
    fn concurrent_pollers_have_one_token_winner() {
        let store = Arc::new(PendingAuthStore::new(Duration::from_secs(60)));
        store.insert_pending("auth", "verifier".into(), "auth".into(), String::new(), String::new());
        assert!(store.complete("auth", tokens()));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let winners = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..16)
                .map(|_| {
                    let store = store.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        usize::from(matches!(
                            store.poll_and_consume("auth"),
                            Some(PendingAuth::Complete(_))
                        ))
                    })
                })
                .collect();
            tasks.into_iter().map(|t| t.join().unwrap()).sum::<usize>()
        });
        assert_eq!(winners, 1);
        assert!(store.poll_and_consume("auth").is_none());
    }

    #[test]
    fn pending_and_exchanging_polling_are_repeatable_and_callback_is_claimed_once() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("state", "verifier".into(), "state".into(), String::new(), String::new());
        for _ in 0..3 {
            assert!(matches!(
                store.poll_and_consume("state"),
                Some(PendingAuth::AwaitingCallback { .. })
            ));
        }
        assert_eq!(store.take_callback_verifier("state"), Ok("verifier".into()));
        assert!(store.take_callback_verifier("state").is_err());
        for _ in 0..3 {
            assert!(matches!(
                store.poll_and_consume("state"),
                Some(PendingAuth::Exchanging)
            ));
        }
        assert!(store.complete("state", tokens()));
        assert!(!store.fail("state", "late callback failure".into()));
        assert!(matches!(
            store.poll_and_consume("state"),
            Some(PendingAuth::Complete(_))
        ));
        assert!(!store.complete("state", tokens()));
    }

    #[test]
    fn late_completion_cannot_resurrect_removed_flow() {
        let store = PendingAuthStore::new(Duration::from_secs(60));
        store.insert_pending("state", "verifier".into(), "state".into(), String::new(), String::new());
        store.take_callback_verifier("state").unwrap();
        store.remove("state");
        assert!(!store.complete("state", tokens()));
        assert!(!store.fail("state", "late failure".into()));
        assert!(store.poll("state").is_none());
        assert!(!store.complete("never-issued", tokens()));
    }

    #[test]
    fn completion_preserves_original_flow_deadline() {
        let store = PendingAuthStore::new(Duration::from_millis(1000));
        store.insert_pending("state", "verifier".into(), "state".into(), String::new(), String::new());
        store.take_callback_verifier("state").unwrap();
        std::thread::sleep(Duration::from_millis(600));
        assert!(store.complete("state", tokens()));
        std::thread::sleep(Duration::from_millis(600));
        assert!(
            store.poll("state").is_none(),
            "completion must not restart the TTL"
        );
        assert!(!store.complete("state", tokens()));
    }

    #[test]
    fn concurrent_callbacks_claim_one_verifier() {
        let store = Arc::new(PendingAuthStore::new(Duration::from_secs(60)));
        store.insert_pending("state", "verifier".into(), "state".into(), String::new(), String::new());
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let winners = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    let store = store.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        usize::from(store.take_callback_verifier("state").is_ok())
                    })
                })
                .collect();
            tasks.into_iter().map(|t| t.join().unwrap()).sum::<usize>()
        });
        assert_eq!(winners, 1);
    }
    #[test]
    fn interactive_token_and_pending_debug_do_not_disclose_secrets() {
        let debug = format!("{:?}", PendingAuth::Complete(tokens()));
        for secret in ["access-canary", "id-canary", "refresh-canary"] {
            assert!(!debug.contains(secret));
        }
        let state = PendingAuth::AwaitingCallback {
            code_verifier: "verifier-canary".into(),
            state: "state-canary".into(),
            authorization_url: "url-canary".into(),
            requester_ip: String::new(),
            created_at: Instant::now(),
        };
        let debug = format!("{state:?}");
        for secret in ["verifier-canary", "state-canary", "url-canary"] {
            assert!(!debug.contains(secret));
        }
    }
}
