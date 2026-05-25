//! Per-connection session state keyed by `connection_id`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use moka::sync::Cache;
use sqe_auth::Identity;
use sqe_core::Session as CoreSession;

#[derive(Debug, Clone)]
pub struct Session {
    pub connection_id: String,
    pub bearer_token: String,
    pub identity: Identity,
    /// `sqe_core::Session` built from the `Identity` at connect time. Held so
    /// `PrepareRequest` can hand it directly to the `QueryExecutor` without
    /// rebuilding per query.
    pub core_session: CoreSession,
}

/// Mirror of `sqe-coordinator`'s `SessionManager::identity_to_session`. Lives
/// here so `sqe-quack-server` does not have to depend on `sqe-coordinator`
/// just for this conversion.
pub fn identity_to_core_session(identity: &Identity) -> CoreSession {
    let token_expiry = identity
        .expires_at
        .unwrap_or_else(|| Utc::now() + chrono::Duration::hours(1));
    CoreSession::new(
        identity.user_id.clone(),
        identity.catalog_token.clone().unwrap_or_default(),
        identity.refresh_token.clone(),
        token_expiry,
        identity.roles.clone(),
    )
}

#[derive(Clone)]
pub struct SessionStore {
    inner: Arc<Cache<String, Session>>,
}

impl SessionStore {
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(
                Cache::builder()
                    .time_to_idle(idle_timeout)
                    .max_capacity(10_000)
                    .build(),
            ),
        }
    }

    pub fn insert(&self, session: Session) {
        self.inner.insert(session.connection_id.clone(), session);
    }

    pub fn get(&self, connection_id: &str) -> Option<Session> {
        self.inner.get(connection_id)
    }

    pub fn remove(&self, connection_id: &str) {
        self.inner.invalidate(connection_id);
    }

    pub fn len(&self) -> u64 {
        self.inner.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Force moka to flush deferred operations so `len`/`get` reflect recent
    /// `insert`/`invalidate` calls. Useful in tests; harmless in production.
    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
    }
}
