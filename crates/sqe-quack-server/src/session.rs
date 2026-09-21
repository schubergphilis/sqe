//! Per-connection session state keyed by `connection_id`.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use moka::sync::Cache;
use sqe_auth::Identity;
use sqe_core::SecretString;
use sqe_core::Session as CoreSession;

/// Default idle window: a connection that goes this long without a request is
/// dropped.
pub const DEFAULT_SESSION_IDLE: Duration = Duration::from_secs(600);

/// Default absolute ceiling on a connection's lifetime, independent of
/// activity (finding 606). Without it a client polling once every nine minutes
/// keeps a session alive forever, long after the OIDC token it was minted from
/// has expired.
pub const DEFAULT_SESSION_MAX_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);

#[derive(Debug, Clone)]
pub struct Session {
    pub connection_id: String,
    /// The user's live OIDC bearer token. Wrapped in `SecretString` so a
    /// `debug!(?session)` or panic redacts it to `<set>` instead of leaking the
    /// raw credential into logs, and the material is zeroized on drop.
    pub bearer_token: SecretString,
    pub identity: Identity,
    /// `sqe_core::Session` built from the `Identity` at connect time. Held so
    /// `PrepareRequest` can hand it directly to the `QueryExecutor` without
    /// rebuilding per query.
    pub core_session: CoreSession,
    /// Resolved client IP key (`client_key()` in `app.rs`) of the request that
    /// created this session. The `connection_id` travels in every later request
    /// body, so on a plaintext channel it is a replayable bearer credential;
    /// binding it to the client key means a captured id is only usable from the
    /// same source (finding 606).
    pub client_key: String,
}

/// Why [`SessionStore::get_authorized`] refused a request.
///
/// The variants exist so the *server log* can say which check failed. The wire
/// must not: `Unknown` and `ClientMismatch` deliberately map to the same
/// client-visible message, so a caller replaying a stolen `connection_id` from
/// the wrong source cannot tell "wrong IP" from "no such session" and use the
/// endpoint as a session-id oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRejection {
    /// No such `connection_id` (never existed, or already evicted/disconnected).
    Unknown,
    /// The session exists but the token it was minted from has expired.
    Expired,
    /// The session exists but the request came from a different client key.
    ClientMismatch,
}

#[derive(Clone)]
pub struct SessionStore {
    inner: Arc<Cache<String, Session>>,
}

impl SessionStore {
    /// `idle_timeout` bounds inactivity; `max_lifetime` bounds total age
    /// regardless of activity. Both are enforced by moka; the per-request
    /// token-expiry check in [`Self::get_authorized`] is the third, tighter
    /// bound (it follows the IdP's own `exp`).
    pub fn new(idle_timeout: Duration, max_lifetime: Duration) -> Self {
        Self {
            inner: Arc::new(
                Cache::builder()
                    .time_to_idle(idle_timeout)
                    .time_to_live(max_lifetime)
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

    /// Resolve a session for a post-connect request, enforcing both the token
    /// expiry and the client binding.
    ///
    /// An expired session is invalidated on the way out so a subsequent request
    /// takes the `Unknown` path rather than re-testing a dead token.
    pub fn get_authorized(
        &self,
        connection_id: &str,
        client_key: &str,
    ) -> Result<Session, SessionRejection> {
        let session = self
            .inner
            .get(connection_id)
            .ok_or(SessionRejection::Unknown)?;

        // Token expiry beats every cache TTL: the OIDC token is the actual
        // authority here, and moka's idle/live windows are only a backstop.
        if session.core_session.token_expiry() <= Utc::now() {
            self.inner.invalidate(connection_id);
            return Err(SessionRejection::Expired);
        }

        if session.client_key != client_key {
            tracing::warn!(
                connection_id = %connection_id,
                expected_client = %session.client_key,
                actual_client = %client_key,
                "Quack connection_id presented from a different client key; refusing"
            );
            return Err(SessionRejection::ClientMismatch);
        }

        Ok(session)
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

impl Default for SessionStore {
    fn default() -> Self {
        Self::new(DEFAULT_SESSION_IDLE, DEFAULT_SESSION_MAX_LIFETIME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn test_identity() -> Identity {
        Identity {
            user_id: "alice".to_string(),
            display_name: "alice".to_string(),
            roles: vec!["test-role".to_string()],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
            scopes: vec![],
        }
    }

    fn session_with(connection_id: &str, client_key: &str, identity: Identity) -> Session {
        let core_session = identity.to_session(None);
        Session {
            connection_id: connection_id.to_string(),
            bearer_token: SecretString::new("tok".to_string()),
            identity,
            core_session,
            client_key: client_key.to_string(),
        }
    }

    /// Regression guard for issue #197: a `Session` carries the live OIDC bearer
    /// token, so its `Debug` output must never expose the raw credential. The
    /// token lives in a `SecretString`, which renders as `<set>`.
    #[test]
    fn debug_does_not_leak_bearer_token() {
        let raw_token = "ey-super-secret-bearer-token-do-not-log";
        let identity = test_identity();
        let session = Session {
            connection_id: "conn-1".to_string(),
            bearer_token: SecretString::new(raw_token.to_string()),
            core_session: identity.to_session(None),
            identity,
            client_key: "127.0.0.1".to_string(),
        };

        let rendered = format!("{session:?}");
        assert!(
            !rendered.contains(raw_token),
            "Debug output leaked the bearer token: {rendered}"
        );
        assert!(
            rendered.contains("<set>"),
            "expected redacted SecretString sentinel `<set>` in Debug output: {rendered}"
        );
    }

    #[test]
    fn unknown_connection_id_is_rejected() {
        let store = SessionStore::default();
        assert_eq!(
            store.get_authorized("nope", "127.0.0.1").unwrap_err(),
            SessionRejection::Unknown
        );
    }

    /// 606: the token's own `exp` is authoritative, so a session whose token
    /// has passed expiry is refused even though moka's idle/live windows are
    /// nowhere near elapsed — and it is invalidated, so the next call sees
    /// `Unknown` rather than paying the expiry check again.
    #[test]
    fn expired_token_is_rejected_and_invalidated() {
        let store = SessionStore::default();
        let mut identity = test_identity();
        identity.expires_at = Some(Utc::now() - ChronoDuration::seconds(1));
        store.insert(session_with("c1", "127.0.0.1", identity));
        store.run_pending_tasks();

        assert_eq!(
            store.get_authorized("c1", "127.0.0.1").unwrap_err(),
            SessionRejection::Expired
        );
        store.run_pending_tasks();
        assert_eq!(
            store.get_authorized("c1", "127.0.0.1").unwrap_err(),
            SessionRejection::Unknown,
            "an expired session must be dropped, not merely refused"
        );
    }

    /// 606: the connection_id alone is not an authorisation. A different client
    /// key presenting a live id is refused.
    #[test]
    fn client_key_mismatch_is_rejected() {
        let store = SessionStore::default();
        let mut identity = test_identity();
        identity.expires_at = Some(Utc::now() + ChronoDuration::hours(1));
        store.insert(session_with("c1", "203.0.113.5", identity));
        store.run_pending_tasks();

        assert_eq!(
            store.get_authorized("c1", "198.51.100.9").unwrap_err(),
            SessionRejection::ClientMismatch
        );
        // The legitimate client is unaffected: a mismatch must not invalidate.
        assert!(store.get_authorized("c1", "203.0.113.5").is_ok());
    }
}
