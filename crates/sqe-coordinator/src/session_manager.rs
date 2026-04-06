use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use sqe_auth::{AuthProvider, FlightCredentials, Identity};
use sqe_auth::Authenticator;
use sqe_core::Session;

/// Manages authenticated sessions for the coordinator.
///
/// Sessions are created during the Flight SQL handshake via the pluggable
/// `AuthProvider` chain and stored in a concurrent map keyed by session ID.
/// The session ID is returned to the client as a bearer token for subsequent
/// requests.
///
/// Supports two construction modes:
/// - `new(Arc<Authenticator>)` — legacy path, backwards compatible
/// - `with_provider(Arc<dyn AuthProvider>)` — new pluggable path
///
/// On each `get_session` call, the manager checks if the session's token
/// has expired and evicts it if so. Expired sessions are evicted automatically.
pub struct SessionManager {
    /// The pluggable auth provider (may be a single provider or an AuthChain).
    auth_provider: Arc<dyn AuthProvider>,
    /// Legacy authenticator kept for token cache lookups (background refresh).
    /// Will be `None` when constructed via `with_provider`.
    legacy_authenticator: Option<Arc<Authenticator>>,
    sessions: DashMap<String, Arc<Session>>,
}

impl SessionManager {
    /// Create a new `SessionManager` with the legacy `Authenticator`.
    ///
    /// The `Authenticator` is used both as an `AuthProvider` (via its trait impl)
    /// and for its token cache (background refresh).
    pub fn new(authenticator: Arc<Authenticator>) -> Self {
        Self {
            auth_provider: authenticator.clone() as Arc<dyn AuthProvider>,
            legacy_authenticator: Some(authenticator),
            sessions: DashMap::new(),
        }
    }

    /// Create a new `SessionManager` with a pluggable `AuthProvider`.
    ///
    /// Use this for the new auth chain architecture. Background token refresh
    /// via the legacy token cache is not available in this mode; providers
    /// should handle refresh via `refresh_catalog_token` instead.
    pub fn with_provider(provider: Arc<dyn AuthProvider>) -> Self {
        Self {
            auth_provider: provider,
            legacy_authenticator: None,
            sessions: DashMap::new(),
        }
    }

    /// Authenticate using `FlightCredentials` via the configured provider chain.
    ///
    /// Returns the session wrapped in an Arc. The session ID can be used
    /// as a bearer token for subsequent Flight SQL requests.
    pub async fn authenticate_credentials(
        &self,
        credentials: &FlightCredentials,
    ) -> sqe_core::Result<Arc<Session>> {
        let identity = self
            .auth_provider
            .authenticate(credentials)
            .await
            .map_err(|e| sqe_core::SqeError::Auth(e.to_string()))?;

        let session = self.identity_to_session(&identity);
        let session_id = session.id.clone();
        let session = Arc::new(session);
        self.sessions.insert(session_id.clone(), session.clone());

        info!(user_id = %identity.user_id, "Session created");
        debug!(session_id = %session_id, user_id = %identity.user_id, "Session details");

        Ok(session)
    }

    /// Authenticate a user via username/password (legacy convenience method).
    ///
    /// Wraps the credentials into `FlightCredentials` and delegates to
    /// `authenticate_credentials`. This preserves backward compatibility
    /// with the existing Flight SQL handshake flow.
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<Arc<Session>> {
        let credentials = FlightCredentials {
            username: Some(username.to_string()),
            password: Some(password.to_string()),
            ..Default::default()
        };
        self.authenticate_credentials(&credentials).await
    }

    /// Convert an `Identity` into a `Session`.
    fn identity_to_session(&self, identity: &Identity) -> Session {
        let token_expiry = Utc::now() + chrono::Duration::hours(1);
        Session::new(
            identity.user_id.clone(),
            identity.catalog_token.clone().unwrap_or_default(),
            identity.refresh_token.clone(),
            token_expiry,
            identity.roles.clone(),
        )
    }

    /// Look up a session by its ID (bearer token).
    ///
    /// If the legacy authenticator is present and the background refresh task
    /// has updated the token in the cache, the stored session is updated with
    /// the fresh token. If the token has expired and is no longer in the cache,
    /// the session is evicted.
    ///
    /// Each successful lookup also updates the session's `last_activity`
    /// timestamp so that the idle-timeout sweeper can detect stale sessions.
    pub fn get_session(&self, session_id: &str) -> Option<Arc<Session>> {
        let session = self.sessions.get(session_id)?.clone();

        // Check if the legacy background task refreshed this token
        if let Some(ref authenticator) = self.legacy_authenticator {
            if let Some(cached) = authenticator.get_cached_token(session_id) {
                if cached.access_token != session.access_token {
                    let mut updated = (*session).clone();
                    updated.access_token = cached.access_token;
                    updated.refresh_token = cached.refresh_token;
                    updated.token_expiry = cached.expiry;
                    updated.touch();
                    let updated = Arc::new(updated);
                    self.sessions.insert(session_id.to_string(), updated.clone());
                    debug!(session_id = %session_id, "Session updated with refreshed token");
                    return Some(updated);
                }
                // Token unchanged — just touch the session for idle tracking
                let mut touched = (*session).clone();
                touched.touch();
                let touched = Arc::new(touched);
                self.sessions.insert(session_id.to_string(), touched.clone());
                return Some(touched);
            }
        }

        // Token is no longer in cache (or no legacy authenticator) — check if expired
        if session.token_expiry <= Utc::now() {
            warn!(session_id = %session_id, "Session token expired, evicting");
            self.sessions.remove(session_id);
            return None;
        }

        // Touch the session for idle tracking
        let mut touched = (*session).clone();
        touched.touch();
        let touched = Arc::new(touched);
        self.sessions.insert(session_id.to_string(), touched.clone());
        Some(touched)
    }

    /// Save current sessions to a JSON file for crash recovery.
    ///
    /// Only key fields are serialized (id, username, access_token, expires_at).
    /// Failure is non-fatal: errors are logged and the caller receives an `Err`.
    pub fn snapshot_to_file(&self, path: &str) -> Result<(), String> {
        let sessions: Vec<_> = self
            .sessions
            .iter()
            .map(|entry| {
                let session = entry.value();
                serde_json::json!({
                    "id": session.id,
                    "username": session.user.username,
                    "access_token": session.access_token,
                    "expires_at": session.token_expiry.to_rfc3339(),
                })
            })
            .collect();

        let json = serde_json::to_string_pretty(&sessions)
            .map_err(|e| format!("Failed to serialize sessions: {e}"))?;
        std::fs::write(path, json)
            .map_err(|e| format!("Failed to write session snapshot: {e}"))?;
        tracing::debug!(path = path, count = sessions.len(), "Session snapshot saved");
        Ok(())
    }

    /// Restore sessions from a JSON snapshot file (best-effort).
    ///
    /// If the file does not exist or cannot be parsed the method returns
    /// silently — a missing snapshot is not an error condition.
    /// Full restore (re-creating live `Session` objects) requires re-validating
    /// tokens against the OIDC provider and is deferred to a future iteration.
    pub fn restore_from_file(&self, path: &str) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return, // No snapshot to restore
        };
        match serde_json::from_str::<serde_json::Value>(&content) {
            Ok(serde_json::Value::Array(entries)) => {
                tracing::info!(
                    path = path,
                    count = entries.len(),
                    "Found session snapshot — full restore requires token re-validation (not yet implemented)"
                );
            }
            Ok(_) => {
                tracing::warn!(path = path, "Session snapshot has unexpected format, skipping restore");
            }
            Err(e) => {
                tracing::warn!(path = path, error = %e, "Failed to parse session snapshot, skipping restore");
            }
        }
    }

    /// Remove a session from the manager.
    pub fn remove_session(&self, id: &str) {
        if self.sessions.remove(id).is_some() {
            debug!(session_id = %id, "Session removed");
        }
    }

    /// Sweep sessions that have exceeded the idle or absolute timeout.
    ///
    /// Returns the number of sessions removed. This is intended to be
    /// called periodically by a background task (e.g. the credential
    /// refresh loop).
    pub fn sweep_expired_sessions(
        &self,
        idle_timeout_secs: u64,
        absolute_timeout_secs: u64,
    ) -> usize {
        let mut removed = 0;
        // Collect IDs to remove (avoid holding DashMap shard locks during removal).
        let expired_ids: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| {
                let session = entry.value();
                session.is_idle(idle_timeout_secs)
                    || session.is_absolute_expired(absolute_timeout_secs)
            })
            .map(|entry| entry.key().clone())
            .collect();

        for id in expired_ids {
            if let Some((_, session)) = self.sessions.remove(&id) {
                let reason = if session.is_absolute_expired(absolute_timeout_secs) {
                    "absolute timeout"
                } else {
                    "idle timeout"
                };
                warn!(
                    session_id = %id,
                    username = %session.user.username,
                    reason = reason,
                    "Session expired, evicting"
                );
                removed += 1;
            }
        }

        if removed > 0 {
            info!(count = removed, "Swept expired sessions");
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};
    use sqe_core::config::AuthConfig;
    use sqe_core::Session;

    /// Build a minimal `AuthConfig` that constructs an HTTP client without any
    /// network access. The OIDC password grant path is selected because
    /// `keycloak_url` is non-empty and `token_endpoint` is empty.
    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            keycloak_url: "http://localhost:18080".to_string(),
            realm: "test".to_string(),
            client_id: "test-client".to_string(),
            client_secret: "secret".to_string(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: false,
            providers: Vec::new(),
            role_mappings: std::collections::HashMap::new(),
            external: None,
        }
    }

    /// Build an `Authenticator` synchronously. `Authenticator::new` only
    /// constructs an HTTP client — it makes no network calls.
    async fn make_authenticator() -> Arc<Authenticator> {
        Arc::new(
            Authenticator::new(&test_auth_config())
                .await
                .expect("Authenticator::new should not make network calls"),
        )
    }

    /// Build a fresh `Session` with a token that expires one hour from now.
    fn make_session(username: &str) -> Session {
        Session::new(
            username.to_string(),
            "access_tok".to_string(),
            Some("refresh_tok".to_string()),
            Utc::now() + Duration::hours(1),
            vec!["analyst".to_string()],
        )
    }

    /// Build a `Session` whose token has already expired (in the past).
    fn make_expired_token_session(username: &str) -> Session {
        Session::new(
            username.to_string(),
            "expired_tok".to_string(),
            None,
            Utc::now() - Duration::minutes(5),
            vec![],
        )
    }

    // -----------------------------------------------------------------------
    // Test: get_session returns None on an empty manager
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn get_session_returns_none_on_empty_manager() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        assert!(
            manager.get_session("nonexistent-id").is_none(),
            "Expected None for unknown session ID on empty manager"
        );
    }

    // -----------------------------------------------------------------------
    // Test: creating a session and retrieving it by ID
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn insert_and_retrieve_session() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        let session = Arc::new(make_session("alice"));
        let id = session.id.clone();

        // Insert directly via the private DashMap (allowed because this test
        // module is a child of the module that owns `SessionManager`).
        manager.sessions.insert(id.clone(), session.clone());

        // get_session should find the session. Note: get_session always calls
        // authenticator.get_cached_token first; since no token was inserted
        // into the authenticator's cache the `None` branch is taken, then the
        // token_expiry check passes (token is valid for 1 h), and the session
        // is returned with a refreshed last_activity timestamp.
        let retrieved = manager
            .get_session(&id)
            .expect("Session should be found by its ID");

        assert_eq!(retrieved.id, id, "Retrieved session ID must match");
        assert_eq!(retrieved.user.username, "alice");
    }

    // -----------------------------------------------------------------------
    // Test: unknown session ID returns None
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn get_session_returns_none_for_unknown_id() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Insert one real session so the map is not trivially empty
        let session = Arc::new(make_session("bob"));
        manager.sessions.insert(session.id.clone(), session);

        assert!(
            manager.get_session("completely-wrong-id").is_none(),
            "Should return None for an ID that was never inserted"
        );
    }

    // -----------------------------------------------------------------------
    // Test: get_session evicts sessions whose token has expired (no cache hit)
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn get_session_evicts_expired_token_session() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // A session whose token_expiry is in the past and is NOT in the
        // authenticator's token cache — the manager should evict it.
        let session = Arc::new(make_expired_token_session("charlie"));
        let id = session.id.clone();
        manager.sessions.insert(id.clone(), session);

        // get_session: no cache hit → token_expiry <= Utc::now() → evict → None
        let result = manager.get_session(&id);
        assert!(
            result.is_none(),
            "Expired token session with no cache entry should be evicted and return None"
        );

        // Confirm the session was actually removed from the map
        assert!(
            manager.sessions.get(&id).is_none(),
            "Evicted session must not remain in the sessions map"
        );
    }

    // -----------------------------------------------------------------------
    // Test: remove_session removes an existing session
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn remove_session_removes_existing_session() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        let session = Arc::new(make_session("dave"));
        let id = session.id.clone();
        manager.sessions.insert(id.clone(), session);

        // Confirm it exists before removal
        assert!(manager.sessions.get(&id).is_some());

        manager.remove_session(&id);

        assert!(
            manager.sessions.get(&id).is_none(),
            "Session should be absent after remove_session"
        );
    }

    // -----------------------------------------------------------------------
    // Test: remove_session is a no-op for non-existent ID
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn remove_session_noop_for_unknown_id() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Should not panic
        manager.remove_session("no-such-id");

        assert_eq!(
            manager.sessions.len(),
            0,
            "Manager should still be empty after removing unknown ID"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sweep_expired_sessions returns 0 for an empty manager
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn sweep_empty_manager_returns_zero() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        let removed = manager.sweep_expired_sessions(900, 28800);
        assert_eq!(removed, 0, "Empty manager sweep should remove nothing");
    }

    // -----------------------------------------------------------------------
    // Test: sweep_expired_sessions removes idle sessions
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn sweep_removes_idle_sessions() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Create a session and backdate its last_activity so it looks idle
        let mut session = make_session("eve");
        // 20 minutes ago — exceeds the 15-minute (900 s) idle timeout used below
        session.last_activity = Utc::now() - Duration::seconds(1200);
        let id = session.id.clone();
        manager.sessions.insert(id.clone(), Arc::new(session));

        let removed = manager.sweep_expired_sessions(900, 28800);

        assert_eq!(removed, 1, "One idle session should have been swept");
        assert!(
            manager.sessions.get(&id).is_none(),
            "Idle session must be absent after sweep"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sweep_expired_sessions removes absolutely-expired sessions
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn sweep_removes_absolutely_expired_sessions() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Create a session and backdate created_at beyond the absolute timeout
        let mut session = make_session("frank");
        // 9 hours ago — exceeds the 8-hour (28800 s) absolute timeout
        session.created_at = Utc::now() - Duration::hours(9);
        // Keep last_activity recent so idle timeout would NOT trigger
        session.last_activity = Utc::now();
        let id = session.id.clone();
        manager.sessions.insert(id.clone(), Arc::new(session));

        let removed = manager.sweep_expired_sessions(900, 28800);

        assert_eq!(removed, 1, "One absolutely-expired session should have been swept");
        assert!(
            manager.sessions.get(&id).is_none(),
            "Absolutely-expired session must be absent after sweep"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sweep_expired_sessions does not remove active sessions
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn sweep_retains_active_sessions() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Active session — both last_activity and created_at are recent
        let session = Arc::new(make_session("grace"));
        let id = session.id.clone();
        manager.sessions.insert(id.clone(), session);

        // Very short timeouts to ensure the session is NOT triggered by edge
        // cases — 15 min idle, 8 h absolute, session is fresh
        let removed = manager.sweep_expired_sessions(900, 28800);

        assert_eq!(removed, 0, "Active session must not be swept");
        assert!(
            manager.sessions.get(&id).is_some(),
            "Active session must still be present after sweep"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sweep handles a mix of expired and active sessions correctly
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn sweep_mixed_sessions_removes_only_expired() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Active session
        let active = Arc::new(make_session("henry"));
        let active_id = active.id.clone();
        manager.sessions.insert(active_id.clone(), active);

        // Idle session (last_activity 20 minutes ago)
        let mut idle_session = make_session("irene");
        idle_session.last_activity = Utc::now() - Duration::seconds(1200);
        let idle_id = idle_session.id.clone();
        manager.sessions.insert(idle_id.clone(), Arc::new(idle_session));

        // Absolutely-expired session (created 9 hours ago)
        let mut abs_session = make_session("jake");
        abs_session.created_at = Utc::now() - Duration::hours(9);
        abs_session.last_activity = Utc::now();
        let abs_id = abs_session.id.clone();
        manager.sessions.insert(abs_id.clone(), Arc::new(abs_session));

        let removed = manager.sweep_expired_sessions(900, 28800);

        assert_eq!(removed, 2, "Exactly two expired sessions should have been swept");
        assert!(manager.sessions.get(&active_id).is_some(), "Active session must survive sweep");
        assert!(manager.sessions.get(&idle_id).is_none(), "Idle session must be swept");
        assert!(manager.sessions.get(&abs_id).is_none(), "Absolutely-expired session must be swept");
    }

    // -----------------------------------------------------------------------
    // Test: snapshot_to_file writes a JSON file; restore_from_file reads it back
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_session_snapshot_and_restore() {
        let auth = make_authenticator().await;
        let manager = SessionManager::new(auth);

        // Insert a session so the snapshot is non-empty
        let session = Arc::new(make_session("snapshot-user"));
        let session_id = session.id.clone();
        manager.sessions.insert(session_id.clone(), session);

        // Snapshot to a temporary file
        let tmp_path = format!("/tmp/sqe-test-snapshot-{}.json", uuid_simple());
        manager
            .snapshot_to_file(&tmp_path)
            .expect("snapshot_to_file should succeed");

        // File must exist and contain valid JSON
        let content = std::fs::read_to_string(&tmp_path)
            .expect("Snapshot file must exist after snapshot_to_file");
        let parsed: serde_json::Value =
            serde_json::from_str(&content).expect("Snapshot file must contain valid JSON");
        let entries = parsed.as_array().expect("Snapshot JSON must be an array");
        assert_eq!(entries.len(), 1, "Snapshot must contain exactly one entry");
        assert_eq!(
            entries[0]["username"], "snapshot-user",
            "Snapshot entry must include the correct username"
        );

        // restore_from_file is best-effort / logs only — it must not panic
        manager.restore_from_file(&tmp_path);

        // Clean up
        let _ = std::fs::remove_file(&tmp_path);
    }

    /// Produce a short random hex string suitable for use in temp file names.
    fn uuid_simple() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        format!("{nanos:08x}")
    }

    // -----------------------------------------------------------------------
    // Test: concurrent access — multiple threads insert and sweep safely
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn concurrent_insert_and_sweep() {
        use std::sync::Arc as StdArc;

        let auth = make_authenticator().await;
        let manager = StdArc::new(SessionManager::new(auth));

        // Spawn several tasks that each insert a session concurrently.
        let mut handles = Vec::new();
        for i in 0..10u32 {
            let mgr = StdArc::clone(&manager);
            handles.push(tokio::spawn(async move {
                let session = Arc::new(make_session(&format!("user_{i}")));
                mgr.sessions.insert(session.id.clone(), session);
            }));
        }

        for handle in handles {
            handle.await.expect("Concurrent insert task panicked");
        }

        assert_eq!(
            manager.sessions.len(),
            10,
            "All 10 sessions should be present after concurrent inserts"
        );

        // Sweep with large timeouts well within chrono::Duration bounds — nothing should be removed.
        // chrono::Duration stores nanoseconds as i64, so max seconds ≈ i64::MAX / 1e9 ≈ 9.2e9.
        // 100 years in seconds is ~3.15e9, safely within bounds.
        let large_timeout: u64 = 86400 * 365 * 100;
        let removed = manager.sweep_expired_sessions(large_timeout, large_timeout);
        assert_eq!(removed, 0, "No active sessions should be swept");
        assert_eq!(manager.sessions.len(), 10, "All sessions should survive the sweep");
    }
}
