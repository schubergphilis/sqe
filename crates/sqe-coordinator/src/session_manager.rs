use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tracing::{debug, info, warn};

use sqe_auth::Authenticator;
use sqe_core::Session;

/// Manages authenticated sessions for the coordinator.
///
/// Sessions are created during the Flight SQL handshake via Keycloak ROPC
/// authentication and stored in a concurrent map keyed by session ID. The
/// session ID is returned to the client as a bearer token for subsequent
/// requests.
///
/// On each `get_session` call, the manager checks the `TokenCache` for
/// tokens that were refreshed by the background task and updates the
/// stored session accordingly. Expired sessions are evicted automatically.
pub struct SessionManager {
    authenticator: Arc<Authenticator>,
    sessions: DashMap<String, Arc<Session>>,
}

impl SessionManager {
    pub fn new(authenticator: Arc<Authenticator>) -> Self {
        Self {
            authenticator,
            sessions: DashMap::new(),
        }
    }

    /// Authenticate a user via Keycloak, create a session, and store it.
    ///
    /// Returns the session wrapped in an Arc. The session ID can be used
    /// as a bearer token for subsequent Flight SQL requests.
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<Arc<Session>> {
        let session = self.authenticator.authenticate(username, password).await?;
        let session_id = session.id.clone();
        let session = Arc::new(session);
        self.sessions.insert(session_id.clone(), session.clone());

        info!(
            session_id = %session_id,
            username = username,
            "Session created"
        );

        Ok(session)
    }

    /// Look up a session by its ID (bearer token).
    ///
    /// If the background refresh task has updated the token in the cache,
    /// the stored session is updated with the fresh token. If the token
    /// has expired and is no longer in the cache, the session is evicted.
    pub fn get_session(&self, session_id: &str) -> Option<Arc<Session>> {
        let session = self.sessions.get(session_id)?.clone();

        // Check if the background task refreshed this token
        if let Some(cached) = self.authenticator.get_cached_token(session_id) {
            if cached.access_token != session.access_token {
                let mut updated = (*session).clone();
                updated.access_token = cached.access_token;
                updated.refresh_token = cached.refresh_token;
                updated.token_expiry = cached.expiry;
                let updated = Arc::new(updated);
                self.sessions.insert(session_id.to_string(), updated.clone());
                debug!(session_id = %session_id, "Session updated with refreshed token");
                return Some(updated);
            }
            return Some(session);
        }

        // Token is no longer in cache — check if it's expired
        if session.token_expiry <= Utc::now() {
            warn!(session_id = %session_id, "Session token expired, evicting");
            self.sessions.remove(session_id);
            return None;
        }

        Some(session)
    }

    /// Remove a session from the manager.
    pub fn remove_session(&self, id: &str) {
        if self.sessions.remove(id).is_some() {
            debug!(session_id = %id, "Session removed");
        }
    }
}
