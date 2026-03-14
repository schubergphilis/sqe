use std::sync::Arc;

use dashmap::DashMap;
use tracing::{debug, info};

use sqe_auth::Authenticator;
use sqe_core::Session;

/// Manages authenticated sessions for the coordinator.
///
/// Sessions are created during the Flight SQL handshake via Keycloak ROPC
/// authentication and stored in a concurrent map keyed by session ID. The
/// session ID is returned to the client as a bearer token for subsequent
/// requests.
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
    pub fn get_session(&self, session_id: &str) -> Option<Arc<Session>> {
        self.sessions.get(session_id).map(|entry| entry.clone())
    }

    /// Remove a session from the manager.
    pub fn remove_session(&self, id: &str) {
        if self.sessions.remove(id).is_some() {
            debug!(session_id = %id, "Session removed");
        }
    }
}
