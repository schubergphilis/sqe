use std::sync::Arc;

use chrono::{Duration, Utc};
use tracing::{debug, error, info};

use sqe_core::config::AuthConfig;
use sqe_core::Session;

use crate::keycloak::KeycloakClient;
use crate::token_cache::{CachedToken, TokenCache};

pub struct Authenticator {
    keycloak: KeycloakClient,
    cache: TokenCache,
    refresh_buffer_secs: u64,
}

impl Authenticator {
    pub async fn new(config: &AuthConfig) -> sqe_core::Result<Self> {
        let keycloak = KeycloakClient::new(config)?;
        let cache = TokenCache::new();
        let refresh_buffer_secs = config.token_refresh_buffer_secs;

        info!(
            refresh_buffer_secs = refresh_buffer_secs,
            "Authenticator initialized"
        );

        Ok(Self {
            keycloak,
            cache,
            refresh_buffer_secs,
        })
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<Session> {
        let token_response = self.keycloak.exchange_credentials(username, password).await?;
        let roles = self.keycloak.extract_roles(&token_response.access_token);

        let token_expiry = Utc::now() + Duration::seconds(token_response.expires_in as i64);

        let session = Session::new(
            username.to_string(),
            token_response.access_token.clone(),
            token_response.refresh_token.clone(),
            token_expiry,
            roles,
        );

        self.cache.insert(
            &session.id,
            CachedToken {
                access_token: token_response.access_token,
                refresh_token: token_response.refresh_token,
                expiry: token_expiry,
            },
        );

        debug!(
            session_id = session.id,
            username = username,
            "Session created and cached"
        );

        Ok(session)
    }

    /// Look up the latest cached token for a session.
    ///
    /// Used by `SessionManager` to pick up tokens refreshed by the background task.
    pub fn get_cached_token(&self, session_id: &str) -> Option<CachedToken> {
        self.cache.get(session_id)
    }

    pub async fn refresh_session(&self, session: &mut Session) -> sqe_core::Result<()> {
        let refresh_token = session
            .refresh_token
            .as_deref()
            .ok_or_else(|| sqe_core::SqeError::Auth("No refresh token available".to_string()))?;

        let token_response = self.keycloak.refresh_token(refresh_token).await?;
        let token_expiry = Utc::now() + Duration::seconds(token_response.expires_in as i64);

        session.access_token = token_response.access_token.clone();
        session.refresh_token = token_response.refresh_token.clone();
        session.token_expiry = token_expiry;

        self.cache.insert(
            &session.id,
            CachedToken {
                access_token: token_response.access_token,
                refresh_token: token_response.refresh_token,
                expiry: token_expiry,
            },
        );

        debug!(session_id = session.id, "Session token refreshed");

        Ok(())
    }

    /// Spawns a background task that periodically checks the cache for expiring
    /// sessions and refreshes them. Errors are logged but do not crash the task.
    pub fn start_refresh_task(self: &Arc<Self>) {
        let this = Arc::clone(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                interval.tick().await;

                let expiring = this.cache.expiring_sessions(this.refresh_buffer_secs);
                if expiring.is_empty() {
                    continue;
                }

                debug!(
                    count = expiring.len(),
                    "Found expiring sessions, attempting refresh"
                );

                for session_id in expiring {
                    let cached = match this.cache.get(&session_id) {
                        Some(c) => c,
                        None => continue,
                    };

                    let refresh_token = match cached.refresh_token.as_deref() {
                        Some(rt) => rt,
                        None => {
                            debug!(
                                session_id = session_id,
                                "No refresh token, removing from cache"
                            );
                            this.cache.remove(&session_id);
                            continue;
                        }
                    };

                    match this.keycloak.refresh_token(refresh_token).await {
                        Ok(token_response) => {
                            let expiry = Utc::now()
                                + Duration::seconds(token_response.expires_in as i64);
                            this.cache.insert(
                                &session_id,
                                CachedToken {
                                    access_token: token_response.access_token,
                                    refresh_token: token_response.refresh_token,
                                    expiry,
                                },
                            );
                            debug!(session_id = session_id, "Background token refresh succeeded");
                        }
                        Err(e) => {
                            error!(
                                session_id = session_id,
                                error = %e,
                                "Background token refresh failed, removing session"
                            );
                            this.cache.remove(&session_id);
                        }
                    }
                }
            }
        });
    }
}
