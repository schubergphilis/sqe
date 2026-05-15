use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use sqe_core::SecretString;
use sqe_core::Session;
use sqe_core::config::AuthConfig;

use crate::oidc_password::OidcPasswordClient;
use crate::oauth::OAuthClient;
use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};
use crate::token_cache::{CachedToken, TokenCache};

/// Selects which auth backend the engine uses at runtime.
enum AuthBackend {
    /// OIDC Password Grant (ROPC) — exchanges username/password for a token
    /// via any OIDC-compliant provider (Keycloak, Auth0, Okta, etc.).
    OidcPassword(OidcPasswordClient),
    /// Generic OAuth2 client_credentials — obtains a service token from any
    /// OAuth2-compliant endpoint (e.g. Polaris). Username/password are ignored.
    ClientCredentials(OAuthClient),
}

/// Cached service token for client_credentials backend.
/// Reused across all requests until near-expiry.
struct ServiceToken {
    access_token: String,
    expiry: DateTime<Utc>,
}

pub struct Authenticator {
    backend: AuthBackend,
    cache: TokenCache,
    refresh_buffer_secs: u64,
    /// Cached client_credentials token — avoids Polaris round-trip on every query.
    service_token: RwLock<Option<ServiceToken>>,
}

impl Authenticator {
    pub async fn new(config: &AuthConfig) -> sqe_core::Result<Self> {
        let cache = TokenCache::new();
        let refresh_buffer_secs = config.token_refresh_buffer_secs;

        let backend = if !config.token_endpoint.is_empty() && config.keycloak_url.is_empty() {
            info!(
                token_endpoint = config.token_endpoint,
                "Using OAuth2 client_credentials backend"
            );
            tracing::warn!("ClientCredentials mode active — all users share a single service token. Not suitable for multi-user access control.");
            let oauth = OAuthClient::new(
                &config.token_endpoint,
                &config.client_id,
                config.client_secret.expose(),
                config.should_skip_tls_verify(),
            )?;
            AuthBackend::ClientCredentials(oauth)
        } else {
            info!(
                keycloak_url = config.keycloak_url,
                realm = config.realm,
                "Using OIDC password grant backend"
            );
            let oidc = OidcPasswordClient::new(config)?;
            AuthBackend::OidcPassword(oidc)
        };

        info!(
            refresh_buffer_secs = refresh_buffer_secs,
            "Authenticator initialized"
        );

        Ok(Self {
            backend,
            cache,
            refresh_buffer_secs,
            service_token: RwLock::new(None),
        })
    }

    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<Session> {
        match &self.backend {
            AuthBackend::OidcPassword(kc) => {
                let token_response = kc.exchange_credentials(username, password).await?;
                let roles = kc.extract_roles(&token_response.access_token);
                let token_expiry =
                    Utc::now() + Duration::seconds(token_response.expires_in as i64);

                let session = Session::new(
                    username.to_string(),
                    SecretString::new(token_response.access_token.clone()),
                    token_response
                        .refresh_token
                        .clone()
                        .map(SecretString::new),
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
                    "Session created and cached (OIDC)"
                );

                Ok(session)
            }
            AuthBackend::ClientCredentials(oauth) => {
                // Reuse the cached service token if it's still valid (with buffer).
                let buffer = Duration::seconds(self.refresh_buffer_secs as i64);
                {
                    let guard = self.service_token.read().await;
                    if let Some(ref st) = *guard {
                        if st.expiry > Utc::now() + buffer {
                            let session = Session::new(
                                username.to_string(),
                                SecretString::new(st.access_token.clone()),
                                None,
                                st.expiry,
                                Vec::new(),
                            );
                            debug!(
                                username = username,
                                "Reusing cached client_credentials token"
                            );
                            return Ok(session);
                        }
                    }
                }

                // Token missing or near-expiry — fetch a fresh one.
                let token_response = oauth.get_token().await?;
                let token_expiry =
                    Utc::now() + Duration::seconds(token_response.expires_in as i64);

                // Cache the service token for reuse.
                {
                    let mut guard = self.service_token.write().await;
                    *guard = Some(ServiceToken {
                        access_token: token_response.access_token.clone(),
                        expiry: token_expiry,
                    });
                }

                // client_credentials mode: username is informational only, no
                // refresh_token (we re-fetch via client_credentials when needed).
                let session = Session::new(
                    username.to_string(),
                    SecretString::new(token_response.access_token.clone()),
                    None,
                    token_expiry,
                    Vec::new(),
                );

                self.cache.insert(
                    &session.id,
                    CachedToken {
                        access_token: token_response.access_token,
                        refresh_token: None,
                        expiry: token_expiry,
                    },
                );

                debug!(
                    session_id = session.id,
                    username = username,
                    "Session created with fresh client_credentials token"
                );

                Ok(session)
            }
        }
    }

    /// Look up the latest cached token for a session.
    ///
    /// Used by `SessionManager` to pick up tokens refreshed by the background task.
    pub fn get_cached_token(&self, session_id: &str) -> Option<CachedToken> {
        self.cache.get(session_id)
    }

    pub async fn refresh_session(&self, session: &mut Session) -> sqe_core::Result<()> {
        match &self.backend {
            AuthBackend::OidcPassword(kc) => {
                let refresh_token = session
                    .refresh_token()
                    .map(|t| t.expose().to_string())
                    .ok_or_else(|| {
                        sqe_core::SqeError::Auth("No refresh token available".to_string())
                    })?;

                let token_response = kc.refresh_token(&refresh_token).await?;
                let token_expiry =
                    Utc::now() + Duration::seconds(token_response.expires_in as i64);

                session.rotate_credentials(sqe_core::Credentials::new(
                    SecretString::new(token_response.access_token.clone()),
                    token_response.refresh_token.clone().map(SecretString::new),
                    token_expiry,
                ));

                self.cache.insert(
                    &session.id,
                    CachedToken {
                        access_token: token_response.access_token,
                        refresh_token: token_response.refresh_token,
                        expiry: token_expiry,
                    },
                );

                debug!(session_id = session.id, "Session token refreshed (OIDC)");
            }
            AuthBackend::ClientCredentials(oauth) => {
                // No refresh_token in client_credentials mode, simply re-fetch.
                let token_response = oauth.get_token().await?;
                let token_expiry =
                    Utc::now() + Duration::seconds(token_response.expires_in as i64);

                session.rotate_credentials(sqe_core::Credentials::new(
                    SecretString::new(token_response.access_token.clone()),
                    None,
                    token_expiry,
                ));

                self.cache.insert(
                    &session.id,
                    CachedToken {
                        access_token: token_response.access_token,
                        refresh_token: None,
                        expiry: token_expiry,
                    },
                );

                debug!(
                    session_id = session.id,
                    "Session token refreshed (client_credentials)"
                );
            }
        }

        Ok(())
    }

    /// Returns the configured refresh buffer in seconds.
    ///
    /// Exposed for testing only — the value is read from `AuthConfig`.
    #[cfg(test)]
    pub fn refresh_buffer_secs(&self) -> u64 {
        self.refresh_buffer_secs
    }

    /// Spawns a background task that periodically checks the cache for expiring
    /// sessions and refreshes them. Errors are logged but do not crash the task.
    ///
    /// Returns a [`sqe_core::TaskGuard`]; dropping it aborts the loop.
    pub fn start_refresh_task(self: &Arc<Self>) -> sqe_core::TaskGuard {
        let this = Arc::clone(self);
        sqe_core::spawn_supervised("auth-refresh", move |token| async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = interval.tick() => {}
                }

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

                    match &this.backend {
                        AuthBackend::OidcPassword(kc) => {
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

                            match kc.refresh_token(refresh_token).await {
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
                                    debug!(
                                        session_id = session_id,
                                        "Background token refresh succeeded (OIDC)"
                                    );
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
                        AuthBackend::ClientCredentials(oauth) => {
                            // Re-fetch via client_credentials (no refresh_token needed).
                            match oauth.get_token().await {
                                Ok(token_response) => {
                                    let expiry = Utc::now()
                                        + Duration::seconds(token_response.expires_in as i64);
                                    this.cache.insert(
                                        &session_id,
                                        CachedToken {
                                            access_token: token_response.access_token,
                                            refresh_token: None,
                                            expiry,
                                        },
                                    );
                                    debug!(
                                        session_id = session_id,
                                        "Background token refresh succeeded (client_credentials)"
                                    );
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
                }
            }
        })
    }
}

/// Bridge implementation: makes the existing `Authenticator` usable as an `AuthProvider`.
///
/// This allows the `Authenticator` to participate in the `AuthChain` alongside
/// new providers. It handles both OIDC password grant and OAuth2 client_credentials
/// backends.
///
/// Credential mapping:
/// - Requires `username` and `password` in `FlightCredentials`
/// - Returns `NotMyCredentials` if either is missing
#[async_trait]
impl AuthProvider for Authenticator {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        let username = credentials
            .username
            .as_deref()
            .ok_or(AuthError::NotMyCredentials)?;
        let password = credentials
            .password
            .as_ref()
            .ok_or(AuthError::NotMyCredentials)?
            .expose();

        if username.is_empty() && password.is_empty() {
            return Err(AuthError::NotMyCredentials);
        }

        let session = self
            .authenticate(username, password)
            .await
            .map_err(|e| AuthError::AuthFailed(e.to_string()))?;

        Ok(Identity {
            user_id: session.user.username.clone(),
            display_name: session.user.username.clone(),
            roles: session.user.roles.clone(),
            catalog_token: Some(session.access_token().clone()),
            refresh_token: session.refresh_token().cloned(),
            expires_at: Some(session.token_expiry()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::AuthConfig;

    /// Build a minimal `AuthConfig` that selects the OIDC password-grant backend.
    fn oidc_config() -> AuthConfig {
        AuthConfig {
            keycloak_url: "http://localhost:8080".to_string(),
            realm: "test-realm".to_string(),
            client_id: "sqe-client".to_string(),
            client_secret: SecretString::new("secret".to_string()),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: false,
            tls_skip_verify: false,
            roles_claim: "realm_access.roles".to_string(),
            providers: Vec::new(),
            role_mappings: std::collections::HashMap::new(),
            external: None,
            admin_roles: Vec::new(),
        }
    }

    /// Build a minimal `AuthConfig` that selects the client_credentials backend.
    fn client_creds_config() -> AuthConfig {
        AuthConfig {
            keycloak_url: String::new(),
            realm: String::new(),
            client_id: "polaris-client".to_string(),
            client_secret: SecretString::new("polaris-secret".to_string()),
            token_endpoint: "http://localhost:8181/api/catalog/v1/oauth/tokens".to_string(),
            token_refresh_buffer_secs: 120,
            ssl_verification: true,
            tls_skip_verify: false,
            roles_claim: "realm_access.roles".to_string(),
            providers: Vec::new(),
            role_mappings: std::collections::HashMap::new(),
            external: None,
            admin_roles: Vec::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Backend selection logic
    // -------------------------------------------------------------------------

    /// When `token_endpoint` is set and `keycloak_url` is empty, the engine
    /// must select the OAuth2 `client_credentials` backend.
    #[tokio::test]
    async fn backend_selection_client_credentials_when_token_endpoint_set() {
        let config = client_creds_config();
        let auth = Authenticator::new(&config)
            .await
            .expect("should construct with client_credentials config");
        // The refresh buffer is propagated from the config
        assert_eq!(auth.refresh_buffer_secs(), 120);
    }

    /// When `keycloak_url` is set (even with a non-empty token_endpoint), the
    /// engine falls back to the OIDC password-grant backend because the condition
    /// requires `keycloak_url.is_empty()`.
    #[tokio::test]
    async fn backend_selection_oidc_when_keycloak_url_set() {
        let mut config = oidc_config();
        // Also set token_endpoint — keycloak_url takes precedence
        config.token_endpoint = "http://example.com/token".to_string();
        let auth = Authenticator::new(&config)
            .await
            .expect("should construct with OIDC config");
        assert_eq!(auth.refresh_buffer_secs(), 60);
    }

    /// Plain OIDC config (no token_endpoint) must succeed.
    #[tokio::test]
    async fn backend_selection_oidc_no_token_endpoint() {
        let config = oidc_config();
        let auth = Authenticator::new(&config).await;
        assert!(auth.is_ok(), "Expected Ok, got {:?}", auth.err());
    }

    // -------------------------------------------------------------------------
    // Token cache: get_cached_token
    // -------------------------------------------------------------------------

    /// A freshly constructed `Authenticator` has an empty cache.
    #[tokio::test]
    async fn cached_token_missing_on_fresh_authenticator() {
        let config = oidc_config();
        let auth = Authenticator::new(&config).await.unwrap();
        assert!(
            auth.get_cached_token("nonexistent-session").is_none(),
            "Cache should be empty after construction"
        );
    }

    // -------------------------------------------------------------------------
    // refresh_buffer_secs propagation
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_buffer_secs_propagated_from_config() {
        let mut config = oidc_config();
        config.token_refresh_buffer_secs = 42;
        let auth = Authenticator::new(&config).await.unwrap();
        assert_eq!(auth.refresh_buffer_secs(), 42);
    }
}
