//! OIDC machine-to-machine (M2M) auth provider.
//!
//! Unity Catalog, Databricks REST, and similar services accept OAuth2 tokens
//! obtained via the `client_credentials` grant. This provider wraps the token
//! endpoint call, caches the resulting bearer token, and hands it out through
//! the standard `AuthProvider` contract. A preemptive refresh kicks in 60
//! seconds before the token would expire so catalog calls never see a stale
//! token.
//!
//! Unlike `OidcPasswordProvider`, M2M credentials live in the SQE config (not
//! the Flight handshake). The provider accepts any `FlightCredentials`; a
//! matching deployment pairs it with `AnonymousProvider` on the client side or
//! simply wires the bearer token into a server-to-server catalog call.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// Token endpoint response for the `client_credentials` grant.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    #[serde(default)]
    pub token_type: String,
}

/// Configuration for the OIDC M2M provider.
#[derive(Debug, Clone)]
pub struct OidcM2mConfig {
    /// Full URL of the OAuth2 token endpoint.
    ///
    /// Examples:
    /// - Unity Catalog: `https://<workspace>.cloud.databricks.com/oidc/v1/token`
    /// - Generic IdP: `https://idp.example.com/oauth2/token`
    pub token_endpoint: String,
    /// OAuth2 client ID registered with the IdP.
    pub client_id: String,
    /// OAuth2 client secret.
    pub client_secret: String,
    /// Optional scope, passed as `scope=<value>` in the form body.
    pub scope: Option<String>,
    /// Display name for the service identity.
    pub user_id: String,
    /// Roles to attach to the `Identity`. Passed through to the policy engine.
    pub roles: Vec<String>,
    /// Refresh the token this long before it expires. Default 60s.
    pub refresh_skew: Duration,
    /// Accept invalid TLS certificates (dev/self-signed environments).
    pub accept_invalid_certs: bool,
    /// Per-request timeout for the token endpoint. Default 5s.
    pub request_timeout: Duration,
}

impl OidcM2mConfig {
    pub fn new(
        token_endpoint: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            token_endpoint: token_endpoint.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: None,
            user_id: "sqe-m2m".to_string(),
            roles: vec!["service".to_string()],
            refresh_skew: Duration::from_secs(60),
            accept_invalid_certs: false,
            request_timeout: Duration::from_secs(5),
        }
    }
}

/// Cached access token with an absolute expiry instant.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

/// OIDC M2M provider.
pub struct OidcM2mProvider {
    client: reqwest::Client,
    config: OidcM2mConfig,
    cache: Arc<RwLock<Option<CachedToken>>>,
    // Serialises concurrent refreshes so only one task hits the IdP per
    // expiry window. The HTTP call happens under this lock, but the cache
    // RwLock is released first so readers never block on the network.
    refresh: Arc<tokio::sync::Mutex<()>>,
}

impl OidcM2mProvider {
    /// Create a new provider. The HTTP client is built lazily on the first
    /// token request; construction fails fast if the endpoint string is empty.
    pub fn new(config: OidcM2mConfig) -> Result<Self, String> {
        if config.token_endpoint.is_empty() {
            return Err("OidcM2mConfig.token_endpoint must be non-empty".to_string());
        }
        if config.client_id.is_empty() {
            return Err("OidcM2mConfig.client_id must be non-empty".to_string());
        }
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .timeout(config.request_timeout)
            .build()
            .map_err(|e| format!("build http client: {e}"))?;
        Ok(Self {
            client,
            config,
            cache: Arc::new(RwLock::new(None)),
            refresh: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Return a fresh access token, refreshing if the cached one is within
    /// `refresh_skew` of expiry.
    pub async fn get_token(&self) -> Result<String, AuthError> {
        // Fast path under read lock.
        if let Some(token) = self.cached_if_fresh().await {
            debug!("OIDC M2M cache hit");
            return Ok(token);
        }

        // Single-flight refresh. The cache RwLock is NOT held across the
        // HTTP call: a hung IdP only blocks tasks waiting on `refresh`, not
        // readers checking the cache.
        let _refresh_guard = self.refresh.lock().await;

        // Another task may have refreshed while we waited on `refresh`.
        if let Some(token) = self.cached_if_fresh().await {
            return Ok(token);
        }

        let resp = self.fetch_token().await?;
        let lifetime = Duration::from_secs(resp.expires_in.clamp(1, 24 * 60 * 60));
        let expires_at = Instant::now() + lifetime;
        let access = resp.access_token.clone();
        {
            let mut guard = self.cache.write().await;
            *guard = Some(CachedToken {
                access_token: access.clone(),
                expires_at,
            });
        }
        info!(
            lifetime_secs = lifetime.as_secs(),
            "OIDC M2M token refreshed"
        );
        Ok(access)
    }

    async fn cached_if_fresh(&self) -> Option<String> {
        let guard = self.cache.read().await;
        guard.as_ref().and_then(|cached| {
            if cached.expires_at.saturating_duration_since(Instant::now())
                > self.config.refresh_skew
            {
                Some(cached.access_token.clone())
            } else {
                None
            }
        })
    }

    async fn fetch_token(&self) -> Result<TokenResponse, AuthError> {
        let mut params = vec![
            ("grant_type", "client_credentials"),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
        ];
        if let Some(scope) = self.config.scope.as_deref() {
            params.push(("scope", scope));
        }

        let resp = self
            .client
            .post(&self.config.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("M2M token request failed: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(
                status = %status,
                body = %body.chars().take(200).collect::<String>(),
                "OIDC M2M token endpoint rejected credentials"
            );
            return Err(AuthError::AuthFailed(format!(
                "M2M token endpoint returned {status}"
            )));
        }

        resp.json::<TokenResponse>()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("parse M2M response: {e}")))
    }
}

#[async_trait]
impl AuthProvider for OidcM2mProvider {
    async fn authenticate(
        &self,
        _credentials: &FlightCredentials,
    ) -> Result<Identity, AuthError> {
        let token = self.get_token().await?;
        Ok(Identity {
            user_id: self.config.user_id.clone(),
            display_name: self.config.user_id.clone(),
            roles: self.config.roles.clone(),
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: Some(sqe_core::SecretString::new(token)),
            refresh_token: None,
            expires_at: None,
        })
    }

    async fn refresh_catalog_token(
        &self,
        _identity: &Identity,
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
        let token = self.get_token().await?;
        Ok(Some(sqe_core::SecretString::new(token)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_empty_token_endpoint() {
        let cfg = OidcM2mConfig::new("", "client", "secret");
        assert!(OidcM2mProvider::new(cfg).is_err());
    }

    #[test]
    fn new_rejects_empty_client_id() {
        let cfg = OidcM2mConfig::new("https://idp.test/token", "", "secret");
        assert!(OidcM2mProvider::new(cfg).is_err());
    }

    #[test]
    fn new_accepts_valid_config() {
        let cfg = OidcM2mConfig::new("https://idp.test/token", "client", "secret");
        assert!(OidcM2mProvider::new(cfg).is_ok());
    }

    #[test]
    fn config_defaults_refresh_skew_to_60s() {
        let cfg = OidcM2mConfig::new("https://idp.test/token", "c", "s");
        assert_eq!(cfg.refresh_skew, Duration::from_secs(60));
    }

    #[test]
    fn config_defaults_request_timeout_to_5s() {
        let cfg = OidcM2mConfig::new("https://idp.test/token", "c", "s");
        assert_eq!(cfg.request_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn get_token_fails_on_unreachable_endpoint() {
        // Use an unresolvable host so the request fails fast without network.
        let cfg = OidcM2mConfig::new(
            "http://127.0.0.1:1/token", // port 1 is typically closed
            "client",
            "secret",
        );
        let provider = OidcM2mProvider::new(cfg).unwrap();
        let err = provider.get_token().await.unwrap_err();
        // Connection refused becomes an Internal error, not AuthFailed.
        match err {
            AuthError::Internal(_) => {}
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cache_reuses_token_within_skew_window() {
        // Build a provider and manually populate the cache with a long-lived
        // token so no HTTP call is made.
        let cfg = OidcM2mConfig::new("https://idp.test/token", "c", "s");
        let provider = OidcM2mProvider::new(cfg).unwrap();
        {
            let mut guard = provider.cache.write().await;
            *guard = Some(CachedToken {
                access_token: "cached-token".to_string(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            });
        }
        let t = provider.get_token().await.unwrap();
        assert_eq!(t, "cached-token");
    }

    #[tokio::test]
    async fn authenticate_returns_identity_with_catalog_token() {
        let cfg = OidcM2mConfig::new("https://idp.test/token", "c", "s");
        let provider = OidcM2mProvider::new(cfg).unwrap();
        {
            let mut guard = provider.cache.write().await;
            *guard = Some(CachedToken {
                access_token: "cached-token".to_string(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            });
        }
        let identity = provider
            .authenticate(&FlightCredentials::default())
            .await
            .unwrap();
        assert_eq!(identity.user_id, "sqe-m2m");
        assert_eq!(
            identity.catalog_token.as_ref().map(|t| t.expose()),
            Some("cached-token"),
        );
        assert!(identity.roles.contains(&"service".to_string()));
    }
}
