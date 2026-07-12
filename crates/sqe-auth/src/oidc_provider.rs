//! `OidcPasswordProvider` — generalised OIDC Resource Owner Password Credentials (ROPC) provider.
//!
//! Works with any OIDC-compliant identity provider (Keycloak, Auth0, Okta, Zitadel,
//! Authentik, Entra ID legacy mode, etc.) that supports `grant_type=password`.
//!
//! Unlike the legacy `OidcPasswordClient` (which hardwires Keycloak URL + realm),
//! this provider takes a direct `token_url` and a configurable `roles_claim`.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::provider::{truncate_for_log, AuthError, AuthProvider, FlightCredentials, Identity};

/// Configuration for the OIDC password grant provider.
#[derive(Debug, Clone)]
pub struct OidcPasswordProviderConfig {
    /// Full token endpoint URL (e.g. `https://idp.example.com/realms/myapp/protocol/openid-connect/token`).
    pub token_url: String,
    /// OAuth2 client_id.
    pub client_id: String,
    /// OAuth2 client_secret. Empty for public clients.
    pub client_secret: String,
    /// Dot-separated JSON path to the roles array in the JWT payload.
    /// Default: `"realm_access.roles"` (Keycloak convention).
    pub roles_claim: String,
    /// JWT claim to extract as the canonical subject identifier.
    /// Default: `"sub"`. Set to empty to disable subject extraction.
    pub subject_claim: String,
    /// JWT claim to extract as the email address. Default: `""` (disabled).
    pub email_claim: String,
    /// Dot-separated JSON path to the groups array. Default: `""` (disabled).
    pub groups_claim: String,
    /// Whether to skip TLS certificate verification (dev/test only).
    pub accept_invalid_certs: bool,
    /// When `true`, a token-endpoint rejection of the ROPC grant returns
    /// `NotMyCredentials` (defer to the next provider) instead of `AuthFailed`,
    /// so this provider can share a Basic-auth listener with a following
    /// `client_credentials_passthrough` provider. Infra errors still surface as
    /// `Internal` and stop the chain. (#276)
    pub fallthrough_on_reject: bool,
}

impl Default for OidcPasswordProviderConfig {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            accept_invalid_certs: false,
            fallthrough_on_reject: false,
        }
    }
}

/// Token response from the OIDC token endpoint (password grant).
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    #[allow(dead_code)]
    expires_in: u64,
    #[allow(dead_code)]
    token_type: String,
}

/// OIDC password grant authentication provider.
///
/// Accepts `username + password` from Flight Basic auth, exchanges them for
/// tokens via the configured OIDC token endpoint, and extracts roles from
/// the JWT claims.
pub struct OidcPasswordProvider {
    client: reqwest::Client,
    config: OidcPasswordProviderConfig,
}

impl OidcPasswordProvider {
    /// Create a new provider from the given configuration.
    pub fn new(config: OidcPasswordProviderConfig) -> Result<Self, AuthError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("Failed to build HTTP client: {e}"))
            })?;

        Ok(Self { client, config })
    }

    /// Exchange username/password for tokens via the OIDC password grant.
    async fn exchange_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> Result<TokenResponse, AuthError> {
        debug!(
            username = username,
            "Exchanging credentials via OIDC ROPC grant"
        );

        let mut params = vec![
            ("grant_type", "password".to_string()),
            ("client_id", self.config.client_id.clone()),
            ("username", username.to_string()),
            ("password", password.to_string()),
        ];

        if !self.config.client_secret.is_empty() {
            params.push(("client_secret", self.config.client_secret.clone()));
        }

        let response = self
            .client
            .post(&self.config.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("OIDC token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "OIDC provider rejected credentials");
            return Err(AuthError::AuthFailed("Authentication failed".to_string()));
        }

        response.json::<TokenResponse>().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("Failed to parse OIDC token response: {e}"))
        })
    }

    /// Refresh an access token using a refresh_token.
    async fn do_refresh_token(&self, refresh_token: &str) -> Result<TokenResponse, AuthError> {
        debug!("Refreshing token via OIDC provider");

        let mut params = vec![
            ("grant_type", "refresh_token".to_string()),
            ("client_id", self.config.client_id.clone()),
            ("refresh_token", refresh_token.to_string()),
        ];

        if !self.config.client_secret.is_empty() {
            params.push(("client_secret", self.config.client_secret.clone()));
        }

        let response = self
            .client
            .post(&self.config.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("OIDC token refresh failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "OIDC provider rejected token refresh");
            return Err(AuthError::AuthFailed("Authentication failed".to_string()));
        }

        response.json::<TokenResponse>().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!(
                "Failed to parse OIDC refresh response: {e}"
            ))
        })
    }

    /// Extract the `sub` claim from a JWT payload (without signature verification).
    ///
    /// The OIDC provider has already validated the token; we just need the claims.
    fn extract_sub(access_token: &str) -> Option<String> {
        let claims = Self::decode_jwt_payload(access_token)?;
        claims.get("sub").and_then(|v| v.as_str()).map(String::from)
    }

    /// Extract a scalar string claim by dot-separated path from a JWT payload.
    ///
    /// Returns `None` when the path is missing or the value is not a string.
    fn extract_claim_str(access_token: &str, claim_path: &str) -> Option<String> {
        let claims = Self::decode_jwt_payload(access_token)?;
        let mut current = &claims;
        for segment in claim_path.split('.') {
            current = current.get(segment)?;
        }
        current.as_str().map(String::from)
    }

    /// Extract roles from a JWT payload using a dot-separated claim path.
    ///
    /// For example, `"realm_access.roles"` navigates to `{"realm_access": {"roles": [...]}}`.
    /// Returns an empty Vec for malformed tokens or missing claims.
    fn extract_roles_from_claim(access_token: &str, roles_claim: &str) -> Vec<String> {
        let claims = match Self::decode_jwt_payload(access_token) {
            Some(c) => c,
            None => return Vec::new(),
        };

        let mut current = &claims;
        for segment in roles_claim.split('.') {
            match current.get(segment) {
                Some(v) => current = v,
                None => {
                    debug!(
                        claim = roles_claim,
                        segment = segment,
                        "Roles claim segment not found in JWT"
                    );
                    return Vec::new();
                }
            }
        }

        current
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Decode a JWT payload (base64url-encoded JSON) without signature verification.
    fn decode_jwt_payload(token: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            warn!("Token is not a valid JWT (expected 3 parts)");
            return None;
        }

        let payload = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("Failed to base64-decode JWT payload: {e}");
                return None;
            }
        };

        match serde_json::from_slice(&payload) {
            Ok(v) => Some(v),
            Err(e) => {
                warn!("Failed to parse JWT payload as JSON: {e}");
                None
            }
        }
    }
}

#[async_trait]
impl AuthProvider for OidcPasswordProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // This provider requires username + password (Flight Basic auth).
        let username = match &credentials.username {
            Some(u) if !u.is_empty() => u,
            _ => return Err(AuthError::NotMyCredentials),
        };

        let password = match &credentials.password {
            Some(p) if !p.is_empty() => p.expose(),
            _ => return Err(AuthError::NotMyCredentials),
        };

        // If the password looks like a JWT, it's probably meant for a BearerTokenProvider.
        if password.starts_with("eyJ") {
            return Err(AuthError::NotMyCredentials);
        }

        let token_response = match self.exchange_credentials(username, password).await {
            Ok(t) => t,
            // On a clean grant rejection, defer to the next provider when
            // configured for a mixed Basic-auth listener (#276). Infra errors
            // (Internal) still propagate and stop the chain.
            Err(AuthError::AuthFailed(_)) if self.config.fallthrough_on_reject => {
                return Err(AuthError::NotMyCredentials);
            }
            Err(e) => return Err(e),
        };

        let user_id =
            Self::extract_sub(&token_response.access_token).unwrap_or_else(|| username.clone());

        let roles =
            Self::extract_roles_from_claim(&token_response.access_token, &self.config.roles_claim);

        let subject = if self.config.subject_claim.is_empty() {
            None
        } else {
            Self::extract_claim_str(&token_response.access_token, &self.config.subject_claim)
        };

        let email = if self.config.email_claim.is_empty() {
            None
        } else {
            Self::extract_claim_str(&token_response.access_token, &self.config.email_claim)
        };

        let groups = if self.config.groups_claim.is_empty() {
            vec![]
        } else {
            Self::extract_roles_from_claim(&token_response.access_token, &self.config.groups_claim)
        };

        let expires_at = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::seconds(token_response.expires_in as i64));

        Ok(Identity {
            user_id: user_id.clone(),
            display_name: user_id,
            roles,
            subject,
            email,
            groups,
            catalog_token: Some(sqe_core::SecretString::new(token_response.access_token)),
            refresh_token: token_response
                .refresh_token
                .map(sqe_core::SecretString::new),
            expires_at,
        })
    }

    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
        let refresh_token = match &identity.refresh_token {
            Some(rt) if !rt.is_empty() => rt.expose(),
            _ => return Ok(None),
        };

        let token_response = self.do_refresh_token(refresh_token).await?;
        Ok(Some(sqe_core::SecretString::new(
            token_response.access_token,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake JWT (header.payload.signature) from a JSON claims object.
    fn fake_jwt(claims: &serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(b"{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(claims).unwrap());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"fake-sig");
        format!("{header}.{payload}.{signature}")
    }

    // -----------------------------------------------------------------------
    // extract_roles_from_claim
    // -----------------------------------------------------------------------

    #[test]
    fn extract_roles_default_claim_path() {
        let claims = serde_json::json!({
            "sub": "user1",
            "realm_access": {
                "roles": ["admin", "user", "data_engineer"]
            }
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "realm_access.roles");
        assert_eq!(roles, vec!["admin", "user", "data_engineer"]);
    }

    #[test]
    fn extract_roles_custom_claim_path() {
        let claims = serde_json::json!({
            "sub": "user1",
            "custom": {
                "nested": {
                    "roles": ["viewer", "editor"]
                }
            }
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "custom.nested.roles");
        assert_eq!(roles, vec!["viewer", "editor"]);
    }

    #[test]
    fn extract_roles_flat_claim() {
        let claims = serde_json::json!({
            "sub": "user1",
            "groups": ["engineering", "platform"]
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "groups");
        assert_eq!(roles, vec!["engineering", "platform"]);
    }

    #[test]
    fn extract_roles_missing_claim() {
        let claims = serde_json::json!({
            "sub": "user1"
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_partial_path() {
        let claims = serde_json::json!({
            "sub": "user1",
            "realm_access": { "other": "value" }
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_not_a_jwt() {
        let roles =
            OidcPasswordProvider::extract_roles_from_claim("not-a-jwt", "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_skips_non_string_values() {
        let claims = serde_json::json!({
            "roles": ["admin", 42, null, "user"]
        });
        let token = fake_jwt(&claims);

        let roles = OidcPasswordProvider::extract_roles_from_claim(&token, "roles");
        assert_eq!(roles, vec!["admin", "user"]);
    }

    // -----------------------------------------------------------------------
    // extract_sub
    // -----------------------------------------------------------------------

    #[test]
    fn extract_sub_from_valid_jwt() {
        let claims = serde_json::json!({
            "sub": "user-abc-123",
            "name": "Alice"
        });
        let token = fake_jwt(&claims);
        assert_eq!(
            OidcPasswordProvider::extract_sub(&token),
            Some("user-abc-123".to_string())
        );
    }

    #[test]
    fn extract_sub_missing() {
        let claims = serde_json::json!({
            "name": "Alice"
        });
        let token = fake_jwt(&claims);
        assert_eq!(OidcPasswordProvider::extract_sub(&token), None);
    }

    #[test]
    fn extract_sub_not_a_jwt() {
        assert_eq!(OidcPasswordProvider::extract_sub("garbage"), None);
    }

    // -----------------------------------------------------------------------
    // OidcPasswordProviderConfig defaults
    // -----------------------------------------------------------------------

    #[test]
    fn config_defaults() {
        let config = OidcPasswordProviderConfig::default();
        assert_eq!(config.roles_claim, "realm_access.roles");
        assert!(!config.accept_invalid_certs);
        assert!(config.token_url.is_empty());
        assert!(config.client_id.is_empty());
        assert!(config.client_secret.is_empty());
    }

    // -----------------------------------------------------------------------
    // Provider construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_succeeds_with_valid_config() {
        let config = OidcPasswordProviderConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            client_secret: "secret".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            accept_invalid_certs: false,
            fallthrough_on_reject: false,
        };
        assert!(OidcPasswordProvider::new(config).is_ok());
    }

    // -----------------------------------------------------------------------
    // authenticate: credential detection
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn authenticate_skips_when_no_username() {
        let config = OidcPasswordProviderConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = OidcPasswordProvider::new(config).unwrap();

        let creds = FlightCredentials {
            username: None,
            password: Some(sqe_core::SecretString::new("pass".to_string())),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    #[tokio::test]
    async fn authenticate_skips_when_no_password() {
        let config = OidcPasswordProviderConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = OidcPasswordProvider::new(config).unwrap();

        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: None,
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    #[tokio::test]
    async fn authenticate_skips_when_password_looks_like_jwt() {
        let config = OidcPasswordProviderConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = OidcPasswordProvider::new(config).unwrap();

        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some(sqe_core::SecretString::new(
                "eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string(),
            )),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    #[tokio::test]
    async fn authenticate_skips_empty_username() {
        let config = OidcPasswordProviderConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = OidcPasswordProvider::new(config).unwrap();

        let creds = FlightCredentials {
            username: Some(String::new()),
            password: Some(sqe_core::SecretString::new("pass".to_string())),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }
}
