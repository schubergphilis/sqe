//! `TokenExchangeProvider` — OAuth2 Token Exchange (RFC 8693) authentication provider.
//!
//! Exchanges an incoming credential (bearer token, username+password) for a user-scoped
//! JWT via an OIDC token endpoint. This is a **catch-all** provider: it accepts any
//! credential that carries a bearer token or username+password. Place it **last** in
//! the auth chain so more specific providers get first crack.
//!
//! The exchange follows RFC 8693:
//!
//! ```text
//! POST {token_url}
//! Content-Type: application/x-www-form-urlencoded
//!
//! grant_type=urn:ietf:params:oauth:grant-type:token-exchange
//! &subject_token={incoming credential}
//! &subject_token_type=urn:ietf:params:oauth:token-type:access_token
//! &client_id={client_id}
//! &client_secret={client_secret}    (optional)
//! &audience={audience}              (optional)
//! &requested_token_type=urn:ietf:params:oauth:token-type:access_token
//! ```
//!
//! The returned access_token is decoded (without signature verification — it's fresh
//! from the IdP) to extract user identity and roles, then used as the catalog token
//! for Polaris.

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::provider::{truncate_for_log, AuthError, AuthProvider, FlightCredentials, Identity};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// RFC 8693 grant type for token exchange.
const GRANT_TYPE_TOKEN_EXCHANGE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";

/// RFC 8693 token type for access tokens.
const TOKEN_TYPE_ACCESS_TOKEN: &str = "urn:ietf:params:oauth:token-type:access_token";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the token exchange provider.
#[derive(Debug, Clone)]
pub struct TokenExchangeConfig {
    /// Full token endpoint URL (e.g. `https://keycloak.example.com/realms/sqe/protocol/openid-connect/token`).
    pub token_url: String,
    /// OAuth2 client_id.
    pub client_id: String,
    /// OAuth2 client_secret. `None` for public clients.
    pub client_secret: Option<String>,
    /// Target audience for the exchanged token (e.g. `"polaris"`).
    pub audience: Option<String>,
    /// JWT claim that carries the user identifier. Default: `"sub"`.
    pub user_claim: String,
    /// Dot-separated JSON path to the roles array in the JWT payload.
    /// Default: `"realm_access.roles"`.
    pub roles_claim: String,
    /// Whether to skip TLS certificate verification (dev/test only).
    pub accept_invalid_certs: bool,
}

impl Default for TokenExchangeConfig {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            client_id: String::new(),
            client_secret: None,
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            accept_invalid_certs: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Token response
// ---------------------------------------------------------------------------

/// Standard OAuth2 token response (RFC 8693 section 2.2.1).
#[derive(Debug, Deserialize)]
struct TokenExchangeResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: String,
    #[allow(dead_code)]
    expires_in: Option<u64>,
    /// RFC 8693 may return an `issued_token_type` — we don't need it.
    #[allow(dead_code)]
    issued_token_type: Option<String>,
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// OAuth2 Token Exchange (RFC 8693) authentication provider.
///
/// Accepts any credential that provides a `bearer_token` or `username + password`,
/// extracts a subject token, and exchanges it at the configured OIDC token endpoint
/// for a user-scoped JWT. The returned JWT becomes the catalog token for Polaris.
///
/// This is a catch-all provider — place it **last** in the `AuthChain`.
pub struct TokenExchangeProvider {
    client: reqwest::Client,
    config: TokenExchangeConfig,
}

impl TokenExchangeProvider {
    /// Create a new token exchange provider from the given configuration.
    pub fn new(config: TokenExchangeConfig) -> Result<Self, AuthError> {
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

    /// Extract the subject token from the credentials.
    ///
    /// Prefers `bearer_token`; falls back to `password` (for username+password
    /// flows where the password *is* the external token being exchanged).
    fn extract_subject_token(credentials: &FlightCredentials) -> Option<String> {
        // Prefer an explicit bearer token.
        if let Some(ref token) = credentials.bearer_token {
            if !token.is_empty() {
                return Some(token.expose().to_string());
            }
        }

        // Fall back to password (the credential being exchanged).
        if let Some(ref password) = credentials.password {
            if !password.is_empty() {
                return Some(password.expose().to_string());
            }
        }

        None
    }

    /// Perform the RFC 8693 token exchange.
    async fn exchange(&self, subject_token: &str) -> Result<TokenExchangeResponse, AuthError> {
        debug!("Performing RFC 8693 token exchange");

        let mut params = vec![
            ("grant_type", GRANT_TYPE_TOKEN_EXCHANGE.to_string()),
            ("subject_token", subject_token.to_string()),
            (
                "subject_token_type",
                TOKEN_TYPE_ACCESS_TOKEN.to_string(),
            ),
            ("client_id", self.config.client_id.clone()),
            (
                "requested_token_type",
                TOKEN_TYPE_ACCESS_TOKEN.to_string(),
            ),
        ];

        if let Some(ref secret) = self.config.client_secret {
            params.push(("client_secret", secret.clone()));
        }

        if let Some(ref audience) = self.config.audience {
            params.push(("audience", audience.clone()));
        }

        let response = self
            .client
            .post(&self.config.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("Token exchange request failed: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "Token exchange endpoint rejected credentials");
            return Err(AuthError::AuthFailed(
                "Authentication failed".to_string(),
            ));
        }

        response
            .json::<TokenExchangeResponse>()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!(
                    "Failed to parse token exchange response: {e}"
                ))
            })
    }

    /// Decode a JWT payload (base64url middle segment) without signature verification.
    ///
    /// We trust the token because we just received it from the IdP.
    fn decode_jwt_payload(token: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            warn!("Exchanged token is not a valid JWT (expected 3 parts)");
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

    /// Extract a string claim from a JWT payload using the configured `user_claim`.
    fn extract_user_claim(claims: &serde_json::Value, user_claim: &str) -> Option<String> {
        navigate_claim(claims, user_claim).and_then(|v| v.as_str().map(String::from))
    }

    /// Build an [`Identity`] from a freshly exchanged access token.
    ///
    /// On any of these conditions we refuse the request:
    /// 1. The exchanged token does not decode as a JWT payload.
    /// 2. The configured user claim is missing or not a string.
    ///
    /// Decode failure must not fall back to a client-supplied username: an
    /// attacker who can mint a JWT the IdP will exchange, but shape the
    /// response so our decoder fails, could otherwise claim any user_id
    /// they want by stuffing it into `FlightCredentials.username` (#39).
    fn identity_from_exchanged_token(
        access_token: &str,
        subject_token: &str,
        user_claim: &str,
        roles_claim: &str,
    ) -> Result<Identity, AuthError> {
        let claims = Self::decode_jwt_payload(access_token).ok_or_else(|| {
            warn!("Failed to decode exchanged JWT payload; refusing to mint identity");
            AuthError::Internal(anyhow::anyhow!(
                "exchanged token from IdP could not be decoded as JWT"
            ))
        })?;

        let user_id = Self::extract_user_claim(&claims, user_claim).ok_or_else(|| {
            warn!(
                user_claim = %user_claim,
                "Exchanged JWT is missing the configured user claim; refusing to mint identity"
            );
            AuthError::AuthFailed(format!("exchanged token has no '{user_claim}' claim"))
        })?;

        let roles = Self::extract_roles(&claims, roles_claim);

        debug!(
            user_id = %user_id,
            roles = ?roles,
            "Token exchange authentication successful"
        );

        let expires_at = claims
            .get("exp")
            .and_then(|v| v.as_i64())
            .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0));

        Ok(Identity {
            user_id: user_id.clone(),
            display_name: user_id,
            roles,
            catalog_token: Some(sqe_core::SecretString::new(access_token.to_string())),
            refresh_token: Some(sqe_core::SecretString::new(subject_token.to_string())),
            expires_at,
        })
    }

    /// Extract roles from a JWT payload using a dot-separated claim path.
    fn extract_roles(claims: &serde_json::Value, roles_claim: &str) -> Vec<String> {
        match navigate_claim(claims, roles_claim) {
            Some(v) => v
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }
}

/// Navigate a dot-separated claim path through a JSON value.
///
/// For example, `"realm_access.roles"` navigates to `json["realm_access"]["roles"]`.
fn navigate_claim<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[async_trait]
impl AuthProvider for TokenExchangeProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // Extract a subject token from the credentials. If there's nothing to
        // exchange, this provider can't help.
        let subject_token = match Self::extract_subject_token(credentials) {
            Some(t) => t,
            None => return Err(AuthError::NotMyCredentials),
        };

        // Perform the RFC 8693 token exchange.
        let response = self.exchange(&subject_token).await?;

        // Refuse to mint an identity unless the exchanged token decodes
        // and contains the configured user claim. See `identity_from_exchanged_token`.
        Self::identity_from_exchanged_token(
            &response.access_token,
            &subject_token,
            &self.config.user_claim,
            &self.config.roles_claim,
        )
    }

    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
        // Re-exchange using the stored subject token (saved in refresh_token field).
        let subject_token = match &identity.refresh_token {
            Some(t) if !t.is_empty() => t.expose(),
            _ => return Ok(None),
        };

        // TODO: check if the current catalog token is still valid (not expired)
        // before re-exchanging. For now, always re-exchange.
        let response = self.exchange(subject_token).await?;
        Ok(Some(sqe_core::SecretString::new(response.access_token)))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a fake JWT (header.payload.signature) from a JSON claims object.
    fn fake_jwt(claims: &serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(b"{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(claims).unwrap());
        let signature =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"fake-sig");
        format!("{header}.{payload}.{signature}")
    }

    // -----------------------------------------------------------------------
    // Config
    // -----------------------------------------------------------------------

    #[test]
    fn config_defaults() {
        let config = TokenExchangeConfig::default();
        assert_eq!(config.user_claim, "sub");
        assert_eq!(config.roles_claim, "realm_access.roles");
        assert!(config.token_url.is_empty());
        assert!(config.client_id.is_empty());
        assert!(config.client_secret.is_none());
        assert!(config.audience.is_none());
        assert!(!config.accept_invalid_certs);
    }

    #[test]
    fn config_with_all_fields() {
        let config = TokenExchangeConfig {
            token_url: "https://keycloak.example.com/realms/sqe/protocol/openid-connect/token"
                .to_string(),
            client_id: "sqe".to_string(),
            client_secret: Some("changeme".to_string()),
            audience: Some("polaris".to_string()),
            user_claim: "preferred_username".to_string(),
            roles_claim: "resource_access.sqe.roles".to_string(),
            accept_invalid_certs: false,
        };
        assert_eq!(
            config.token_url,
            "https://keycloak.example.com/realms/sqe/protocol/openid-connect/token"
        );
        assert_eq!(config.client_id, "sqe");
        assert_eq!(config.client_secret.as_deref(), Some("changeme"));
        assert_eq!(config.audience.as_deref(), Some("polaris"));
        assert_eq!(config.user_claim, "preferred_username");
        assert_eq!(config.roles_claim, "resource_access.sqe.roles");
    }

    // -----------------------------------------------------------------------
    // Provider construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_succeeds_with_valid_config() {
        let config = TokenExchangeConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        assert!(TokenExchangeProvider::new(config).is_ok());
    }

    // -----------------------------------------------------------------------
    // JWT payload decoding
    // -----------------------------------------------------------------------

    #[test]
    fn decode_jwt_payload_valid() {
        let claims = serde_json::json!({
            "sub": "alice",
            "name": "Alice Wonderland"
        });
        let token = fake_jwt(&claims);
        let decoded = TokenExchangeProvider::decode_jwt_payload(&token);
        assert!(decoded.is_some());
        let decoded = decoded.unwrap();
        assert_eq!(decoded["sub"], "alice");
        assert_eq!(decoded["name"], "Alice Wonderland");
    }

    #[test]
    fn decode_jwt_payload_not_a_jwt() {
        assert!(TokenExchangeProvider::decode_jwt_payload("not-a-jwt").is_none());
    }

    #[test]
    fn decode_jwt_payload_invalid_base64() {
        assert!(
            TokenExchangeProvider::decode_jwt_payload("header.!!!invalid!!!.sig").is_none()
        );
    }

    #[test]
    fn decode_jwt_payload_non_json() {
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"not json");
        let token = format!("header.{payload}.sig");
        assert!(TokenExchangeProvider::decode_jwt_payload(&token).is_none());
    }

    // -----------------------------------------------------------------------
    // Claims extraction: user_claim
    // -----------------------------------------------------------------------

    #[test]
    fn extract_user_claim_sub() {
        let claims = serde_json::json!({
            "sub": "user-abc-123",
            "name": "Alice"
        });
        assert_eq!(
            TokenExchangeProvider::extract_user_claim(&claims, "sub"),
            Some("user-abc-123".to_string())
        );
    }

    #[test]
    fn extract_user_claim_preferred_username() {
        let claims = serde_json::json!({
            "sub": "user-abc-123",
            "preferred_username": "alice@example.com"
        });
        assert_eq!(
            TokenExchangeProvider::extract_user_claim(&claims, "preferred_username"),
            Some("alice@example.com".to_string())
        );
    }

    #[test]
    fn extract_user_claim_nested() {
        let claims = serde_json::json!({
            "user": { "id": "deep-nested-id" }
        });
        assert_eq!(
            TokenExchangeProvider::extract_user_claim(&claims, "user.id"),
            Some("deep-nested-id".to_string())
        );
    }

    #[test]
    fn extract_user_claim_missing() {
        let claims = serde_json::json!({ "name": "Alice" });
        assert_eq!(
            TokenExchangeProvider::extract_user_claim(&claims, "sub"),
            None
        );
    }

    // -----------------------------------------------------------------------
    // Claims extraction: roles
    // -----------------------------------------------------------------------

    #[test]
    fn extract_roles_default_claim_path() {
        let claims = serde_json::json!({
            "realm_access": {
                "roles": ["admin", "user", "data_engineer"]
            }
        });
        let roles = TokenExchangeProvider::extract_roles(&claims, "realm_access.roles");
        assert_eq!(roles, vec!["admin", "user", "data_engineer"]);
    }

    #[test]
    fn extract_roles_flat_claim() {
        let claims = serde_json::json!({
            "groups": ["engineering", "platform"]
        });
        let roles = TokenExchangeProvider::extract_roles(&claims, "groups");
        assert_eq!(roles, vec!["engineering", "platform"]);
    }

    #[test]
    fn extract_roles_deeply_nested() {
        let claims = serde_json::json!({
            "resource_access": {
                "sqe": {
                    "roles": ["viewer", "editor"]
                }
            }
        });
        let roles =
            TokenExchangeProvider::extract_roles(&claims, "resource_access.sqe.roles");
        assert_eq!(roles, vec!["viewer", "editor"]);
    }

    #[test]
    fn extract_roles_missing_claim() {
        let claims = serde_json::json!({ "sub": "alice" });
        let roles = TokenExchangeProvider::extract_roles(&claims, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_partial_path() {
        let claims = serde_json::json!({
            "realm_access": { "other": "value" }
        });
        let roles = TokenExchangeProvider::extract_roles(&claims, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_skips_non_string_values() {
        let claims = serde_json::json!({
            "roles": ["admin", 42, null, "user"]
        });
        let roles = TokenExchangeProvider::extract_roles(&claims, "roles");
        assert_eq!(roles, vec!["admin", "user"]);
    }

    // -----------------------------------------------------------------------
    // navigate_claim
    // -----------------------------------------------------------------------

    #[test]
    fn navigate_claim_single_segment() {
        let val = serde_json::json!({ "sub": "alice" });
        assert_eq!(navigate_claim(&val, "sub").unwrap(), &serde_json::json!("alice"));
    }

    #[test]
    fn navigate_claim_multi_segment() {
        let val = serde_json::json!({ "a": { "b": { "c": 42 } } });
        assert_eq!(navigate_claim(&val, "a.b.c").unwrap(), &serde_json::json!(42));
    }

    #[test]
    fn navigate_claim_missing() {
        let val = serde_json::json!({ "x": 1 });
        assert!(navigate_claim(&val, "y").is_none());
    }

    // -----------------------------------------------------------------------
    // extract_subject_token
    // -----------------------------------------------------------------------

    #[test]
    fn subject_token_from_bearer() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("my-bearer-token".to_string())),
            password: Some(sqe_core::SecretString::new("my-password".to_string())),
            ..Default::default()
        };
        // bearer_token takes priority.
        assert_eq!(
            TokenExchangeProvider::extract_subject_token(&creds),
            Some("my-bearer-token".to_string())
        );
    }

    #[test]
    fn subject_token_from_password() {
        let creds = FlightCredentials {
            bearer_token: None,
            password: Some(sqe_core::SecretString::new("my-password-token".to_string())),
            ..Default::default()
        };
        assert_eq!(
            TokenExchangeProvider::extract_subject_token(&creds),
            Some("my-password-token".to_string())
        );
    }

    #[test]
    fn subject_token_empty_bearer_falls_back_to_password() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::default()),
            password: Some(sqe_core::SecretString::new("fallback-password".to_string())),
            ..Default::default()
        };
        assert_eq!(
            TokenExchangeProvider::extract_subject_token(&creds),
            Some("fallback-password".to_string())
        );
    }

    #[test]
    fn subject_token_none_when_no_credentials() {
        let creds = FlightCredentials::default();
        assert!(TokenExchangeProvider::extract_subject_token(&creds).is_none());
    }

    #[test]
    fn subject_token_none_when_both_empty() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::default()),
            password: Some(sqe_core::SecretString::default()),
            ..Default::default()
        };
        assert!(TokenExchangeProvider::extract_subject_token(&creds).is_none());
    }

    // -----------------------------------------------------------------------
    // authenticate: credential detection
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn authenticate_returns_not_my_credentials_when_no_subject_token() {
        let config = TokenExchangeConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = TokenExchangeProvider::new(config).unwrap();

        let creds = FlightCredentials::default();
        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    #[tokio::test]
    async fn authenticate_returns_not_my_credentials_when_only_username() {
        let config = TokenExchangeConfig {
            token_url: "http://localhost:8080/token".to_string(),
            client_id: "sqe".to_string(),
            ..Default::default()
        };
        let provider = TokenExchangeProvider::new(config).unwrap();

        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            ..Default::default()
        };
        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    // -----------------------------------------------------------------------
    // Full claims extraction from mock JWT
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // identity_from_exchanged_token: never trusts client-supplied input (#39)
    // -----------------------------------------------------------------------

    #[test]
    fn identity_from_exchanged_token_rejects_undecodable_jwt() {
        // The IdP's response was syntactically a JSON object with an
        // `access_token` field but the token itself is malformed. Before
        // the fix this would have fallen back to credentials.username;
        // now it must refuse.
        let result = TokenExchangeProvider::identity_from_exchanged_token(
            "not-a-jwt",
            "subject-token",
            "sub",
            "realm_access.roles",
        );
        assert!(
            matches!(result, Err(AuthError::Internal(_))),
            "decode failure must produce AuthError::Internal, got {result:?}"
        );
    }

    #[test]
    fn identity_from_exchanged_token_rejects_missing_user_claim() {
        // The token decodes, but the configured user claim is absent.
        // We refuse rather than label the session "unknown" or fall back
        // to the client-supplied username.
        let claims = serde_json::json!({
            "name": "Alice",
            "realm_access": { "roles": ["analyst"] }
        });
        let token = fake_jwt(&claims);
        let result = TokenExchangeProvider::identity_from_exchanged_token(
            &token,
            "subject-token",
            "sub",
            "realm_access.roles",
        );
        assert!(
            matches!(result, Err(AuthError::AuthFailed(_))),
            "missing user claim must produce AuthError::AuthFailed, got {result:?}"
        );
    }

    #[test]
    fn identity_from_exchanged_token_accepts_valid_jwt() {
        let claims = serde_json::json!({
            "sub": "alice",
            "realm_access": { "roles": ["analyst", "writer"] }
        });
        let token = fake_jwt(&claims);
        let identity = TokenExchangeProvider::identity_from_exchanged_token(
            &token,
            "subject-token",
            "sub",
            "realm_access.roles",
        )
        .expect("valid JWT yields identity");
        assert_eq!(identity.user_id, "alice");
        assert_eq!(identity.display_name, "alice");
        assert_eq!(identity.roles, vec!["analyst", "writer"]);
        assert_eq!(
            identity.catalog_token.as_ref().map(|s| s.expose()),
            Some(token.as_str())
        );
        assert_eq!(
            identity.refresh_token.as_ref().map(|s| s.expose()),
            Some("subject-token")
        );
    }

    #[test]
    fn identity_from_exchanged_token_ignores_username_fallback() {
        // Regression for #39: even when the exchanged JWT fails to
        // decode, we must NOT take user_id from any caller-controlled
        // source. The contract is that the helper does not even see the
        // FlightCredentials struct.
        let result = TokenExchangeProvider::identity_from_exchanged_token(
            "garbage.garbage.garbage",
            "subject-token",
            "sub",
            "realm_access.roles",
        );
        match result {
            Err(AuthError::Internal(e)) => {
                let msg = e.to_string();
                assert!(
                    !msg.to_lowercase().contains("alice"),
                    "error must not leak any user identifier, got: {msg}"
                );
            }
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    #[test]
    fn full_claims_extraction_from_mock_jwt() {
        let claims = serde_json::json!({
            "sub": "user-42",
            "preferred_username": "alice@acme.com",
            "realm_access": {
                "roles": ["analyst", "writer"]
            }
        });
        let token = fake_jwt(&claims);

        // Decode and extract with default config
        let decoded = TokenExchangeProvider::decode_jwt_payload(&token).unwrap();

        let user_id = TokenExchangeProvider::extract_user_claim(&decoded, "sub");
        assert_eq!(user_id, Some("user-42".to_string()));

        let alt_user = TokenExchangeProvider::extract_user_claim(&decoded, "preferred_username");
        assert_eq!(alt_user, Some("alice@acme.com".to_string()));

        let roles = TokenExchangeProvider::extract_roles(&decoded, "realm_access.roles");
        assert_eq!(roles, vec!["analyst", "writer"]);
    }
}
