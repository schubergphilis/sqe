//! `OidcClientCredentialsProvider` — per-connection OAuth2 `client_credentials` passthrough.
//!
//! The end-user client presents its own OAuth2 `client_id` and `client_secret`
//! on the connection (Flight Basic auth: `username` = client_id, `password` =
//! client_secret). This provider runs the `client_credentials` grant against the
//! configured token endpoint with those per-connection credentials and forwards
//! the resulting bearer token to the catalog. Each distinct client is a distinct
//! service principal, so authorization is per-connection rather than a single
//! server-baked identity.
//!
//! This is deliberately different from the config-driven `client_credentials`
//! backend (which holds one server-wide `client_id`/`client_secret` and ignores
//! the handshake) and from `OidcM2mProvider` (same, config-held). It is also
//! different from `OidcPasswordProvider`, which runs `grant_type=password` with
//! SQE's own confidential client.
//!
//! Deployment constraint: this provider and `OidcPasswordProvider` both consume
//! `username`/`password`, so they cannot share one listener — a human username
//! would be tried as a `client_id` and rejected. Deploy this provider as the
//! sole username/password credential provider (service-principal-only access).

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use dashmap::DashMap;
use serde::Deserialize;
use tracing::{debug, warn};

use sqe_core::SecretString;

use crate::provider::{truncate_for_log, AuthError, AuthProvider, FlightCredentials, Identity};

/// Configuration for the per-connection `client_credentials` passthrough provider.
///
/// Note the absence of `client_id` / `client_secret`: those arrive per
/// connection on the handshake, not from server config. That is the whole point.
#[derive(Debug, Clone)]
pub struct OidcClientCredentialsConfig {
    /// Full token endpoint URL (e.g.
    /// `http://keycloak:8080/realms/iceberg-sp/protocol/openid-connect/token`).
    pub token_url: String,
    /// Dot-separated JSON path to the roles array in the JWT payload.
    /// Default: `"realm_access.roles"` (Keycloak convention).
    pub roles_claim: String,
    /// JWT claim to extract as the canonical subject identifier.
    /// Default: `"sub"`. Empty disables subject extraction.
    pub subject_claim: String,
    /// Optional OAuth `scope`. Sent only when set. No Polaris-specific default
    /// (the legacy `OAuthClient` hardcodes `PRINCIPAL_ROLE:ALL`, which is wrong
    /// against a Keycloak token endpoint).
    pub scope: Option<String>,
    /// Skip TLS certificate verification (dev/test only).
    pub accept_invalid_certs: bool,
    /// When `true`, a token-endpoint rejection returns `NotMyCredentials`
    /// (defer to the next provider) instead of `AuthFailed`, so this provider
    /// can share a Basic-auth listener with `OidcPasswordProvider`. Infra
    /// errors (connection refused, timeouts) still surface as `Internal` and
    /// stop the chain. (#276)
    pub fallthrough_on_reject: bool,
}

impl Default for OidcClientCredentialsConfig {
    fn default() -> Self {
        Self {
            token_url: String::new(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            scope: None,
            accept_invalid_certs: false,
            fallthrough_on_reject: false,
        }
    }
}

/// Token response from the `client_credentials` grant. No refresh token is
/// issued by this grant type.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: String,
}

/// Per-connection `client_credentials` passthrough provider.
pub struct OidcClientCredentialsProvider {
    client: reqwest::Client,
    config: OidcClientCredentialsConfig,
    /// `client_id` -> `client_secret`, populated on each successful auth so that
    /// `refresh_catalog_token` can re-run the grant for a long-lived session
    /// (the `client_credentials` grant issues no refresh token). Secrets are
    /// wrapped in `SecretString`, never logged or persisted, and only valid
    /// secrets (ones that already succeeded a grant) are ever stored.
    secrets: DashMap<String, SecretString>,
}

impl OidcClientCredentialsProvider {
    /// Create a new provider from configuration.
    pub fn new(config: OidcClientCredentialsConfig) -> Result<Self, AuthError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("Failed to build HTTP client: {e}"))
            })?;
        Ok(Self {
            client,
            config,
            secrets: DashMap::new(),
        })
    }

    /// Run the `client_credentials` grant with the supplied per-connection
    /// credentials.
    async fn fetch_token(
        &self,
        client_id: &str,
        client_secret: &str,
    ) -> Result<TokenResponse, AuthError> {
        debug!(
            client_id = client_id,
            "Exchanging client credentials via client_credentials grant"
        );

        let mut params = vec![
            ("grant_type", "client_credentials".to_string()),
            ("client_id", client_id.to_string()),
            ("client_secret", client_secret.to_string()),
        ];
        if let Some(scope) = self.config.scope.as_deref().filter(|s| !s.is_empty()) {
            params.push(("scope", scope.to_string()));
        }

        let response = self
            .client
            .post(&self.config.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("Token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "Token endpoint rejected client credentials");
            return Err(AuthError::AuthFailed("Authentication failed".to_string()));
        }

        response.json::<TokenResponse>().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("Failed to parse token response: {e}"))
        })
    }

    /// Extract a scalar string claim by dot-separated path from a JWT payload.
    fn extract_claim_str(access_token: &str, claim_path: &str) -> Option<String> {
        let claims = Self::decode_jwt_payload(access_token)?;
        let mut current = &claims;
        for segment in claim_path.split('.') {
            current = current.get(segment)?;
        }
        current.as_str().map(String::from)
    }

    /// Extract roles from a JWT payload using a dot-separated claim path.
    /// Returns an empty Vec for malformed tokens, missing claims, or non-array
    /// values.
    fn extract_roles_from_claim(access_token: &str, roles_claim: &str) -> Vec<String> {
        let claims = match Self::decode_jwt_payload(access_token) {
            Some(c) => c,
            None => return Vec::new(),
        };
        let mut current = &claims;
        for segment in roles_claim.split('.') {
            match current.get(segment) {
                Some(v) => current = v,
                None => return Vec::new(),
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

    /// Decode a JWT payload (base64url JSON) without signature verification.
    /// The token is forwarded to the catalog, which validates it.
    fn decode_jwt_payload(token: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return None;
        }
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .ok()?;
        serde_json::from_slice(&payload).ok()
    }

    /// Build an `Identity` from a freshly issued access token and the connecting
    /// `client_id`.
    fn identity_from_token(
        &self,
        client_id: &str,
        access_token: String,
        expires_in: u64,
    ) -> Identity {
        let roles = Self::extract_roles_from_claim(&access_token, &self.config.roles_claim);
        let subject = if self.config.subject_claim.is_empty() {
            None
        } else {
            Self::extract_claim_str(&access_token, &self.config.subject_claim)
        };
        // The forwarded token carries `preferred_username` for the catalog's
        // principal mapping; surface it as the display name. The SQE-side
        // `user_id` is the connecting `client_id` (stable, known at handshake,
        // and the key under which the secret is cached for refresh).
        let display_name = Self::extract_claim_str(&access_token, "preferred_username")
            .unwrap_or_else(|| client_id.to_string());
        let expires_at =
            chrono::Utc::now().checked_add_signed(chrono::Duration::seconds(expires_in as i64));

        Identity {
            user_id: client_id.to_string(),
            display_name,
            roles,
            subject,
            email: None,
            groups: vec![],
            catalog_token: Some(SecretString::new(access_token)),
            refresh_token: None,
            expires_at,
        }
    }
}

#[async_trait]
impl AuthProvider for OidcClientCredentialsProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // username = client_id, password = client_secret.
        let client_id = match &credentials.username {
            Some(u) if !u.is_empty() => u.clone(),
            _ => return Err(AuthError::NotMyCredentials),
        };
        let client_secret = match &credentials.password {
            Some(p) if !p.expose().is_empty() => p.expose().to_string(),
            _ => return Err(AuthError::NotMyCredentials),
        };

        // A JWT in the password field belongs to a bearer provider, not here.
        if client_secret.starts_with("eyJ") {
            return Err(AuthError::NotMyCredentials);
        }

        let token = match self.fetch_token(&client_id, &client_secret).await {
            Ok(t) => t,
            // On a clean grant rejection, defer to the next provider when
            // configured for a mixed Basic-auth listener (#276). Infra errors
            // (Internal) still propagate and stop the chain.
            Err(AuthError::AuthFailed(_)) if self.config.fallthrough_on_reject => {
                return Err(AuthError::NotMyCredentials);
            }
            Err(e) => return Err(e),
        };

        // Cache the (validated) secret so refresh can re-run the grant.
        self.secrets
            .insert(client_id.clone(), SecretString::new(client_secret));

        Ok(self.identity_from_token(&client_id, token.access_token, token.expires_in))
    }

    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<SecretString>, AuthError> {
        let secret = match self.secrets.get(&identity.user_id) {
            Some(s) => s.expose().to_string(),
            None => return Ok(None),
        };
        let token = self.fetch_token(&identity.user_id, &secret).await?;
        Ok(Some(SecretString::new(token.access_token)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_jwt(claims: &serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(b"{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(claims).unwrap());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"fake-sig");
        format!("{header}.{payload}.{signature}")
    }

    fn provider_with(token_url: &str) -> OidcClientCredentialsProvider {
        OidcClientCredentialsProvider::new(OidcClientCredentialsConfig {
            token_url: token_url.to_string(),
            ..Default::default()
        })
        .unwrap()
    }

    // --- config defaults ---

    #[test]
    fn config_defaults() {
        let c = OidcClientCredentialsConfig::default();
        assert_eq!(c.roles_claim, "realm_access.roles");
        assert_eq!(c.subject_claim, "sub");
        assert!(c.scope.is_none());
        assert!(!c.accept_invalid_certs);
        assert!(c.token_url.is_empty());
    }

    #[test]
    fn new_succeeds_with_valid_config() {
        assert!(
            OidcClientCredentialsProvider::new(OidcClientCredentialsConfig {
                token_url: "http://localhost:8080/token".to_string(),
                ..Default::default()
            })
            .is_ok()
        );
    }

    // --- credential detection short-circuits (chain-friendly) ---

    #[tokio::test]
    async fn authenticate_skips_when_no_username() {
        let p = provider_with("http://localhost:8080/token");
        let creds = FlightCredentials {
            username: None,
            password: Some(SecretString::new("secret".to_string())),
            ..Default::default()
        };
        assert!(matches!(
            p.authenticate(&creds).await,
            Err(AuthError::NotMyCredentials)
        ));
    }

    #[tokio::test]
    async fn authenticate_skips_when_no_password() {
        let p = provider_with("http://localhost:8080/token");
        let creds = FlightCredentials {
            username: Some("sp-reader".to_string()),
            password: None,
            ..Default::default()
        };
        assert!(matches!(
            p.authenticate(&creds).await,
            Err(AuthError::NotMyCredentials)
        ));
    }

    #[tokio::test]
    async fn authenticate_skips_empty_username() {
        let p = provider_with("http://localhost:8080/token");
        let creds = FlightCredentials {
            username: Some(String::new()),
            password: Some(SecretString::new("secret".to_string())),
            ..Default::default()
        };
        assert!(matches!(
            p.authenticate(&creds).await,
            Err(AuthError::NotMyCredentials)
        ));
    }

    #[tokio::test]
    async fn authenticate_skips_when_password_looks_like_jwt() {
        let p = provider_with("http://localhost:8080/token");
        let creds = FlightCredentials {
            username: Some("sp-reader".to_string()),
            password: Some(SecretString::new(
                "eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string(),
            )),
            ..Default::default()
        };
        assert!(matches!(
            p.authenticate(&creds).await,
            Err(AuthError::NotMyCredentials)
        ));
    }

    #[tokio::test]
    async fn authenticate_grant_failure_is_internal_on_unreachable_endpoint() {
        // Port 1 is closed: the grant request fails to connect -> Internal,
        // not AuthFailed (which is reserved for a 4xx from the endpoint).
        let p = provider_with("http://127.0.0.1:1/token");
        let creds = FlightCredentials {
            username: Some("sp-reader".to_string()),
            password: Some(SecretString::new("a-secret".to_string())),
            ..Default::default()
        };
        match p.authenticate(&creds).await {
            Err(AuthError::Internal(_)) => {}
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    // --- claim extraction ---

    #[test]
    fn extract_roles_default_claim_path() {
        let token = fake_jwt(&serde_json::json!({
            "realm_access": { "roles": ["sqe_reader", "service"] }
        }));
        let roles =
            OidcClientCredentialsProvider::extract_roles_from_claim(&token, "realm_access.roles");
        assert_eq!(roles, vec!["sqe_reader", "service"]);
    }

    #[test]
    fn extract_roles_missing_claim_is_empty() {
        let token = fake_jwt(&serde_json::json!({ "sub": "x" }));
        let roles =
            OidcClientCredentialsProvider::extract_roles_from_claim(&token, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_skips_non_string_values() {
        let token = fake_jwt(&serde_json::json!({ "roles": ["a", 1, null, "b"] }));
        let roles = OidcClientCredentialsProvider::extract_roles_from_claim(&token, "roles");
        assert_eq!(roles, vec!["a", "b"]);
    }

    #[test]
    fn extract_claim_str_reads_preferred_username() {
        let token = fake_jwt(&serde_json::json!({ "preferred_username": "sp-reader" }));
        assert_eq!(
            OidcClientCredentialsProvider::extract_claim_str(&token, "preferred_username"),
            Some("sp-reader".to_string())
        );
    }

    #[test]
    fn extract_claim_str_not_a_jwt_is_none() {
        assert_eq!(
            OidcClientCredentialsProvider::extract_claim_str("garbage", "sub"),
            None
        );
    }

    // --- Identity construction ---

    #[test]
    fn identity_uses_client_id_as_user_id_and_preferred_username_as_display() {
        let p = provider_with("http://localhost:8080/token");
        let token = fake_jwt(&serde_json::json!({
            "sub": "service-account-sp-reader",
            "preferred_username": "sp-reader",
            "realm_access": { "roles": ["sqe_reader"] }
        }));
        let id = p.identity_from_token("client-xyz", token, 300);
        assert_eq!(id.user_id, "client-xyz");
        assert_eq!(id.display_name, "sp-reader");
        assert_eq!(id.subject.as_deref(), Some("service-account-sp-reader"));
        assert_eq!(id.roles, vec!["sqe_reader"]);
        assert!(id.catalog_token.is_some());
        assert!(id.refresh_token.is_none());
        assert!(id.expires_at.is_some());
    }

    #[test]
    fn identity_display_name_falls_back_to_client_id() {
        let p = provider_with("http://localhost:8080/token");
        let token = fake_jwt(&serde_json::json!({ "sub": "s" }));
        let id = p.identity_from_token("client-xyz", token, 300);
        assert_eq!(id.display_name, "client-xyz");
    }

    // --- refresh ---

    #[tokio::test]
    async fn refresh_returns_none_when_no_cached_secret() {
        let p = provider_with("http://localhost:8080/token");
        let id = Identity {
            user_id: "unknown-client".to_string(),
            display_name: "unknown-client".to_string(),
            roles: vec![],
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };
        assert!(p.refresh_catalog_token(&id).await.unwrap().is_none());
    }

    // --- #276: fallthrough_on_reject (mixed Basic-auth listener) ---

    /// Mock token endpoint that rejects every grant with HTTP 401.
    async fn start_rejecting_token_server() -> (tokio::task::JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/token");
        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let body = "{\"error\":\"invalid_client\"}";
                let resp = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });
        (handle, url)
    }

    fn provider_fallthrough(token_url: &str, fallthrough: bool) -> OidcClientCredentialsProvider {
        OidcClientCredentialsProvider::new(OidcClientCredentialsConfig {
            token_url: token_url.to_string(),
            fallthrough_on_reject: fallthrough,
            ..Default::default()
        })
        .unwrap()
    }

    fn basic(user: &str, pass: &str) -> FlightCredentials {
        FlightCredentials {
            username: Some(user.to_string()),
            password: Some(SecretString::new(pass.to_string())),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn reject_defers_when_fallthrough_enabled() {
        // A human ROPC credential hitting this SP provider: the grant is
        // rejected, and with fallthrough it defers so the chain can try ROPC.
        let (_h, url) = start_rejecting_token_server().await;
        let p = provider_fallthrough(&url, true);
        assert!(matches!(
            p.authenticate(&basic("alice", "human-password")).await,
            Err(AuthError::NotMyCredentials)
        ));
    }

    #[tokio::test]
    async fn reject_fails_hard_when_fallthrough_disabled() {
        // Default behavior is unchanged: a rejection is a hard AuthFailed.
        let (_h, url) = start_rejecting_token_server().await;
        let p = provider_fallthrough(&url, false);
        assert!(matches!(
            p.authenticate(&basic("sp", "wrong-secret")).await,
            Err(AuthError::AuthFailed(_))
        ));
    }

    #[tokio::test]
    async fn infra_error_still_stops_even_with_fallthrough() {
        // Connection refused -> Internal, never NotMyCredentials, so an IdP
        // outage stops the chain instead of silently falling through.
        let p = provider_fallthrough("http://127.0.0.1:1/token", true);
        assert!(matches!(
            p.authenticate(&basic("sp", "secret")).await,
            Err(AuthError::Internal(_))
        ));
    }
}
