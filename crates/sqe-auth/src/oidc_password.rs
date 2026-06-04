use std::time::Duration;

use base64::Engine;
use serde::Deserialize;
use sqe_core::SecretString;
use sqe_core::config::AuthConfig;
use tracing::{debug, warn};

use crate::provider::truncate_for_log;

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: u64,
    pub token_type: String,
}

pub struct OidcPasswordClient {
    client: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: SecretString,
    /// Dot-separated JSON path to the roles array in the JWT payload. The
    /// legacy default `realm_access.roles` is the Keycloak shape; Auth0,
    /// Okta, and AzureAD typically use `groups`, a `cognito:groups`-style
    /// path, or a custom claim. Without this plumbing those users
    /// authenticated successfully but got `roles = []` and every
    /// role-gated policy denied them (issue #13).
    roles_claim: String,
}

impl OidcPasswordClient {
    pub fn new(config: &AuthConfig) -> sqe_core::Result<Self> {
        let token_url = format!(
            "{}/realms/{}/protocol/openid-connect/token",
            config.keycloak_url.trim_end_matches('/'),
            config.realm
        );

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(config.should_skip_tls_verify())
            .build()
            .map_err(|e| sqe_core::SqeError::Auth(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            client,
            token_url,
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            roles_claim: config.roles_claim.clone(),
        })
    }

    pub async fn exchange_credentials(
        &self,
        username: &str,
        password: &str,
    ) -> sqe_core::Result<TokenResponse> {
        debug!(username = username, "Exchanging credentials via ROPC grant");

        let params = [
            ("grant_type", "password"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.expose()),
            ("username", username),
            ("password", password),
        ];

        let response = self
            .client
            .post(&self.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Token request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "OIDC provider rejected credentials");
            return Err(sqe_core::SqeError::Auth(
                "Authentication failed".to_string(),
            ));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Failed to parse token response: {e}")))
    }

    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> sqe_core::Result<TokenResponse> {
        debug!("Refreshing token via OIDC provider");

        let params = [
            ("grant_type", "refresh_token"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.expose()),
            ("refresh_token", refresh_token),
        ];

        let response = self
            .client
            .post(&self.token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Auth(format!("Token refresh request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            let body = truncate_for_log(&body, 500);
            warn!(status = %status, body = %body, "OIDC provider rejected token refresh");
            return Err(sqe_core::SqeError::Auth(
                "Authentication failed".to_string(),
            ));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to parse refresh response: {e}"))
            })
    }

    /// Decode JWT payload without signature verification (OIDC provider
    /// already validated) and extract role names from the configured
    /// `roles_claim` path. Default path is `realm_access.roles` (Keycloak
    /// shape); Auth0/Okta/AzureAD callers can point at `groups` or another
    /// custom claim.
    ///
    /// Returns an empty `Vec` for malformed tokens, missing claims, or
    /// non-array role values.
    pub fn extract_roles(&self, access_token: &str) -> Vec<String> {
        let parts: Vec<&str> = access_token.split('.').collect();
        if parts.len() != 3 {
            warn!("Access token is not a valid JWT (expected 3 parts)");
            return Vec::new();
        }

        let payload = match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("Failed to base64-decode JWT payload: {e}");
                return Vec::new();
            }
        };

        let claims: serde_json::Value = match serde_json::from_slice(&payload) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to parse JWT payload as JSON: {e}");
                return Vec::new();
            }
        };

        Self::extract_roles_at_path(&claims, &self.roles_claim)
    }

    /// Walk a dot-separated path through the claim tree and collect role
    /// strings. `realm_access.roles`, `groups`, or
    /// `resource_access.sqe.roles` all work.
    fn extract_roles_at_path(claims: &serde_json::Value, path: &str) -> Vec<String> {
        let mut current = claims;
        for segment in path.split('.') {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use sqe_core::config::AuthConfig;

    /// Build a fake JWT (header.payload.signature) from a JSON claims object.
    fn fake_jwt(claims: &serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(b"{\"alg\":\"RS256\",\"typ\":\"JWT\"}");
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(claims).unwrap());
        let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"fake-sig");
        format!("{header}.{payload}.{signature}")
    }

    fn test_config() -> AuthConfig {
        AuthConfig {
            keycloak_url: "http://localhost:8080".to_string(),
            realm: "test".to_string(),
            client_id: "test-client".to_string(),
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

    fn make_client() -> OidcPasswordClient {
        OidcPasswordClient::new(&test_config()).unwrap()
    }

    #[test]
    fn extract_roles_from_valid_jwt() {
        let client = make_client();
        let claims = serde_json::json!({
            "sub": "user1",
            "realm_access": {
                "roles": ["admin", "user", "data_engineer"]
            }
        });

        let roles = client.extract_roles(&fake_jwt(&claims));
        assert_eq!(roles, vec!["admin", "user", "data_engineer"]);
    }

    #[test]
    fn extract_roles_empty_when_no_realm_access() {
        let client = make_client();
        let claims = serde_json::json!({ "sub": "user1" });

        let roles = client.extract_roles(&fake_jwt(&claims));
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_empty_when_roles_missing() {
        let client = make_client();
        let claims = serde_json::json!({
            "realm_access": { "other": "value" }
        });

        let roles = client.extract_roles(&fake_jwt(&claims));
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_handles_not_a_jwt() {
        let client = make_client();
        let roles = client.extract_roles("not-a-jwt");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_handles_invalid_base64_payload() {
        let client = make_client();
        let roles = client.extract_roles("header.!!!invalid!!!.sig");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_handles_non_json_payload() {
        let client = make_client();
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"not json");
        let token = format!("header.{payload}.sig");
        let roles = client.extract_roles(&token);
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_skips_non_string_role_values() {
        let client = make_client();
        let claims = serde_json::json!({
            "realm_access": {
                "roles": ["admin", 42, null, "user"]
            }
        });

        let roles = client.extract_roles(&fake_jwt(&claims));
        assert_eq!(roles, vec!["admin", "user"]);
    }

    // --- roles_claim plumbing (issue #13 regression tests) ---

    #[test]
    fn extract_roles_honours_flat_groups_path() {
        // Auth0 / Okta / AzureAD shape: roles live at top-level `groups`.
        // Before the roles_claim plumbing landed, this returned [] because
        // the legacy path hardcoded `realm_access.roles`.
        let mut cfg = test_config();
        cfg.roles_claim = "groups".to_string();
        let client = OidcPasswordClient::new(&cfg).unwrap();
        let claims = serde_json::json!({ "groups": ["dba", "analyst"] });
        let roles = client.extract_roles(&fake_jwt(&claims));
        assert_eq!(roles, vec!["dba", "analyst"]);
    }

    #[test]
    fn extract_roles_honours_nested_resource_access_path() {
        // Keycloak resource_access.<client>.roles shape.
        let mut cfg = test_config();
        cfg.roles_claim = "resource_access.sqe.roles".to_string();
        let client = OidcPasswordClient::new(&cfg).unwrap();
        let claims = serde_json::json!({
            "resource_access": {
                "sqe": { "roles": ["viewer"] }
            }
        });
        let roles = client.extract_roles(&fake_jwt(&claims));
        assert_eq!(roles, vec!["viewer"]);
    }

    #[test]
    fn extract_roles_default_claim_still_matches_keycloak() {
        // Existing Keycloak deployments must keep working with the legacy
        // default path.
        let client = make_client();
        let claims = serde_json::json!({
            "realm_access": { "roles": ["admin"] }
        });
        let roles = client.extract_roles(&fake_jwt(&claims));
        assert_eq!(roles, vec!["admin"]);
    }

    #[test]
    fn token_url_construction() {
        let client = make_client();
        assert_eq!(
            client.token_url,
            "http://localhost:8080/realms/test/protocol/openid-connect/token"
        );
    }

    #[test]
    fn token_url_strips_trailing_slash() {
        let mut config = test_config();
        config.keycloak_url = "http://localhost:8080/".to_string();
        let client = OidcPasswordClient::new(&config).unwrap();
        assert_eq!(
            client.token_url,
            "http://localhost:8080/realms/test/protocol/openid-connect/token"
        );
    }
}
