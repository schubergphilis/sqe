use base64::Engine;
use serde::Deserialize;
use sqe_core::config::AuthConfig;
use tracing::{debug, warn};

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
    client_secret: String,
}

impl OidcPasswordClient {
    pub fn new(config: &AuthConfig) -> sqe_core::Result<Self> {
        let token_url = format!(
            "{}/realms/{}/protocol/openid-connect/token",
            config.keycloak_url.trim_end_matches('/'),
            config.realm
        );

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(!config.ssl_verification)
            .build()
            .map_err(|e| sqe_core::SqeError::Auth(format!("Failed to build HTTP client: {e}")))?;

        Ok(Self {
            client,
            token_url,
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
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
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
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
            let body = if body.len() > 500 {
                format!("{}...[truncated]", &body[..500])
            } else {
                body
            };
            return Err(sqe_core::SqeError::Auth(format!(
                "OIDC provider returned {status}: {body}"
            )));
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
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
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
            let body = if body.len() > 500 {
                format!("{}...[truncated]", &body[..500])
            } else {
                body
            };
            return Err(sqe_core::SqeError::Auth(format!(
                "OIDC refresh returned {status}: {body}"
            )));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to parse refresh response: {e}"))
            })
    }

    /// Decode JWT payload without signature verification (OIDC provider already validated).
    /// Extracts `realm_access.roles` from the claims.
    ///
    /// Returns an empty `Vec` for malformed tokens.
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

        claims
            .get("realm_access")
            .and_then(|ra| ra.get("roles"))
            .and_then(|roles| roles.as_array())
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
            client_secret: "secret".to_string(),
            token_endpoint: String::new(),
            token_refresh_buffer_secs: 60,
            ssl_verification: false,
            providers: Vec::new(),
            role_mappings: std::collections::HashMap::new(),
            external: None,
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
