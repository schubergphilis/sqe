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

pub struct KeycloakClient {
    client: reqwest::Client,
    token_url: String,
    client_id: String,
    client_secret: String,
}

impl KeycloakClient {
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
            return Err(sqe_core::SqeError::Auth(format!(
                "Keycloak returned {status}: {body}"
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
        debug!("Refreshing token via Keycloak");

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
            return Err(sqe_core::SqeError::Auth(format!(
                "Keycloak refresh returned {status}: {body}"
            )));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to parse refresh response: {e}"))
            })
    }

    /// Decode JWT payload without signature verification (Keycloak already validated).
    /// Extracts `realm_access.roles` from the claims.
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
