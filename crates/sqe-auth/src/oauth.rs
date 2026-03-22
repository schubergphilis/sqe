use serde::Deserialize;
use tracing::debug;

/// Token response from a generic OAuth2 token endpoint (client_credentials grant).
///
/// This is intentionally separate from `oidc_password::TokenResponse` — the two
/// endpoints can return different shapes and we don't want to couple them.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: u64,
    pub token_type: String,
}

/// A minimal OAuth2 client that obtains tokens via the `client_credentials` grant.
///
/// Used when the engine is configured with a generic `token_endpoint` (e.g. Polaris)
/// instead of Keycloak ROPC.
pub struct OAuthClient {
    client: reqwest::Client,
    token_endpoint: String,
    client_id: String,
    client_secret: String,
}

impl OAuthClient {
    pub fn new(
        token_endpoint: &str,
        client_id: &str,
        client_secret: &str,
        accept_invalid_certs: bool,
    ) -> sqe_core::Result<Self> {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to build OAuth HTTP client: {e}"))
            })?;

        Ok(Self {
            client,
            token_endpoint: token_endpoint.to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
    }

    /// Obtain an access token via the OAuth2 `client_credentials` grant.
    ///
    /// The returned token is a bearer token scoped to `PRINCIPAL_ROLE:ALL`.
    pub async fn get_token(&self) -> sqe_core::Result<TokenResponse> {
        debug!(
            endpoint = self.token_endpoint,
            "Requesting token via client_credentials grant"
        );

        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("scope", "PRINCIPAL_ROLE:ALL"),
        ];

        let response = self
            .client
            .post(&self.token_endpoint)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("OAuth token request failed: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "unable to read body".to_string());
            return Err(sqe_core::SqeError::Auth(format!(
                "OAuth token endpoint returned {status}: {body}"
            )));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to parse OAuth token response: {e}"))
            })
    }
}
