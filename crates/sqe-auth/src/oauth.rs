use serde::Deserialize;
use tracing::{debug, warn};

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
    /// Optional OAuth `scope` parameter. When `None`, defaults to
    /// `PRINCIPAL_ROLE:ALL` (legacy Polaris compatibility).
    scope: Option<String>,
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
            scope: None,
        })
    }

    /// Override the OAuth `scope` parameter. Use this when a catalog needs
    /// a reduced or alternative scope (e.g. `PRINCIPAL_ROLE:READ_ONLY`)
    /// rather than the default `PRINCIPAL_ROLE:ALL`. Silently dropping a
    /// caller-supplied scope used to broaden every catalog's effective
    /// rights to ALL — see issue #17.
    #[must_use = "with_scope consumes self; bind the returned client"]
    pub fn with_scope(mut self, scope: Option<String>) -> Self {
        self.scope = scope.filter(|s| !s.is_empty());
        self
    }

    /// Obtain an access token via the OAuth2 `client_credentials` grant.
    ///
    /// The returned token is a bearer token scoped to whatever `scope` was
    /// configured (default `PRINCIPAL_ROLE:ALL`).
    pub async fn get_token(&self) -> sqe_core::Result<TokenResponse> {
        debug!(
            endpoint = self.token_endpoint,
            "Requesting token via client_credentials grant"
        );

        let scope = self.scope.as_deref().unwrap_or("PRINCIPAL_ROLE:ALL");
        let params = [
            ("grant_type", "client_credentials"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("scope", scope),
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
            warn!(status = %status, body = %body, "OAuth token endpoint rejected credentials");
            return Err(sqe_core::SqeError::Auth(
                "Authentication failed".to_string(),
            ));
        }

        response
            .json::<TokenResponse>()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Auth(format!("Failed to parse OAuth token response: {e}"))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // OAuthClient construction
    // -------------------------------------------------------------------------

    /// `OAuthClient::new` must succeed with valid parameters (no HTTP call is made).
    #[test]
    fn new_succeeds_with_valid_parameters() {
        let result = OAuthClient::new(
            "http://localhost:8181/api/catalog/v1/oauth/tokens",
            "polaris-client",
            "polaris-secret",
            false, // accept_invalid_certs
        );
        assert!(result.is_ok(), "Expected Ok, got {:?}", result.err());
    }

    /// `OAuthClient::new` with `accept_invalid_certs = true` must also succeed.
    #[test]
    fn new_succeeds_with_invalid_certs_accepted() {
        let result = OAuthClient::new(
            "https://internal.corp/oauth/token",
            "client",
            "secret",
            true, // skip TLS verification
        );
        assert!(result.is_ok(), "Expected Ok with accept_invalid_certs=true");
    }

    /// Constructing an `OAuthClient` with an empty token endpoint is allowed —
    /// the URL is only used when `get_token()` is called, not during construction.
    #[test]
    fn new_allows_empty_token_endpoint() {
        let result = OAuthClient::new("", "client", "secret", false);
        assert!(
            result.is_ok(),
            "Construction should succeed even with an empty endpoint"
        );
    }

    /// Constructing an `OAuthClient` with empty credentials is allowed — the
    /// server rejects bad creds, not the client constructor.
    #[test]
    fn new_allows_empty_credentials() {
        let result = OAuthClient::new("http://localhost/token", "", "", false);
        assert!(
            result.is_ok(),
            "Construction should succeed even with empty client_id/secret"
        );
    }

    // -------------------------------------------------------------------------
    // TokenResponse deserialization
    // -------------------------------------------------------------------------

    /// `TokenResponse` must deserialize a standard OAuth2 JSON body correctly.
    #[test]
    fn token_response_deserialises_standard_json() {
        let json = r#"{
            "access_token": "eyJhbGciOiJSUzI1NiJ9.payload.sig",
            "expires_in": 3600,
            "token_type": "Bearer"
        }"#;

        let response: TokenResponse =
            serde_json::from_str(json).expect("should deserialise standard response");

        assert_eq!(
            response.access_token,
            "eyJhbGciOiJSUzI1NiJ9.payload.sig"
        );
        assert_eq!(response.expires_in, 3600);
        assert_eq!(response.token_type, "Bearer");
    }

    /// `TokenResponse` with lowercase `bearer` must also deserialize correctly.
    #[test]
    fn token_response_deserialises_lowercase_bearer() {
        let json = r#"{
            "access_token": "tok123",
            "expires_in": 1800,
            "token_type": "bearer"
        }"#;

        let response: TokenResponse =
            serde_json::from_str(json).expect("should deserialise lowercase bearer");
        assert_eq!(response.token_type, "bearer");
        assert_eq!(response.expires_in, 1800);
    }

    /// `TokenResponse` missing `access_token` must fail deserialization.
    #[test]
    fn token_response_fails_without_access_token() {
        let json = r#"{"expires_in": 3600, "token_type": "Bearer"}"#;
        let result: Result<TokenResponse, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Deserialization should fail when access_token is missing"
        );
    }

    /// `TokenResponse` missing `expires_in` must fail deserialization.
    #[test]
    fn token_response_fails_without_expires_in() {
        let json = r#"{"access_token": "tok", "token_type": "Bearer"}"#;
        let result: Result<TokenResponse, _> = serde_json::from_str(json);
        assert!(
            result.is_err(),
            "Deserialization should fail when expires_in is missing"
        );
    }
}
