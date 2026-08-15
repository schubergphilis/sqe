//! RFC 8628 Device Authorization Grant — start a device code flow and poll for tokens.

use std::sync::Arc;

use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::oidc_discovery::OidcDiscovery;
use crate::pending_auth::TokenSet;
use crate::provider::AuthError;

// ── Public types ──────────────────────────────────────────────────────────────

/// Session state returned by the device authorization endpoint.
///
/// The caller should display `user_code` and `verification_uri` to the user,
/// then call [`DeviceCodeService::poll`] at `interval`-second intervals until
/// a terminal result is returned.
#[derive(Debug, Clone)]
pub struct DeviceAuthSession {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    /// Complete URI with `user_code` embedded (convenience for QR codes).
    pub verification_uri_complete: Option<String>,
    /// Seconds until the device code expires.
    pub expires_in: u64,
    /// Minimum polling interval in seconds.
    pub interval: u64,
}

/// Result of a single poll against the token endpoint.
#[derive(Debug)]
pub enum DevicePollResult {
    /// User has not yet authorized — poll again after `interval` seconds.
    Pending,
    /// Server asked the client to slow down — increase the polling interval.
    SlowDown,
    /// Authorization was granted; tokens are enclosed.
    Complete(TokenSet),
    /// The user denied the request.
    AccessDenied,
    /// The device code has expired; restart the flow.
    ExpiredToken,
}

// ── Internal serde types ──────────────────────────────────────────────────────

/// Raw JSON body returned by the device authorization endpoint.
#[derive(Debug, Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: u64,
    #[serde(default = "default_interval")]
    interval: u64,
}

fn default_interval() -> u64 {
    5
}

/// Successful token response from the token endpoint.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default = "default_expires_in")]
    expires_in: u64,
}

fn default_expires_in() -> u64 {
    3600
}

/// Error response from the token endpoint (RFC 6749 §5.2 / RFC 8628 §3.5).
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

// ── Service ───────────────────────────────────────────────────────────────────

/// Drives the RFC 8628 Device Authorization Grant.
///
/// # Usage
/// ```ignore
/// let svc = DeviceCodeService::new(discovery, "my-client".to_string(), vec!["openid".to_string()]);
/// let session = svc.start().await?;
/// // show session.user_code and session.verification_uri to the user
/// loop {
///     tokio::time::sleep(Duration::from_secs(session.interval)).await;
///     match svc.poll(&session.device_code).await? {
///         DevicePollResult::Pending   => continue,
///         DevicePollResult::SlowDown  => { /* increase interval */ continue }
///         DevicePollResult::Complete(tokens) => { /* use tokens */ break }
///         DevicePollResult::AccessDenied  => { /* user denied */ break }
///         DevicePollResult::ExpiredToken  => { /* restart flow */ break }
///     }
/// }
/// ```
pub struct DeviceCodeService {
    discovery: Arc<OidcDiscovery>,
    client_id: String,
    scopes: Vec<String>,
    http: reqwest::Client,
}

impl DeviceCodeService {
    /// Create a new `DeviceCodeService`.
    ///
    /// `scopes` should include at least `"openid"`.  The values are joined with
    /// a space character before being sent to the IdP.
    pub fn new(discovery: Arc<OidcDiscovery>, client_id: String, scopes: Vec<String>) -> Self {
        Self {
            discovery,
            client_id,
            scopes,
            http: reqwest::Client::new(),
        }
    }

    /// POST to the device authorization endpoint and return a `DeviceAuthSession`.
    pub async fn start(&self) -> Result<DeviceAuthSession, AuthError> {
        let endpoint = self.discovery.device_authorization_endpoint().await?;
        let scope = self.scopes.join(" ");

        debug!(endpoint, client_id = %self.client_id, scope, "Starting device authorization flow");

        let resp = self
            .http
            .post(endpoint)
            .form(&[("client_id", self.client_id.as_str()), ("scope", &scope)])
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("device_authorization POST failed: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(status = %status, body = %body, "Device authorization endpoint rejected request");
            return Err(AuthError::Internal(anyhow::anyhow!(
                "device_authorization endpoint returned HTTP {status}"
            )));
        }

        let dar: DeviceAuthResponse = resp.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!(
                "device_authorization response parse failed: {e}"
            ))
        })?;

        info!(
            user_code = %dar.user_code,
            verification_uri = %dar.verification_uri,
            expires_in = dar.expires_in,
            interval = dar.interval,
            "Device authorization started"
        );

        Ok(DeviceAuthSession {
            device_code: dar.device_code,
            user_code: dar.user_code,
            verification_uri: dar.verification_uri,
            verification_uri_complete: dar.verification_uri_complete,
            expires_in: dar.expires_in,
            interval: dar.interval,
        })
    }

    /// POST to the token endpoint with the device code and return a poll result.
    ///
    /// This must be called at least `interval` seconds apart.  The caller is
    /// responsible for timing; this method does not sleep.
    pub async fn poll(&self, device_code: &str) -> Result<DevicePollResult, AuthError> {
        let endpoint = self.discovery.token_endpoint().await?;

        debug!(endpoint, client_id = %self.client_id, "Polling device code token endpoint");

        let resp = self
            .http
            .post(endpoint)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", device_code),
                ("client_id", &self.client_id),
            ])
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("device code token POST failed: {e}"))
            })?;

        if resp.status().is_success() {
            let tr: TokenResponse = resp.json().await.map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("token response parse failed: {e}"))
            })?;

            info!(
                expires_in = tr.expires_in,
                "Device code flow complete — token issued"
            );

            return Ok(DevicePollResult::Complete(TokenSet {
                access_token: tr.access_token,
                id_token: tr.id_token,
                refresh_token: tr.refresh_token,
                expires_in: tr.expires_in,
            }));
        }

        // Non-success — parse the error body (RFC 6749 §5.2 / RFC 8628 §3.5).
        let body = resp.text().await.unwrap_or_default();
        let err: ErrorResponse = serde_json::from_str(&body).map_err(|e| {
            AuthError::Internal(anyhow::anyhow!(
                "token error response parse failed: {e}; body: {body}"
            ))
        })?;

        debug!(error = %err.error, description = ?err.error_description, "Device code poll returned error");

        match err.error.as_str() {
            "authorization_pending" => Ok(DevicePollResult::Pending),
            "slow_down" => Ok(DevicePollResult::SlowDown),
            "access_denied" => Ok(DevicePollResult::AccessDenied),
            "expired_token" => Ok(DevicePollResult::ExpiredToken),
            other => Err(AuthError::AuthFailed(format!(
                "{other}: {}",
                err.error_description.unwrap_or_default()
            ))),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── DeviceAuthResponse ────────────────────────────────────────────────────

    #[test]
    fn parse_device_auth_response_full() {
        let json = serde_json::json!({
            "device_code": "dev-code-abc",
            "user_code": "ABCD-1234",
            "verification_uri": "https://idp.example.com/activate",
            "verification_uri_complete": "https://idp.example.com/activate?user_code=ABCD-1234",
            "expires_in": 1800,
            "interval": 5
        });
        let dar: DeviceAuthResponse = serde_json::from_value(json).unwrap();
        assert_eq!(dar.device_code, "dev-code-abc");
        assert_eq!(dar.user_code, "ABCD-1234");
        assert_eq!(dar.verification_uri, "https://idp.example.com/activate");
        assert_eq!(
            dar.verification_uri_complete.as_deref(),
            Some("https://idp.example.com/activate?user_code=ABCD-1234")
        );
        assert_eq!(dar.expires_in, 1800);
        assert_eq!(dar.interval, 5);
    }

    #[test]
    fn parse_device_auth_response_minimal() {
        // `verification_uri_complete` is optional; `interval` defaults to 5.
        let json = serde_json::json!({
            "device_code": "dev-code-xyz",
            "user_code": "EFGH-5678",
            "verification_uri": "https://idp.example.com/activate",
            "expires_in": 600
        });
        let dar: DeviceAuthResponse = serde_json::from_value(json).unwrap();
        assert_eq!(dar.device_code, "dev-code-xyz");
        assert!(dar.verification_uri_complete.is_none());
        assert_eq!(dar.interval, 5); // default
    }

    // ── TokenResponse ─────────────────────────────────────────────────────────

    #[test]
    fn parse_token_response_full() {
        let json = serde_json::json!({
            "access_token": "at.access",
            "id_token": "at.id",
            "refresh_token": "at.refresh",
            "expires_in": 3600
        });
        let tr: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(tr.access_token, "at.access");
        assert_eq!(tr.id_token.as_deref(), Some("at.id"));
        assert_eq!(tr.refresh_token.as_deref(), Some("at.refresh"));
        assert_eq!(tr.expires_in, 3600);
    }

    #[test]
    fn parse_token_response_minimal() {
        // `id_token`, `refresh_token` are optional; `expires_in` defaults to 3600.
        let json = serde_json::json!({
            "access_token": "at.only"
        });
        let tr: TokenResponse = serde_json::from_value(json).unwrap();
        assert_eq!(tr.access_token, "at.only");
        assert!(tr.id_token.is_none());
        assert!(tr.refresh_token.is_none());
        assert_eq!(tr.expires_in, 3600); // default
    }

    // ── ErrorResponse ─────────────────────────────────────────────────────────

    #[test]
    fn parse_error_response_with_description() {
        let json = serde_json::json!({
            "error": "access_denied",
            "error_description": "The user denied the request"
        });
        let er: ErrorResponse = serde_json::from_value(json).unwrap();
        assert_eq!(er.error, "access_denied");
        assert_eq!(
            er.error_description.as_deref(),
            Some("The user denied the request")
        );
    }

    #[test]
    fn parse_error_response_without_description() {
        let json = serde_json::json!({
            "error": "authorization_pending"
        });
        let er: ErrorResponse = serde_json::from_value(json).unwrap();
        assert_eq!(er.error, "authorization_pending");
        assert!(er.error_description.is_none());
    }
}
