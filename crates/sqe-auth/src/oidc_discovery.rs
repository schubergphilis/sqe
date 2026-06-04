//! OIDC Discovery — fetch and cache `.well-known/openid-configuration`.

use std::time::Duration;

use serde::Deserialize;
use tracing::{info, warn};

use crate::provider::AuthError;

#[derive(Debug, Clone, Deserialize)]
pub struct DiscoveredEndpoints {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub device_authorization_endpoint: Option<String>,
    pub jwks_uri: String,
    pub issuer: String,
    #[serde(default)]
    pub userinfo_endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OidcDiscoveryConfig {
    pub issuer: String,
    pub authorization_endpoint_override: Option<String>,
    pub token_endpoint_override: Option<String>,
    pub device_authorization_endpoint_override: Option<String>,
    pub accept_invalid_certs: bool,
}

pub struct OidcDiscovery {
    config: OidcDiscoveryConfig,
    endpoints: tokio::sync::OnceCell<DiscoveredEndpoints>,
    http: reqwest::Client,
}

impl OidcDiscovery {
    pub fn new(config: OidcDiscoveryConfig) -> Result<Self, AuthError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .map_err(|e| AuthError::Internal(e.into()))?;
        Ok(Self {
            config,
            endpoints: tokio::sync::OnceCell::new(),
            http,
        })
    }

    pub async fn endpoints(&self) -> Result<&DiscoveredEndpoints, AuthError> {
        self.endpoints
            .get_or_try_init(|| self.fetch_and_apply_overrides())
            .await
    }

    async fn fetch_and_apply_overrides(&self) -> Result<DiscoveredEndpoints, AuthError> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            self.config.issuer.trim_end_matches('/')
        );
        info!(url = %url, "Fetching OIDC discovery document");

        let resp = self.http.get(&url).send().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("OIDC discovery fetch failed: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(AuthError::Internal(anyhow::anyhow!(
                "OIDC discovery returned HTTP {}", resp.status()
            )));
        }

        let mut endpoints: DiscoveredEndpoints = resp.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("OIDC discovery parse failed: {e}"))
        })?;

        if let Some(ref ep) = self.config.authorization_endpoint_override {
            endpoints.authorization_endpoint = ep.clone();
        }
        if let Some(ref ep) = self.config.token_endpoint_override {
            endpoints.token_endpoint = ep.clone();
        }
        if let Some(ref ep) = self.config.device_authorization_endpoint_override {
            endpoints.device_authorization_endpoint = Some(ep.clone());
        }

        if endpoints.device_authorization_endpoint.is_none() {
            warn!("IdP does not advertise device_authorization_endpoint — device code flow unavailable");
        }

        info!(
            issuer = %endpoints.issuer,
            authorization_endpoint = %endpoints.authorization_endpoint,
            token_endpoint = %endpoints.token_endpoint,
            device_authorization_endpoint = ?endpoints.device_authorization_endpoint,
            "OIDC discovery complete"
        );

        Ok(endpoints)
    }

    pub async fn device_authorization_endpoint(&self) -> Result<&str, AuthError> {
        let ep = self.endpoints().await?;
        ep.device_authorization_endpoint.as_deref().ok_or_else(|| {
            AuthError::Internal(anyhow::anyhow!("IdP does not support device authorization grant"))
        })
    }

    pub async fn token_endpoint(&self) -> Result<&str, AuthError> {
        Ok(&self.endpoints().await?.token_endpoint)
    }

    pub async fn authorization_endpoint(&self) -> Result<&str, AuthError> {
        Ok(&self.endpoints().await?.authorization_endpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_discovered_endpoints() {
        let json = serde_json::json!({
            "issuer": "https://idp.example.com",
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            "device_authorization_endpoint": "https://idp.example.com/device",
            "jwks_uri": "https://idp.example.com/certs",
            "userinfo_endpoint": "https://idp.example.com/userinfo"
        });
        let endpoints: DiscoveredEndpoints = serde_json::from_value(json).unwrap();
        assert_eq!(endpoints.issuer, "https://idp.example.com");
        assert_eq!(endpoints.authorization_endpoint, "https://idp.example.com/authorize");
        assert_eq!(endpoints.token_endpoint, "https://idp.example.com/token");
        assert_eq!(endpoints.device_authorization_endpoint.as_deref(), Some("https://idp.example.com/device"));
        assert_eq!(endpoints.jwks_uri, "https://idp.example.com/certs");
    }

    #[test]
    fn parse_endpoints_without_device() {
        let json = serde_json::json!({
            "issuer": "https://idp.example.com",
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            "jwks_uri": "https://idp.example.com/certs"
        });
        let endpoints: DiscoveredEndpoints = serde_json::from_value(json).unwrap();
        assert!(endpoints.device_authorization_endpoint.is_none());
        assert!(endpoints.userinfo_endpoint.is_none());
    }

    #[test]
    fn new_succeeds_with_valid_config() {
        let config = OidcDiscoveryConfig {
            issuer: "https://idp.example.com".to_string(),
            authorization_endpoint_override: None,
            token_endpoint_override: None,
            device_authorization_endpoint_override: None,
            accept_invalid_certs: false,
        };
        assert!(OidcDiscovery::new(config).is_ok());
    }
}
