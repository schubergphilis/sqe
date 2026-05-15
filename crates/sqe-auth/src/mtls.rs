//! `MtlsProvider` — mutual TLS (client certificate) authentication provider.
//!
//! Extracts identity from the TLS peer certificate's Common Name (CN).
//! Optionally maps Organizational Unit (OU) and Subject Alternative Name (SAN)
//! fields to groups, which are then resolved to roles via role mappings.
//!
//! # Credential Detection
//!
//! This provider checks `FlightCredentials::client_cert_cn`. If no client cert
//! is present (the field is `None`), it returns `NotMyCredentials` so the chain
//! falls through to the next provider.
//!
//! # Certificate Field Parsing
//!
//! The `client_cert_cn` field is expected to carry the subject CN. Additional
//! metadata (OU, SAN DNS names) can be passed as a structured string:
//!
//! ```text
//! CN=service-a,OU=platform,SAN=service-a.internal
//! ```
//!
//! If the field contains just a plain string (no `CN=` prefix), it is treated
//! as the CN directly.

use std::collections::HashMap;

use async_trait::async_trait;
use tracing::debug;

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// Configuration for the mTLS auth provider.
#[derive(Debug, Clone)]
pub struct MtlsProviderConfig {
    /// Whether to extract OU from the cert subject as a group.
    pub extract_ou: bool,
    /// Whether to extract SAN DNS names as groups.
    pub extract_san: bool,
    /// Group → roles mapping.
    pub role_mappings: HashMap<String, Vec<String>>,
}

impl Default for MtlsProviderConfig {
    fn default() -> Self {
        Self {
            extract_ou: true,
            extract_san: false,
            role_mappings: HashMap::new(),
        }
    }
}

/// Mutual TLS authentication provider.
pub struct MtlsProvider {
    config: MtlsProviderConfig,
}

impl MtlsProvider {
    /// Create a new mTLS provider with the given configuration.
    pub fn new(config: MtlsProviderConfig) -> Self {
        Self { config }
    }

    /// Parse a structured cert info string into (CN, groups).
    ///
    /// Accepts either:
    /// - Plain string: `"service-a"` → CN = `"service-a"`, no groups
    /// - Structured: `"CN=service-a,OU=platform,SAN=svc.internal"` → CN, OU, SAN as groups
    fn parse_cert_info(&self, raw: &str) -> (String, Vec<String>) {
        let trimmed = raw.trim();
        if !trimmed.contains('=') {
            // Plain CN string.
            return (trimmed.to_string(), Vec::new());
        }

        let mut cn = String::new();
        let mut groups = Vec::new();

        for part in trimmed.split(',') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("CN=") {
                cn = value.to_string();
            } else if let Some(value) = part.strip_prefix("OU=") {
                if self.config.extract_ou {
                    groups.push(value.to_string());
                }
            } else if let Some(value) = part.strip_prefix("SAN=") {
                if self.config.extract_san {
                    groups.push(value.to_string());
                }
            }
        }

        (cn, groups)
    }

    /// Resolve groups to roles via role mappings.
    fn resolve_roles(&self, groups: &[String]) -> Vec<String> {
        let mut roles = Vec::new();
        for group in groups {
            if let Some(mapped) = self.config.role_mappings.get(group) {
                roles.extend(mapped.iter().cloned());
            }
        }
        roles.sort();
        roles.dedup();
        roles
    }
}

#[async_trait]
impl AuthProvider for MtlsProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        let cert_cn = match &credentials.client_cert_cn {
            Some(cn) if !cn.is_empty() => cn,
            _ => return Err(AuthError::NotMyCredentials),
        };

        let (cn, groups) = self.parse_cert_info(cert_cn);
        if cn.is_empty() {
            return Err(AuthError::AuthFailed(
                "client certificate has empty CN".to_string(),
            ));
        }

        let roles = self.resolve_roles(&groups);

        debug!(
            cn = %cn,
            groups = ?groups,
            roles = ?roles,
            "mTLS authentication succeeded"
        );

        Ok(Identity {
            user_id: cn.clone(),
            display_name: cn,
            roles,
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        })
    }

    // refresh_catalog_token: default (Ok(None)) — mTLS has no bearer tokens.
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MtlsProviderConfig {
        let mut role_mappings = HashMap::new();
        role_mappings.insert(
            "platform".to_string(),
            vec!["admin".to_string(), "reader".to_string()],
        );
        role_mappings.insert(
            "services".to_string(),
            vec!["reader".to_string()],
        );
        MtlsProviderConfig {
            extract_ou: true,
            extract_san: true,
            role_mappings,
        }
    }

    // -----------------------------------------------------------------------
    // Plain CN → correct identity
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn plain_cn_returns_identity() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            client_cert_cn: Some("service-a".to_string()),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "service-a");
        assert_eq!(identity.display_name, "service-a");
        assert!(identity.roles.is_empty()); // No groups in plain CN
        assert!(identity.catalog_token.is_none());
    }

    // -----------------------------------------------------------------------
    // Structured CN with OU → groups mapped to roles
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn structured_cn_with_ou_maps_roles() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            client_cert_cn: Some("CN=worker-1,OU=platform".to_string()),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "worker-1");
        assert_eq!(identity.roles, vec!["admin", "reader"]);
    }

    // -----------------------------------------------------------------------
    // Structured CN with OU + SAN
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn structured_cn_with_ou_and_san() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            client_cert_cn: Some("CN=svc,OU=platform,SAN=svc.internal".to_string()),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "svc");
        // "platform" maps to [admin, reader], "svc.internal" has no mapping
        assert_eq!(identity.roles, vec!["admin", "reader"]);
    }

    // -----------------------------------------------------------------------
    // No client cert → NotMyCredentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_cert_returns_not_my_credentials() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials::default();

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Empty CN string → NotMyCredentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_cn_returns_not_my_credentials() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            client_cert_cn: Some("".to_string()),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::NotMyCredentials) => {}
            other => panic!("expected NotMyCredentials, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Structured but missing CN → AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn structured_missing_cn_returns_auth_failed() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            client_cert_cn: Some("OU=platform,SAN=svc.internal".to_string()),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => assert!(msg.contains("empty CN")),
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // OU extraction disabled
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ou_extraction_disabled() {
        let config = MtlsProviderConfig {
            extract_ou: false,
            extract_san: false,
            role_mappings: HashMap::new(),
        };
        let provider = MtlsProvider::new(config);
        let creds = FlightCredentials {
            client_cert_cn: Some("CN=svc,OU=platform".to_string()),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "svc");
        assert!(identity.roles.is_empty());
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token returns None
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_returns_none() {
        let provider = MtlsProvider::new(MtlsProviderConfig::default());
        let identity = Identity {
            user_id: "test".to_string(),
            display_name: "test".to_string(),
            roles: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };

        assert!(provider.refresh_catalog_token(&identity).await.unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Other credential fields are ignored
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ignores_username_and_password() {
        let provider = MtlsProvider::new(test_config());
        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some(sqe_core::SecretString::new("secret".to_string())),
            bearer_token: Some(sqe_core::SecretString::new("eyJ...".to_string())),
            client_cert_cn: Some("service-x".to_string()),
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "service-x");
    }
}
