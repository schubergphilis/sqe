//! `AnonymousProvider` — fixed-identity provider for development and testing.
//!
//! Returns a preconfigured identity for any credentials (or no credentials at all).
//! Should be placed last in the `AuthChain` when used in production-adjacent setups.

use async_trait::async_trait;
use tracing::debug;

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// Configuration for the anonymous auth provider.
#[derive(Debug, Clone)]
pub struct AnonymousProviderConfig {
    /// The fixed user name to assign.
    pub user: String,
    /// The fixed set of roles to assign.
    pub roles: Vec<String>,
}

impl Default for AnonymousProviderConfig {
    fn default() -> Self {
        Self {
            user: "anonymous".to_string(),
            roles: Vec::new(),
        }
    }
}

/// Anonymous authentication provider.
///
/// Accepts any credentials (or none) and returns a fixed identity with the
/// configured user name and roles. Useful for development, testing, and
/// trusted-network deployments where authentication is not needed.
pub struct AnonymousProvider {
    config: AnonymousProviderConfig,
}

impl AnonymousProvider {
    /// Create a new anonymous provider with the given configuration.
    pub fn new(config: AnonymousProviderConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthProvider for AnonymousProvider {
    async fn authenticate(&self, _credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        debug!(
            user = %self.config.user,
            roles = ?self.config.roles,
            "Anonymous authentication — returning fixed identity"
        );

        Ok(Identity {
            user_id: self.config.user.clone(),
            display_name: self.config.user.clone(),
            roles: self.config.roles.clone(),
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        })
    }

    // refresh_catalog_token: uses the default (Ok(None)) — anonymous has no tokens.
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Any credentials → fixed identity
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn returns_configured_user_and_roles() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig {
            user: "dev-user".to_string(),
            roles: vec!["admin".to_string(), "reader".to_string()],
        });

        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some(sqe_core::SecretString::new("secret".to_string())),
            ..Default::default()
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "dev-user");
        assert_eq!(identity.display_name, "dev-user");
        assert_eq!(identity.roles, vec!["admin", "reader"]);
        assert!(identity.catalog_token.is_none());
        assert!(identity.refresh_token.is_none());
    }

    // -----------------------------------------------------------------------
    // No credentials → still returns identity
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn returns_identity_with_no_credentials() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig {
            user: "anonymous".to_string(),
            roles: vec!["public".to_string()],
        });

        let creds = FlightCredentials::default();

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "anonymous");
        assert_eq!(identity.roles, vec!["public"]);
    }

    // -----------------------------------------------------------------------
    // Empty roles
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn returns_empty_roles_when_none_configured() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig {
            user: "test".to_string(),
            roles: Vec::new(),
        });

        let creds = FlightCredentials::default();
        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert!(identity.roles.is_empty());
    }

    // -----------------------------------------------------------------------
    // Default config
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn default_config_returns_anonymous_user() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig::default());
        let creds = FlightCredentials::default();

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "anonymous");
        assert!(identity.roles.is_empty());
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token always returns None
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_returns_none() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig::default());
        let identity = Identity {
            user_id: "anonymous".to_string(),
            display_name: "anonymous".to_string(),
            roles: Vec::new(),
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };

        let result = provider.refresh_catalog_token(&identity).await;
        assert!(result.unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Ignores all credential fields
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ignores_bearer_token_and_client_cert() {
        let provider = AnonymousProvider::new(AnonymousProviderConfig {
            user: "anon".to_string(),
            roles: vec!["guest".to_string()],
        });

        let creds = FlightCredentials {
            username: Some("whoever".to_string()),
            password: Some(sqe_core::SecretString::new("anything".to_string())),
            bearer_token: Some(sqe_core::SecretString::new("eyJ...".to_string())),
            client_cert_cn: Some("client.example.com".to_string()),
        };

        let identity = provider.authenticate(&creds).await.expect("should succeed");
        assert_eq!(identity.user_id, "anon");
        assert_eq!(identity.roles, vec!["guest"]);
    }
}
