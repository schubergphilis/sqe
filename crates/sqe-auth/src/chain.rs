use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// A chain of `AuthProvider` implementations tried in order.
///
/// Authentication logic:
/// - Try each provider in sequence.
/// - First `Ok(Identity)` wins — return immediately.
/// - `NotMyCredentials` — skip to the next provider.
/// - `AuthFailed` or `Internal` — stop immediately and propagate the error.
/// - If all providers return `NotMyCredentials`, return `AuthFailed`.
///
/// Token refresh logic:
/// - Delegate to each provider in order.
/// - First provider that returns `Ok(Some(token))` wins.
/// - If all return `Ok(None)`, the chain returns `Ok(None)`.
pub struct AuthChain {
    providers: Vec<Arc<dyn AuthProvider>>,
}

impl AuthChain {
    /// Create a new chain from an ordered list of providers.
    pub fn new(providers: Vec<Arc<dyn AuthProvider>>) -> Self {
        Self { providers }
    }

    /// Returns the number of providers in the chain.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Returns `true` if the chain has no providers.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

#[async_trait]
impl AuthProvider for AuthChain {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        if self.providers.is_empty() {
            return Err(AuthError::AuthFailed(
                "no auth providers configured".to_string(),
            ));
        }

        for (i, provider) in self.providers.iter().enumerate() {
            match provider.authenticate(credentials).await {
                Ok(identity) => {
                    debug!(
                        provider_index = i,
                        user_id = %identity.user_id,
                        "Authentication succeeded"
                    );
                    return Ok(identity);
                }
                Err(AuthError::NotMyCredentials) => {
                    debug!(provider_index = i, "Provider skipped (NotMyCredentials)");
                    continue;
                }
                Err(e @ AuthError::AuthFailed(_)) => {
                    warn!(provider_index = i, error = %e, "Provider rejected credentials");
                    return Err(e);
                }
                Err(e @ AuthError::Internal(_)) => {
                    warn!(provider_index = i, error = %e, "Provider internal error");
                    return Err(e);
                }
            }
        }

        Err(AuthError::AuthFailed(
            "no provider accepted the credentials".to_string(),
        ))
    }

    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<String>, AuthError> {
        for provider in &self.providers {
            match provider.refresh_catalog_token(identity).await {
                Ok(Some(token)) => return Ok(Some(token)),
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A test provider that always returns `NotMyCredentials`.
    struct SkipProvider;

    #[async_trait]
    impl AuthProvider for SkipProvider {
        async fn authenticate(
            &self,
            _credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            Err(AuthError::NotMyCredentials)
        }
    }

    /// A test provider that always returns a fixed identity.
    struct FixedProvider {
        user_id: String,
    }

    #[async_trait]
    impl AuthProvider for FixedProvider {
        async fn authenticate(
            &self,
            _credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            Ok(Identity {
                user_id: self.user_id.clone(),
                display_name: self.user_id.clone(),
                roles: vec!["test-role".to_string()],
                catalog_token: Some("test-token".to_string()),
                refresh_token: None,
            })
        }
    }

    /// A test provider that always returns `AuthFailed`.
    struct RejectProvider {
        message: String,
    }

    #[async_trait]
    impl AuthProvider for RejectProvider {
        async fn authenticate(
            &self,
            _credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            Err(AuthError::AuthFailed(self.message.clone()))
        }
    }

    /// A test provider that returns a refresh token.
    struct RefreshProvider {
        token: String,
    }

    #[async_trait]
    impl AuthProvider for RefreshProvider {
        async fn authenticate(
            &self,
            _credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            Err(AuthError::NotMyCredentials)
        }

        async fn refresh_catalog_token(
            &self,
            _identity: &Identity,
        ) -> Result<Option<String>, AuthError> {
            Ok(Some(self.token.clone()))
        }
    }

    // -----------------------------------------------------------------------
    // Two-provider chain: first returns NotMyCredentials, second returns Ok
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_first_skips_second_succeeds() {
        let chain = AuthChain::new(vec![
            Arc::new(SkipProvider),
            Arc::new(FixedProvider {
                user_id: "alice".to_string(),
            }),
        ]);

        let creds = FlightCredentials::default();
        let result = chain.authenticate(&creds).await;
        let identity = result.expect("second provider should succeed");
        assert_eq!(identity.user_id, "alice");
    }

    // -----------------------------------------------------------------------
    // Single-provider chain: returns AuthFailed -> chain returns AuthFailed
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_single_provider_auth_failed() {
        let chain = AuthChain::new(vec![Arc::new(RejectProvider {
            message: "wrong password".to_string(),
        })]);

        let creds = FlightCredentials::default();
        let result = chain.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => assert_eq!(msg, "wrong password"),
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Empty chain: returns AuthFailed
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_empty_returns_auth_failed() {
        let chain = AuthChain::new(vec![]);
        assert!(chain.is_empty());

        let creds = FlightCredentials::default();
        let result = chain.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("no auth providers"), "got: {msg}");
            }
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // AuthFailed stops the chain — second provider is never tried
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_auth_failed_stops_chain() {
        let chain = AuthChain::new(vec![
            Arc::new(RejectProvider {
                message: "account locked".to_string(),
            }),
            Arc::new(FixedProvider {
                user_id: "should-not-reach".to_string(),
            }),
        ]);

        let creds = FlightCredentials::default();
        let result = chain.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => assert_eq!(msg, "account locked"),
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // All providers skip -> "no provider accepted"
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_all_skip_returns_auth_failed() {
        let chain = AuthChain::new(vec![Arc::new(SkipProvider), Arc::new(SkipProvider)]);

        let creds = FlightCredentials::default();
        let result = chain.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("no provider accepted"), "got: {msg}");
            }
            other => panic!("expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token: first provider with Some wins
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_refresh_first_some_wins() {
        let chain = AuthChain::new(vec![
            Arc::new(SkipProvider), // returns Ok(None) via default impl
            Arc::new(RefreshProvider {
                token: "refreshed-token".to_string(),
            }),
        ]);

        let identity = Identity {
            user_id: "test".to_string(),
            display_name: "Test".to_string(),
            roles: vec![],
            catalog_token: None,
            refresh_token: None,
        };

        let result = chain.refresh_catalog_token(&identity).await;
        assert_eq!(result.unwrap(), Some("refreshed-token".to_string()));
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token: all None -> None
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn chain_refresh_all_none() {
        let chain = AuthChain::new(vec![Arc::new(SkipProvider), Arc::new(SkipProvider)]);

        let identity = Identity {
            user_id: "test".to_string(),
            display_name: "Test".to_string(),
            roles: vec![],
            catalog_token: None,
            refresh_token: None,
        };

        let result = chain.refresh_catalog_token(&identity).await;
        assert_eq!(result.unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // Utility methods
    // -----------------------------------------------------------------------
    #[test]
    fn chain_len_and_is_empty() {
        let empty = AuthChain::new(vec![]);
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let one = AuthChain::new(vec![Arc::new(SkipProvider)]);
        assert_eq!(one.len(), 1);
        assert!(!one.is_empty());
    }
}
