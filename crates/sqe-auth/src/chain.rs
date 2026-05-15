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
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
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
                catalog_token: Some(sqe_core::SecretString::new("test-token".to_string())),
                refresh_token: None,
                expires_at: None,
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
        ) -> Result<Option<sqe_core::SecretString>, AuthError> {
            Ok(Some(sqe_core::SecretString::new(self.token.clone())))
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
            expires_at: None,
        };

        let result = chain.refresh_catalog_token(&identity).await;
        let got = result.unwrap().expect("provider returned a token");
        assert_eq!(got.expose(), "refreshed-token");
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
            expires_at: None,
        };

        let result = chain.refresh_catalog_token(&identity).await;
        assert!(result.unwrap().is_none());
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

    // -----------------------------------------------------------------------
    // Bug regression: bearer-only credentials must skip a username/password
    // provider sitting first in the chain.
    //
    // The user's hypothesis was that NotMyCredentials is being converted to
    // AuthFailed somewhere in the chain. This test proves the chain control
    // flow itself is correct: a provider that demands username + password
    // must return NotMyCredentials for bearer-only credentials, and the
    // chain must continue to the next provider.
    // -----------------------------------------------------------------------

    /// A provider that requires `username` + `password`, returning
    /// `NotMyCredentials` when either is missing. Mirrors the real
    /// `OidcPasswordProvider` and the legacy `Authenticator::authenticate`
    /// shape so this regression test covers the exact production case
    /// without making any network calls.
    struct UsernamePasswordOnlyProvider;

    #[async_trait]
    impl AuthProvider for UsernamePasswordOnlyProvider {
        async fn authenticate(
            &self,
            credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            match (&credentials.username, &credentials.password) {
                (Some(u), Some(p)) if !u.is_empty() && !p.is_empty() => Ok(Identity {
                    user_id: u.clone(),
                    display_name: u.clone(),
                    roles: vec!["user".to_string()],
                    catalog_token: Some(p.clone()),
                    refresh_token: None,
                    expires_at: None,
                }),
                _ => Err(AuthError::NotMyCredentials),
            }
        }
    }

    /// A provider that accepts any non-empty `bearer_token`. Mirrors the
    /// detect-and-validate shape of `BearerTokenProvider` without doing
    /// JWKS network calls.
    struct BearerOnlyProvider;

    #[async_trait]
    impl AuthProvider for BearerOnlyProvider {
        async fn authenticate(
            &self,
            credentials: &FlightCredentials,
        ) -> Result<Identity, AuthError> {
            match &credentials.bearer_token {
                Some(t) if !t.is_empty() => Ok(Identity {
                    user_id: "bearer-user".to_string(),
                    display_name: "bearer-user".to_string(),
                    roles: vec!["api".to_string()],
                    catalog_token: Some(t.clone()),
                    refresh_token: None,
                    expires_at: None,
                }),
                _ => Err(AuthError::NotMyCredentials),
            }
        }
    }


    /// The user's reported scenario: Flight SQL hands the chain a JWT in
    /// `bearer_token` only (no username, no password). The chain must skip
    /// the OIDC password provider that sits first and let the bearer
    /// provider succeed.
    #[tokio::test]
    async fn chain_bearer_only_credentials_skip_oidc_password_first() {
        let chain = AuthChain::new(vec![
            Arc::new(UsernamePasswordOnlyProvider),
            Arc::new(BearerOnlyProvider),
        ]);

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("eyJtest.payload.sig".to_string())),
            ..Default::default()
        };

        let identity = chain
            .authenticate(&creds)
            .await
            .expect("chain must fall through to bearer provider");
        assert_eq!(identity.user_id, "bearer-user");
        assert_eq!(
            identity.catalog_token.as_ref().map(|t| t.expose()),
            Some("eyJtest.payload.sig"),
        );
    }

    /// Documented workaround: putting `bearer_token` before `oidc_password`
    /// in the provider chain. The bearer provider sees the JWT first and
    /// succeeds without ever consulting the username/password provider.
    /// Lock the contract so users on older builds know the workaround
    /// stays viable.
    #[tokio::test]
    async fn chain_bearer_first_workaround_succeeds() {
        let chain = AuthChain::new(vec![
            Arc::new(BearerOnlyProvider),
            Arc::new(UsernamePasswordOnlyProvider),
        ]);

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("eyJtest.payload.sig".to_string())),
            ..Default::default()
        };

        let identity = chain
            .authenticate(&creds)
            .await
            .expect("bearer-first chain must succeed on bearer credentials");
        assert_eq!(identity.user_id, "bearer-user");
    }

    /// Username/password credentials still work after the bearer-first
    /// workaround: bearer skips, oidc_password handles.
    #[tokio::test]
    async fn chain_bearer_first_falls_through_for_username_password() {
        let chain = AuthChain::new(vec![
            Arc::new(BearerOnlyProvider),
            Arc::new(UsernamePasswordOnlyProvider),
        ]);

        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some(sqe_core::SecretString::new("secret".to_string())),
            ..Default::default()
        };

        let identity = chain
            .authenticate(&creds)
            .await
            .expect("chain must fall through bearer to oidc_password");
        assert_eq!(identity.user_id, "alice");
    }
}
