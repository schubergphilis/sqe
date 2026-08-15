//! `BearerPassthroughProvider` — accepts any non-empty bearer token and uses
//! it verbatim as the catalog token, without local validation.
//!
//! Use this when an upstream component (a TLS-terminating reverse proxy, an
//! ALB / Envoy with OIDC, a sidecar) has already validated the JWT and is
//! relaying it to SQE. SQE then forwards the same bearer to the downstream
//! catalog (Polaris, Glue, etc.) so the catalog enforces ACLs against the
//! authenticated principal.
//!
//! This is also the right provider for development against catalogs that
//! issue tokens we cannot independently validate (e.g. Polaris's
//! `client_credentials` flow, which uses an internal HS256 key with no
//! published JWKS endpoint).
//!
//! **Security**: this provider does *not* validate the bearer signature,
//! expiry, audience, or issuer. Deploy it only when one of these is true:
//!
//! - An upstream proxy already validates the JWT before it reaches SQE.
//! - The downstream catalog rejects invalid tokens (the request will
//!   propagate that rejection back to the client as a `SQE-EXEC` error
//!   surfacing whatever HTTP status the catalog returned).
//! - You are running in a trusted dev / test environment.

use async_trait::async_trait;
use tracing::{debug, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

#[derive(Debug, Clone)]
pub struct BearerPassthroughProviderConfig {
    /// User name assigned to the resulting `Identity`. The catalog still
    /// enforces ACLs against the bearer itself; this is what shows up in
    /// SQE audit logs and metrics.
    pub user: String,
    /// Roles assigned to the resulting `Identity`. Used by SQE policy
    /// enforcement (row filters, column masks) but not by the catalog.
    pub roles: Vec<String>,
}

impl Default for BearerPassthroughProviderConfig {
    fn default() -> Self {
        Self {
            user: "bearer-passthrough".to_string(),
            roles: Vec::new(),
        }
    }
}

pub struct BearerPassthroughProvider {
    config: BearerPassthroughProviderConfig,
}

impl BearerPassthroughProvider {
    pub fn new(config: BearerPassthroughProviderConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthProvider for BearerPassthroughProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        let token = credentials
            .bearer_token
            .as_ref()
            .ok_or(AuthError::NotMyCredentials)?;
        if token.expose().is_empty() {
            return Err(AuthError::AuthFailed("empty bearer token".to_string()));
        }
        debug!(
            user = %self.config.user,
            "BearerPassthroughProvider forwarding bearer as catalog_token"
        );
        Ok(Identity {
            user_id: self.config.user.clone(),
            display_name: self.config.user.clone(),
            roles: self.config.roles.clone(),
            subject: None,
            email: None,
            groups: vec![],
            catalog_token: Some(token.clone()),
            refresh_token: None,
            expires_at: None,
        })
    }

    /// Bearer passthrough has no refresh path — the upstream proxy / client
    /// is responsible for rotating tokens before they expire.
    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
        Ok(identity.catalog_token.clone())
    }
}

/// Emit the loud security warning that the provider is active. The factory
/// calls this once at startup; the message stays in the log so operators
/// remember the deployment assumption.
pub fn warn_active() {
    warn!(
        "SECURITY: BearerPassthroughProvider is active — bearer signatures are NOT \
         validated by SQE. The deployment must validate the JWT before it reaches \
         SQE (TLS-terminating proxy, ALB/Envoy OIDC, etc.), or rely on the \
         downstream catalog to reject invalid tokens."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> BearerPassthroughProvider {
        BearerPassthroughProvider::new(BearerPassthroughProviderConfig {
            user: "alice".to_string(),
            roles: vec!["analyst".to_string()],
        })
    }

    #[tokio::test]
    async fn forwards_bearer_as_catalog_token() {
        let bearer = "eyJhbGciOiJIUzI1NiJ9.fake.signature";
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(bearer.to_string())),
            ..Default::default()
        };
        let identity = provider()
            .authenticate(&creds)
            .await
            .expect("accept bearer");
        assert_eq!(identity.user_id, "alice");
        assert_eq!(identity.roles, vec!["analyst".to_string()]);
        assert_eq!(
            identity
                .catalog_token
                .as_ref()
                .map(|s| s.expose().to_string()),
            Some(bearer.to_string()),
        );
    }

    #[tokio::test]
    async fn rejects_credentials_without_bearer_as_not_my_credentials() {
        let creds = FlightCredentials {
            username: Some("u".to_string()),
            ..Default::default()
        };
        let err = provider().authenticate(&creds).await.unwrap_err();
        assert!(matches!(err, AuthError::NotMyCredentials));
    }

    #[tokio::test]
    async fn rejects_empty_bearer_as_auth_failed() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(String::new())),
            ..Default::default()
        };
        let err = provider().authenticate(&creds).await.unwrap_err();
        assert!(matches!(err, AuthError::AuthFailed(_)));
    }

    #[tokio::test]
    async fn refresh_returns_same_token() {
        let bearer = "the-token";
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(bearer.to_string())),
            ..Default::default()
        };
        let identity = provider().authenticate(&creds).await.unwrap();
        let refreshed = provider().refresh_catalog_token(&identity).await.unwrap();
        assert_eq!(
            refreshed.as_ref().map(|s| s.expose().to_string()),
            Some(bearer.to_string()),
        );
    }
}
