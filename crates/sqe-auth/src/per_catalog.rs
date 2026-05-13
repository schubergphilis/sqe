//! Per-catalog auth resolver.
//!
//! V6 attached every catalog with the user's session bearer token,
//! which works when one OIDC provider fronts every Iceberg REST
//! endpoint in the deployment. V7 lets each catalog override that
//! choice via a `[catalogs.<name>.auth]` block. The variants come
//! from [`CatalogAuthConfig`] in `sqe_core::config`. This module
//! resolves them into the actual bearer string the catalog client
//! sends in `Authorization: Bearer ...`.
//!
//! ## Variants
//!
//! - `SessionBearer` (default): pass the user's session token
//!   through unchanged. Identical to V6 behaviour.
//! - `Static`: configured token, used verbatim. Useful for
//!   internal gateways and integration tests.
//! - `Anonymous`: empty string. The catalog client should skip the
//!   `Authorization` header entirely; the session catalog code
//!   already does this when the token is empty.
//! - `ClientCredentials`: OAuth2 client_credentials grant against
//!   the catalog's own token endpoint. Token is fetched at session
//!   build time and reused for the session lifetime. Refresh on
//!   expiry is a future change; for now the assumption is that
//!   `expires_in` from the token endpoint exceeds the session TTL,
//!   which holds for typical Polaris and Auth0 setups (1 hour
//!   tokens, 5-minute sessions).
//! - `Aws`: returns an empty string and lets the AWS SDK provider
//!   chain handle the actual signing path. Used by REST catalogs
//!   pointed at AWS Glue / S3 Tables, where the catalog's
//!   `/v1/config` flips on SigV4 mode.

use sqe_core::config::CatalogAuthConfig;

use crate::oauth::OAuthClient;

/// Resolve the bearer token for one catalog.
///
/// `session_bearer` is the user's session access token (the same
/// one V6 used for every catalog). Variants that don't need a
/// fresh token return either the session bearer or an empty
/// string; `ClientCredentials` performs an OAuth2 round-trip
/// against the catalog's own endpoint.
///
/// Returns the bearer to put in the `Authorization` header. An
/// empty string signals "no header" — the catalog client paths
/// already treat empty bearers as anonymous.
pub async fn resolve_bearer(
    auth: &CatalogAuthConfig,
    session_bearer: &str,
) -> sqe_core::Result<String> {
    match auth {
        CatalogAuthConfig::SessionBearer => Ok(session_bearer.to_string()),
        CatalogAuthConfig::Static { token } => Ok(token.clone()),
        CatalogAuthConfig::Anonymous => Ok(String::new()),
        CatalogAuthConfig::Aws => {
            // AWS auth is handled by the SDK provider chain at the
            // catalog client level; SQE doesn't supply a bearer.
            Ok(String::new())
        }
        CatalogAuthConfig::ClientCredentials {
            token_endpoint,
            client_id,
            client_secret,
            scope,
        } => {
            // Forward `scope` to OAuthClient. The previous `let _ = scope;`
            // dropped the field silently — a deployment that mounted
            // Polaris with `PRINCIPAL_ROLE:READ_ONLY` actually attached
            // with `PRINCIPAL_ROLE:ALL`, broadening rights without any
            // warning. Issue #17.
            let client = OAuthClient::new(token_endpoint, client_id, client_secret, false)?
                .with_scope(scope.clone());
            let resp = client.get_token().await?;
            Ok(resp.access_token)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_bearer_returns_session_token() {
        let auth = CatalogAuthConfig::SessionBearer;
        let resolved = resolve_bearer(&auth, "user-session-xyz").await.unwrap();
        assert_eq!(resolved, "user-session-xyz");
    }

    #[tokio::test]
    async fn default_variant_is_session_bearer() {
        let auth = CatalogAuthConfig::default();
        let resolved = resolve_bearer(&auth, "tok").await.unwrap();
        assert_eq!(resolved, "tok");
    }

    #[tokio::test]
    async fn static_returns_configured_token_not_session() {
        let auth = CatalogAuthConfig::Static {
            token: "configured".into(),
        };
        let resolved = resolve_bearer(&auth, "session").await.unwrap();
        assert_eq!(resolved, "configured");
    }

    #[tokio::test]
    async fn anonymous_returns_empty() {
        let auth = CatalogAuthConfig::Anonymous;
        let resolved = resolve_bearer(&auth, "session").await.unwrap();
        assert!(resolved.is_empty());
    }

    #[tokio::test]
    async fn aws_returns_empty_for_sdk_chain() {
        let auth = CatalogAuthConfig::Aws;
        let resolved = resolve_bearer(&auth, "session").await.unwrap();
        assert!(resolved.is_empty());
    }

    /// Live `ClientCredentials` against a real token endpoint is
    /// covered by the cluster integration tests, not here. We only
    /// assert that the variant deserializes cleanly with a `scope`
    /// field present.
    #[test]
    fn client_credentials_deserializes_with_scope() {
        let toml = r#"
type = "client_credentials"
token_endpoint = "https://example.com/oauth/tokens"
client_id = "id"
client_secret = "secret"
scope = "PRINCIPAL_ROLE:READ"
"#;
        let auth: CatalogAuthConfig = toml::from_str(toml).expect("deserialize");
        match auth {
            CatalogAuthConfig::ClientCredentials {
                token_endpoint,
                client_id,
                client_secret,
                scope,
            } => {
                assert_eq!(token_endpoint, "https://example.com/oauth/tokens");
                assert_eq!(client_id, "id");
                assert_eq!(client_secret, "secret");
                assert_eq!(scope.as_deref(), Some("PRINCIPAL_ROLE:READ"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
