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
//!   the catalog's own token endpoint. Tokens are cached globally
//!   per `(token_endpoint, client_id, scope)` and reused across
//!   session-context rebuilds. Cache TTL derives from the IdP's
//!   `expires_in` minus a 60-second safety margin, so callers never
//!   serve a soon-to-expire token. Issue #31.
//! - `Aws`: returns an empty string and lets the AWS SDK provider
//!   chain handle the actual signing path. Used by REST catalogs
//!   pointed at AWS Glue / S3 Tables, where the catalog's
//!   `/v1/config` flips on SigV4 mode.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use moka::future::Cache;

use sqe_core::config::CatalogAuthConfig;

use crate::oauth::OAuthClient;

/// Safety margin subtracted from the IdP-reported `expires_in` before the
/// cached token is considered stale. 60 seconds covers clock skew plus the
/// round-trip needed to refresh on the next request.
const TOKEN_EXPIRY_SAFETY_MARGIN: Duration = Duration::from_secs(60);

/// Smallest TTL the cache will hold a token for, in case the IdP returns
/// pathologically small `expires_in` values. Below this we just bypass the
/// cache rather than serve a near-expired bearer.
const MIN_CACHED_TTL: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

fn token_cache() -> &'static Cache<String, CachedToken> {
    static CACHE: OnceLock<Cache<String, CachedToken>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(1024)
            .time_to_live(Duration::from_secs(3600))
            .build()
    })
}

fn cache_key(token_endpoint: &str, client_id: &str, scope: &Option<String>) -> String {
    format!(
        "{}|{}|{}",
        token_endpoint,
        client_id,
        scope.as_deref().unwrap_or("")
    )
}

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
            // dropped the field silently. Issue #17.
            // Cache the resulting bearer per (endpoint, client_id, scope) so
            // session-context rebuilds and concurrent users do not hammer the
            // IdP. Issue #31.
            let key = cache_key(token_endpoint, client_id, scope);
            let cache = token_cache();
            let now = Instant::now();
            if let Some(cached) = cache.get(&key).await {
                if cached.expires_at > now {
                    return Ok(cached.access_token);
                }
            }

            let client = OAuthClient::new(token_endpoint, client_id, client_secret, false)?
                .with_scope(scope.clone());
            let resp = client.get_token().await?;

            let lifetime = Duration::from_secs(resp.expires_in);
            let cacheable = lifetime
                .checked_sub(TOKEN_EXPIRY_SAFETY_MARGIN)
                .unwrap_or_default();
            if cacheable >= MIN_CACHED_TTL {
                cache
                    .insert(
                        key,
                        CachedToken {
                            access_token: resp.access_token.clone(),
                            expires_at: now + cacheable,
                        },
                    )
                    .await;
            }
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

    #[test]
    fn cache_key_separates_scope_and_client() {
        let scope_a = Some("PRINCIPAL_ROLE:READ".to_string());
        let scope_b = Some("PRINCIPAL_ROLE:ALL".to_string());
        let none_scope = None;
        let a = cache_key("https://idp/token", "client", &scope_a);
        let b = cache_key("https://idp/token", "client", &scope_b);
        let c = cache_key("https://idp/token", "other", &scope_a);
        let d = cache_key("https://idp/token", "client", &none_scope);
        assert_ne!(a, b, "scope must split the cache");
        assert_ne!(a, c, "client_id must split the cache");
        assert_ne!(a, d, "None vs Some scope must split the cache");
        // Same triple yields same key.
        assert_eq!(a, cache_key("https://idp/token", "client", &scope_a));
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
