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
//!   serve a soon-to-expire token. Issue #31. The secret comes from
//!   `client_secret_file` (read on every resolve) or the inline value; its
//!   fingerprint is part of the cache key, so a rotated file gets a new
//!   token on the next resolve without a restart (#937).
//! - `Aws`: returns an empty string and lets the AWS SDK provider
//!   chain handle the actual signing path. Used by REST catalogs
//!   pointed at AWS Glue / S3 Tables, where the catalog's
//!   `/v1/config` flips on SigV4 mode.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use moka::future::Cache;
use sha2::{Digest, Sha256};

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

/// The last secret fingerprint that minted a token, per identity
/// (`endpoint|client_id|scope`). A token minted with a different one is a
/// rotation.
fn last_fingerprints() -> &'static Mutex<HashMap<String, String>> {
    static LAST: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    LAST.get_or_init(|| Mutex::new(HashMap::new()))
}

fn identity_key(token_endpoint: &str, client_id: &str, scope: &Option<String>) -> String {
    format!(
        "{}|{}|{}",
        token_endpoint,
        client_id,
        scope.as_deref().unwrap_or("")
    )
}

/// A short one-way fingerprint of the secret. Kept in process memory as part
/// of the cache key only, never logged.
fn secret_fingerprint(secret: &str) -> String {
    hex::encode(&Sha256::digest(secret.as_bytes())[..16])
}

fn cache_key(identity: &str, fingerprint: &str) -> String {
    format!("{identity}|{fingerprint}")
}

/// A resolved bearer, and whether this resolve was the first to mint a token
/// with a rotated `client_credentials` secret (once per rotation, not on the
/// first token after start).
pub struct ResolvedBearer {
    pub bearer: String,
    pub secret_rotated: bool,
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
    Ok(resolve_bearer_tracked(auth, session_bearer).await?.bearer)
}

/// [`resolve_bearer`], also saying whether a rotated secret was first used.
pub async fn resolve_bearer_tracked(
    auth: &CatalogAuthConfig,
    session_bearer: &str,
) -> sqe_core::Result<ResolvedBearer> {
    let bearer = match auth {
        CatalogAuthConfig::SessionBearer => session_bearer.to_string(),
        CatalogAuthConfig::Static { token } => token.clone(),
        CatalogAuthConfig::Anonymous => String::new(),
        CatalogAuthConfig::Aws { .. } => {
            // AWS auth is handled by the SDK provider chain at the
            // catalog client level; SQE doesn't supply a bearer.
            String::new()
        }
        CatalogAuthConfig::ClientCredentials {
            token_endpoint,
            client_id,
            scope,
            ..
        } => {
            // The file is read on every resolve (#937): an unreadable or
            // empty file fails here, never falling back to a cached token.
            let client_secret = auth
                .client_secret_value()
                .map_err(|e| sqe_core::SqeError::Auth(format!("catalog credentials: {e}")))?
                .unwrap_or_default();
            // Forward `scope` to OAuthClient. The previous `let _ = scope;`
            // dropped the field silently. Issue #17.
            // Cache the resulting bearer per (endpoint, client_id, scope,
            // secret fingerprint) so session-context rebuilds and concurrent
            // users do not hammer the IdP (#31), and a rotated secret misses.
            let identity = identity_key(token_endpoint, client_id, scope);
            let fingerprint = secret_fingerprint(&client_secret);
            let key = cache_key(&identity, &fingerprint);
            let cache = token_cache();
            let now = Instant::now();
            if let Some(cached) = cache.get(&key).await {
                if cached.expires_at > now {
                    return Ok(ResolvedBearer {
                        bearer: cached.access_token,
                        secret_rotated: false,
                    });
                }
            }

            let client = OAuthClient::new(token_endpoint, client_id, &client_secret, false)?
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
                            access_token: resp.access_token.expose().to_string(),
                            expires_at: now + cacheable,
                        },
                    )
                    .await;
            }
            // Only a secret that minted a token counts as rotated in.
            let previous = last_fingerprints()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(identity.clone(), fingerprint.clone());
            let secret_rotated = previous.as_ref().is_some_and(|p| *p != fingerprint);
            if let Some(previous) = previous.filter(|_| secret_rotated) {
                cache.invalidate(&cache_key(&identity, &previous)).await;
            }
            return Ok(ResolvedBearer {
                bearer: resp.access_token.expose().to_string(),
                secret_rotated,
            });
        }
    };
    Ok(ResolvedBearer {
        bearer,
        secret_rotated: false,
    })
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
        let auth = CatalogAuthConfig::Aws {
            signing_name: None,
            signing_region: None,
        };
        let resolved = resolve_bearer(&auth, "session").await.unwrap();
        assert!(resolved.is_empty());
    }

    #[test]
    fn cache_key_separates_scope_client_and_secret() {
        let scope_a = Some("PRINCIPAL_ROLE:READ".to_string());
        let scope_b = Some("PRINCIPAL_ROLE:ALL".to_string());
        let none_scope = None;
        let fp = secret_fingerprint("s");
        let key = |client: &str, scope: &Option<String>| {
            cache_key(&identity_key("https://idp/token", client, scope), &fp)
        };
        let a = key("client", &scope_a);
        assert_ne!(a, key("client", &scope_b), "scope must split the cache");
        assert_ne!(a, key("other", &scope_a), "client_id must split the cache");
        assert_ne!(
            a,
            key("client", &none_scope),
            "None vs Some scope must split"
        );
        let rotated = cache_key(
            &identity_key("https://idp/token", "client", &scope_a),
            &secret_fingerprint("s2"),
        );
        assert_ne!(a, rotated, "a rotated secret must miss the cache");
        assert!(
            !a.contains("|s|") && !a.ends_with("|s"),
            "no raw secret in key"
        );
        assert_eq!(a, key("client", &scope_a));
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
                ..
            } => {
                assert_eq!(token_endpoint, "https://example.com/oauth/tokens");
                assert_eq!(client_id, "id");
                assert_eq!(client_secret, "secret");
                assert_eq!(scope.as_deref(), Some("PRINCIPAL_ROLE:READ"));
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    mod rotation {
        use super::*;
        use wiremock::matchers::{body_string_contains, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        fn token(name: &str) -> ResponseTemplate {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": name,
                "token_type": "bearer",
                "expires_in": 3600
            }))
        }

        fn auth(server: &MockServer, secret: &str, file: Option<&str>) -> CatalogAuthConfig {
            CatalogAuthConfig::ClientCredentials {
                token_endpoint: format!("{}/token", server.uri()),
                client_id: "sqe-engine".into(),
                client_secret: secret.into(),
                client_secret_file: file.map(str::to_string),
                scope: None,
            }
        }

        fn secret_path(name: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!("sqe-937-auth-{}-{name}", std::process::id()))
        }

        /// The IdP accepts only the new secret after rotation: the next
        /// resolve reads the rewritten file, sends it, and reports the
        /// rotation once.
        #[tokio::test]
        async fn rotated_secret_file_is_sent_on_the_next_resolve() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(body_string_contains("client_secret=secret-a"))
                .respond_with(token("tok-a"))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(body_string_contains("client_secret=secret-b"))
                .respond_with(token("tok-b"))
                .expect(1)
                .mount(&server)
                .await;
            let path = secret_path("rotate");
            std::fs::write(&path, "secret-a\n").expect("write");
            let auth = auth(&server, "", path.to_str());

            let first = resolve_bearer_tracked(&auth, "").await.expect("first");
            assert_eq!(first.bearer, "tok-a");
            assert!(!first.secret_rotated, "first token is not a rotation");
            let cached = resolve_bearer_tracked(&auth, "").await.expect("cached");
            assert_eq!(cached.bearer, "tok-a");

            std::fs::write(&path, "secret-b\n").expect("rotate");
            let rotated = resolve_bearer_tracked(&auth, "").await.expect("rotated");
            assert_eq!(rotated.bearer, "tok-b");
            assert!(rotated.secret_rotated, "rotation reported");
            let again = resolve_bearer_tracked(&auth, "").await.expect("again");
            assert_eq!(again.bearer, "tok-b");
            assert!(!again.secret_rotated, "reported once per rotation");
            let _ = std::fs::remove_file(&path);
        }

        /// An unreadable file fails the resolve, even with a cached token
        /// for the old secret, and names the path.
        #[tokio::test]
        async fn unreadable_secret_file_is_an_error() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(token("tok"))
                .mount(&server)
                .await;
            let path = secret_path("unreadable");
            std::fs::write(&path, "secret").expect("write");
            let auth = auth(&server, "", path.to_str());
            resolve_bearer(&auth, "").await.expect("readable");
            std::fs::remove_file(&path).expect("remove");
            let err = resolve_bearer(&auth, "").await.expect_err("unreadable");
            assert!(
                err.to_string().contains(path.to_str().expect("utf8")),
                "{err}"
            );
        }

        /// The inline secret still works and is cached: one token request.
        #[tokio::test]
        async fn inline_secret_makes_one_token_request() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(body_string_contains("client_secret=inline"))
                .respond_with(token("tok-inline"))
                .expect(1)
                .mount(&server)
                .await;
            let auth = auth(&server, "inline", None);
            for _ in 0..3 {
                let got = resolve_bearer_tracked(&auth, "").await.expect("inline");
                assert_eq!(got.bearer, "tok-inline");
                assert!(!got.secret_rotated);
            }
        }
    }
}
