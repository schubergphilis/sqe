//! `BearerTokenProvider` -- JWT bearer token validation against a JWKS endpoint.
//!
//! Clients pre-obtain a JWT (e.g. Kubernetes ServiceAccount token, Workload Identity,
//! CI OIDC token, or a service-account PAT) and pass it as the `bearer_token` field
//! in `FlightCredentials`, or via the Flight `Authorization: Bearer` header.
//!
//! This provider:
//! 1. Detects credentials that look like a JWT (starts with `eyJ`)
//! 2. Fetches and caches the JWKS from the configured endpoint
//! 3. Validates the JWT signature (RS256), expiry, and optionally audience/issuer
//! 4. Extracts user identity and roles from configurable claim paths
//! 5. Returns the same JWT as the catalog token (passthrough)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use jsonwebtoken::{
    decode, decode_header, Algorithm, DecodingKey, TokenData, Validation,
};
use moka::future::Cache;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::provider::{AuthError, AuthProvider, FlightCredentials, Identity};

/// Configuration for the bearer token provider.
#[derive(Debug, Clone)]
pub struct BearerTokenProviderConfig {
    /// URL to fetch the JSON Web Key Set (JWKS) from.
    /// Example: `https://idp.example.com/.well-known/jwks.json`
    pub jwks_url: String,
    /// Optional: expected JWT `iss` (issuer) claim. Skipped if empty.
    pub issuer: Option<String>,
    /// Expected JWT `aud` (audience) claim. Required by default — without
    /// audience binding, any JWT signed by the configured JWKS issuer would
    /// be accepted, including tokens minted for unrelated SaaS apps that
    /// share the IdP (confused-deputy). Operators who genuinely want
    /// token-soup mode must set `allow_unbounded_audience = true` (visible
    /// in config diffs). See issue #8.
    pub audience: Option<String>,
    /// JWT claim to use as the user ID. Default: `"sub"`.
    pub user_claim: String,
    /// Dot-separated JSON path to the roles array in the JWT payload.
    /// Default: `"realm_access.roles"`.
    pub roles_claim: String,
    /// Accept invalid TLS certificates (self-signed, expired, wrong hostname).
    /// Set `true` for development/Docker environments with self-signed certs.
    pub accept_invalid_certs: bool,
    /// Explicit opt-in to accept tokens with any audience. Defaults to
    /// `false`: an empty/missing `audience` then yields a config error at
    /// construction. Setting this `true` acknowledges that tokens issued
    /// for any service sharing the IdP will be accepted.
    pub allow_unbounded_audience: bool,
    /// Explicit opt-in to allow a non-`https` `jwks_url`. Defaults to
    /// `false`: a `http://` (or any non-https) JWKS endpoint then yields a
    /// config error at construction. The JWKS is the highest-trust input in
    /// the auth path. an on-path attacker who can rewrite an unencrypted
    /// JWKS response substitutes their own RSA public key and mints tokens
    /// SQE accepts as valid. Setting this `true` (visible in config diffs)
    /// acknowledges that risk and is intended only for local/dev setups.
    pub allow_insecure_jwks: bool,
}

impl Default for BearerTokenProviderConfig {
    fn default() -> Self {
        Self {
            jwks_url: String::new(),
            issuer: None,
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            accept_invalid_certs: false,
            allow_unbounded_audience: false,
            allow_insecure_jwks: false,
        }
    }
}

/// A single JSON Web Key from a JWKS response.
#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    /// Key ID (used to match against JWT `kid` header).
    kid: Option<String>,
    /// Key type (e.g. "RSA").
    kty: String,
    /// RSA modulus (base64url-encoded).
    n: Option<String>,
    /// RSA exponent (base64url-encoded).
    e: Option<String>,
    /// Algorithm (e.g. "RS256").
    #[serde(default)]
    #[allow(dead_code)]
    alg: Option<String>,
}

/// JWKS response from the identity provider.
#[derive(Debug, Clone, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

/// Cached JWKS: maps `kid` -> `DecodingKey`.
type JwksMap = HashMap<String, DecodingKey>;

/// Bearer token authentication provider.
///
/// Validates pre-obtained JWTs against a remote JWKS endpoint. The JWKS is
/// cached with a 15-minute TTL and refreshed on cache miss or `kid` mismatch.
pub struct BearerTokenProvider {
    client: reqwest::Client,
    config: BearerTokenProviderConfig,
    /// Cached JWKS keyed by a fixed cache key. The value is an `Arc<JwksMap>`.
    jwks_cache: Cache<String, Arc<JwksMap>>,
    /// Mutex to prevent concurrent JWKS fetches (thundering herd).
    fetch_mutex: Mutex<()>,
    /// Monotonic instant of the last successful or attempted refetch. Used to
    /// rate-limit refetches triggered by unknown-`kid` / signature-mismatch
    /// bearers so a stream of bad tokens cannot hammer the IdP (AUTH-07).
    last_refetch: Mutex<Option<std::time::Instant>>,
}

/// Minimum interval between JWKS refetches triggered by an unknown `kid` or a
/// signature mismatch. Independent of the request-level rate limiter so the
/// IdP is protected even when rate limiting is disabled. A legitimate key
/// rotation is still picked up within this window or by the 15-minute TTL.
const REFETCH_MIN_INTERVAL: Duration = Duration::from_secs(30);

impl BearerTokenProvider {
    /// Create a new bearer token provider with the given configuration.
    pub fn new(config: BearerTokenProviderConfig) -> Result<Self, AuthError> {
        // Audience binding is required by default. Without it, any JWT
        // signed by the configured JWKS issuer would be accepted —
        // including tokens minted for other SaaS apps that share the IdP.
        // Operators who genuinely want token-soup mode must set
        // `allow_unbounded_audience = true` (visible in config diffs).
        // See issue #8.
        let has_audience = config
            .audience
            .as_deref()
            .map(|a| !a.is_empty())
            .unwrap_or(false);
        if !has_audience {
            if config.allow_unbounded_audience {
                tracing::warn!(
                    "JWT audience validation is DISABLED via allow_unbounded_audience = true. \
                     Tokens issued for any service sharing the IdP will be accepted."
                );
            } else {
                return Err(AuthError::Internal(anyhow::anyhow!(
                    "bearer_token provider requires a non-empty `audience` (set audience, \
                     or set allow_unbounded_audience = true to opt in to token-soup mode)"
                )));
            }
        }

        // The JWKS is the highest-trust input in the auth path: a forged
        // public key on an unencrypted channel lets an attacker mint tokens
        // SQE accepts as valid. Reject a non-`https` `jwks_url` unless the
        // operator explicitly opts in (mirrors `allow_unbounded_audience`).
        let is_https = config
            .jwks_url
            .trim()
            .to_ascii_lowercase()
            .starts_with("https://");
        if !is_https {
            if config.allow_insecure_jwks {
                tracing::warn!(
                    jwks_url = %config.jwks_url,
                    "JWKS endpoint is not HTTPS and allow_insecure_jwks = true. \
                     An on-path attacker can substitute the signing keys and forge \
                     identities. Use this only in local/dev environments."
                );
            } else {
                return Err(AuthError::Internal(anyhow::anyhow!(
                    "bearer_token provider requires an https `jwks_url` (got '{}'); \
                     set allow_insecure_jwks = true to opt in to an insecure JWKS \
                     endpoint for local/dev use",
                    config.jwks_url
                )));
            }
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("Failed to build HTTP client: {e}"))
            })?;

        let jwks_cache = Cache::builder()
            .max_capacity(1)
            .time_to_live(Duration::from_secs(15 * 60)) // 15 minutes
            .build();

        Ok(Self {
            client,
            config,
            jwks_cache,
            fetch_mutex: Mutex::new(()),
            last_refetch: Mutex::new(None),
        })
    }

    /// Create a new provider with a custom reqwest client (for testing).
    #[cfg(test)]
    #[allow(dead_code)]
    fn with_client(
        config: BearerTokenProviderConfig,
        client: reqwest::Client,
    ) -> Self {
        let jwks_cache = Cache::builder()
            .max_capacity(1)
            .time_to_live(Duration::from_secs(15 * 60))
            .build();

        Self {
            client,
            config,
            jwks_cache,
            fetch_mutex: Mutex::new(()),
            last_refetch: Mutex::new(None),
        }
    }

    /// The fixed cache key for the JWKS.
    const CACHE_KEY: &'static str = "jwks";

    /// Get the cached JWKS, or fetch it from the endpoint.
    async fn get_jwks(&self) -> Result<Arc<JwksMap>, AuthError> {
        if let Some(cached) = self.jwks_cache.get(Self::CACHE_KEY).await {
            return Ok(cached);
        }
        self.fetch_and_cache_jwks().await
    }

    /// Fetch the JWKS from the configured URL and parse it into a key map.
    ///
    /// Does not touch the cache; callers decide how to store the result.
    async fn fetch_jwks_map(&self) -> Result<JwksMap, AuthError> {
        debug!(url = %self.config.jwks_url, "Fetching JWKS from endpoint");

        let response = self
            .client
            .get(&self.config.jwks_url)
            .send()
            .await
            .map_err(|e| {
                AuthError::Internal(anyhow::anyhow!("JWKS fetch failed: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(AuthError::Internal(anyhow::anyhow!(
                "JWKS endpoint returned {status}"
            )));
        }

        let jwks: JwksResponse = response.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("Failed to parse JWKS response: {e}"))
        })?;

        let mut map = HashMap::new();
        for key in &jwks.keys {
            // Only process RSA keys.
            if key.kty != "RSA" {
                continue;
            }

            let (n, e) = match (&key.n, &key.e) {
                (Some(n), Some(e)) => (n.as_str(), e.as_str()),
                _ => continue,
            };

            let decoding_key = match DecodingKey::from_rsa_components(n, e) {
                Ok(dk) => dk,
                Err(err) => {
                    warn!(kid = ?key.kid, error = %err, "Skipping malformed RSA key");
                    continue;
                }
            };

            if let Some(kid) = &key.kid {
                map.insert(kid.clone(), decoding_key);
            }
        }

        debug!(key_count = map.len(), "JWKS loaded");
        Ok(map)
    }

    /// Fetch the JWKS from the configured URL and cache it.
    ///
    /// Uses a mutex to prevent concurrent fetches (thundering herd protection).
    async fn fetch_and_cache_jwks(&self) -> Result<Arc<JwksMap>, AuthError> {
        let _guard = self.fetch_mutex.lock().await;

        // Double-check: another task may have populated the cache while we waited.
        if let Some(cached) = self.jwks_cache.get(Self::CACHE_KEY).await {
            return Ok(cached);
        }

        let map = self.fetch_jwks_map().await?;
        let cached = Arc::new(map);
        self.jwks_cache
            .insert(Self::CACHE_KEY.to_string(), Arc::clone(&cached))
            .await;
        Ok(cached)
    }

    /// Refetch the JWKS and merge new keys into the cached map.
    ///
    /// Unlike a naive invalidate-then-fetch, the existing cache is never
    /// dropped until a fresh fetch succeeds, so a transient IdP outage (or a
    /// flood of unknown-`kid` bearers) cannot wipe good keys for legitimate
    /// users (AUTH-07). Refetches are rate-limited to one per
    /// `REFETCH_MIN_INTERVAL` to bound IdP amplification independent of the
    /// request-level limiter. Within the cooldown the current cache is
    /// returned unchanged.
    async fn refetch_jwks(&self) -> Result<Arc<JwksMap>, AuthError> {
        let _guard = self.fetch_mutex.lock().await;

        // Rate-limit: skip the network fetch if we refetched very recently.
        {
            let mut last = self.last_refetch.lock().await;
            if let Some(prev) = *last {
                if prev.elapsed() < REFETCH_MIN_INTERVAL {
                    debug!("JWKS refetch suppressed (within cooldown); using cached keys");
                    if let Some(cached) = self.jwks_cache.get(Self::CACHE_KEY).await {
                        return Ok(cached);
                    }
                    // No cache yet and still cooling down: fetch fresh below.
                }
            }
            *last = Some(std::time::Instant::now());
        }

        let fresh = self.fetch_jwks_map().await?;

        // Merge fresh keys over the existing cache. New keys win; keys that
        // disappeared from the JWKS are retained until the TTL expires so an
        // in-flight token signed by a still-valid-but-rotating key is not
        // rejected mid-rotation.
        let mut merged: JwksMap = match self.jwks_cache.get(Self::CACHE_KEY).await {
            Some(existing) => (*existing).clone(),
            None => HashMap::new(),
        };
        for (kid, key) in fresh {
            merged.insert(kid, key);
        }

        let cached = Arc::new(merged);
        self.jwks_cache
            .insert(Self::CACHE_KEY.to_string(), Arc::clone(&cached))
            .await;
        Ok(cached)
    }

    /// Validate and decode a JWT token against the cached JWKS.
    ///
    /// On `kid` mismatch, refetches the JWKS once and retries (handles key rotation).
    async fn validate_jwt(
        &self,
        token: &str,
    ) -> Result<TokenData<serde_json::Value>, AuthError> {
        // Decode the JWT header to find the `kid`.
        let header = decode_header(token).map_err(|e| {
            AuthError::AuthFailed(format!("Invalid JWT header: {e}"))
        })?;

        let kid = header.kid.ok_or_else(|| {
            AuthError::AuthFailed("JWT header missing `kid` field".to_string())
        })?;

        // Build the validation configuration.
        let mut validation = Validation::new(Algorithm::RS256);

        // Configure audience validation.
        match &self.config.audience {
            Some(aud) if !aud.is_empty() => {
                validation.set_audience(&[aud]);
            }
            _ => {
                validation.validate_aud = false;
            }
        }

        // Configure issuer validation.
        match &self.config.issuer {
            Some(iss) if !iss.is_empty() => {
                validation.set_issuer(&[iss]);
            }
            _ => {}
        }

        // exp is validated by default in jsonwebtoken.

        // First attempt: use cached JWKS.
        let jwks = self.get_jwks().await?;

        if let Some(decoding_key) = jwks.get(&kid) {
            match decode::<serde_json::Value>(token, decoding_key, &validation) {
                Ok(token_data) => return Ok(token_data),
                Err(e) => {
                    // Only a signature mismatch is worth a JWKS refetch (a key
                    // may have rotated). Match on the typed `ErrorKind`, not the
                    // Display string, so upstream wording changes do not silently
                    // disable rotation handling (AUTH-05).
                    use jsonwebtoken::errors::ErrorKind;
                    if !matches!(e.kind(), ErrorKind::InvalidSignature) {
                        return Err(Self::map_jwt_error(e));
                    }
                    debug!(kid = %kid, "Signature validation failed, will try JWKS refetch");
                }
            }
        } else {
            debug!(kid = %kid, "Key ID not found in cached JWKS, refetching");
        }

        // Second attempt: refetch JWKS and retry (handles key rotation).
        let jwks = self.refetch_jwks().await?;

        let decoding_key = jwks.get(&kid).ok_or_else(|| {
            AuthError::AuthFailed(format!(
                "Key ID '{kid}' not found in JWKS after refresh"
            ))
        })?;

        decode::<serde_json::Value>(token, decoding_key, &validation)
            .map_err(Self::map_jwt_error)
    }

    /// Map a `jsonwebtoken::errors::Error` to an `AuthError`.
    fn map_jwt_error(err: jsonwebtoken::errors::Error) -> AuthError {
        use jsonwebtoken::errors::ErrorKind;
        match err.kind() {
            ErrorKind::ExpiredSignature => {
                AuthError::AuthFailed("JWT has expired".to_string())
            }
            ErrorKind::InvalidAudience => {
                AuthError::AuthFailed("JWT audience mismatch".to_string())
            }
            ErrorKind::InvalidIssuer => {
                AuthError::AuthFailed("JWT issuer mismatch".to_string())
            }
            ErrorKind::InvalidSignature => {
                AuthError::AuthFailed("JWT signature verification failed".to_string())
            }
            _ => AuthError::AuthFailed(format!("JWT validation failed: {err}")),
        }
    }

    /// Extract a claim value by a dot-separated path (e.g. "realm_access.roles").
    fn extract_claim_by_path<'a>(
        claims: &'a serde_json::Value,
        path: &str,
    ) -> Option<&'a serde_json::Value> {
        let mut current = claims;
        for segment in path.split('.') {
            current = current.get(segment)?;
        }
        Some(current)
    }

    /// Extract the user ID from JWT claims using the configured `user_claim`.
    fn extract_user_id(
        claims: &serde_json::Value,
        user_claim: &str,
    ) -> Option<String> {
        Self::extract_claim_by_path(claims, user_claim)
            .and_then(|v| v.as_str())
            .map(String::from)
    }

    /// Extract roles from JWT claims using the configured `roles_claim` path.
    fn extract_roles(claims: &serde_json::Value, roles_claim: &str) -> Vec<String> {
        Self::extract_claim_by_path(claims, roles_claim)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Detect if the credentials contain a JWT meant for this provider.
    ///
    /// A JWT is detected by the `eyJ` prefix (base64-encoded `{"` header start).
    /// Checks `bearer_token` first, then falls back to `password`.
    fn detect_jwt(credentials: &FlightCredentials) -> Option<String> {
        // Primary: explicit bearer_token field.
        if let Some(token) = &credentials.bearer_token {
            let raw = token.expose();
            if raw.starts_with("eyJ") {
                return Some(raw.to_string());
            }
        }

        // Fallback: password field that looks like a JWT (Flight Basic auth workaround).
        if let Some(password) = &credentials.password {
            let raw = password.expose();
            if raw.starts_with("eyJ") {
                return Some(raw.to_string());
            }
        }

        None
    }
}

#[async_trait]
impl AuthProvider for BearerTokenProvider {
    async fn authenticate(&self, credentials: &FlightCredentials) -> Result<Identity, AuthError> {
        // Step 1: Detect if credentials contain a JWT.
        let token = match Self::detect_jwt(credentials) {
            Some(t) => t,
            None => return Err(AuthError::NotMyCredentials),
        };

        // Step 2-3: Validate the JWT (signature, expiry, audience, issuer).
        let token_data = self.validate_jwt(&token).await?;
        let claims = &token_data.claims;

        // Step 4: Extract user identity and roles.
        let user_id = Self::extract_user_id(claims, &self.config.user_claim)
            .ok_or_else(|| {
                AuthError::AuthFailed(format!(
                    "JWT missing '{}' claim",
                    self.config.user_claim
                ))
            })?;

        let roles = Self::extract_roles(claims, &self.config.roles_claim);

        // Read JWT `exp` (Unix seconds) so the session expires when the bearer
        // actually expires, not on a hard-coded 1h. Issue #26.
        let expires_at = claims
            .get("exp")
            .and_then(|v| v.as_i64())
            .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0));

        debug!(
            user_id = %user_id,
            roles = ?roles,
            "Bearer token authentication succeeded"
        );

        Ok(Identity {
            user_id: user_id.clone(),
            display_name: user_id,
            roles,
            catalog_token: Some(sqe_core::SecretString::new(token)),
            refresh_token: None,
            expires_at,
        })
    }

    /// Return the same JWT as the catalog token (passthrough).
    async fn refresh_catalog_token(
        &self,
        identity: &Identity,
    ) -> Result<Option<sqe_core::SecretString>, AuthError> {
        Ok(identity.catalog_token.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;
    use std::sync::LazyLock;

    const TEST_KID: &str = "test-key-1";

    // -----------------------------------------------------------------------
    // Static RSA 2048-bit test keypair — NOT for production use.
    // Generated with: openssl genrsa 2048
    // Eliminates rsa crate dev-dependency (RUSTSEC-2023-0071).
    // -----------------------------------------------------------------------

    /// PKCS#1 PEM-encoded RSA private key for test JWT signing.
    const TEST_RSA_PRIVATE_KEY_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAvX0NLGQkeecTCMwoDPx6Qkutsf0PWBEjZnNm4vkmTTvHD1F0
NE1XHFBwxxm3eZYaEI59au96PkH5RZS0A7XivkKOj9xcmG4MtZLa4f0UKW6hWXBB
RG7ilMaCLcBxSxp6aewxWvYfv5wu1VCfDZ55jYRTx9JMm0vHo89kHb5XMHX2jhVH
XAqOaNG4EQLLxku8NsuCCft1M54WMKqx5VNdzpNh6jqESGEU3LDI3fNvvFbuXFzC
PYqfVOiTk7DfSWx/IgKeFbQWuWXBixNg4l72AUlbGU3wcnHr7l3GMb+Es7wt0AlW
+CHdDzJgvZfvmS2u9vIhb7Qu54x3cBLFGSqGCQIDAQABAoIBAHQdjVUGiNOqph9d
+6z5inHVrjrDuANffTHqxcGQq8foObNJVsw2GIthP7rCJ4x6Tr6WkoRU+7Pq+bWJ
ykX7z1aHspS1lIhT57XcqAST8SbyhC0qfNRSnsZMXrlqlAJR13HRKu1ypUHlk01k
ehL+ab4uuKhaVldTuKLJE7CmUwd+MvJYxlEJO9ywI5mZ4Ks+Dc7uJw3A6IEbFeZK
50HzLkve+7yy1RE771EllWm2y1i/VRycH2axUR8gZhdQNl39vi2FkMKokf4CCg0Q
ZHSPaIdDkfsNMC5w3Luf9BeGrNVoPGm1QB+A5wl0i/01JLPNBRGT13wMBsHurC1q
D+8Cc4ECgYEA8PNcEBUKFz0lNxXqXpfjLQg2IJv/laheK4S7Ek4Lgu8ELyb5pEP9
91IhghIHPBcupj63QWBqhHPM5fJwgqOtuDPxSu3PGTUzteh6Xs3wd7+7y7vQ52b/
HopK5A0Sri+mrJs5kh9T/pr9CyeRjHAxo2J3NbOrcGGuHZ45+ORaFaMCgYEAyVLY
rR4bGu6bKdFLdYMR6Q5hOXmPanG6kYWU99WIAaLkNg3Rlys9X/Tn98HZgUOVu5i+
NfvSCAIKttysLqkVst4iD/4eykZCuIRxhwu813ThPI5zLChoU/DYCSiVjlSgGawH
mQ/fkunvb2D7XCrpUvu9xRSKddDMT3EEQ2CNuGMCgYEAiiOro1i8mUgn/uXkoWjJ
CLdNePKW3HFT0/Vb3wm5lc58gqAAvclxYArJRS4a0bukthD8tVGWn+tYDHkrQeqf
HR1CeCfQ9O3IgMEQ7yt4ct8MxqgeA5zMJPE6MHbCP/T3xLuVjQ3C9RRcgLmlu3NT
Mg2wtKwWXO7TiQ1+xQ/+CasCgYByeYItRe4hrUVbTN/8bM/1VjDgboem/g4ZCvz+
w1M3ovji546ix3p5opd4IKjdwKFWb27Q4WS3GvoeqnHZglmNQJPbxiKZ38O2idDH
+luho5sjRNimZj+UY2FkK8iGiwYSMuiLFySItC5qhZnH+bp8bhqlAp4MifJyxY+o
BDHxgwKBgDlcrdCtPqUVy0gp+1NpboOFvbi9QBp3GV0g0hcu1dFyw7pB7ts0Tu7H
1vJmTV7qtPF2vnSeNX+W42ZPGFbT9nswiQ8rMod5QFqywTyvuUUTqoxkEbhTPqQB
fGaGdPurwOnXPCbnSxiTHsQWwcx2KhPWpUsg/msrL8LU3DRravWV
-----END RSA PRIVATE KEY-----";

    /// Base64url-encoded RSA modulus (n) for JWKS mock responses.
    const TEST_RSA_N: &str = "vX0NLGQkeecTCMwoDPx6Qkutsf0PWBEjZnNm4vkmTTvHD1F0NE1XHFBwxxm3eZYaEI59au96PkH5RZS0A7XivkKOj9xcmG4MtZLa4f0UKW6hWXBBRG7ilMaCLcBxSxp6aewxWvYfv5wu1VCfDZ55jYRTx9JMm0vHo89kHb5XMHX2jhVHXAqOaNG4EQLLxku8NsuCCft1M54WMKqx5VNdzpNh6jqESGEU3LDI3fNvvFbuXFzCPYqfVOiTk7DfSWx_IgKeFbQWuWXBixNg4l72AUlbGU3wcnHr7l3GMb-Es7wt0AlW-CHdDzJgvZfvmS2u9vIhb7Qu54x3cBLFGSqGCQ";

    /// Base64url-encoded RSA public exponent (e) for JWKS mock responses.
    const TEST_RSA_E: &str = "AQAB";

    /// RSA key pair loaded once at test-time (thread-safe lazy init).
    struct TestKeyPair {
        encoding_key: EncodingKey,
        /// Base64url-encoded RSA modulus (n) for JWKS.
        n: String,
        /// Base64url-encoded RSA exponent (e) for JWKS.
        e: String,
    }

    static TEST_KEYS: LazyLock<TestKeyPair> = LazyLock::new(|| {
        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes()).unwrap();
        TestKeyPair {
            encoding_key,
            n: TEST_RSA_N.to_string(),
            e: TEST_RSA_E.to_string(),
        }
    });

    /// Build a valid JWT signed with the test RSA key.
    fn build_signed_jwt(claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());

        encode(&header, claims, &TEST_KEYS.encoding_key).unwrap()
    }

    /// Build a JWKS JSON response containing the test public key.
    fn build_jwks_json() -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "kid": TEST_KID,
                "alg": "RS256",
                "use": "sig",
                "n": &TEST_KEYS.n,
                "e": &TEST_KEYS.e
            }]
        })
    }

    /// Default config for tests.
    ///
    /// Tests opt in to `allow_unbounded_audience` because they cover JWKS,
    /// signature, and claim-handling behaviour rather than the audience
    /// guard itself. The audience-required default is exercised by
    /// [`requires_audience_unless_explicitly_unbounded`].
    fn test_config(jwks_url: &str) -> BearerTokenProviderConfig {
        BearerTokenProviderConfig {
            jwks_url: jwks_url.to_string(),
            issuer: None,
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            accept_invalid_certs: false,
            allow_unbounded_audience: true,
            // Tests serve JWKS over a local http listener; opt in explicitly.
            allow_insecure_jwks: true,
        }
    }

    // -----------------------------------------------------------------------
    // detect_jwt
    // -----------------------------------------------------------------------

    #[test]
    fn detect_jwt_from_bearer_token() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string())),
            ..Default::default()
        };
        let token = BearerTokenProvider::detect_jwt(&creds);
        assert!(token.is_some());
        assert!(token.unwrap().starts_with("eyJ"));
    }

    #[test]
    fn detect_jwt_from_password_field() {
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string())),
            ..Default::default()
        };
        let token = BearerTokenProvider::detect_jwt(&creds);
        assert!(token.is_some());
    }

    #[test]
    fn detect_jwt_bearer_token_takes_precedence() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("eyJbearer".to_string())),
            password: Some(sqe_core::SecretString::new("eyJpassword".to_string())),
            ..Default::default()
        };
        let token = BearerTokenProvider::detect_jwt(&creds);
        assert_eq!(token.unwrap(), "eyJbearer");
    }

    #[test]
    fn detect_jwt_returns_none_for_non_jwt() {
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new("regular-password".to_string())),
            ..Default::default()
        };
        assert!(BearerTokenProvider::detect_jwt(&creds).is_none());
    }

    #[test]
    fn detect_jwt_returns_none_for_empty_credentials() {
        let creds = FlightCredentials::default();
        assert!(BearerTokenProvider::detect_jwt(&creds).is_none());
    }

    // -----------------------------------------------------------------------
    // extract_claim_by_path
    // -----------------------------------------------------------------------

    #[test]
    fn extract_claim_single_segment() {
        let claims = json!({"sub": "alice"});
        let result = BearerTokenProvider::extract_claim_by_path(&claims, "sub");
        assert_eq!(result.unwrap().as_str().unwrap(), "alice");
    }

    #[test]
    fn extract_claim_nested_path() {
        let claims = json!({
            "realm_access": {
                "roles": ["admin", "user"]
            }
        });
        let result =
            BearerTokenProvider::extract_claim_by_path(&claims, "realm_access.roles");
        assert!(result.unwrap().is_array());
    }

    #[test]
    fn extract_claim_missing_path() {
        let claims = json!({"sub": "alice"});
        let result =
            BearerTokenProvider::extract_claim_by_path(&claims, "nonexistent.path");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // extract_user_id
    // -----------------------------------------------------------------------

    #[test]
    fn extract_user_id_default_claim() {
        let claims = json!({"sub": "user-123"});
        assert_eq!(
            BearerTokenProvider::extract_user_id(&claims, "sub"),
            Some("user-123".to_string())
        );
    }

    #[test]
    fn extract_user_id_custom_claim() {
        let claims = json!({"email": "alice@example.com"});
        assert_eq!(
            BearerTokenProvider::extract_user_id(&claims, "email"),
            Some("alice@example.com".to_string())
        );
    }

    #[test]
    fn extract_user_id_missing_claim() {
        let claims = json!({"other": "value"});
        assert!(BearerTokenProvider::extract_user_id(&claims, "sub").is_none());
    }

    // -----------------------------------------------------------------------
    // extract_roles
    // -----------------------------------------------------------------------

    #[test]
    fn extract_roles_nested() {
        let claims = json!({
            "realm_access": {
                "roles": ["admin", "user"]
            }
        });
        let roles =
            BearerTokenProvider::extract_roles(&claims, "realm_access.roles");
        assert_eq!(roles, vec!["admin", "user"]);
    }

    #[test]
    fn extract_roles_flat() {
        let claims = json!({"groups": ["engineering", "platform"]});
        let roles = BearerTokenProvider::extract_roles(&claims, "groups");
        assert_eq!(roles, vec!["engineering", "platform"]);
    }

    #[test]
    fn extract_roles_missing_returns_empty() {
        let claims = json!({"sub": "alice"});
        let roles =
            BearerTokenProvider::extract_roles(&claims, "realm_access.roles");
        assert!(roles.is_empty());
    }

    #[test]
    fn extract_roles_skips_non_strings() {
        let claims = json!({"roles": ["admin", 42, null, "user"]});
        let roles = BearerTokenProvider::extract_roles(&claims, "roles");
        assert_eq!(roles, vec!["admin", "user"]);
    }

    // -----------------------------------------------------------------------
    // config defaults
    // -----------------------------------------------------------------------

    #[test]
    fn config_defaults() {
        let config = BearerTokenProviderConfig::default();
        assert!(config.jwks_url.is_empty());
        assert!(config.issuer.is_none());
        assert!(config.audience.is_none());
        assert_eq!(config.user_claim, "sub");
        assert_eq!(config.roles_claim, "realm_access.roles");
    }

    // -----------------------------------------------------------------------
    // Integration tests with mock JWKS server
    // -----------------------------------------------------------------------

    /// Start a mock HTTP server that serves a JWKS response.
    async fn start_jwks_server(
        jwks_json: serde_json::Value,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());

        let jwks_body = serde_json::to_string(&jwks_json).unwrap();

        let handle = tokio::spawn(async move {
            // Serve up to 10 requests then stop.
            for _ in 0..10 {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    jwks_body.len(),
                    jwks_body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });

        (handle, url)
    }

    /// Start a mock HTTP server that tracks request counts (for JWKS refetch testing).
    async fn start_counting_jwks_server(
        jwks_json: serde_json::Value,
    ) -> (tokio::task::JoinHandle<()>, String, Arc<std::sync::atomic::AtomicUsize>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let jwks_body = serde_json::to_string(&jwks_json).unwrap();

        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    jwks_body.len(),
                    jwks_body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });

        (handle, url, counter)
    }

    // -----------------------------------------------------------------------
    // 3.7: Valid JWT → returns Identity with correct claims
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn valid_jwt_returns_identity() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = test_config(&jwks_url);
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "realm_access": {
                "roles": ["admin", "reader"]
            },
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token.clone())),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should authenticate successfully");

        assert_eq!(identity.user_id, "alice");
        assert_eq!(identity.display_name, "alice");
        assert_eq!(identity.roles, vec!["admin", "reader"]);
        assert_eq!(
            identity.catalog_token.as_ref().map(|t| t.expose()),
            Some(token.as_str()),
        );
        assert!(identity.refresh_token.is_none());
        // Issue #26: JWT `exp` must propagate into the Identity so SessionManager
        // can evict the cached session when the underlying bearer actually
        // expires, not on a hard-coded 1h.
        let expected_exp = chrono::DateTime::from_timestamp((now + 3600) as i64, 0)
            .expect("valid timestamp");
        assert_eq!(identity.expires_at, Some(expected_exp));
    }

    // -----------------------------------------------------------------------
    // 3.7: Expired JWT → returns AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn expired_jwt_returns_auth_failed() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = test_config(&jwks_url);
        let provider = BearerTokenProvider::new(config).unwrap();

        let claims = json!({
            "sub": "alice",
            "realm_access": { "roles": ["admin"] },
            "exp": 1000000000, // long expired
            "iat": 999999000
        });

        let token = build_signed_jwt(&claims);

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(
                    msg.contains("expired"),
                    "Expected expiry error, got: {msg}"
                );
            }
            other => panic!("Expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // 3.7: No bearer token in credentials → returns NotMyCredentials
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_bearer_token_returns_not_my_credentials() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        // No bearer_token, no password, nothing JWT-like.
        let creds = FlightCredentials {
            username: Some("alice".to_string()),
            password: Some(sqe_core::SecretString::new("regular-password".to_string())),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        assert!(
            matches!(result, Err(AuthError::NotMyCredentials)),
            "Expected NotMyCredentials, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn empty_credentials_returns_not_my_credentials() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let creds = FlightCredentials::default();
        let result = provider.authenticate(&creds).await;
        assert!(matches!(result, Err(AuthError::NotMyCredentials)));
    }

    // -----------------------------------------------------------------------
    // 3.7: Unknown kid → triggers JWKS refetch
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_kid_triggers_jwks_refetch() {
        // Start with a JWKS that has the correct key (the refetch will also
        // return the correct key). The key point is verifying that the JWKS
        // endpoint is called more than once when the kid is initially unknown.
        let jwks = build_jwks_json();
        let (_handle, jwks_url, counter) =
            start_counting_jwks_server(jwks).await;

        let config = test_config(&jwks_url);
        let provider = BearerTokenProvider::new(config).unwrap();

        // Pre-populate cache with a JWKS that does NOT contain our kid.
        let wrong_jwks: Arc<JwksMap> = Arc::new(HashMap::new());
        provider
            .jwks_cache
            .insert(BearerTokenProvider::CACHE_KEY.to_string(), wrong_jwks)
            .await;

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "bob",
            "realm_access": { "roles": ["viewer"] },
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        // This should fail to find the kid in the pre-populated cache, refetch, and succeed.
        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should succeed after JWKS refetch");

        assert_eq!(identity.user_id, "bob");
        assert_eq!(identity.roles, vec!["viewer"]);

        // The JWKS endpoint should have been called at least once (the refetch).
        let fetch_count =
            counter.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            fetch_count >= 1,
            "Expected at least 1 JWKS fetch, got {fetch_count}"
        );
    }

    // -----------------------------------------------------------------------
    // Audience validation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn audience_mismatch_returns_auth_failed() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            audience: Some("expected-audience".to_string()),
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "aud": "wrong-audience",
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(
                    msg.contains("audience"),
                    "Expected audience error, got: {msg}"
                );
            }
            other => panic!("Expected AuthFailed for audience mismatch, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn audience_match_succeeds() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            audience: Some("sqe".to_string()),
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "aud": "sqe",
            "realm_access": { "roles": ["admin"] },
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should succeed with matching audience");
        assert_eq!(identity.user_id, "alice");
    }

    // -----------------------------------------------------------------------
    // Issuer validation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn issuer_mismatch_returns_auth_failed() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            issuer: Some("https://expected.example.com".to_string()),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "iss": "https://wrong.example.com",
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(
                    msg.contains("issuer"),
                    "Expected issuer error, got: {msg}"
                );
            }
            other => panic!("Expected AuthFailed for issuer mismatch, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // JWT passed as password field (Flight Basic auth workaround)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn jwt_in_password_field_works() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = test_config(&jwks_url);
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "charlie",
            "realm_access": { "roles": ["developer"] },
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            username: Some("ignored".to_string()),
            password: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should accept JWT from password field");
        assert_eq!(identity.user_id, "charlie");
        assert_eq!(identity.roles, vec!["developer"]);
    }

    // -----------------------------------------------------------------------
    // Custom user_claim and roles_claim
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn custom_claims_extraction() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            user_claim: "email".to_string(),
            roles_claim: "groups".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "user-123",
            "email": "alice@example.com",
            "groups": ["engineering", "platform"],
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should use custom claims");
        assert_eq!(identity.user_id, "alice@example.com");
        assert_eq!(identity.roles, vec!["engineering", "platform"]);
    }

    // -----------------------------------------------------------------------
    // Missing user claim → AuthFailed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn missing_user_claim_returns_auth_failed() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            user_claim: "email".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "user-123",
            // no "email" claim
            "exp": now + 3600,
            "iat": now
        });

        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        let result = provider.authenticate(&creds).await;
        match result {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("email"), "Expected mention of claim, got: {msg}");
            }
            other => panic!("Expected AuthFailed, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // refresh_catalog_token returns the same JWT (passthrough)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn refresh_returns_same_token() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let identity = Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec!["admin".to_string()],
            catalog_token: Some(sqe_core::SecretString::new("the-jwt-token".to_string())),
            refresh_token: None,
            expires_at: None,
        };

        let result = provider
            .refresh_catalog_token(&identity)
            .await
            .expect("should succeed");
        assert_eq!(result.as_ref().map(|t| t.expose()), Some("the-jwt-token"));
    }

    #[tokio::test]
    async fn refresh_returns_none_when_no_catalog_token() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let identity = Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec![],
            catalog_token: None,
            refresh_token: None,
            expires_at: None,
        };

        let result = provider
            .refresh_catalog_token(&identity)
            .await
            .expect("should succeed");
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Provider construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_succeeds_with_valid_config() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:8080/.well-known/jwks.json".to_string(),
            issuer: Some("https://idp.example.com".to_string()),
            audience: Some("sqe".to_string()),
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            accept_invalid_certs: false,
            allow_unbounded_audience: false,
            allow_insecure_jwks: true,
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    // --- Issue #8: JWT audience required by default ---

    #[test]
    fn new_rejects_missing_audience_by_default() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            audience: None,
            allow_unbounded_audience: false,
            ..Default::default()
        };
        match BearerTokenProvider::new(config) {
            Err(AuthError::Internal(e)) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("audience") || msg.contains("allow_unbounded_audience"),
                    "error must mention audience guard: {msg}"
                );
            }
            Err(other) => panic!("expected Internal config error, got: {other:?}"),
            Ok(_) => panic!("must reject empty audience"),
        }
    }

    #[test]
    fn new_rejects_empty_string_audience() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            audience: Some(String::new()),
            allow_unbounded_audience: false,
            ..Default::default()
        };
        match BearerTokenProvider::new(config) {
            Err(AuthError::Internal(e)) => {
                assert!(format!("{e}").to_lowercase().contains("audience"));
            }
            Err(other) => panic!("expected Internal config error, got: {other:?}"),
            Ok(_) => panic!("must reject empty audience"),
        }
    }

    #[test]
    fn new_accepts_missing_audience_when_explicit_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            audience: None,
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    #[test]
    fn new_accepts_non_empty_audience_without_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:0/jwks".to_string(),
            audience: Some("sqe".to_string()),
            allow_unbounded_audience: false,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    // --- AUTH-02: JWKS URL must be HTTPS unless explicitly opted in ---

    #[test]
    fn new_rejects_http_jwks_by_default() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            allow_unbounded_audience: false,
            allow_insecure_jwks: false,
            ..Default::default()
        };
        match BearerTokenProvider::new(config) {
            Err(AuthError::Internal(e)) => {
                let msg = format!("{e}").to_lowercase();
                assert!(
                    msg.contains("https") || msg.contains("allow_insecure_jwks"),
                    "error must mention the https/jwks guard: {msg}"
                );
            }
            Err(other) => panic!("expected Internal config error, got: {other:?}"),
            Ok(_) => panic!("must reject http jwks_url without opt-in"),
        }
    }

    #[test]
    fn new_accepts_https_jwks_without_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "https://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            allow_unbounded_audience: false,
            allow_insecure_jwks: false,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    #[test]
    fn new_accepts_http_jwks_with_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "http://localhost:8080/jwks".to_string(),
            audience: Some("sqe".to_string()),
            allow_unbounded_audience: false,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }
}
