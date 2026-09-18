//! `BearerTokenProvider` -- JWT bearer token validation against a JWKS endpoint.
//!
//! Clients pre-obtain a JWT (e.g. Kubernetes ServiceAccount token, Workload Identity,
//! CI OIDC token, or a service-account PAT) and pass it as the `bearer_token` field
//! in `FlightCredentials`, or via the Flight `Authorization: Bearer` header.
//!
//! This provider:
//! 1. Detects credentials that look like a JWT (starts with `eyJ`)
//! 2. Fetches and caches the JWKS from the configured endpoint
//! 3. Validates the JWT signature (RSA or EC, algorithm pinned per JWKS key),
//!    `exp`, `nbf`, audience and issuer
//! 4. Extracts user identity and roles from configurable claim paths
//! 5. Returns the same JWT as the catalog token (passthrough)

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
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
    /// Expected JWT `iss` (issuer) claim. Required by default — a
    /// multi-tenant IdP (Entra, an Okta org with several authorization
    /// servers) serves one JWKS for many issuers, so audience binding alone
    /// does not stop a token minted by a sibling issuer for the same
    /// audience string. Operators who genuinely need to accept any issuer
    /// must set `allow_unbounded_issuer = true` (visible in config diffs).
    /// See finding 207.
    ///
    /// There is no OIDC-discovery hook on this provider: the JWKS URL is
    /// taken verbatim from config, so the config loader (or the operator) is
    /// responsible for setting `issuer` to the discovery document's `issuer`
    /// value when the JWKS URL came from a discovery document.
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
    /// JWT claim to extract as the canonical subject identifier.
    /// Default: `"sub"`. Set to empty to disable subject extraction.
    pub subject_claim: String,
    /// JWT claim to extract as the email address. Default: `""` (disabled).
    /// Set to `"email"` (or a custom path) to populate `Identity.email`.
    pub email_claim: String,
    /// Dot-separated JSON path to the groups array in the JWT payload.
    /// Default: `""` (disabled). Separate from `roles_claim`.
    pub groups_claim: String,
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
    /// Explicit opt-in to accept tokens from any issuer. Defaults to
    /// `false`: an empty/missing `issuer` then yields a config error at
    /// construction. Setting this `true` acknowledges that a token minted by
    /// any issuer sharing the JWKS will be accepted.
    pub allow_unbounded_issuer: bool,
    /// Clock-skew allowance, in seconds, applied to `exp` and `nbf`.
    /// Defaults to 60 (the `jsonwebtoken` default, now visible in config).
    /// `nbf` is validated — pre-dated tokens are rejected once they are more
    /// than `leeway_secs` in the future.
    pub leeway_secs: u64,
}

/// Default clock-skew allowance for `exp`/`nbf`, in seconds. Matches the
/// historical `jsonwebtoken` default so existing deployments see no change.
pub const DEFAULT_LEEWAY_SECS: u64 = 60;

impl Default for BearerTokenProviderConfig {
    fn default() -> Self {
        Self {
            jwks_url: String::new(),
            issuer: None,
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            accept_invalid_certs: false,
            allow_unbounded_audience: false,
            allow_insecure_jwks: false,
            allow_unbounded_issuer: false,
            leeway_secs: DEFAULT_LEEWAY_SECS,
        }
    }
}

/// A single JSON Web Key from a JWKS response.
#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    /// Key ID (used to match against JWT `kid` header).
    kid: Option<String>,
    /// Key type (`"RSA"` or `"EC"`).
    kty: String,
    /// RSA modulus (base64url-encoded).
    n: Option<String>,
    /// RSA exponent (base64url-encoded).
    e: Option<String>,
    /// EC curve name (e.g. `"P-256"`).
    #[serde(default)]
    crv: Option<String>,
    /// EC public key x coordinate (base64url-encoded).
    #[serde(default)]
    x: Option<String>,
    /// EC public key y coordinate (base64url-encoded).
    #[serde(default)]
    y: Option<String>,
    /// Algorithm (e.g. "RS256", "ES256"). Pins the algorithm accepted for
    /// this key so a token cannot downgrade/confuse the signature family.
    #[serde(default)]
    alg: Option<String>,
}

/// JWKS response from the identity provider.
#[derive(Debug, Clone, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

/// A JWKS key plus the metadata needed to validate with it.
#[derive(Clone)]
struct CachedJwk {
    /// The decoding key material.
    key: DecodingKey,
    /// Algorithm pinned to this key (from the JWK `alg`, else derived from
    /// `kty`/`crv`). Validation accepts only this algorithm, so an attacker
    /// cannot present, say, an `ES256` token against an RSA key entry.
    alg: Algorithm,
    /// When this key was last seen in a JWKS fetch. Drives the retirement
    /// grace window in [`BearerTokenProvider::refetch_jwks`].
    last_seen: Instant,
}

/// Cached JWKS: maps `kid` -> [`CachedJwk`].
type JwksMap = HashMap<String, CachedJwk>;

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
    last_refetch: Mutex<Option<Instant>>,
    /// How long a key that has disappeared from the JWKS is still honoured
    /// after a successful refetch. Injectable so tests can set it to zero.
    key_grace_window: Duration,
}

/// Minimum interval between JWKS refetches triggered by an unknown `kid` or a
/// signature mismatch. Independent of the request-level rate limiter so the
/// IdP is protected even when rate limiting is disabled. A legitimate key
/// rotation is still picked up within this window or by the 15-minute TTL.
const REFETCH_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// How long a key that has vanished from a freshly fetched JWKS is still
/// accepted.
///
/// Trade-off: rotation and revocation look identical to a relying party. A
/// long window is friendly to rotation (tokens signed just before the IdP
/// dropped the old key keep working); a short window is the right answer for
/// revocation (a compromised key stops being honoured as soon as the IdP
/// removes it). Five minutes is the compromise — it covers the seconds-scale
/// propagation delay of a rotation without keeping a revoked key alive for
/// the full 15-minute cache TTL, which is what the previous merge-on-refetch
/// did. See finding 207.
const KEY_RETIREMENT_GRACE: Duration = Duration::from_secs(5 * 60);

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

        // Issuer binding is required by default for the same reason audience
        // binding is: a multi-tenant IdP shares one JWKS across issuers, so a
        // token minted by a sibling issuer for the same audience string would
        // otherwise be accepted. Finding 207.
        let has_issuer = config
            .issuer
            .as_deref()
            .map(|i| !i.is_empty())
            .unwrap_or(false);
        if !has_issuer {
            if config.allow_unbounded_issuer {
                tracing::warn!(
                    "JWT issuer validation is DISABLED via allow_unbounded_issuer = true. \
                     Tokens minted by any issuer sharing this JWKS will be accepted."
                );
            } else {
                return Err(AuthError::Internal(anyhow::anyhow!(
                    "bearer_token provider requires a non-empty `issuer` (set issuer to the \
                     IdP's `iss` value — the OIDC discovery document's `issuer` field — or \
                     set allow_unbounded_issuer = true to accept any issuer)"
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
            key_grace_window: KEY_RETIREMENT_GRACE,
        })
    }

    /// Override the key-retirement grace window (tests only).
    #[cfg(test)]
    fn with_key_grace_window(mut self, window: Duration) -> Self {
        self.key_grace_window = window;
        self
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
            .map_err(|e| AuthError::Internal(anyhow::anyhow!("JWKS fetch failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(AuthError::Internal(anyhow::anyhow!(
                "JWKS endpoint returned {status}"
            )));
        }

        let jwks: JwksResponse = response.json().await.map_err(|e| {
            AuthError::Internal(anyhow::anyhow!("Failed to parse JWKS response: {e}"))
        })?;

        let now = Instant::now();
        let mut map = HashMap::new();
        for key in &jwks.keys {
            let Some(kid) = key.kid.clone() else {
                warn!(kty = %key.kty, "Skipping JWKS key without a `kid`");
                continue;
            };

            let Some(alg) = Self::key_algorithm(key) else {
                warn!(
                    kid = %kid,
                    kty = %key.kty,
                    alg = ?key.alg,
                    crv = ?key.crv,
                    "Skipping JWKS key with an unsupported algorithm"
                );
                continue;
            };

            let decoding_key = match key.kty.as_str() {
                "RSA" => {
                    let (Some(n), Some(e)) = (&key.n, &key.e) else {
                        warn!(kid = %kid, "Skipping RSA key without `n`/`e`");
                        continue;
                    };
                    match DecodingKey::from_rsa_components(n, e) {
                        Ok(dk) => dk,
                        Err(err) => {
                            warn!(kid = %kid, error = %err, "Skipping malformed RSA key");
                            continue;
                        }
                    }
                }
                "EC" => {
                    let (Some(x), Some(y)) = (&key.x, &key.y) else {
                        warn!(kid = %kid, "Skipping EC key without `x`/`y`");
                        continue;
                    };
                    match DecodingKey::from_ec_components(x, y) {
                        Ok(dk) => dk,
                        Err(err) => {
                            warn!(kid = %kid, error = %err, "Skipping malformed EC key");
                            continue;
                        }
                    }
                }
                other => {
                    debug!(kid = %kid, kty = %other, "Skipping JWKS key of unsupported type");
                    continue;
                }
            };

            map.insert(
                kid,
                CachedJwk {
                    key: decoding_key,
                    alg,
                    last_seen: now,
                },
            );
        }

        if map.is_empty() {
            // Every bearer will now fail until the next successful fetch, so
            // say so loudly rather than emitting "Key ID not found in JWKS"
            // per request with no explanation of why.
            warn!(
                url = %self.config.jwks_url,
                advertised_keys = jwks.keys.len(),
                "JWKS contains no loadable key (only RSA and EC keys with a `kid` are supported) \
                 — all bearer token authentication will fail"
            );
        } else {
            debug!(key_count = map.len(), "JWKS loaded");
        }
        Ok(map)
    }

    /// Resolve the algorithm to pin to a JWKS key.
    ///
    /// Prefers the JWK's own `alg`; falls back to the key type (and, for EC,
    /// the curve) when the IdP omits it. Returns `None` for anything
    /// `jsonwebtoken` cannot verify, or for an `alg` whose family does not
    /// match `kty` (an RSA entry advertising `ES256` is malformed at best and
    /// an algorithm-confusion attempt at worst).
    fn key_algorithm(key: &Jwk) -> Option<Algorithm> {
        if let Some(alg) = key.alg.as_deref().filter(|a| !a.is_empty()) {
            let parsed = Algorithm::from_str(alg).ok()?;
            let family_matches = match key.kty.as_str() {
                "RSA" => matches!(
                    parsed,
                    Algorithm::RS256
                        | Algorithm::RS384
                        | Algorithm::RS512
                        | Algorithm::PS256
                        | Algorithm::PS384
                        | Algorithm::PS512
                ),
                "EC" => matches!(parsed, Algorithm::ES256 | Algorithm::ES384),
                _ => false,
            };
            return family_matches.then_some(parsed);
        }

        match key.kty.as_str() {
            "RSA" => Some(Algorithm::RS256),
            "EC" => match key.crv.as_deref() {
                Some("P-256") => Some(Algorithm::ES256),
                Some("P-384") => Some(Algorithm::ES384),
                _ => None,
            },
            _ => None,
        }
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

    /// Refetch the JWKS and replace the cached map with the fresh one.
    ///
    /// Unlike a naive invalidate-then-fetch, the existing cache is never
    /// dropped until a fresh fetch succeeds, so a transient IdP outage (or a
    /// flood of unknown-`kid` bearers) cannot wipe good keys for legitimate
    /// users (AUTH-07). Refetches are rate-limited to one per
    /// `REFETCH_MIN_INTERVAL` to bound IdP amplification independent of the
    /// request-level limiter. Within the cooldown the current cache is
    /// returned unchanged.
    ///
    /// On a successful fetch the fresh JWKS *replaces* the cache rather than
    /// merging into it: a key the IdP has dropped is a revocation as often as
    /// it is a rotation, and the safe reading is the revocation one. Keys
    /// that disappeared are kept only while they are within
    /// `key_grace_window` of when they were last seen, and only when the
    /// fresh JWKS still carries at least one key. A successful fetch that
    /// yields zero loadable keys gets no grace at all: the cache is emptied
    /// and every bearer fails until the next successful fetch (at most
    /// `REFETCH_MIN_INTERVAL` later). That is deliberate — "the IdP published
    /// an empty key set" must not be the one input that extends the life of a
    /// retired key. See `KEY_RETIREMENT_GRACE` for the rotation-vs-revocation
    /// trade-off.
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

        let mut fresh = self.fetch_jwks_map().await?;

        // Replace-on-success. Retired keys survive only inside the grace
        // window, and only when the IdP still published at least one key.
        if !fresh.is_empty() && !self.key_grace_window.is_zero() {
            if let Some(existing) = self.jwks_cache.get(Self::CACHE_KEY).await {
                for (kid, entry) in existing.iter() {
                    if fresh.contains_key(kid) {
                        continue;
                    }
                    if entry.last_seen.elapsed() <= self.key_grace_window {
                        debug!(
                            kid = %kid,
                            "Key absent from fresh JWKS, honouring it for the rest of the \
                             retirement grace window"
                        );
                        fresh.insert(kid.clone(), entry.clone());
                    } else {
                        warn!(
                            kid = %kid,
                            "Key absent from fresh JWKS and past the retirement grace window, \
                             dropping it"
                        );
                    }
                }
            }
        }

        let cached = Arc::new(fresh);
        self.jwks_cache
            .insert(Self::CACHE_KEY.to_string(), Arc::clone(&cached))
            .await;
        Ok(cached)
    }

    /// Validate and decode a JWT token against the cached JWKS.
    ///
    /// On `kid` mismatch, refetches the JWKS once and retries (handles key rotation).
    async fn validate_jwt(&self, token: &str) -> Result<TokenData<serde_json::Value>, AuthError> {
        // Decode the JWT header to find the `kid`.
        let header = decode_header(token)
            .map_err(|e| AuthError::AuthFailed(format!("Invalid JWT header: {e}")))?;

        let kid = header
            .kid
            .ok_or_else(|| AuthError::AuthFailed("JWT header missing `kid` field".to_string()))?;

        // First attempt: use cached JWKS.
        let jwks = self.get_jwks().await?;

        if let Some(entry) = jwks.get(&kid) {
            let validation = self.build_validation(entry.alg);
            match decode::<serde_json::Value>(token, &entry.key, &validation) {
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

        let entry = jwks.get(&kid).ok_or_else(|| {
            AuthError::AuthFailed(format!("Key ID '{kid}' not found in JWKS after refresh"))
        })?;

        let validation = self.build_validation(entry.alg);
        decode::<serde_json::Value>(token, &entry.key, &validation).map_err(Self::map_jwt_error)
    }

    /// Build the `Validation` used for one JWKS key.
    ///
    /// The accepted algorithm is pinned to the key's own `alg` rather than a
    /// global RS256, `nbf` is checked, and the clock-skew allowance comes
    /// from `leeway_secs` instead of the library default.
    fn build_validation(&self, alg: Algorithm) -> Validation {
        let mut validation = Validation::new(alg);

        // exp is validated by default in jsonwebtoken; nbf is not.
        validation.validate_nbf = true;
        validation.leeway = self.config.leeway_secs;

        // Configure audience validation.
        match &self.config.audience {
            Some(aud) if !aud.is_empty() => {
                validation.set_audience(&[aud]);
            }
            _ => {
                validation.validate_aud = false;
            }
        }

        // Configure issuer validation. Required unless the operator opted
        // into `allow_unbounded_issuer`, which `new` already warned about.
        // `iss` also joins the required-claims set: jsonwebtoken only
        // compares the issuer when the claim is *present*, so without this a
        // token that simply omits `iss` would sail past a configured issuer.
        match &self.config.issuer {
            Some(iss) if !iss.is_empty() => {
                validation.set_issuer(&[iss]);
                validation.set_required_spec_claims(&["exp", "iss"]);
            }
            _ => {}
        }

        validation
    }

    /// Map a `jsonwebtoken::errors::Error` to an `AuthError`.
    fn map_jwt_error(err: jsonwebtoken::errors::Error) -> AuthError {
        use jsonwebtoken::errors::ErrorKind;
        match err.kind() {
            ErrorKind::ExpiredSignature => AuthError::AuthFailed("JWT has expired".to_string()),
            ErrorKind::ImmatureSignature => {
                AuthError::AuthFailed("JWT is not valid yet (nbf is in the future)".to_string())
            }
            ErrorKind::InvalidAlgorithm => AuthError::AuthFailed(
                "JWT algorithm does not match the signing key's algorithm".to_string(),
            ),
            ErrorKind::InvalidAudience => {
                AuthError::AuthFailed("JWT audience mismatch".to_string())
            }
            ErrorKind::InvalidIssuer => AuthError::AuthFailed("JWT issuer mismatch".to_string()),
            ErrorKind::MissingRequiredClaim(claim) => {
                AuthError::AuthFailed(format!("JWT missing required claim '{claim}'"))
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
    fn extract_user_id(claims: &serde_json::Value, user_claim: &str) -> Option<String> {
        Self::extract_claim_by_path(claims, user_claim)
            .and_then(|v| v.as_str())
            .map(String::from)
    }

    /// Extract a string list from JWT claims using a dot-separated path.
    ///
    /// Returns an empty `Vec` when `path` is empty or the claim is missing/not
    /// an array. Used by both `extract_roles` and groups extraction so they
    /// share one traversal helper (DRY).
    pub(crate) fn extract_string_list(claims: &serde_json::Value, path: &str) -> Vec<String> {
        if path.is_empty() {
            return Vec::new();
        }
        Self::extract_claim_by_path(claims, path)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Extract roles from JWT claims using the configured `roles_claim` path.
    fn extract_roles(claims: &serde_json::Value, roles_claim: &str) -> Vec<String> {
        Self::extract_string_list(claims, roles_claim)
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
    async fn authenticate_oauth(
        &self,
        credentials: &FlightCredentials,
    ) -> Result<Identity, AuthError> {
        if self.config.allow_unbounded_audience
            && self.config.audience.as_deref().is_none_or(str::is_empty)
        {
            return Err(AuthError::AuthFailed(
                "OAuth requires a configured token audience".into(),
            ));
        }
        self.authenticate(credentials).await
    }

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
        let user_id = Self::extract_user_id(claims, &self.config.user_claim).ok_or_else(|| {
            AuthError::AuthFailed(format!("JWT missing '{}' claim", self.config.user_claim))
        })?;

        let roles = Self::extract_roles(claims, &self.config.roles_claim);

        let subject = if self.config.subject_claim.is_empty() {
            None
        } else {
            Self::extract_claim_by_path(claims, &self.config.subject_claim)
                .and_then(|v| v.as_str())
                .map(String::from)
        };

        let email = if self.config.email_claim.is_empty() {
            None
        } else {
            Self::extract_claim_by_path(claims, &self.config.email_claim)
                .and_then(|v| v.as_str())
                .map(String::from)
        };

        let groups = Self::extract_string_list(claims, &self.config.groups_claim);

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
            subject,
            email,
            groups,
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
    const TEST_EC_KID: &str = "test-ec-key-1";
    const TEST_ISSUER: &str = "https://idp.example.com";

    // -----------------------------------------------------------------------
    // Static RSA 2048-bit test keypair — NOT for production use.
    // Generated with: openssl genrsa 2048
    // Eliminates rsa crate dev-dependency (RUSTSEC-2023-0071).
    // -----------------------------------------------------------------------

    /// PKCS#1 PEM-encoded RSA private key for test JWT signing.
    // nosemgrep: opt.aikido.semgrep-rules.generic.secrets.gitleaks.private-key -- throwaway test-only keypair, never used outside cfg(test)
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
        let encoding_key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes()).unwrap();
        TestKeyPair {
            encoding_key,
            n: TEST_RSA_N.to_string(),
            e: TEST_RSA_E.to_string(),
        }
    });

    // -----------------------------------------------------------------------
    // Static EC P-256 test keypair — NOT for production use.
    // Generated with:
    //   openssl ecparam -name prime256v1 -genkey -noout -out ec.pem
    //   openssl pkcs8 -topk8 -nocrypt -in ec.pem
    // The x/y coordinates below are the uncompressed public point, split and
    // base64url-encoded. Static material keeps EC coverage without adding a
    // key-generation dev-dependency (same reasoning as the RSA pair above).
    // -----------------------------------------------------------------------

    /// PKCS#8 PEM-encoded EC P-256 private key for test ES256 JWT signing.
    const TEST_EC_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgcb/PT1Xj+xW0FvVc
0YZLwJCPra8OSopguEXNw3d3TbihRANCAASkMxNpk8e4ypVo60ZUB3Q4TIHOcXSJ
Gsk+TB4BOZOZvvRKOPj1Dj5tcKc/tv2Hx8kiS4egVEC2qWmKaBDbv9kN
-----END PRIVATE KEY-----";

    /// Base64url-encoded EC public key x coordinate for JWKS mock responses.
    const TEST_EC_X: &str = "pDMTaZPHuMqVaOtGVAd0OEyBznF0iRrJPkweATmTmb4";

    /// Base64url-encoded EC public key y coordinate for JWKS mock responses.
    const TEST_EC_Y: &str = "9Eo4-PUOPm1wpz-2_YfHySJLh6BUQLapaYpoENu_2Q0";

    static TEST_EC_ENCODING_KEY: LazyLock<EncodingKey> =
        LazyLock::new(|| EncodingKey::from_ec_pem(TEST_EC_PRIVATE_KEY_PEM.as_bytes()).unwrap());

    /// Build a valid JWT signed with the test RSA key.
    fn build_signed_jwt(claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());

        encode(&header, claims, &TEST_KEYS.encoding_key).unwrap()
    }

    /// Build a valid ES256 JWT signed with the test EC key.
    fn build_signed_es256_jwt(claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(TEST_EC_KID.to_string());

        encode(&header, claims, &TEST_EC_ENCODING_KEY).unwrap()
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

    /// Build a JWKS JSON response containing the test EC public key.
    fn build_ec_jwks_json() -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "EC",
                "kid": TEST_EC_KID,
                "alg": "ES256",
                "crv": "P-256",
                "use": "sig",
                "x": TEST_EC_X,
                "y": TEST_EC_Y
            }]
        })
    }

    /// Build a JWKS JSON response containing a single unrelated RSA key, so
    /// the document is non-empty but no longer carries [`TEST_KID`].
    fn build_jwks_json_without_test_key() -> serde_json::Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "kid": "some-other-key",
                "alg": "RS256",
                "use": "sig",
                "n": &TEST_KEYS.n,
                "e": &TEST_KEYS.e
            }]
        })
    }

    /// Default config for tests.
    ///
    /// Tests opt in to `allow_unbounded_audience` and
    /// `allow_unbounded_issuer` because they cover JWKS, signature, and
    /// claim-handling behaviour rather than the audience/issuer guards
    /// themselves. Those defaults are exercised by
    /// [`new_rejects_missing_audience_by_default`] and
    /// [`new_rejects_missing_issuer_by_default`].
    fn test_config(jwks_url: &str) -> BearerTokenProviderConfig {
        BearerTokenProviderConfig {
            jwks_url: jwks_url.to_string(),
            issuer: None,
            audience: None,
            user_claim: "sub".to_string(),
            roles_claim: "realm_access.roles".to_string(),
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            accept_invalid_certs: false,
            allow_unbounded_audience: true,
            // Tests serve JWKS over a local http listener; opt in explicitly.
            allow_insecure_jwks: true,
            allow_unbounded_issuer: true,
            leeway_secs: DEFAULT_LEEWAY_SECS,
        }
    }

    // -----------------------------------------------------------------------
    // detect_jwt
    // -----------------------------------------------------------------------

    #[test]
    fn detect_jwt_from_bearer_token() {
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(
                "eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string(),
            )),
            ..Default::default()
        };
        let token = BearerTokenProvider::detect_jwt(&creds);
        assert!(token.is_some());
        assert!(token.unwrap().starts_with("eyJ"));
    }

    #[test]
    fn detect_jwt_from_password_field() {
        let creds = FlightCredentials {
            password: Some(sqe_core::SecretString::new(
                "eyJhbGciOiJSUzI1NiJ9.payload.sig".to_string(),
            )),
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
        let result = BearerTokenProvider::extract_claim_by_path(&claims, "realm_access.roles");
        assert!(result.unwrap().is_array());
    }

    #[test]
    fn extract_claim_missing_path() {
        let claims = json!({"sub": "alice"});
        let result = BearerTokenProvider::extract_claim_by_path(&claims, "nonexistent.path");
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
        let roles = BearerTokenProvider::extract_roles(&claims, "realm_access.roles");
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
        let roles = BearerTokenProvider::extract_roles(&claims, "realm_access.roles");
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
        // Finding 207: the unbounded escapes are opt-in, and the clock-skew
        // allowance is visible rather than inherited from jsonwebtoken.
        assert!(!config.allow_unbounded_issuer);
        assert!(!config.allow_unbounded_audience);
        assert_eq!(config.leeway_secs, DEFAULT_LEEWAY_SECS);
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
    ) -> (
        tokio::task::JoinHandle<()>,
        String,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
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

    /// Start a mock JWKS server that serves a scripted sequence of bodies:
    /// request *n* gets `responses[n]`, and every request past the end of the
    /// script repeats the last entry. Responses carry `Connection: close` so
    /// reqwest does not reuse a pooled connection the server has already
    /// stopped reading from.
    async fn start_scripted_jwks_server(
        responses: Vec<serde_json::Value>,
    ) -> (tokio::task::JoinHandle<()>, String) {
        assert!(!responses.is_empty(), "script must have at least one body");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());

        let bodies: Vec<String> = responses
            .iter()
            .map(|v| serde_json::to_string(v).unwrap())
            .collect();

        let handle = tokio::spawn(async move {
            for served in 0usize..10 {
                let (mut stream, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;

                let body = &bodies[served.min(bodies.len() - 1)];

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            }
        });

        (handle, url)
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
        let expected_exp =
            chrono::DateTime::from_timestamp((now + 3600) as i64, 0).expect("valid timestamp");
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
                assert!(msg.contains("expired"), "Expected expiry error, got: {msg}");
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
            allow_unbounded_issuer: true,
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
            allow_unbounded_issuer: true,
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
        let (_handle, jwks_url, counter) = start_counting_jwks_server(jwks).await;

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
        let fetch_count = counter.load(std::sync::atomic::Ordering::SeqCst);
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
            allow_unbounded_issuer: true,
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
            allow_unbounded_issuer: true,
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
                assert!(msg.contains("issuer"), "Expected issuer error, got: {msg}");
            }
            other => panic!("Expected AuthFailed for issuer mismatch, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn issuer_match_succeeds() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            issuer: Some(TEST_ISSUER.to_string()),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "iss": TEST_ISSUER,
            "exp": now + 3600,
            "iat": now
        });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("should succeed with a matching issuer");
        assert_eq!(identity.user_id, "alice");
    }

    /// A token that simply omits `iss` must not slip past a configured
    /// issuer: `jsonwebtoken` only compares the claim when it is present, so
    /// the provider marks `iss` required. Finding 207.
    #[tokio::test]
    async fn missing_issuer_claim_rejected_when_issuer_configured() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let config = BearerTokenProviderConfig {
            jwks_url: jwks_url.clone(),
            issuer: Some(TEST_ISSUER.to_string()),
            allow_unbounded_audience: true,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            // no "iss" claim at all
            "exp": now + 3600,
            "iat": now
        });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(msg.contains("iss"), "Expected an issuer claim error: {msg}");
            }
            other => panic!("Expected AuthFailed for a missing iss claim, got: {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // nbf validation (finding 207)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn nbf_in_the_future_returns_auth_failed() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url)).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            // Well outside the 60 s default leeway.
            "nbf": now + 3600,
            "exp": now + 7200,
            "iat": now
        });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(
                    msg.contains("not valid yet"),
                    "Expected an nbf error, got: {msg}"
                );
            }
            other => panic!("Expected AuthFailed for a future nbf, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn nbf_in_the_past_is_accepted() {
        let jwks = build_jwks_json();
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url)).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "alice",
            "nbf": now - 60,
            "exp": now + 3600,
            "iat": now - 60
        });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("a token whose nbf has passed must still authenticate");
        assert_eq!(identity.user_id, "alice");
    }

    // -----------------------------------------------------------------------
    // EC (ES256) JWKS support (finding 207)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn es256_token_validates_against_ec_jwks() {
        let (_handle, jwks_url) = start_jwks_server(build_ec_jwks_json()).await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url)).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({
            "sub": "erika",
            "realm_access": { "roles": ["analyst"] },
            "exp": now + 3600,
            "iat": now
        });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_es256_jwt(&claims))),
            ..Default::default()
        };

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("an ES256 token must validate against an EC JWKS");
        assert_eq!(identity.user_id, "erika");
        assert_eq!(identity.roles, vec!["analyst"]);
    }

    /// The algorithm is pinned per key, so an RSA-signed token presented
    /// against an EC key entry is rejected rather than silently accepted.
    #[tokio::test]
    async fn rs256_token_rejected_against_ec_key_entry() {
        // EC JWKS that reuses the RSA key's `kid`, so the lookup hits the EC
        // entry with the RSA-signed token.
        let jwks = json!({
            "keys": [{
                "kty": "EC",
                "kid": TEST_KID,
                "alg": "ES256",
                "crv": "P-256",
                "use": "sig",
                "x": TEST_EC_X,
                "y": TEST_EC_Y
            }]
        });
        let (_handle, jwks_url) = start_jwks_server(jwks).await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url)).unwrap();

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({ "sub": "mallory", "exp": now + 3600, "iat": now });

        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        assert!(
            provider.authenticate(&creds).await.is_err(),
            "an RS256 token must not validate against an ES256 key entry"
        );
    }

    #[test]
    fn key_algorithm_pins_alg_per_key() {
        let rsa = Jwk {
            kid: Some("k".into()),
            kty: "RSA".into(),
            n: Some("n".into()),
            e: Some("AQAB".into()),
            crv: None,
            x: None,
            y: None,
            alg: Some("RS512".into()),
        };
        assert_eq!(
            BearerTokenProvider::key_algorithm(&rsa),
            Some(Algorithm::RS512)
        );

        // No `alg`: derived from kty/crv.
        let ec_p256 = Jwk {
            kid: Some("k".into()),
            kty: "EC".into(),
            n: None,
            e: None,
            crv: Some("P-256".into()),
            x: Some("x".into()),
            y: Some("y".into()),
            alg: None,
        };
        assert_eq!(
            BearerTokenProvider::key_algorithm(&ec_p256),
            Some(Algorithm::ES256)
        );

        // Family mismatch (RSA key advertising an EC alg) is rejected.
        let confused = Jwk {
            alg: Some("ES256".into()),
            ..rsa.clone()
        };
        assert_eq!(BearerTokenProvider::key_algorithm(&confused), None);

        // Symmetric algorithms never come from a JWKS we would trust.
        let hmac = Jwk {
            alg: Some("HS256".into()),
            ..rsa
        };
        assert_eq!(BearerTokenProvider::key_algorithm(&hmac), None);

        // Unsupported key type.
        let oct = Jwk {
            kid: Some("k".into()),
            kty: "oct".into(),
            n: None,
            e: None,
            crv: None,
            x: None,
            y: None,
            alg: None,
        };
        assert_eq!(BearerTokenProvider::key_algorithm(&oct), None);
    }

    // -----------------------------------------------------------------------
    // Key retirement on refetch (finding 207)
    // -----------------------------------------------------------------------

    /// A key the IdP has dropped from its JWKS must stop being honoured once
    /// the grace window has passed — rotation and revocation look identical
    /// from here, and revocation is the safe reading.
    #[tokio::test]
    async fn key_removed_from_jwks_is_rejected_after_refetch() {
        let (_handle, jwks_url) =
            start_scripted_jwks_server(vec![build_jwks_json(), build_jwks_json_without_test_key()])
                .await;

        // Zero grace window: a key absent from the fresh JWKS is dropped
        // immediately.
        let provider = BearerTokenProvider::new(test_config(&jwks_url))
            .unwrap()
            .with_key_grace_window(Duration::ZERO);

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({ "sub": "alice", "exp": now + 3600, "iat": now });
        let token = build_signed_jwt(&claims);
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(token)),
            ..Default::default()
        };

        // First fetch serves the key: authentication succeeds.
        provider
            .authenticate(&creds)
            .await
            .expect("first authentication should succeed while the key is published");

        // The IdP retires the key; a refetch picks up the new key set.
        provider
            .refetch_jwks()
            .await
            .expect("refetch should succeed");

        // The retired key is gone, so the same token is now rejected. (The
        // refetch cooldown keeps this second attempt from re-fetching.)
        match provider.authenticate(&creds).await {
            Err(AuthError::AuthFailed(msg)) => {
                assert!(
                    msg.contains(TEST_KID),
                    "Expected the retired kid to be reported, got: {msg}"
                );
            }
            other => panic!("Expected AuthFailed after key retirement, got: {other:?}"),
        }
    }

    /// Inside the grace window the same retired key is still honoured, so a
    /// routine rotation does not reject in-flight tokens.
    #[tokio::test]
    async fn key_removed_from_jwks_is_honoured_inside_grace_window() {
        let (_handle, jwks_url) =
            start_scripted_jwks_server(vec![build_jwks_json(), build_jwks_json_without_test_key()])
                .await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url))
            .unwrap()
            .with_key_grace_window(Duration::from_secs(300));

        let now = chrono::Utc::now().timestamp() as u64;
        let claims = json!({ "sub": "alice", "exp": now + 3600, "iat": now });
        let creds = FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new(build_signed_jwt(&claims))),
            ..Default::default()
        };

        provider
            .authenticate(&creds)
            .await
            .expect("first authentication should succeed");

        provider
            .refetch_jwks()
            .await
            .expect("refetch should succeed");

        let identity = provider
            .authenticate(&creds)
            .await
            .expect("a just-retired key must stay valid inside the grace window");
        assert_eq!(identity.user_id, "alice");
    }

    /// A JWKS that parses but carries no loadable key must not resurrect the
    /// previous key set.
    #[tokio::test]
    async fn empty_fresh_jwks_clears_the_cache() {
        let (_handle, jwks_url) =
            start_scripted_jwks_server(vec![build_jwks_json(), json!({ "keys": [] })]).await;

        let provider = BearerTokenProvider::new(test_config(&jwks_url))
            .unwrap()
            .with_key_grace_window(Duration::from_secs(300));

        let jwks = provider.get_jwks().await.expect("initial fetch");
        assert!(jwks.contains_key(TEST_KID));

        let jwks = provider.refetch_jwks().await.expect("refetch");
        assert!(
            jwks.is_empty(),
            "an empty fresh JWKS must replace the cache, not be ignored"
        );
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
            allow_unbounded_issuer: true,
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
            allow_unbounded_issuer: true,
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
                assert!(
                    msg.contains("email"),
                    "Expected mention of claim, got: {msg}"
                );
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
            allow_unbounded_issuer: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let identity = Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec!["admin".to_string()],
            subject: None,
            email: None,
            groups: vec![],
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
            allow_unbounded_issuer: true,
            ..Default::default()
        };
        let provider = BearerTokenProvider::new(config).unwrap();

        let identity = Identity {
            user_id: "alice".to_string(),
            display_name: "Alice".to_string(),
            roles: vec![],
            subject: None,
            email: None,
            groups: vec![],
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
            subject_claim: "sub".to_string(),
            email_claim: String::new(),
            groups_claim: String::new(),
            accept_invalid_certs: false,
            allow_unbounded_audience: false,
            allow_insecure_jwks: true,
            allow_unbounded_issuer: false,
            leeway_secs: DEFAULT_LEEWAY_SECS,
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
            allow_unbounded_issuer: true,
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
            allow_unbounded_issuer: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    // --- Finding 207: JWT issuer required by default ---

    #[test]
    fn new_rejects_missing_issuer_by_default() {
        let config = BearerTokenProviderConfig {
            jwks_url: "https://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            issuer: None,
            allow_unbounded_issuer: false,
            ..Default::default()
        };
        match BearerTokenProvider::new(config) {
            Err(AuthError::Internal(e)) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("issuer") || msg.contains("allow_unbounded_issuer"),
                    "error must mention the issuer guard: {msg}"
                );
            }
            Err(other) => panic!("expected Internal config error, got: {other:?}"),
            Ok(_) => panic!("must reject a missing issuer"),
        }
    }

    #[test]
    fn new_rejects_empty_string_issuer() {
        let config = BearerTokenProviderConfig {
            jwks_url: "https://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            issuer: Some(String::new()),
            allow_unbounded_issuer: false,
            ..Default::default()
        };
        match BearerTokenProvider::new(config) {
            Err(AuthError::Internal(e)) => {
                assert!(format!("{e}").to_lowercase().contains("issuer"));
            }
            Err(other) => panic!("expected Internal config error, got: {other:?}"),
            Ok(_) => panic!("must reject an empty issuer"),
        }
    }

    #[test]
    fn new_accepts_missing_issuer_when_explicit_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "https://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            issuer: None,
            allow_unbounded_issuer: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    #[test]
    fn new_accepts_non_empty_issuer_without_opt_in() {
        let config = BearerTokenProviderConfig {
            jwks_url: "https://idp.example.com/jwks".to_string(),
            audience: Some("sqe".to_string()),
            issuer: Some(TEST_ISSUER.to_string()),
            allow_unbounded_issuer: false,
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
            issuer: Some(TEST_ISSUER.to_string()),
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
            issuer: Some(TEST_ISSUER.to_string()),
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
            issuer: Some(TEST_ISSUER.to_string()),
            allow_unbounded_audience: false,
            allow_insecure_jwks: true,
            ..Default::default()
        };
        assert!(BearerTokenProvider::new(config).is_ok());
    }

    // -----------------------------------------------------------------------
    // extract_string_list and email/groups extraction
    // -----------------------------------------------------------------------

    #[test]
    fn extracts_email_and_groups_when_claims_configured() {
        let claims = serde_json::json!({
            "sub": "u-42",
            "email": "alice@corp.example",
            "groups": ["hr", "finance"],
            "realm_access": { "roles": ["analyst"] }
        });
        let email = BearerTokenProvider::extract_claim_by_path(&claims, "email")
            .and_then(|v| v.as_str())
            .map(String::from);
        assert_eq!(email, Some("alice@corp.example".to_string()));
        let groups = BearerTokenProvider::extract_string_list(&claims, "groups");
        assert_eq!(groups, vec!["hr".to_string(), "finance".to_string()]);
    }

    #[test]
    fn empty_groups_claim_yields_no_groups() {
        let claims = serde_json::json!({ "sub": "u-1" });
        assert!(BearerTokenProvider::extract_string_list(&claims, "").is_empty());
    }
}
