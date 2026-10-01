//! Rate limiter for the SQE coordinator.
//!
//! Provides per-user and global token-bucket rate limiting using the
//! [`governor`] crate. Per-identity token buckets use bounded admission and
//! idle reclamation to keep attacker-controlled cardinality finite.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use governor::clock::{Clock, DefaultClock, Reference};
use governor::{Quota, RateLimiter};
use sha2::{Digest, Sha256};
use sqe_core::config::RateLimitConfig;
use sqe_core::error::{SqeError, SqeErrorCode};

/// Hard admission limit per keyed limiter. Keys are SHA-256 fingerprints,
/// so retained identity width is bounded as well as cardinality. At capacity,
/// existing identities keep their buckets and new identities fail closed.
const MAX_RATE_LIMIT_KEYS: usize = 16_384;
const RATE_LIMIT_PRUNE_INTERVAL: Duration = Duration::from_secs(60);

type DirectBucket<C> = RateLimiter<
    governor::state::direct::NotKeyed,
    governor::state::InMemoryState,
    C,
    governor::middleware::NoOpMiddleware<<C as Clock>::Instant>,
>;

struct KeyBucket<C: Clock> {
    limiter: DirectBucket<C>,
    last_seen: C::Instant,
}

struct KeyedBuckets<C: Clock> {
    buckets: HashMap<[u8; 32], KeyBucket<C>>,
    last_prune: C::Instant,
}

/// Keeps governor's per-identity quotas while bounding stored state. A short
/// synchronous lock serializes lookup/admission/consumption, so concurrent new
/// identities cannot overshoot the cap. Only fully replenished idle buckets
/// may be removed: evicting active buckets would reset brute-force budgets.
struct BoundedKeyedLimiter<C: Clock = DefaultClock> {
    quota: Quota,
    clock: C,
    max_keys: usize,
    idle_retention: Duration,
    state: Mutex<KeyedBuckets<C>>,
}

enum KeyedDenial<I: Reference> {
    Quota(governor::NotUntil<I>),
    Capacity,
}

impl<I: Reference> KeyedDenial<I> {
    fn retry_after_secs(&self, now: I) -> u64 {
        match self {
            Self::Quota(not_until) => retry_after_secs(not_until, now),
            Self::Capacity => RATE_LIMIT_PRUNE_INTERVAL.as_secs(),
        }
    }
}

impl BoundedKeyedLimiter {
    fn new(quota: Quota) -> Self {
        Self::with_clock(quota, MAX_RATE_LIMIT_KEYS, DefaultClock::default())
    }
}

impl<C: Clock + Clone> BoundedKeyedLimiter<C> {
    fn with_clock(quota: Quota, max_keys: usize, clock: C) -> Self {
        // checked multiplication in Duration avoids the u64 nanosecond wrap
        // possible for unusually large configured bursts.
        let idle_retention = quota
            .replenish_interval()
            .saturating_mul(quota.burst_size().get())
            .max(RATE_LIMIT_PRUNE_INTERVAL);
        let last_prune = clock.now();
        Self {
            quota,
            clock,
            max_keys,
            idle_retention,
            state: Mutex::new(KeyedBuckets {
                buckets: HashMap::new(),
                last_prune,
            }),
        }
    }

    fn clock(&self) -> &C {
        &self.clock
    }

    fn check_key(&self, key: &str) -> Result<(), KeyedDenial<C::Instant>> {
        self.check_parts(&[key])
    }

    fn check_parts(&self, parts: &[&str]) -> Result<(), KeyedDenial<C::Instant>> {
        let mut hash = Sha256::new();
        for part in parts {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        let key: [u8; 32] = hash.finalize().into();
        let mut state = self.state.lock().map_err(|_| KeyedDenial::Capacity)?;
        let now = self.clock.now();
        if now.duration_since(state.last_prune) >= RATE_LIMIT_PRUNE_INTERVAL.into() {
            state.buckets.retain(|_, bucket| {
                now.duration_since(bucket.last_seen) < self.idle_retention.into()
            });
            state.last_prune = now;
        }
        if !state.buckets.contains_key(&key) {
            if state.buckets.len() >= self.max_keys {
                return Err(KeyedDenial::Capacity);
            }
            state.buckets.insert(
                key,
                KeyBucket {
                    limiter: RateLimiter::new(
                        self.quota,
                        governor::state::InMemoryState::default(),
                        self.clock.clone(),
                    ),
                    last_seen: now,
                },
            );
        }
        let bucket = state.buckets.get_mut(&key).expect("admitted key exists");
        // Denied requests also count as activity: a continuously attacked key
        // must keep its consumed budget rather than be pruned and reset.
        bucket.last_seen = now;
        bucket.limiter.check().map_err(KeyedDenial::Quota)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state.lock().unwrap().buckets.len()
    }
}

/// Pre-auth rate limit keyed by (peer-ip, username). The username
/// comes from the Basic-auth header (Flight handshake) or the
/// `x-trino-user` / Basic-auth equivalents on the Trino-compat side,
/// so attackers can iterate it freely. The peer IP is the trusted
/// source (after `SecurityConfig::resolve_client_ip`), so the
/// composite key keeps one IP from exhausting the budget for every
/// username it tries while still limiting per-username brute force
/// from a single attacker.
pub struct AuthRateLimiter {
    enabled: bool,
    limiter: Option<Arc<BoundedKeyedLimiter>>,
    /// Per-IP-only limiter that catches credential stuffing across
    /// rotating usernames from a single source IP.
    by_ip: Option<Arc<BoundedKeyedLimiter>>,
}

impl AuthRateLimiter {
    pub fn new(config: &RateLimitConfig) -> Self {
        if !config.enabled {
            return Self {
                enabled: false,
                limiter: None,
                by_ip: None,
            };
        }
        let rate = NonZeroU32::new(config.auth_attempts_per_minute)
            .unwrap_or(NonZeroU32::new(10).unwrap());
        // The burst is what one client may present at once (an MCP agent's
        // parallel metadata calls, each with the same bearer); the rate is
        // what it may sustain. Below the rate, the burst is the rate.
        let burst =
            NonZeroU32::new(config.auth_burst.max(config.auth_attempts_per_minute)).unwrap_or(rate);
        let quota = Quota::per_minute(rate).allow_burst(burst);
        // Per-IP budget is intentionally larger than the per-tuple
        // budget so legitimate shared NAT egress isn't penalised but
        // a stuffer rotating usernames still trips the IP cap.
        let ip_quota = Quota::per_minute(
            NonZeroU32::new(config.auth_attempts_per_minute.saturating_mul(5))
                .unwrap_or(NonZeroU32::new(50).unwrap()),
        )
        .allow_burst(NonZeroU32::new(burst.get().saturating_mul(5)).unwrap_or(burst));
        Self {
            enabled: true,
            limiter: Some(Arc::new(BoundedKeyedLimiter::new(quota))),
            by_ip: Some(Arc::new(BoundedKeyedLimiter::new(ip_quota))),
        }
    }

    /// Returns `Ok(())` when the (ip, username) tuple is under budget.
    /// Returns `SqeError::Auth("rate-limited".to_string())` otherwise, so the caller
    /// can map to the protocol-specific status code without leaking
    /// detail to the unauthenticated peer.
    pub fn check(&self, peer_ip: &str, username: &str) -> Result<(), SqeError> {
        if !self.enabled {
            return Ok(());
        }
        // Spend the trusted source budget before admitting any credential key.
        // Denials from an exhausted IP therefore allocate no per-credential state.
        if let Some(ref l) = self.by_ip {
            l.check_key(peer_ip)
                .map_err(|_| SqeError::Auth("rate-limited".to_string()))?;
        }
        if let Some(ref l) = self.limiter {
            l.check_parts(&[peer_ip, username])
                .map_err(|_| SqeError::Auth("rate-limited".to_string()))?;
        }
        Ok(())
    }
}

/// Seconds until the bucket a denial came from admits one more request,
/// rounded up and at least one, so a client can wait exactly that long
/// instead of guessing.
fn retry_after_secs<P: governor::clock::Reference>(
    not_until: &governor::NotUntil<P>,
    now: P,
) -> u64 {
    let wait = not_until.wait_time_from(now);
    wait.as_secs() + u64::from(wait.subsec_nanos() > 0).max(u64::from(wait.is_zero()))
}

/// One denial from the query or metadata limiter, with what the caller needs
/// to behave: how long to wait and which knob sets the budget. Renders as the
/// `SqeError` text every protocol already carries; `retry after {n}s` at the
/// end is what the MCP adapter reads back for `Retry-After`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitDenial {
    pub limiter: &'static str,
    pub knob: &'static str,
    pub retry_after: Duration,
    pub detail: String,
}

impl RateLimitDenial {
    pub fn into_error(self) -> SqeError {
        SqeError::with_code(
            SqeErrorCode::ResourceExhausted,
            format!(
                "Rate limit exceeded: {} ({}, per minute); retry after {}s",
                self.detail,
                self.knob,
                self.retry_after.as_secs()
            ),
        )
    }
}

/// The pre-auth rate limit as an [`AuthProvider`] wrapper around the chain
/// (#562).
///
/// Every credential that reaches a provider passes here first, so no entry
/// point can forget the gate: the wrapper is installed once, where the chain
/// is built, and Flight, MCP and the dashboard inherit it. The budget key is
/// `(peer_ip, credential)`: the username for a password credential, the
/// certificate CN for mTLS, a SHA-256 fingerprint prefix for a bearer (never
/// the token itself). A denial is [`AuthError::RateLimited`], and the inner
/// provider is not consulted, so a garbage bearer costs no JWKS refetch and
/// no IdP or STS round trip.
///
/// A credential with no `peer_ip` passes through ungated, with a warning.
/// That is the migration seam: an adapter that sets the peer address has
/// dropped its own check and relies on this one; an adapter that has not
/// (Trino, Quack as of this change) still counts on its own, and counting
/// twice here would halve every budget.
pub struct RateLimitedAuthProvider {
    inner: Arc<dyn sqe_auth::AuthProvider>,
    limiter: Arc<AuthRateLimiter>,
}

impl RateLimitedAuthProvider {
    pub fn new(inner: Arc<dyn sqe_auth::AuthProvider>, limiter: Arc<AuthRateLimiter>) -> Self {
        Self { inner, limiter }
    }

    /// The non-secret half of the budget key for `credentials`.
    fn principal(credentials: &sqe_auth::FlightCredentials) -> String {
        if let Some(user) = &credentials.username {
            return user.clone();
        }
        if let Some(token) = &credentials.bearer_token {
            return sqe_trino_compat::server::bearer_fingerprint(token.expose());
        }
        if let Some(cn) = &credentials.client_cert_cn {
            return format!("cn:{cn}");
        }
        "no-credential".to_string()
    }

    fn gate(&self, credentials: &sqe_auth::FlightCredentials) -> Result<(), sqe_auth::AuthError> {
        let Some(peer_ip) = credentials.peer_ip.as_deref() else {
            tracing::debug!(
                "credential reached the provider chain without a peer address; \
                 the pre-auth limiter did not apply (the adapter keeps its own gate)"
            );
            return Ok(());
        };
        let principal = Self::principal(credentials);
        self.limiter.check(peer_ip, &principal).map_err(|_| {
            tracing::warn!(
                peer_ip,
                principal = %principal,
                "credential rejected by the pre-auth rate limiter"
            );
            sqe_auth::AuthError::RateLimited
        })
    }
}

#[async_trait::async_trait]
impl sqe_auth::AuthProvider for RateLimitedAuthProvider {
    async fn authenticate(
        &self,
        credentials: &sqe_auth::FlightCredentials,
    ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
        self.gate(credentials)?;
        self.inner.authenticate(credentials).await
    }

    async fn authenticate_oauth(
        &self,
        credentials: &sqe_auth::FlightCredentials,
    ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
        self.gate(credentials)?;
        self.inner.authenticate_oauth(credentials).await
    }

    async fn refresh_catalog_token(
        &self,
        identity: &sqe_auth::Identity,
    ) -> Result<Option<sqe_core::SecretString>, sqe_auth::AuthError> {
        // A refresh is not a new credential presentation; it spends no budget.
        self.inner.refresh_catalog_token(identity).await
    }
}

/// Lower-budget rate limiter for catalog browse paths (Flight
/// do_get_tables / do_get_schemas / do_get_catalogs / prepared
/// statement schema lookup). Each call fans out N+1 Polaris REST
/// calls, so the budget is per-user but separate from
/// `QueryRateLimiter` so SQL doesn't pay against the same budget as
/// JDBC `DatabaseMetaData` refresh loops.
pub struct MetadataRateLimiter {
    enabled: bool,
    per_user: Option<Arc<BoundedKeyedLimiter>>,
}

impl MetadataRateLimiter {
    pub fn new(config: &RateLimitConfig) -> Self {
        if !config.enabled {
            return Self {
                enabled: false,
                per_user: None,
            };
        }
        let quota = Quota::per_minute(
            NonZeroU32::new(config.metadata_per_user_per_minute)
                .unwrap_or(NonZeroU32::new(120).unwrap()),
        );
        Self {
            enabled: true,
            per_user: Some(Arc::new(BoundedKeyedLimiter::new(quota))),
        }
    }

    pub fn check(&self, user_id: &str) -> Result<(), SqeError> {
        if !self.enabled {
            return Ok(());
        }
        if let Some(ref l) = self.per_user {
            l.check_key(user_id).map_err(|not_until| {
                let secs = not_until.retry_after_secs(l.clock().now());
                RateLimitDenial {
                    limiter: "metadata",
                    knob: "rate_limit.metadata_per_user_per_minute",
                    retry_after: Duration::from_secs(secs),
                    detail: format!("metadata browse limit reached for user '{user_id}'"),
                }
                .into_error()
            })?;
        }
        Ok(())
    }
}

/// Coordinator-level rate limiter enforcing per-user and global query limits.
///
/// When rate limiting is disabled via config, all checks are no-ops.
pub struct QueryRateLimiter {
    enabled: bool,
    /// Per-user token bucket (keyed by user_id string).
    per_user: Option<Arc<BoundedKeyedLimiter>>,
    /// Global token bucket shared across all users.
    global: Option<Arc<governor::DefaultDirectRateLimiter>>,
}

impl QueryRateLimiter {
    /// Create a new rate limiter from the given configuration.
    ///
    /// If `config.enabled` is `false`, both limiters are `None` and
    /// [`check`](Self::check) always returns `Ok(())`.
    pub fn new(config: &RateLimitConfig) -> Self {
        if !config.enabled {
            return Self {
                enabled: false,
                per_user: None,
                global: None,
            };
        }

        let per_user_quota = Quota::per_minute(
            NonZeroU32::new(config.per_user_queries_per_minute)
                .unwrap_or(NonZeroU32::new(60).unwrap()),
        );
        let global_quota = Quota::per_minute(
            NonZeroU32::new(config.global_queries_per_minute)
                .unwrap_or(NonZeroU32::new(1000).unwrap()),
        );

        Self {
            enabled: true,
            per_user: Some(Arc::new(BoundedKeyedLimiter::new(per_user_quota))),
            global: Some(Arc::new(RateLimiter::direct(global_quota))),
        }
    }

    /// Check whether the given user is allowed to execute a query.
    ///
    /// Checks the per-user bucket first, then the global bucket.
    /// On failure returns `SqeError::Execution` with a descriptive message.
    /// The caller's session is **not** dropped on rate-limit errors.
    pub fn check(&self, user_id: &str) -> Result<(), SqeError> {
        if !self.enabled {
            return Ok(());
        }

        // Per-user check
        if let Some(ref limiter) = self.per_user {
            limiter.check_key(user_id).map_err(|not_until| {
                let secs = not_until.retry_after_secs(limiter.clock().now());
                RateLimitDenial {
                    limiter: "query",
                    knob: "rate_limit.per_user_queries_per_minute",
                    retry_after: Duration::from_secs(secs),
                    detail: format!("per-user limit reached for user '{user_id}'"),
                }
                .into_error()
            })?;
        }

        // Global check
        if let Some(ref limiter) = self.global {
            limiter.check().map_err(|not_until| {
                let secs =
                    retry_after_secs(&not_until, governor::clock::Clock::now(limiter.clock()));
                RateLimitDenial {
                    limiter: "query",
                    knob: "rate_limit.global_queries_per_minute",
                    retry_after: Duration::from_secs(secs),
                    detail: "global query limit reached".to_string(),
                }
                .into_error()
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #562: the wrapper denies before the inner provider is called, keys on
    /// the peer address plus a bearer fingerprint, and lets a credential
    /// without a peer address through untouched (the adapter kept its gate).
    #[tokio::test]
    async fn rate_limited_provider_denies_before_the_inner_provider() {
        use sqe_auth::AuthProvider as _;
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct Counting(AtomicUsize);
        #[async_trait::async_trait]
        impl sqe_auth::AuthProvider for Counting {
            async fn authenticate(
                &self,
                _c: &sqe_auth::FlightCredentials,
            ) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
                self.0.fetch_add(1, AtomicOrdering::SeqCst);
                Err(sqe_auth::AuthError::AuthFailed("stub".into()))
            }
        }
        let inner = Arc::new(Counting(AtomicUsize::new(0)));
        let limiter = Arc::new(AuthRateLimiter::new(&RateLimitConfig {
            enabled: true,
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            ..Default::default()
        }));
        let wrapped = RateLimitedAuthProvider::new(
            Arc::clone(&inner) as Arc<dyn sqe_auth::AuthProvider>,
            limiter,
        );
        let bearer = |peer: Option<&str>| sqe_auth::FlightCredentials {
            bearer_token: Some(sqe_core::SecretString::new("ey.garbage.token".into())),
            peer_ip: peer.map(str::to_string),
            ..Default::default()
        };

        // First attempt reaches the inner provider, second is refused before it.
        assert!(matches!(
            wrapped.authenticate(&bearer(Some("203.0.113.5"))).await,
            Err(sqe_auth::AuthError::AuthFailed(_))
        ));
        assert!(matches!(
            wrapped.authenticate(&bearer(Some("203.0.113.5"))).await,
            Err(sqe_auth::AuthError::RateLimited)
        ));
        assert_eq!(inner.0.load(AtomicOrdering::SeqCst), 1);

        // A different peer has its own budget.
        assert!(matches!(
            wrapped.authenticate(&bearer(Some("203.0.113.6"))).await,
            Err(sqe_auth::AuthError::AuthFailed(_))
        ));
        // No peer address: not migrated, passes through to the inner provider.
        assert!(matches!(
            wrapped.authenticate(&bearer(None)).await,
            Err(sqe_auth::AuthError::AuthFailed(_))
        ));
        assert_eq!(inner.0.load(AtomicOrdering::SeqCst), 3);
    }

    #[test]
    fn disabled_limiter_always_passes() {
        let config = RateLimitConfig {
            enabled: false,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        // Even with limits of 1, disabled means all requests pass.
        for _ in 0..100 {
            assert!(limiter.check("alice").is_ok());
        }
    }

    #[test]
    fn below_limit_passes() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 10,
            global_queries_per_minute: 100,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        // The first request should always succeed (well within limits).
        assert!(limiter.check("bob").is_ok());
    }

    #[test]
    fn per_user_limit_fires() {
        let config = RateLimitConfig {
            enabled: true,
            // Allow only 1 query per minute per user.
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1000,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        // First request succeeds.
        assert!(limiter.check("carol").is_ok());
        // Second request should be rate-limited.
        let err = limiter.check("carol").unwrap_err();
        assert!(
            err.to_string().contains("per-user limit"),
            "Expected per-user rate limit error, got: {err}"
        );
    }

    #[test]
    fn global_limit_fires() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1000,
            // Allow only 1 query per minute globally.
            global_queries_per_minute: 1,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        // First request succeeds.
        assert!(limiter.check("dave").is_ok());
        // Second request (different user) should hit the global limit.
        let err = limiter.check("eve").unwrap_err();
        assert!(
            err.to_string().contains("global query limit"),
            "Expected global rate limit error, got: {err}"
        );
    }

    #[test]
    fn per_user_limits_are_independent() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1000,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        // User A uses their quota.
        assert!(limiter.check("user_a").is_ok());
        assert!(limiter.check("user_a").is_err());

        // User B still has their own independent quota.
        assert!(limiter.check("user_b").is_ok());
    }

    /// One agent's parallel metadata calls: a dozen presentations of the same
    /// bearer inside one second pass at the defaults, and the denial that
    /// follows the burst says how long to wait and which knob sets it.
    #[test]
    fn a_dozen_concurrent_presentations_pass_at_the_defaults() {
        let limiter = AuthRateLimiter::new(&RateLimitConfig {
            enabled: true,
            ..RateLimitConfig::default()
        });
        for i in 0..12 {
            limiter
                .check("10.0.0.7", "bearer:abc")
                .unwrap_or_else(|e| panic!("call {i} denied: {e}"));
        }
        let config = RateLimitConfig::default();
        let mut denied = 0;
        for _ in 12..(config.auth_burst + 5) {
            if limiter.check("10.0.0.7", "bearer:abc").is_err() {
                denied += 1;
            }
        }
        assert!(denied >= 1, "the burst is finite");
        assert_eq!(config.auth_burst, 20);

        let query = QueryRateLimiter::new(&RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1,
            ..RateLimitConfig::default()
        });
        query.check("u").unwrap();
        let err = query.check("u").unwrap_err();
        assert_eq!(err.error_code(), SqeErrorCode::ResourceExhausted);
        let text = err.to_string();
        assert!(
            text.contains("rate_limit.per_user_queries_per_minute"),
            "{text}"
        );
        assert!(text.contains("retry after "), "{text}");
        assert!(text.ends_with('s'), "{text}");
    }

    #[test]
    fn auth_rate_limiter_disabled_is_noop() {
        let config = RateLimitConfig {
            enabled: false,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1,
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            metadata_per_user_per_minute: 1,
        };
        let limiter = AuthRateLimiter::new(&config);
        for _ in 0..100 {
            assert!(limiter.check("1.1.1.1", "alice").is_ok());
        }
    }

    #[test]
    fn auth_rate_limiter_tuple_budget_fires() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1000,
            global_queries_per_minute: 1000,
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            metadata_per_user_per_minute: 1000,
        };
        let limiter = AuthRateLimiter::new(&config);
        assert!(limiter.check("203.0.113.5", "alice").is_ok());
        let err = limiter.check("203.0.113.5", "alice").unwrap_err();
        assert!(matches!(err, SqeError::Auth(_)));
    }

    #[test]
    fn auth_rate_limiter_per_ip_catches_username_rotation() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1000,
            global_queries_per_minute: 1000,
            // tuple budget = 1, per-ip budget = 5
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            metadata_per_user_per_minute: 1000,
        };
        let limiter = AuthRateLimiter::new(&config);
        // Rotate usernames from the same IP; the per-IP cap of 5
        // should fire by the 6th attempt even though tuples don't
        // repeat.
        for i in 0..5 {
            assert!(
                limiter.check("203.0.113.5", &format!("user{i}")).is_ok(),
                "attempt {i} should be allowed"
            );
        }
        let err = limiter.check("203.0.113.5", "user-final").unwrap_err();
        assert!(matches!(err, SqeError::Auth(_)));
    }

    #[test]
    fn metadata_rate_limiter_disabled_is_noop() {
        let config = RateLimitConfig {
            enabled: false,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1,
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            metadata_per_user_per_minute: 1,
        };
        let limiter = MetadataRateLimiter::new(&config);
        for _ in 0..100 {
            assert!(limiter.check("alice").is_ok());
        }
    }

    #[test]
    fn metadata_rate_limiter_per_user_fires() {
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1000,
            global_queries_per_minute: 1000,
            auth_attempts_per_minute: 1000,
            auth_burst: 1000,
            metadata_per_user_per_minute: 1,
        };
        let limiter = MetadataRateLimiter::new(&config);
        assert!(limiter.check("alice").is_ok());
        let err = limiter.check("alice").unwrap_err();
        assert!(err.to_string().contains("metadata browse"));
    }

    #[test]
    fn rate_limit_does_not_drop_session() {
        // Verify the error type is Execution (not Auth), meaning the session survives.
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1000,
            ..Default::default()
        };
        let limiter = QueryRateLimiter::new(&config);

        assert!(limiter.check("frank").is_ok());
        let err = limiter.check("frank").unwrap_err();
        // ResourceExhausted, not Auth, so the session is preserved and the
        // protocol adapters answer 429 / RESOURCE_EXHAUSTED.
        assert!(
            err.error_code() == SqeErrorCode::ResourceExhausted,
            "Expected a ResourceExhausted denial, got: {err:?}"
        );
    }
    #[test]
    fn exhausted_ip_rotation_does_not_admit_credential_keys() {
        let limiter = AuthRateLimiter::new(&RateLimitConfig {
            enabled: true,
            auth_attempts_per_minute: 1,
            auth_burst: 1,
            ..Default::default()
        });
        for i in 0..5 {
            assert!(limiter.check("203.0.113.5", &format!("user-{i}")).is_ok());
        }
        for i in 0..10_000 {
            assert!(limiter
                .check("203.0.113.5", &format!("rotated-{i}"))
                .is_err());
        }
        assert_eq!(limiter.limiter.as_ref().unwrap().len(), 5);
        assert_eq!(limiter.by_ip.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn concurrent_distinct_keys_cannot_exceed_cap_or_reset_existing_budget() {
        let limiter = Arc::new(BoundedKeyedLimiter::with_clock(
            Quota::per_minute(NonZeroU32::new(1).unwrap()),
            8,
            governor::clock::FakeRelativeClock::default(),
        ));
        assert!(limiter.check_key("existing").is_ok());
        let barrier = Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let limiter = limiter.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for key in 0..100 {
                        let _ = limiter.check_key(&format!("{worker}-{key}"));
                    }
                });
            }
        });
        assert_eq!(limiter.len(), 8);
        assert!(matches!(
            limiter.check_key("new"),
            Err(KeyedDenial::Capacity)
        ));
        assert!(matches!(
            limiter.check_key("existing"),
            Err(KeyedDenial::Quota(_))
        ));
    }

    #[test]
    fn idle_pruning_waits_for_full_burst_refill() {
        let clock = governor::clock::FakeRelativeClock::default();
        // A 120-cell burst at one cell/second takes longer than the prune cadence.
        let limiter = BoundedKeyedLimiter::with_clock(
            Quota::with_period(Duration::from_secs(1))
                .unwrap()
                .allow_burst(NonZeroU32::new(120).unwrap()),
            1,
            clock.clone(),
        );
        for _ in 0..120 {
            assert!(limiter.check_key("old").is_ok());
        }
        clock.advance(Duration::from_secs(60));
        assert!(matches!(
            limiter.check_key("new"),
            Err(KeyedDenial::Capacity)
        ));
        assert_eq!(limiter.len(), 1);
        clock.advance(Duration::from_secs(60));
        assert!(limiter.check_key("new").is_ok());
        assert_eq!(limiter.len(), 1);
    }

    #[test]
    fn denied_activity_keeps_consumed_bucket_and_keys_have_fixed_width() {
        let clock = governor::clock::FakeRelativeClock::default();
        let limiter = BoundedKeyedLimiter::with_clock(
            Quota::per_minute(NonZeroU32::new(1).unwrap()),
            1,
            clock.clone(),
        );
        assert!(limiter.check_key(&"x".repeat(100_000)).is_ok());
        clock.advance(Duration::from_secs(30));
        assert!(matches!(
            limiter.check_key(&"x".repeat(100_000)),
            Err(KeyedDenial::Quota(_))
        ));
        clock.advance(Duration::from_secs(30));
        assert!(matches!(
            limiter.check_key("new"),
            Err(KeyedDenial::Capacity)
        ));
        let state = limiter.state.lock().unwrap();
        assert_eq!(state.buckets.keys().next().unwrap().len(), 32);
    }
}
