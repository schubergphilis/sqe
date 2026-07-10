//! Rate limiter for the SQE coordinator.
//!
//! Provides per-user and global token-bucket rate limiting using the
//! [`governor`] crate. Per-user state is maintained internally by
//! governor's keyed rate limiter.

use std::num::NonZeroU32;
use std::sync::Arc;

use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use sqe_core::config::RateLimitConfig;
use sqe_core::error::SqeError;

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
    limiter: Option<Arc<DefaultKeyedRateLimiter<String>>>,
    /// Per-IP-only limiter that catches credential stuffing across
    /// rotating usernames from a single source IP.
    by_ip: Option<Arc<DefaultKeyedRateLimiter<String>>>,
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
        let quota = Quota::per_minute(
            NonZeroU32::new(config.auth_attempts_per_minute)
                .unwrap_or(NonZeroU32::new(10).unwrap()),
        );
        // Per-IP budget is intentionally larger than the per-tuple
        // budget so legitimate shared NAT egress isn't penalised but
        // a stuffer rotating usernames still trips the IP cap.
        let ip_quota = Quota::per_minute(
            NonZeroU32::new(config.auth_attempts_per_minute.saturating_mul(5))
                .unwrap_or(NonZeroU32::new(50).unwrap()),
        );
        Self {
            enabled: true,
            limiter: Some(Arc::new(RateLimiter::keyed(quota))),
            by_ip: Some(Arc::new(RateLimiter::keyed(ip_quota))),
        }
    }

    /// Returns `Ok(())` when the (ip, username) tuple is under budget.
    /// Returns `SqeError::Auth("rate-limited")` otherwise, so the caller
    /// can map to the protocol-specific status code without leaking
    /// detail to the unauthenticated peer.
    pub fn check(&self, peer_ip: &str, username: &str) -> Result<(), SqeError> {
        if !self.enabled {
            return Ok(());
        }
        let key = format!("{peer_ip}|{username}");
        if let Some(ref l) = self.limiter {
            l.check_key(&key)
                .map_err(|_| SqeError::Auth("rate-limited".to_string()))?;
        }
        if let Some(ref l) = self.by_ip {
            l.check_key(&peer_ip.to_string())
                .map_err(|_| SqeError::Auth("rate-limited".to_string()))?;
        }
        Ok(())
    }
}

impl sqe_trino_compat::server::TrinoAuthRateLimiter for AuthRateLimiter {
    fn check(&self, peer_ip: &str, username: &str) -> Result<(), ()> {
        AuthRateLimiter::check(self, peer_ip, username).map_err(|_| ())
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
    per_user: Option<Arc<DefaultKeyedRateLimiter<String>>>,
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
            per_user: Some(Arc::new(RateLimiter::keyed(quota))),
        }
    }

    pub fn check(&self, user_id: &str) -> Result<(), SqeError> {
        if !self.enabled {
            return Ok(());
        }
        if let Some(ref l) = self.per_user {
            l.check_key(&user_id.to_string()).map_err(|_| {
                SqeError::Execution(format!(
                    "Rate limit exceeded: metadata browse limit reached for user '{user_id}'"
                ))
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
    per_user: Option<Arc<DefaultKeyedRateLimiter<String>>>,
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
            per_user: Some(Arc::new(RateLimiter::keyed(per_user_quota))),
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
            limiter
                .check_key(&user_id.to_string())
                .map_err(|_| {
                    SqeError::Execution(format!(
                        "Rate limit exceeded: per-user limit reached for user '{user_id}'"
                    ))
                })?;
        }

        // Global check
        if let Some(ref limiter) = self.global {
            limiter.check().map_err(|_| {
                SqeError::Execution(
                    "Rate limit exceeded: global query limit reached".to_string(),
                )
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn auth_rate_limiter_disabled_is_noop() {
        let config = RateLimitConfig {
            enabled: false,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1,
            auth_attempts_per_minute: 1,
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
        // SqeError::Execution — not Auth — so the session is preserved.
        assert!(
            matches!(err, SqeError::Execution(_)),
            "Expected Execution error variant, got: {err:?}"
        );
    }
}
