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
        };
        let limiter = QueryRateLimiter::new(&config);

        // User A uses their quota.
        assert!(limiter.check("user_a").is_ok());
        assert!(limiter.check("user_a").is_err());

        // User B still has their own independent quota.
        assert!(limiter.check("user_b").is_ok());
    }

    #[test]
    fn rate_limit_does_not_drop_session() {
        // Verify the error type is Execution (not Auth), meaning the session survives.
        let config = RateLimitConfig {
            enabled: true,
            per_user_queries_per_minute: 1,
            global_queries_per_minute: 1000,
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
