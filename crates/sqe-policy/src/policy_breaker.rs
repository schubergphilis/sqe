//! Lightweight three-state circuit breaker shared by HTTP-backed policy stores
//! (OPA, Ranger). Extracted from `opa.rs` so both stores share one impl.
//!
//! Mirrors `sqe_catalog::CircuitBreaker`. The sqe-policy crate cannot depend on
//! sqe-catalog (the dependency direction is the other way around), so the
//! smaller implementation lives here.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

/// Three-state circuit breaker around a remote policy backend call.
pub struct PolicyCircuitBreaker {
    /// Backend label used in log lines (e.g. "OPA", "Ranger").
    name: &'static str,
    failure_count: AtomicU32,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_ms: AtomicU64,
    state: AtomicU32,
}

impl PolicyCircuitBreaker {
    pub fn new(name: &'static str, failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            name,
            failure_count: AtomicU32::new(0),
            failure_threshold,
            recovery_timeout,
            last_failure_ms: AtomicU64::new(0),
            state: AtomicU32::new(STATE_CLOSED),
        }
    }

    /// Returns Err when the breaker is open (caller must fail closed).
    pub fn check(&self) -> Result<(), String> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            STATE_CLOSED => Ok(()),
            STATE_OPEN => {
                let elapsed_ms =
                    now_millis().saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                if elapsed_ms >= self.recovery_timeout.as_millis() as u64
                    && self
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    info!("{} circuit breaker moving to half_open (probe allowed)", self.name);
                    return Ok(());
                }
                Err(format!("{} circuit breaker is open", self.name))
            }
            STATE_HALF_OPEN => Ok(()),
            _ => Ok(()),
        }
    }

    pub fn record_success(&self) {
        if self.state.load(Ordering::Acquire) != STATE_CLOSED {
            self.state.store(STATE_CLOSED, Ordering::Release);
            self.failure_count.store(0, Ordering::Release);
            info!("{} circuit breaker closed after successful probe", self.name);
        } else {
            self.failure_count.store(0, Ordering::Relaxed);
        }
    }

    pub fn record_failure(&self) {
        self.last_failure_ms.store(now_millis(), Ordering::Relaxed);
        let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.failure_threshold
            && self
                .state
                .compare_exchange(STATE_CLOSED, STATE_OPEN, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn!(
                backend = self.name,
                failures = count,
                threshold = self.failure_threshold,
                "circuit breaker opened"
            );
        } else if self.state.load(Ordering::Acquire) == STATE_HALF_OPEN {
            self.state.store(STATE_OPEN, Ordering::Release);
        }
    }

    /// 0 = closed, 1 = half-open, 2 = open (matches the metrics gauge encoding).
    pub fn state_code(&self) -> u8 {
        match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => 2,
            STATE_HALF_OPEN => 1,
            _ => 0,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_breaker_allows() {
        let b = PolicyCircuitBreaker::new("Test", 3, Duration::from_secs(30));
        assert!(b.check().is_ok());
        assert_eq!(b.state_code(), 0);
    }

    #[test]
    fn opens_after_threshold() {
        let b = PolicyCircuitBreaker::new("Test", 2, Duration::from_secs(30));
        b.record_failure();
        b.record_failure();
        assert!(b.check().is_err());
        assert_eq!(b.state_code(), 2);
    }

    #[test]
    fn success_resets_failures() {
        let b = PolicyCircuitBreaker::new("Test", 2, Duration::from_secs(30));
        b.record_failure();
        b.record_success();
        b.record_failure();
        // Only one failure since the reset, breaker stays closed.
        assert!(b.check().is_ok());
    }
}
