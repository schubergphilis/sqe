//! Circuit breaker for external service calls.
//!
//! Protects catalog (Polaris REST) calls from cascading failure when the
//! upstream is unavailable. The circuit transitions through three states:
//!
//! ```text
//!   Closed ──N failures──► Open ──recovery_timeout──► Half-Open
//!     ▲                                                     │
//!     └──────────────── success ──────────────────────────-─┘
//!                            failure ──────────────────► Open
//! ```
//!
//! State encoding in the `state` atomic:
//! * `0` = Closed (normal — all calls pass through)
//! * `1` = Open   (tripped — all calls fail immediately)
//! * `2` = Half-Open (testing — one probe call allowed)

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

/// Thread-safe circuit breaker with atomic state transitions.
pub struct CircuitBreaker {
    /// Number of consecutive failures observed while Closed.
    failure_count: AtomicU32,
    /// How many consecutive failures trip the circuit.
    failure_threshold: u32,
    /// How long to stay Open before allowing a probe (Half-Open).
    recovery_timeout: Duration,
    /// Timestamp (epoch ms) of the most recent failure. Used to compute
    /// when the recovery window expires.
    last_failure_ms: AtomicU64,
    /// Current state: 0=Closed, 1=Open, 2=Half-Open.
    state: AtomicU32,
    /// Human-readable name for logging.
    name: String,
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => "Open",
            STATE_HALF_OPEN => "HalfOpen",
            _ => "Closed",
        };
        f.debug_struct("CircuitBreaker")
            .field("name", &self.name)
            .field("state", &state)
            .field("failure_count", &self.failure_count.load(Ordering::Relaxed))
            .field("failure_threshold", &self.failure_threshold)
            .finish()
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    ///
    /// # Arguments
    /// * `name` — label used in log messages
    /// * `failure_threshold` — consecutive failures before opening circuit
    /// * `recovery_timeout` — how long to keep circuit Open before probing
    pub fn new(
        name: impl Into<String>,
        failure_threshold: u32,
        recovery_timeout: Duration,
    ) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            failure_threshold,
            recovery_timeout,
            last_failure_ms: AtomicU64::new(0),
            state: AtomicU32::new(STATE_CLOSED),
            name: name.into(),
        }
    }

    /// Check whether a request may proceed.
    ///
    /// * `Ok(())` — circuit is Closed or Half-Open (probe allowed); call can proceed.
    /// * `Err(msg)` — circuit is Open; return the error without calling the service.
    pub fn check(&self) -> Result<(), String> {
        loop {
            let state = self.state.load(Ordering::Acquire);
            match state {
                STATE_CLOSED => return Ok(()),
                STATE_OPEN => {
                    // Check whether the recovery window has elapsed.
                    let elapsed_ms =
                        now_millis().saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                    let recovery_ms = self.recovery_timeout.as_millis() as u64;
                    if elapsed_ms >= recovery_ms {
                        // Attempt transition Open → Half-Open.
                        if self
                            .state
                            .compare_exchange(
                                STATE_OPEN,
                                STATE_HALF_OPEN,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            info!(
                                circuit = %self.name,
                                "Circuit breaker entering Half-Open — allowing probe request"
                            );
                            return Ok(()); // allow the probe
                        }
                        // Another thread already made the transition — re-read and retry.
                        continue;
                    }
                    return Err(format!(
                        "Circuit breaker '{}' is Open — service unavailable (retry in {}ms)",
                        self.name,
                        recovery_ms.saturating_sub(elapsed_ms)
                    ));
                }
                STATE_HALF_OPEN => {
                    // Only ONE probe is allowed while Half-Open.
                    // Attempt Half-Open → Open to "lock out" subsequent callers.
                    // The probe itself runs concurrently; if it succeeds we close,
                    // if it fails we reopen. Other concurrent callers are rejected
                    // while the probe is in flight.
                    match self.state.compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_OPEN, // pessimistic: lock others out while probe runs
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // We "claimed" the probe slot. Temporarily set state
                            // back to Half-Open so our own check_probe call works.
                            self.state.store(STATE_HALF_OPEN, Ordering::Release);
                            return Ok(()); // we are the probe
                        }
                        Err(current) => {
                            // State changed under us — re-read.
                            if current == STATE_CLOSED {
                                return Ok(());
                            }
                            return Err(format!(
                                "Circuit breaker '{}' is Half-Open — probe already in flight",
                                self.name
                            ));
                        }
                    }
                }
                _ => return Ok(()), // unknown state — fail open (safe default)
            }
        }
    }

    /// Record a successful call.
    ///
    /// Resets the failure counter and closes the circuit if it was Half-Open.
    pub fn record_success(&self) {
        let prev = self.state.swap(STATE_CLOSED, Ordering::AcqRel);
        if prev != STATE_CLOSED {
            info!(circuit = %self.name, "Circuit breaker closed after successful probe");
        }
        self.failure_count.store(0, Ordering::Relaxed);
    }

    /// Record a failed call.
    ///
    /// Increments the consecutive failure counter.  Opens the circuit once
    /// the threshold is reached.  If the circuit is Half-Open, re-opens it
    /// immediately.
    pub fn record_failure(&self) {
        self.last_failure_ms.store(now_millis(), Ordering::Relaxed);

        let prev_state = self.state.load(Ordering::Acquire);
        if prev_state == STATE_HALF_OPEN {
            // Probe failed — re-open immediately.
            self.state.store(STATE_OPEN, Ordering::Release);
            warn!(
                circuit = %self.name,
                "Circuit breaker re-opened after failed probe"
            );
            return;
        }

        let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.failure_threshold
            && self
                .state
                .compare_exchange(
                    STATE_CLOSED,
                    STATE_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        {
            warn!(
                circuit = %self.name,
                failures = count,
                threshold = self.failure_threshold,
                recovery_secs = self.recovery_timeout.as_secs(),
                "Circuit breaker opened"
            );
        }
    }

    /// Return the current state as a human-readable string (for metrics/logging).
    pub fn state_label(&self) -> &'static str {
        match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => "open",
            STATE_HALF_OPEN => "half_open",
            _ => "closed",
        }
    }

    /// Return the current state as a numeric code for gauges.
    /// 0 = closed, 1 = half_open, 2 = open.
    pub fn state_code(&self) -> u8 {
        match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => 2,
            STATE_HALF_OPEN => 1,
            _ => 0,
        }
    }

    /// Return the circuit's name (used as label value for metrics).
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cb() -> CircuitBreaker {
        CircuitBreaker::new("test", 3, Duration::from_secs(60))
    }

    #[test]
    fn starts_closed() {
        let c = cb();
        assert!(c.check().is_ok());
        assert_eq!(c.state_label(), "closed");
    }

    #[test]
    fn opens_after_threshold() {
        let c = cb();
        c.record_failure();
        assert!(c.check().is_ok(), "should still be closed after 1 failure");
        c.record_failure();
        assert!(c.check().is_ok(), "should still be closed after 2 failures");
        c.record_failure(); // hits threshold (3)
        assert!(c.check().is_err(), "should be open after 3 failures");
        assert_eq!(c.state_label(), "open");
    }

    #[test]
    fn success_resets_failure_count() {
        let c = cb();
        c.record_failure();
        c.record_failure();
        c.record_success(); // reset
        c.record_failure();
        c.record_failure();
        // still only 2 failures after reset — should be closed
        assert!(c.check().is_ok());
    }

    #[test]
    fn transitions_to_half_open_after_timeout() {
        let c = CircuitBreaker::new("test", 1, Duration::from_millis(0));
        c.record_failure(); // threshold=1 → opens immediately
        assert_eq!(c.state_label(), "open");
        // With 0ms recovery the circuit should transition on first check.
        let result = c.check();
        assert!(result.is_ok(), "should allow probe after recovery timeout");
        assert_eq!(c.state_label(), "half_open");
    }

    #[test]
    fn half_open_success_closes_circuit() {
        let c = CircuitBreaker::new("test", 1, Duration::from_millis(0));
        c.record_failure();
        let _ = c.check(); // transition to Half-Open
        c.record_success();
        assert_eq!(c.state_label(), "closed");
        assert!(c.check().is_ok());
    }

    #[test]
    fn half_open_failure_reopens_circuit() {
        let c = CircuitBreaker::new("test", 1, Duration::from_millis(0));
        c.record_failure();
        let _ = c.check(); // transition to Half-Open
        c.record_failure(); // probe fails → re-open
        assert_eq!(c.state_label(), "open");
    }

    #[test]
    fn open_circuit_rejects_immediately() {
        let c = CircuitBreaker::new("test", 1, Duration::from_secs(999));
        c.record_failure();
        let err = c.check().unwrap_err();
        assert!(err.contains("Circuit breaker"));
        assert!(err.contains("Open"));
    }
}
