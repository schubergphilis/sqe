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
    /// How long a Half-Open probe holds its slot before the next caller may
    /// replace it. Defaults to `recovery_timeout` (floored at 1 ms) and is
    /// settable independently via [`CircuitBreaker::with_probe_lease`], so a
    /// breaker can have a zero recovery window and still serialize probes.
    probe_lease: Duration,
    /// Timestamp (epoch ms) of the most recent failure. Used to compute
    /// when the recovery window expires.
    last_failure_ms: AtomicU64,
    /// When the current Half-Open probe was admitted. A dropped or
    /// unanswered probe cannot latch the breaker: after one probe lease
    /// the next caller may replace it.
    half_open_since_ms: AtomicU64,
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
            probe_lease: recovery_timeout.max(Duration::from_millis(1)),
            last_failure_ms: AtomicU64::new(0),
            half_open_since_ms: AtomicU64::new(0),
            state: AtomicU32::new(STATE_CLOSED),
            name: name.into(),
        }
    }

    /// Override how long a Half-Open probe holds its slot.
    ///
    /// The lease exists so a probe that never records an outcome (a dropped
    /// future, or a 403 that `record_breaker_outcome` deliberately ignores)
    /// cannot latch the breaker Half-Open forever. It defaults to
    /// `recovery_timeout`, which is right in production, where that value is
    /// seconds.
    ///
    /// It is separate from `recovery_timeout` because the two windows answer
    /// different questions — "when may we probe again?" versus "how long do we
    /// wait for this probe?" — and a caller that wants to probe immediately
    /// (`recovery_timeout` of zero) still needs probes serialized. Floored at
    /// 1 ms so a zero lease cannot re-admit every caller.
    pub fn with_probe_lease(mut self, probe_lease: Duration) -> Self {
        self.probe_lease = probe_lease.max(Duration::from_millis(1));
        self
    }

    /// Check whether a request may proceed.
    ///
    /// * `Ok(())` — circuit is Closed, or this thread won the Open → Half-Open
    ///   CAS and is the single admitted probe.
    /// * `Err(msg)` — circuit is Open, Half-Open with a probe already in
    ///   flight, or in an unknown state (fail closed).
    pub fn check(&self) -> Result<(), String> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            STATE_CLOSED => Ok(()),
            STATE_OPEN => {
                // Check whether the recovery window has elapsed.
                let elapsed_ms =
                    now_millis().saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                let recovery_ms = self.recovery_timeout.as_millis() as u64;
                if elapsed_ms >= recovery_ms
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
                    self.half_open_since_ms
                        .store(now_millis(), Ordering::Release);
                    info!(
                        circuit = %self.name,
                        "Circuit breaker entering Half-Open — allowing probe request"
                    );
                    return Ok(());
                }
                Err(format!(
                    "Circuit breaker '{}' is Open — service unavailable (retry in {}ms)",
                    self.name,
                    recovery_ms.saturating_sub(elapsed_ms)
                ))
            }
            // A probe is in flight. Deny concurrent callers so we do not
            // stampede the backend. If that probe never records an outcome
            // (dropped future, or a 403 that used to record nothing), the
            // lease expires and a replacement probe is admitted. The lease is
            // `probe_lease`, not `recovery_timeout`: a breaker configured to
            // probe immediately must still serialize its probes.
            STATE_HALF_OPEN => {
                let started = self.half_open_since_ms.load(Ordering::Acquire);
                let now = now_millis();
                let elapsed_ms = now.saturating_sub(started);
                let lease_ms = (self.probe_lease.as_millis() as u64).max(1);
                if elapsed_ms >= lease_ms
                    && self
                        .half_open_since_ms
                        .compare_exchange(started, now, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    info!(
                        circuit = %self.name,
                        elapsed_ms,
                        "Circuit breaker Half-Open probe lease expired — admitting replacement"
                    );
                    return Ok(());
                }
                Err(format!(
                    "Circuit breaker '{}' is Half-Open — probe already in flight",
                    self.name
                ))
            }
            // Unknown state: fail closed. A corrupted atomic must not admit
            // traffic to an upstream we cannot reason about.
            _ => Err(format!(
                "Circuit breaker '{}' is in an unknown state — failing closed",
                self.name
            )),
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

    #[test]
    fn half_open_admits_only_the_cas_winner() {
        // Zero recovery timeout so the first check probes immediately, but a
        // long probe lease so the assertions below cannot race the lease
        // clock. With the lease tied to recovery_timeout this test failed
        // whenever a millisecond ticked between two checks (#429).
        let c = CircuitBreaker::new("test", 1, Duration::from_millis(0))
            .with_probe_lease(Duration::from_secs(60));
        c.record_failure(); // opens
                            // First check after recovery elapses wins the OPEN->HALF_OPEN CAS and
                            // is the single admitted probe.
        assert!(c.check().is_ok(), "CAS winner must be admitted");
        assert_eq!(c.state_label(), "half_open", "now half-open");
        // Every subsequent caller while half-open is denied (no thundering herd
        // of probes) and stays fail-closed until the probe resolves.
        assert!(
            c.check().is_err(),
            "second concurrent caller must be denied"
        );
        assert!(c.check().is_err(), "third concurrent caller must be denied");
        assert_eq!(
            c.state_label(),
            "half_open",
            "still half-open, probe still in flight"
        );
    }

    #[test]
    fn half_open_concurrent_callers_only_one_ok() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        // recovery_timeout=0 so the next check probes; a 60s probe lease so
        // spawning 16 OS threads cannot outlast it and admit a replacement.
        // Tying the lease to recovery_timeout made this test fail on CI with
        // `left: 2` (#429).
        let c = Arc::new(
            CircuitBreaker::new("test", 1, Duration::from_millis(0))
                .with_probe_lease(Duration::from_secs(60)),
        );
        c.record_failure(); // opens; recovery_timeout=0 so the next check probes

        let ok_count = Arc::new(AtomicU32::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let c = Arc::clone(&c);
            let ok_count = Arc::clone(&ok_count);
            handles.push(std::thread::spawn(move || {
                if c.check().is_ok() {
                    ok_count.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Exactly one caller wins the OPEN->HALF_OPEN CAS and gets Ok; the rest
        // are denied. Without the half-open gate, all 16 would get Ok.
        assert_eq!(
            ok_count.load(Ordering::SeqCst),
            1,
            "exactly one probe must be admitted in half-open"
        );
    }

    #[test]
    fn stale_half_open_probe_is_replaced_after_lease() {
        // Long lease, expired deliberately below by rewinding the clock field
        // rather than by sleeping. A short lease made the "still exclusive"
        // assertion depend on how fast the test ran (#429).
        let c = CircuitBreaker::new("test", 1, Duration::from_millis(5))
            .with_probe_lease(Duration::from_secs(60));
        c.record_failure();
        // Force the open recovery window to have elapsed.
        c.last_failure_ms.store(0, Ordering::Relaxed);
        assert!(c.check().is_ok(), "first probe admitted");
        assert_eq!(c.state_label(), "half_open");
        assert!(c.check().is_err(), "in-flight probe still exclusive");
        // Expire the probe lease without recording success or failure
        // (the latch the review found: dropped future / unanswered 403).
        c.half_open_since_ms.store(0, Ordering::Relaxed);
        assert!(
            c.check().is_ok(),
            "expired half-open lease must admit a replacement probe"
        );
        assert_eq!(c.state_label(), "half_open");
        assert!(
            c.check().is_err(),
            "the replacement probe must still be exclusive"
        );
    }
}
