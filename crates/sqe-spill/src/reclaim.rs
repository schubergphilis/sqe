//! Reclaimable memory consumers for operator grants (Phase 5b / Phase 7).
//!
//! Until the negotiating governor ships in Phase 7, grants are fixed carve-outs
//! from `operator_budget` via [`crate::ByteBudget`]. Consumers register desired
//! and minimum bytes; the registry hands out a [`MemoryGrant`] that can later
//! be reclaimed without changing call sites.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::budget::ByteBudget;

/// A consumer that can release memory under pressure (join build, aggregate
/// state, sort runs, shuffle buffers).
///
/// Phase 7 extends this with live registration on the [`crate::MemoryGovernor`].
/// Defaults keep existing implementors compiling; override `current_bytes` /
/// `try_reclaim` when the operator can actually shrink.
pub trait ReclaimableConsumer: Send + Sync {
    /// Human-readable name for metrics and logs.
    fn name(&self) -> &str;

    /// Ideal grant size in bytes for best performance.
    fn desired_bytes(&self) -> usize;

    /// Hard minimum below which the operator must spill or fail.
    fn minimum_bytes(&self) -> usize;

    /// Bytes currently held (default: desired).
    fn current_bytes(&self) -> usize {
        self.desired_bytes()
    }

    /// Best-effort reclaim of up to `target` bytes. Returns bytes actually
    /// freed (may be less). Must not block the calling task indefinitely.
    fn try_reclaim(&self, target: usize) -> usize;
}

/// Fixed grant handed to an operator for its lifetime (or until renegotiated
/// in Phase 7). Holds a [`ByteBudget`] so acquires wait/backpressure under
/// the grant capacity.
#[derive(Clone, Debug)]
pub struct MemoryGrant {
    budget: ByteBudget,
    desired: usize,
    minimum: usize,
}

impl MemoryGrant {
    /// Create a grant with the given capacity (aligned by [`ByteBudget`]).
    pub fn new(name: impl Into<String>, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            budget: ByteBudget::new(name, capacity, None),
            desired: capacity,
            minimum: capacity / 4 + 1,
        }
    }

    /// Create a grant with explicit desired/minimum metadata.
    pub fn with_bounds(
        name: impl Into<String>,
        capacity: usize,
        desired: usize,
        minimum: usize,
    ) -> Self {
        let capacity = capacity.max(1);
        Self {
            budget: ByteBudget::new(name, capacity, None),
            desired: desired.max(capacity),
            minimum: minimum.min(capacity).max(1),
        }
    }

    pub fn budget(&self) -> &ByteBudget {
        &self.budget
    }

    pub fn capacity_bytes(&self) -> usize {
        self.budget.capacity_bytes()
    }

    pub fn desired_bytes(&self) -> usize {
        self.desired
    }

    pub fn minimum_bytes(&self) -> usize {
        self.minimum
    }

    /// Soft watermark (75% of capacity) — Grace join should start spilling
    /// partitions when resident build state crosses this line.
    pub fn soft_limit_bytes(&self) -> usize {
        let cap = self.capacity_bytes();
        (cap.saturating_mul(3) / 4).max(1)
    }
}

/// Process-local registry of reclaimable consumers (placeholder for Phase 7
/// negotiation). Tracks registered desired bytes for diagnostics.
#[derive(Default)]
pub struct GrantRegistry {
    total_desired: AtomicUsize,
    total_minimum: AtomicUsize,
    registrations: AtomicUsize,
}

impl GrantRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a consumer's bounds and return a fixed grant of `capacity`
    /// (caller chooses capacity from operator_budget / policy).
    pub fn register(
        &self,
        consumer: &dyn ReclaimableConsumer,
        capacity: usize,
    ) -> MemoryGrant {
        self.total_desired
            .fetch_add(consumer.desired_bytes(), Ordering::Relaxed);
        self.total_minimum
            .fetch_add(consumer.minimum_bytes(), Ordering::Relaxed);
        self.registrations.fetch_add(1, Ordering::Relaxed);
        MemoryGrant::with_bounds(
            consumer.name(),
            capacity,
            consumer.desired_bytes(),
            consumer.minimum_bytes(),
        )
    }

    pub fn registrations(&self) -> usize {
        self.registrations.load(Ordering::Relaxed)
    }

    pub fn total_desired(&self) -> usize {
        self.total_desired.load(Ordering::Relaxed)
    }

    pub fn total_minimum(&self) -> usize {
        self.total_minimum.load(Ordering::Relaxed)
    }
}

/// Shared registry handle.
pub type SharedGrantRegistry = Arc<GrantRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyJoin {
        desired: usize,
        minimum: usize,
    }

    impl ReclaimableConsumer for DummyJoin {
        fn name(&self) -> &str {
            "dummy-join"
        }
        fn desired_bytes(&self) -> usize {
            self.desired
        }
        fn minimum_bytes(&self) -> usize {
            self.minimum
        }
        fn try_reclaim(&self, _target: usize) -> usize {
            0
        }
    }

    #[test]
    fn register_tracks_bounds_and_soft_limit() {
        let reg = GrantRegistry::new();
        let c = DummyJoin {
            desired: 64 * 1024 * 1024,
            minimum: 8 * 1024 * 1024,
        };
        let grant = reg.register(&c, 16 * 1024 * 1024);
        assert_eq!(reg.registrations(), 1);
        assert_eq!(reg.total_desired(), c.desired);
        assert_eq!(grant.capacity_bytes() % (64 * 1024), 0); // unit aligned
        assert!(grant.soft_limit_bytes() <= grant.capacity_bytes());
        assert!(grant.soft_limit_bytes() >= grant.capacity_bytes() / 2);
    }
}
