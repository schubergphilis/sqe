//! Temporary memory governor (Phase 7 foundation).
//!
//! Concurrent blocking operators (join build, aggregate, sort, shuffle) register
//! as [`ReclaimableConsumer`]s. Until full negotiation lands, the governor:
//!
//! 1. Rejects admission when summed **minima** exceed the worker operator pool.
//! 2. Hands out fixed grants capped by desired bytes, weighted fairly.
//! 3. Preserves spill-read/merge headroom outside the distributable pool.
//!
//! Replaces fair division of `FairSpillPool` across every registered consumer
//! (the TPC-DS q39 pool/N pathology) with explicit grants.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::budget::split_default_read_headroom;
use crate::reclaim::{MemoryGrant, ReclaimableConsumer};

/// Workload class for fairness weights and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    Join,
    Aggregate,
    Sort,
    Shuffle,
    Scan,
    Other,
}

impl WorkloadClass {
    /// Relative weight for residual fair-share distribution.
    pub fn weight(self) -> usize {
        match self {
            Self::Join => 4,
            Self::Aggregate => 3,
            Self::Sort => 3,
            Self::Shuffle => 2,
            Self::Scan => 2,
            Self::Other => 1,
        }
    }
}

/// One registration request before a grant is issued.
#[derive(Debug, Clone)]
pub struct AdmissionRequest {
    pub query_id: String,
    pub name: String,
    pub class: WorkloadClass,
    pub desired_bytes: usize,
    pub minimum_bytes: usize,
}

/// Outcome of an admission attempt.
#[derive(Debug, Clone)]
pub enum AdmissionDecision {
    Granted(MemoryGrant),
    /// Sum of minima cannot fit; caller should queue or fail the query.
    Rejected {
        reason: String,
        pool_bytes: usize,
        minima_sum: usize,
    },
}

/// RAII release of a governor grant when the holder is dropped (success,
/// cancel, or panic). Call [`GrantGuard::disarm`] only if ownership of the
/// grant is transferred elsewhere.
pub struct GrantGuard {
    governor: Arc<MemoryGovernor>,
    query_id: String,
    name: String,
    armed: bool,
}

impl GrantGuard {
    pub fn new(governor: Arc<MemoryGovernor>, query_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            governor,
            query_id: query_id.into(),
            name: name.into(),
            armed: true,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Prevent release on drop (caller takes responsibility).
    pub fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for GrantGuard {
    fn drop(&mut self) {
        if self.armed {
            self.governor.release(&self.query_id, &self.name);
        }
    }
}

/// Process-wide governor for one worker.
pub struct MemoryGovernor {
    /// Total bytes managed (typically operator_budget + shuffle, minus headroom).
    pool_bytes: usize,
    /// Reserved spill-read / merge headroom not handed to writers.
    headroom_bytes: usize,
    /// Distributable capacity (= pool - headroom).
    distributable: usize,
    state: Mutex<GovernorState>,
    admissions: AtomicUsize,
    rejections: AtomicUsize,
}

#[derive(Default)]
struct GovernorState {
    /// Active grants: (query_id, name, minimum, granted).
    active: Vec<ActiveGrant>,
    /// Bytes currently granted (sum of grant capacities).
    granted_sum: usize,
    /// Sum of minima of active grants.
    minima_sum: usize,
}

struct ActiveGrant {
    query_id: String,
    name: String,
    #[allow(dead_code)] // retained for weighted reclaim in later Phase 7 work
    class: WorkloadClass,
    minimum: usize,
    granted: usize,
}

impl MemoryGovernor {
    /// Create a governor over `pool_bytes`, carving default read headroom.
    pub fn new(pool_bytes: usize) -> Self {
        let (distributable, headroom) = split_default_read_headroom(pool_bytes.max(1));
        Self {
            pool_bytes: pool_bytes.max(1),
            headroom_bytes: headroom,
            distributable,
            state: Mutex::new(GovernorState::default()),
            admissions: AtomicUsize::new(0),
            rejections: AtomicUsize::new(0),
        }
    }

    pub fn pool_bytes(&self) -> usize {
        self.pool_bytes
    }

    pub fn headroom_bytes(&self) -> usize {
        self.headroom_bytes
    }

    pub fn distributable_bytes(&self) -> usize {
        self.distributable
    }

    pub fn admissions(&self) -> usize {
        self.admissions.load(Ordering::Relaxed)
    }

    pub fn rejections(&self) -> usize {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Try to admit a consumer. Fails if minima cannot fit.
    pub fn try_admit(
        &self,
        req: AdmissionRequest,
        _consumer: &dyn ReclaimableConsumer,
    ) -> AdmissionDecision {
        let minimum = req.minimum_bytes.max(1).min(req.desired_bytes.max(1));
        let desired = req.desired_bytes.max(minimum);

        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let new_minima = state.minima_sum.saturating_add(minimum);
        if new_minima > self.distributable {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return AdmissionDecision::Rejected {
                reason: format!(
                    "minima {} exceed distributable pool {} (headroom {} reserved)",
                    new_minima, self.distributable, self.headroom_bytes
                ),
                pool_bytes: self.pool_bytes,
                minima_sum: new_minima,
            };
        }

        // Fair residual: base = minimum, plus share of free residual weighted
        // by class. Cap at desired.
        let free = self
            .distributable
            .saturating_sub(state.granted_sum)
            .saturating_sub(minimum);
        let weight = req.class.weight();
        // Simple: take min(desired - minimum, free * weight / (weight+3))
        let bonus = free.saturating_mul(weight) / (weight + 3);
        let grant_size = minimum.saturating_add(bonus).min(desired).max(minimum);

        // Prevent one query from claiming the entire residual.
        let per_query_cap = self.distributable / 2 + minimum;
        let grant_size = grant_size.min(per_query_cap);

        if state.granted_sum.saturating_add(grant_size) > self.distributable {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            return AdmissionDecision::Rejected {
                reason: "insufficient free capacity after fair share".into(),
                pool_bytes: self.pool_bytes,
                minima_sum: new_minima,
            };
        }

        state.granted_sum += grant_size;
        state.minima_sum += minimum;
        state.active.push(ActiveGrant {
            query_id: req.query_id.clone(),
            name: req.name.clone(),
            class: req.class,
            minimum,
            granted: grant_size,
        });
        self.admissions.fetch_add(1, Ordering::Relaxed);

        AdmissionDecision::Granted(MemoryGrant::with_bounds(
            format!("{}:{}", req.query_id, req.name),
            grant_size,
            desired,
            minimum,
        ))
    }

    /// Admit and return a [`GrantGuard`] that releases on drop.
    ///
    /// Convenience for call sites that hold a grant for a single task/scope.
    pub fn try_admit_guarded(
        self: &Arc<Self>,
        req: AdmissionRequest,
        consumer: &dyn ReclaimableConsumer,
    ) -> Result<(MemoryGrant, GrantGuard), AdmissionDecision> {
        let query_id = req.query_id.clone();
        let name = req.name.clone();
        match self.try_admit(req, consumer) {
            AdmissionDecision::Granted(grant) => {
                let guard = GrantGuard::new(self.clone(), query_id, name);
                Ok((grant, guard))
            }
            other => Err(other),
        }
    }

    /// Release a previously admitted grant by query/name.
    pub fn release(&self, query_id: &str, name: &str) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(idx) = state
            .active
            .iter()
            .position(|g| g.query_id == query_id && g.name == name)
        {
            let g = state.active.remove(idx);
            state.granted_sum = state.granted_sum.saturating_sub(g.granted);
            state.minima_sum = state.minima_sum.saturating_sub(g.minimum);
        }
    }

    /// Release all grants for a query (stage completion / cancel).
    pub fn release_query(&self, query_id: &str) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let (keep, drop): (Vec<_>, Vec<_>) = state
            .active
            .drain(..)
            .partition(|g| g.query_id != query_id);
        for g in drop {
            state.granted_sum = state.granted_sum.saturating_sub(g.granted);
            state.minima_sum = state.minima_sum.saturating_sub(g.minimum);
        }
        state.active = keep;
    }

    pub fn active_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .active
            .len()
    }

    pub fn granted_sum(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .granted_sum
    }
}

pub type SharedMemoryGovernor = Arc<MemoryGovernor>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reclaim::ReclaimableConsumer;

    struct Dummy {
        name: &'static str,
        desired: usize,
        minimum: usize,
    }

    impl ReclaimableConsumer for Dummy {
        fn name(&self) -> &str {
            self.name
        }
        fn desired_bytes(&self) -> usize {
            self.desired
        }
        fn minimum_bytes(&self) -> usize {
            self.minimum
        }
        fn try_reclaim(&self, _: usize) -> usize {
            0
        }
    }

    fn req(q: &str, name: &str, class: WorkloadClass, desired: usize, minimum: usize) -> AdmissionRequest {
        AdmissionRequest {
            query_id: q.into(),
            name: name.into(),
            class,
            desired_bytes: desired,
            minimum_bytes: minimum,
        }
    }

    #[test]
    fn admits_within_pool_and_tracks_release() {
        let gov = MemoryGovernor::new(16 * 1024 * 1024);
        let d = Dummy {
            name: "join",
            desired: 8 * 1024 * 1024,
            minimum: 1024 * 1024,
        };
        let decision = gov.try_admit(
            req("q1", "join", WorkloadClass::Join, d.desired, d.minimum),
            &d,
        );
        assert!(matches!(decision, AdmissionDecision::Granted(_)));
        assert_eq!(gov.active_count(), 1);
        assert!(gov.granted_sum() > 0);
        gov.release("q1", "join");
        assert_eq!(gov.active_count(), 0);
        assert_eq!(gov.granted_sum(), 0);
    }

    #[test]
    fn rejects_when_minima_exceed_pool() {
        let gov = MemoryGovernor::new(1024 * 1024); // 1 MiB pool
        let d = Dummy {
            name: "big",
            desired: 8 * 1024 * 1024,
            minimum: 2 * 1024 * 1024, // min > distributable (~768 KiB)
        };
        let decision = gov.try_admit(
            req("q1", "big", WorkloadClass::Sort, d.desired, d.minimum),
            &d,
        );
        assert!(matches!(decision, AdmissionDecision::Rejected { .. }));
        assert_eq!(gov.rejections(), 1);
    }

    #[test]
    fn concurrent_queries_share_without_starvation() {
        let gov = MemoryGovernor::new(32 * 1024 * 1024);
        let mut grants = 0;
        for i in 0..4 {
            let d = Dummy {
                name: "op",
                desired: 16 * 1024 * 1024,
                minimum: 2 * 1024 * 1024,
            };
            let q = format!("q{i}");
            match gov.try_admit(
                req(&q, "op", WorkloadClass::Join, d.desired, d.minimum),
                &d,
            ) {
                AdmissionDecision::Granted(_) => grants += 1,
                AdmissionDecision::Rejected { .. } => break,
            }
        }
        // At least two concurrent minima of 2 MiB should fit in ~24 MiB distributable.
        assert!(grants >= 2, "only {grants} grants");
        assert!(gov.granted_sum() <= gov.distributable_bytes());
        gov.release_query("q0");
        assert!(gov.active_count() < grants);
    }

    #[test]
    fn headroom_is_reserved() {
        let gov = MemoryGovernor::new(4 * 1024 * 1024);
        assert!(gov.headroom_bytes() > 0);
        assert_eq!(
            gov.headroom_bytes() + gov.distributable_bytes(),
            gov.pool_bytes()
        );
    }

    #[test]
    fn grant_guard_releases_on_drop() {
        let gov = Arc::new(MemoryGovernor::new(16 * 1024 * 1024));
        let d = Dummy {
            name: "shuffle",
            desired: 4 * 1024 * 1024,
            minimum: 512 * 1024,
        };
        {
            let (grant, _guard) = gov
                .try_admit_guarded(
                    req("q", "p0", WorkloadClass::Shuffle, d.desired, d.minimum),
                    &d,
                )
                .expect("admit");
            assert!(grant.capacity_bytes() > 0);
            assert_eq!(gov.active_count(), 1);
        }
        assert_eq!(gov.active_count(), 0);
    }

    #[test]
    fn four_concurrent_classes_share_pool() {
        let gov = Arc::new(MemoryGovernor::new(64 * 1024 * 1024));
        let classes = [
            WorkloadClass::Join,
            WorkloadClass::Aggregate,
            WorkloadClass::Sort,
            WorkloadClass::Shuffle,
        ];
        let mut guards = Vec::new();
        for (i, class) in classes.iter().enumerate() {
            let d = Dummy {
                name: "op",
                desired: 20 * 1024 * 1024,
                minimum: 2 * 1024 * 1024,
            };
            let (g, guard) = gov
                .try_admit_guarded(
                    req(&format!("q{i}"), "op", *class, d.desired, d.minimum),
                    &d,
                )
                .unwrap_or_else(|e| panic!("admit {class:?}: {e:?}"));
            assert!(g.capacity_bytes() >= d.minimum);
            guards.push(guard);
        }
        assert_eq!(gov.active_count(), 4);
        assert!(gov.granted_sum() <= gov.distributable_bytes());
        drop(guards);
        assert_eq!(gov.active_count(), 0);
    }
}
