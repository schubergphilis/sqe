//! Process-lifetime identity and load counters the heartbeat reports
//! (review finding 609 / #616).
//!
//! The heartbeat used to carry only the advertise URL, so the coordinator
//! could not tell a restarted worker from a live one, had no capacity vector
//! to schedule on, and re-admitted a worker that failed every scan as long as
//! it kept heartbeating. [`WorkerStatus`] is the single place the Flight
//! service records what the hello needs: a per-process `instance_id`, the
//! number of scans in flight, and the worker-attributable faults since the
//! last heartbeat ([`sqe_planner::WorkerFault::marks_worker_unhealthy`]).
//!
//! Capacity is measured lazily through a probe the Flight service installs
//! once it knows its pool, governor and budgets, so this module does not
//! depend on `flight_service.rs`.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use sqe_planner::{WorkerCapacity, WorkerFault};
use uuid::Uuid;

/// Reads the current load vector. Installed by the Flight service; see
/// [`WorkerStatus::set_capacity_probe`].
pub type CapacityProbe = Arc<dyn Fn() -> WorkerCapacity + Send + Sync + 'static>;

/// Build identifier reported in the hello: the crate version, plus the git
/// sha when the build exported one (`SQE_BUILD_GIT_SHA`, set by the release
/// pipeline; absent in a plain `cargo build`).
pub fn build_identifier() -> String {
    match option_env!("SQE_BUILD_GIT_SHA") {
        Some(sha) if !sha.is_empty() => format!("{}+{sha}", env!("CARGO_PKG_VERSION")),
        _ => env!("CARGO_PKG_VERSION").to_string(),
    }
}

/// Identity and counters shared between the Flight service (writer) and the
/// heartbeat task (reader).
pub struct WorkerStatus {
    instance_id: Uuid,
    inflight_scans: AtomicU32,
    failures_since_last: AtomicU32,
    /// `false` after the coordinator replied `accepted: false`; `true` at boot
    /// and after any accepting reply. Diagnostic only: the worker keeps
    /// serving tickets so a coordinator that flaps does not take every worker
    /// with it.
    coordinator_accepted: AtomicBool,
    capacity_probe: OnceLock<CapacityProbe>,
}

impl std::fmt::Debug for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStatus")
            .field("instance_id", &self.instance_id)
            .field("inflight_scans", &self.inflight_scans())
            .field("failures_since_last", &self.failures_since_last())
            .field("coordinator_accepted", &self.coordinator_accepted())
            .field("capacity_probe", &self.capacity_probe.get().is_some())
            .finish()
    }
}

impl WorkerStatus {
    /// Fresh identity for this process.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            instance_id: Uuid::now_v7(),
            inflight_scans: AtomicU32::new(0),
            failures_since_last: AtomicU32::new(0),
            coordinator_accepted: AtomicBool::new(true),
            capacity_probe: OnceLock::new(),
        })
    }

    /// Stable for the life of the process; changes on restart.
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    /// Mark a scan as in flight until the returned guard drops (stream end,
    /// setup failure or client disconnect alike).
    pub fn scan_started(self: &Arc<Self>) -> InflightScanGuard {
        self.inflight_scans.fetch_add(1, Ordering::AcqRel);
        InflightScanGuard {
            status: Arc::clone(self),
        }
    }

    /// Scans currently executing.
    pub fn inflight_scans(&self) -> u32 {
        self.inflight_scans.load(Ordering::Acquire)
    }

    /// Count a scan failure. Only faults that are evidence against THIS
    /// worker are counted; a user's expired token or a bad ticket says nothing
    /// about the worker and must not cost it its registry health.
    pub fn record_fault(&self, fault: WorkerFault) {
        if fault.marks_worker_unhealthy() {
            self.failures_since_last.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Worker-attributable failures not yet reported to the coordinator.
    pub fn failures_since_last(&self) -> u32 {
        self.failures_since_last.load(Ordering::Acquire)
    }

    /// Forget `reported` failures once a heartbeat carrying them was
    /// acknowledged. Failures that arrived in between stay pending, and a
    /// failed heartbeat acknowledges nothing so they are reported next time.
    pub fn acknowledge_failures(&self, reported: u32) {
        let _ = self
            .failures_since_last
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some(cur.saturating_sub(reported))
            });
    }

    /// Record what the coordinator last said about this worker's admission.
    pub fn set_coordinator_accepted(&self, accepted: bool) {
        self.coordinator_accepted.store(accepted, Ordering::Release);
    }

    /// `false` when the last hello reply carried `accepted: false`.
    pub fn coordinator_accepted(&self) -> bool {
        self.coordinator_accepted.load(Ordering::Acquire)
    }

    /// Install the load-vector reader. First caller wins; later calls are
    /// ignored so a test double cannot be replaced from under a running
    /// heartbeat.
    pub fn set_capacity_probe(&self, probe: CapacityProbe) {
        let _ = self.capacity_probe.set(probe);
    }

    /// Current load vector, with `inflight_scans` always filled from this
    /// struct. Zero everywhere else when no probe is installed; the
    /// coordinator treats zero as "unknown".
    pub fn capacity(&self) -> WorkerCapacity {
        let mut capacity = match self.capacity_probe.get() {
            Some(probe) => probe(),
            None => WorkerCapacity::default(),
        };
        capacity.inflight_scans = self.inflight_scans();
        capacity
    }
}

/// Decrements the in-flight scan count on drop. Hold it for the lifetime of
/// the `do_get` response stream.
pub struct InflightScanGuard {
    status: Arc<WorkerStatus>,
}

impl Drop for InflightScanGuard {
    fn drop(&mut self) {
        let _ = self
            .status
            .inflight_scans
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                Some(cur.saturating_sub(1))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_is_fresh_per_status_and_non_nil() {
        let a = WorkerStatus::new();
        let b = WorkerStatus::new();
        assert_ne!(a.instance_id(), Uuid::nil());
        assert_ne!(a.instance_id(), b.instance_id());
    }

    #[test]
    fn inflight_guard_counts_down_on_drop() {
        let status = WorkerStatus::new();
        let g1 = status.scan_started();
        let g2 = status.scan_started();
        assert_eq!(status.inflight_scans(), 2);
        assert_eq!(status.capacity().inflight_scans, 2);
        drop(g1);
        assert_eq!(status.inflight_scans(), 1);
        drop(g2);
        assert_eq!(status.inflight_scans(), 0);
    }

    #[test]
    fn only_worker_faults_count_and_ack_is_partial() {
        let status = WorkerStatus::new();
        for fault in WorkerFault::ALL {
            status.record_fault(fault);
        }
        // WorkerResourceExhausted + WorkerInternal.
        assert_eq!(status.failures_since_last(), 2);
        status.record_fault(WorkerFault::WorkerInternal);
        assert_eq!(status.failures_since_last(), 3);
        status.acknowledge_failures(2);
        assert_eq!(status.failures_since_last(), 1);
        status.acknowledge_failures(10);
        assert_eq!(status.failures_since_last(), 0);
    }

    #[test]
    fn capacity_probe_is_install_once() {
        let status = WorkerStatus::new();
        assert_eq!(status.capacity(), WorkerCapacity::default());
        status.set_capacity_probe(Arc::new(|| WorkerCapacity {
            memory_limit_bytes: 42,
            ..WorkerCapacity::default()
        }));
        status.set_capacity_probe(Arc::new(|| WorkerCapacity {
            memory_limit_bytes: 99,
            ..WorkerCapacity::default()
        }));
        assert_eq!(status.capacity().memory_limit_bytes, 42);
    }

    #[test]
    fn build_identifier_starts_with_the_crate_version() {
        assert!(build_identifier().starts_with(env!("CARGO_PKG_VERSION")));
    }
}
