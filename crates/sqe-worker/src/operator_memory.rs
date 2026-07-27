//! Operator memory admission via the worker governor (Phase 7).
//!
//! Join, aggregate, and sort executors call [`admit_blocking_operator`] before
//! building large state. The returned guard releases the grant and unregisters
//! the live consumer on drop.

use std::sync::Arc;

use sqe_spill::{
    admit_operator, AdmissionDecision, LiveConsumerRegistry, MemoryGovernor, MemoryGrant,
    OperatorConsumer, OperatorGrantGuard, WorkloadClass,
};

/// Admit a join / aggregate / sort operator under the worker governor.
pub fn admit_blocking_operator(
    governor: &Arc<MemoryGovernor>,
    live: &Arc<LiveConsumerRegistry>,
    query_id: &str,
    name: impl Into<String>,
    class: WorkloadClass,
    desired_bytes: usize,
    minimum_bytes: usize,
) -> Result<(MemoryGrant, OperatorGrantGuard), AdmissionDecision> {
    let consumer = Arc::new(OperatorConsumer::new(
        name,
        class,
        desired_bytes,
        minimum_bytes,
    ));
    admit_operator(governor, live, query_id, consumer)
}

/// Convenience constructors for common operator classes.
pub fn admit_join(
    governor: &Arc<MemoryGovernor>,
    live: &Arc<LiveConsumerRegistry>,
    query_id: &str,
    name: impl Into<String>,
    desired: usize,
    minimum: usize,
) -> Result<(MemoryGrant, OperatorGrantGuard), AdmissionDecision> {
    admit_blocking_operator(
        governor,
        live,
        query_id,
        name,
        WorkloadClass::Join,
        desired,
        minimum,
    )
}

pub fn admit_aggregate(
    governor: &Arc<MemoryGovernor>,
    live: &Arc<LiveConsumerRegistry>,
    query_id: &str,
    name: impl Into<String>,
    desired: usize,
    minimum: usize,
) -> Result<(MemoryGrant, OperatorGrantGuard), AdmissionDecision> {
    admit_blocking_operator(
        governor,
        live,
        query_id,
        name,
        WorkloadClass::Aggregate,
        desired,
        minimum,
    )
}

pub fn admit_sort(
    governor: &Arc<MemoryGovernor>,
    live: &Arc<LiveConsumerRegistry>,
    query_id: &str,
    name: impl Into<String>,
    desired: usize,
    minimum: usize,
) -> Result<(MemoryGrant, OperatorGrantGuard), AdmissionDecision> {
    admit_blocking_operator(
        governor,
        live,
        query_id,
        name,
        WorkloadClass::Sort,
        desired,
        minimum,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admit_three_classes() {
        let gov = Arc::new(MemoryGovernor::new(48 * 1024 * 1024));
        let live = Arc::new(LiveConsumerRegistry::new());
        let (_j, gj) = admit_join(&gov, &live, "q", "j0", 16 * 1024 * 1024, 2 * 1024 * 1024)
            .expect("join");
        let (_a, ga) =
            admit_aggregate(&gov, &live, "q", "a0", 16 * 1024 * 1024, 2 * 1024 * 1024)
                .expect("agg");
        let (_s, gs) =
            admit_sort(&gov, &live, "q", "s0", 16 * 1024 * 1024, 2 * 1024 * 1024).expect("sort");
        assert_eq!(live.len(), 3);
        drop((gj, ga, gs));
        assert!(live.is_empty());
    }
}
