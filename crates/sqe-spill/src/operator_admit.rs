//! Admit join / aggregate / sort operators through the worker governor.
//!
//! Call sites obtain a [`MemoryGrant`] + [`GrantGuard`] before building
//! large operator state. Under pressure the governor clamps to minima and
//! reclaims excess; live consumers registered here receive `try_reclaim`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::governor::{
    AdmissionDecision, AdmissionRequest, GrantGuard, MemoryGovernor, WorkloadClass,
};
use crate::reclaim::{MemoryGrant, ReclaimableConsumer};

/// Live consumer handle registered for reclaim callbacks.
struct LiveConsumer {
    consumer: Arc<dyn ReclaimableConsumer>,
}

/// Registry of live operators that can shrink under pressure.
#[derive(Default)]
pub struct LiveConsumerRegistry {
    inner: Mutex<HashMap<(String, String), LiveConsumer>>,
}

impl LiveConsumerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        query_id: impl Into<String>,
        name: impl Into<String>,
        consumer: Arc<dyn ReclaimableConsumer>,
    ) {
        let key = (query_id.into(), name.into());
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, LiveConsumer { consumer });
    }

    pub fn unregister(&self, query_id: &str, name: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(query_id.to_string(), name.to_string()));
    }

    pub fn unregister_query(&self, query_id: &str) {
        let mut map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|(q, _), _| q != query_id);
    }

    /// Ask every live consumer to free memory; returns total reclaimed.
    pub fn reclaim_all(&self, target: usize) -> usize {
        if target == 0 {
            return 0;
        }
        let map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if map.is_empty() {
            return 0;
        }
        let per = (target / map.len()).max(1);
        let mut got = 0usize;
        for live in map.values() {
            got = got.saturating_add(live.consumer.try_reclaim(per));
            if got >= target {
                break;
            }
        }
        got
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Named operator consumer for join / agg / sort admission.
pub struct OperatorConsumer {
    name: String,
    class: WorkloadClass,
    desired: usize,
    minimum: usize,
    current: std::sync::atomic::AtomicUsize,
}

impl OperatorConsumer {
    pub fn new(
        name: impl Into<String>,
        class: WorkloadClass,
        desired: usize,
        minimum: usize,
    ) -> Self {
        let desired = desired.max(1);
        let minimum = minimum.max(1).min(desired);
        Self {
            name: name.into(),
            class,
            desired,
            minimum,
            current: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn class(&self) -> WorkloadClass {
        self.class
    }

    pub fn set_current(&self, bytes: usize) {
        self.current
            .store(bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ReclaimableConsumer for OperatorConsumer {
    fn name(&self) -> &str {
        &self.name
    }
    fn desired_bytes(&self) -> usize {
        self.desired
    }
    fn minimum_bytes(&self) -> usize {
        self.minimum
    }
    fn current_bytes(&self) -> usize {
        self.current.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn try_reclaim(&self, target: usize) -> usize {
        // Best-effort accounting shrink; operators that hold real state should
        // override via a wrapping type that spills.
        let cur = self.current_bytes();
        let floor = self.minimum;
        if cur <= floor {
            return 0;
        }
        let freeable = cur - floor;
        let take = freeable.min(target);
        self.current
            .store(cur - take, std::sync::atomic::Ordering::Relaxed);
        take
    }
}

/// Admit an operator and register it for live reclaim. Returns grant + guard
/// that unregisters on drop.
pub fn admit_operator(
    governor: &Arc<MemoryGovernor>,
    live: &Arc<LiveConsumerRegistry>,
    query_id: &str,
    consumer: Arc<OperatorConsumer>,
) -> Result<(MemoryGrant, OperatorGrantGuard), AdmissionDecision> {
    let name = consumer.name().to_string();
    let req = AdmissionRequest {
        query_id: query_id.to_string(),
        name: name.clone(),
        class: consumer.class(),
        desired_bytes: consumer.desired_bytes(),
        minimum_bytes: consumer.minimum_bytes(),
    };
    // Under pressure: ask live operators to free before admit.
    if governor.under_pressure() {
        let _ = live.reclaim_all(consumer.minimum_bytes());
        let _ = governor.reclaim_under_pressure(consumer.minimum_bytes());
    }
    match governor.try_admit_guarded(req, consumer.as_ref()) {
        Ok((grant, guard)) => {
            live.register(query_id, &name, consumer.clone());
            consumer.set_current(grant.capacity_bytes());
            Ok((
                grant,
                OperatorGrantGuard {
                    grant_guard: guard,
                    live: live.clone(),
                    query_id: query_id.to_string(),
                    name,
                },
            ))
        }
        Err(e) => Err(e),
    }
}

/// Releases the governor grant and unregisters the live consumer.
pub struct OperatorGrantGuard {
    grant_guard: GrantGuard,
    live: Arc<LiveConsumerRegistry>,
    query_id: String,
    name: String,
}

impl Drop for OperatorGrantGuard {
    fn drop(&mut self) {
        self.live.unregister(&self.query_id, &self.name);
        // grant_guard drops after this and releases the governor grant.
        let _ = &self.grant_guard;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admit_join_agg_sort_and_reclaim() {
        let gov = Arc::new(MemoryGovernor::new(32 * 1024 * 1024));
        let live = Arc::new(LiveConsumerRegistry::new());
        let mut guards = Vec::new();
        for (i, class) in [
            WorkloadClass::Join,
            WorkloadClass::Aggregate,
            WorkloadClass::Sort,
        ]
        .into_iter()
        .enumerate()
        {
            let c = Arc::new(OperatorConsumer::new(
                format!("op{i}"),
                class,
                12 * 1024 * 1024,
                2 * 1024 * 1024,
            ));
            let (grant, guard) =
                admit_operator(&gov, &live, &format!("q{i}"), c).expect("admit");
            assert!(grant.capacity_bytes() >= 2 * 1024 * 1024);
            guards.push(guard);
        }
        assert_eq!(live.len(), 3);
        // Pressure reclaim from live consumers.
        let got = live.reclaim_all(1024 * 1024);
        assert!(got > 0);
        drop(guards);
        assert!(live.is_empty());
        assert_eq!(gov.active_count(), 0);
    }

    #[test]
    fn reject_when_minima_do_not_fit() {
        let gov = Arc::new(MemoryGovernor::new(1024 * 1024));
        let live = Arc::new(LiveConsumerRegistry::new());
        let c = Arc::new(OperatorConsumer::new(
            "huge",
            WorkloadClass::Join,
            64 * 1024 * 1024,
            8 * 1024 * 1024,
        ));
        match admit_operator(&gov, &live, "q", c) {
            Err(AdmissionDecision::Rejected { .. }) => {}
            Ok(_) => panic!("expected rejection"),
            Err(other) => panic!("unexpected: {other:?}"),
        }
    }
}
