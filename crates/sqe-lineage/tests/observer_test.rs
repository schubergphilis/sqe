use sqe_lineage::*;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
struct MockObserver {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl LineageObserver for MockObserver {
    fn on_query_start(&self, _: QueryStartCtx) {
        self.calls.lock().unwrap().push("start");
    }
    fn on_query_complete(&self, _: QueryCompleteCtx) {
        self.calls.lock().unwrap().push("complete");
    }
    fn on_query_fail(&self, _: QueryFailCtx) {
        self.calls.lock().unwrap().push("fail");
    }
}

#[test]
fn observer_trait_object_dispatches_calls() {
    let mock = MockObserver::default();
    let obs: Arc<dyn LineageObserver> = Arc::new(mock.clone());

    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_complete(QueryCompleteCtx::dummy());
    obs.on_query_fail(QueryFailCtx::dummy());

    let calls = mock.calls.lock().unwrap();
    assert_eq!(*calls, vec!["start", "complete", "fail"]);
}

#[tokio::test]
async fn channel_full_drops_newest_and_increments_metric() {
    let (tx, _rx) = tokio::sync::mpsc::channel(2);
    let counter = prometheus::IntCounter::new("sqe_lineage_dropped_test", "test").unwrap();
    let obs = ChannelObserver::new(tx, counter.clone());

    // Fill the channel to capacity (2)
    obs.on_query_start(QueryStartCtx::dummy());
    obs.on_query_start(QueryStartCtx::dummy());

    // Counter should still be 0 — those two fit
    assert_eq!(counter.get(), 0);

    // Third send drops because the receiver hasn't drained
    obs.on_query_start(QueryStartCtx::dummy());

    assert_eq!(counter.get(), 1, "third send should be counted as dropped");

    // Two more drops to confirm increment continues
    obs.on_query_complete(QueryCompleteCtx::dummy());
    obs.on_query_fail(QueryFailCtx::dummy());

    assert_eq!(counter.get(), 3);
}
