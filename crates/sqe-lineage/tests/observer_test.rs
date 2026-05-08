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
