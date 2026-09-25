use sqe_lineage::*;

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
