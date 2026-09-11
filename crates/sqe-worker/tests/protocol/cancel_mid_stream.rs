//! Finding 611 item 4: a client that drops a `do_get` response mid-stream
//! leaves nothing behind -- scan budget, flight-frame budget, in-flight count
//! and the fragment's credential channel all return to zero.

use std::time::Duration;

use futures::StreamExt;
use sqe_spill::ByteBudget;
use sqe_worker::credential_channel::RefreshableCredentials;
use sqe_worker::executor::ScanBudget;

use super::fixtures::{do_get, wait_for, worker_service, StubBucket, NEW_KEY};

#[tokio::test]
async fn cancel_mid_stream() {
    let mut bucket = StubBucket::new();
    // Four row groups of 512 rows: schema frame + four data frames, each
    // batch a 4 KiB array, so there is plenty left to cancel after two frames.
    bucket
        .put_parquet("data/wide.parquet", &[512, 512, 512, 512])
        .await;
    bucket.put_parquet("data/tail.parquet", &[512]).await;

    let scan_budget = ScanBudget::with_granularity(64 * 1024, 4096, None);
    let flight_budget = ByteBudget::with_granularity("flight", 64 * 1024, 1024, None);
    let svc = worker_service(
        bucket.factory(),
        Some(scan_budget.clone()),
        Some(flight_budget.clone()),
    );
    let task = bucket.scan_task("frag-cancel");

    let mut stream = do_get(&svc, &task).await.expect("setup").into_inner();
    assert_eq!(svc.status().inflight_scans(), 1);

    // Consume two frames (schema + first batch), then walk away.
    for _ in 0..2 {
        stream
            .next()
            .await
            .expect("frame")
            .expect("no error before cancel");
    }
    assert!(
        flight_budget.used_bytes() > 0 || scan_budget.used_bytes() > 0,
        "something must be charged while the stream is live"
    );
    drop(stream);

    // Cleanup is asynchronous (the producer notices the closed channel; the
    // credential guard removes its entry via tokio::spawn). Bound the wait.
    wait_for(Duration::from_secs(10), || {
        (svc.status().inflight_scans() == 0
            && scan_budget.used_bytes() == 0
            && flight_budget.used_bytes() == 0)
            .then_some(())
    })
    .await
    .unwrap_or_else(|| {
        panic!(
            "after cancel: inflight={} scan_used={} flight_used={}",
            svc.status().inflight_scans(),
            scan_budget.used_bytes(),
            flight_budget.used_bytes()
        )
    });

    // The credential channel for the fragment is gone: a refresh finds no
    // active scan.
    let creds = RefreshableCredentials {
        fragment_id: "frag-cancel".to_string(),
        access_key_id: NEW_KEY.to_string(),
        secret_access_key: "s".to_string(),
        session_token: String::new(),
        expiry: chrono::Utc::now() + chrono::Duration::hours(1),
    };
    // The guard removes the entry via `tokio::spawn`, so poll: `publish`
    // returns `false` once the channel is gone.
    let store = svc.credential_store().clone();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if !store.publish(creds.clone()).await {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "credential store entry must be removed after cancellation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Cancellation is nobody's fault: nothing counts against the worker.
    assert_eq!(svc.status().failures_since_last(), 0);
}
