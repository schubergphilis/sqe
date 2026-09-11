//! Issue #407: the scan budget is ONE budget per worker, not one per scan.
//!
//! `[worker.memory] scan_budget` used to be parsed, resolved and counted in
//! the startup headroom check while reaching no code; every `do_get` derived
//! a private half-pool budget, so N concurrent scans held N budgets and the
//! only thing that ever stopped them was the pool refusing a reservation,
//! which is a failed query, not backpressure. Sharing the budget has its own
//! trap: a fetch charge that can fill the budget deadlocks every scan at its
//! first batch. Both are pinned here.

use std::time::Duration;

use futures::StreamExt;
use sqe_worker::executor::ScanBudget;

use super::fixtures::{do_get, drain, wait_for, worker_service, StubBucket};

/// A service built without an explicit budget still has exactly one, and
/// `do_get` charges THAT one, so the charge is visible to the heartbeat's
/// capacity probe and shared with the next scan.
#[tokio::test]
async fn unconfigured_service_charges_its_shared_scan_budget() {
    let mut bucket = StubBucket::new();
    bucket.put_parquet("data/f.parquet", &[1024; 8]).await;
    let svc = worker_service(bucket.factory(), None, None);
    assert_eq!(svc.scan_budget().used_bytes(), 0);

    let task = bucket.scan_task("frag-shared-default");
    let mut stream = do_get(&svc, &task).await.expect("setup").into_inner();
    // Pull one frame so the producer has admitted at least one batch, then
    // leave the rest queued: the charge must show on the service's budget.
    stream.next().await.expect("schema frame").expect("ok");
    wait_for(Duration::from_secs(10), || {
        (svc.scan_budget().used_bytes() > 0).then_some(())
    })
    .await
    .expect("queued batches are charged to the service-wide scan budget");

    drop(stream);
    wait_for(Duration::from_secs(10), || {
        (svc.scan_budget().used_bytes() == 0).then_some(())
    })
    .await
    .expect("cancel returns every permit to the shared budget");
}

/// Eight single-file scans against one eight-unit budget. With one lane the
/// eight fetch charges would take every unit and each scan would then wait
/// in its first batch admission for a release that never comes. With the
/// fetch lane bounded to two units the scans queue on the open and every
/// one of them finishes.
#[tokio::test]
async fn concurrent_scans_share_one_budget_and_none_deadlocks() {
    const UNIT: usize = 4096;
    const SCANS: usize = 8;

    let mut bucket = StubBucket::new();
    let keys: Vec<String> = (0..SCANS).map(|i| format!("data/f{i}.parquet")).collect();
    for key in &keys {
        // Two 512-row Int64 groups: two 4 KiB batches, a file of roughly two
        // units, so one file's fetch charge is a whole unit.
        bucket.put_parquet(key, &[512, 512]).await;
    }
    // 8 units: fetch lane 2, decode lane 6.
    let scan_budget = ScanBudget::with_granularity(SCANS * UNIT, UNIT, None);
    assert_eq!(scan_budget.fetch().capacity_bytes(), 2 * UNIT);
    let svc = worker_service(bucket.factory(), Some(scan_budget.clone()), None);

    let mut streams = Vec::with_capacity(SCANS);
    for (i, key) in keys.iter().enumerate() {
        let task = bucket.scan_task_over(&format!("frag-{i}"), &[key.as_str()]);
        streams.push(do_get(&svc, &task).await.expect("setup").into_inner());
    }
    assert_eq!(svc.status().inflight_scans(), SCANS as u32);

    let drained = tokio::time::timeout(
        Duration::from_secs(30),
        futures::future::join_all(streams.into_iter().map(drain)),
    )
    .await
    .expect("every scan must finish: a hang here is the shared-budget deadlock");

    for (i, (frames, err)) in drained.iter().enumerate() {
        assert!(err.is_none(), "scan {i} failed: {err:?}");
        // Schema frame plus two data frames.
        assert!(*frames >= 3, "scan {i} shipped {frames} frames");
    }
    assert_eq!(scan_budget.used_bytes(), 0, "all permits released");
    wait_for(Duration::from_secs(10), || {
        (svc.status().inflight_scans() == 0).then_some(())
    })
    .await
    .expect("in-flight count returns to zero");
}
