//! Phase 1 slow-consumer gate: backpressure under byte budgets.
//!
//! Plan: `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use sqe_spill::ByteBudget;
use sqe_worker::executor::execute_scan_streaming_with_store;

use common::*;

/// Slow-drain a wide-row scan under a 64 MiB scan budget. Peak live ownership
/// must stay within the budget; the scan must complete (or be cancellable)
/// without cumulative ResourcesExhausted.
#[tokio::test]
async fn slow_consumer_wide_rows_stays_within_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // 4x the worker limit of decoded data — enough to force backpressure.
    let fixture = generate_large_parquet(
        tmp.path(),
        "data/slow_consumer.parquet",
        4 * WORKER_MEMORY_LIMIT_BYTES,
    )
    .expect("generate parquet");

    let pool = Arc::new(FairSpillPool::new(WORKER_MEMORY_LIMIT_BYTES));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .expect("runtime"),
    );
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let scan_budget = ByteBudget::new("scan", WORKER_MEMORY_LIMIT_BYTES, Some(pool.clone()));
    let metrics = worker_metrics();
    let store = local_store(tmp.path());
    let task = local_scan_task(
        vec![fixture.object_key.clone()],
        vec![fixture.file_size_bytes],
    );

    let peak_tracked = Arc::new(PeakTracker::new());
    let peak_rss = Arc::new(PeakTracker::new());
    let peak_queue = Arc::new(PeakTracker::new());
    let stop = Arc::new(AtomicBool::new(false));
    let sample_metrics = metrics.clone();
    let sample_pool = pool.clone();
    let sample_peak = peak_tracked.clone();
    let sample_rss = peak_rss.clone();
    let sample_queue = peak_queue.clone();
    let sample_stop = stop.clone();
    let sample_budget = scan_budget.clone();
    let sampler = tokio::spawn(async move {
        while !sample_stop.load(Ordering::Relaxed) {
            sample_peak.observe(sample_pool.reserved().max(sample_budget.used_bytes()));
            sample_queue.observe(sample_metrics.scan_queue_resident_bytes.get() as usize);
            sample_rss.observe(process_rss_bytes() as usize);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let sw = Stopwatch::start();
    let (_schema, mut stream) = execute_scan_streaming_with_store(
        task,
        Some(metrics.clone()),
        ctx,
        store,
        None,
        None,
        None,
        false,
        Some(scan_budget.clone()),
    )
    .await
    .expect("setup");

    let mut rows = 0usize;
    let mut bytes_decoded = 0u64;
    let unit = scan_budget.unit_bytes();

    while let Some(item) = stream.next().await {
        let accounted = item.expect("slow consumer must not exhaust under ownership accounting");
        rows += accounted.get().num_rows();
        bytes_decoded += accounted.logical_bytes() as u64;
        let used = scan_budget.used_bytes();
        peak_tracked.observe(used);
        peak_queue.observe(metrics.scan_queue_resident_bytes.get() as usize);
        assert!(
            used <= WORKER_MEMORY_LIMIT_BYTES + unit,
            "live scan ownership {used} exceeded budget"
        );
        // Simulate a slow Flight client.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    assert!(rows > 0);
    assert_eq!(scan_budget.used_bytes(), 0);
    assert_eq!(pool.reserved(), 0);

    let path = record_baseline_case(BaselineCase {
        name: "slow_consumer_wide_rows".to_string(),
        wall_time_ms: sw.elapsed_ms(),
        bytes_input: fixture.file_size_bytes,
        bytes_decoded_or_buffered: bytes_decoded,
        peak_rss_bytes: peak_rss.get() as u64,
        peak_tracked_bytes: peak_tracked.get() as u64,
        failure_reason: None,
        notes: format!(
            "phase1 green: rows={rows}, peak_queue={}, peak_tracked={}, limit={}",
            peak_queue.get(),
            peak_tracked.get(),
            WORKER_MEMORY_LIMIT_BYTES
        ),
    })
    .expect("write baseline");
    eprintln!(
        "slow_consumer green peak_queue={} peak_tracked={} baseline={}",
        peak_queue.get(),
        peak_tracked.get(),
        path.display()
    );
}

/// Future: pausing 30s must stop further S3 GETs once budgets are full.
#[tokio::test]
#[ignore = "phase-1 stretch: assert no further GETs during 30s pause once budgets full"]
async fn slow_consumer_caps_bytes_when_client_pauses() {
    panic!(
        "Wire a CountingStore and assert GET count freezes for 30s while the \
         client is paused and scan+flight budgets are full"
    );
}
