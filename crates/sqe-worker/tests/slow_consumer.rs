//! Phase 0 red gate: slow consumer of a streaming scan / Flight path.
//!
//! Plan: `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`
//! (Phase 0).
//!
//! A slow consumer fills the item-bounded scan channel with wide batches.
//! Phase 0 records peak queue/tracked bytes. Phase 1 turns the ignored green
//! test on: pausing the client for 30s must cap additional fetched+decoded
//! bytes at scan_budget + flight_budget.
//!
//! ```text
//! cargo test -p sqe-worker --test slow_consumer -- --ignored
//! ```

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use sqe_worker::executor::execute_scan_streaming_with_store;

use common::*;

/// Phase 0 diagnostic: slow-drain a wide-row scan and record peak queue /
/// tracked bytes while the item-bounded channel is under backpressure.
#[tokio::test]
async fn phase0_reproducer_slow_consumer_wide_rows_peak() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Enough data to fill the 16-slot channel many times over and, under the
    // cumulative reservation, hit ResourcesExhausted. 256 MiB decoded is 4x
    // the 64 MiB limit — enough for the red gate without a 1.28 GB fixture.
    let fixture = generate_large_parquet(
        tmp.path(),
        "data/slow_consumer.parquet",
        4 * WORKER_MEMORY_LIMIT_BYTES,
    )
    .expect("generate parquet");

    let ctx = session_with_memory_limit(WORKER_MEMORY_LIMIT_BYTES);
    let pool = ctx.runtime_env().memory_pool.clone();
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
    let sampler = tokio::spawn(async move {
        while !sample_stop.load(Ordering::Relaxed) {
            sample_peak.observe(sample_pool.reserved());
            sample_peak.observe(sample_metrics.scan_decode_resident_bytes.get() as usize);
            sample_queue.observe(sample_metrics.scan_queue_resident_bytes.get() as usize);
            sample_rss.observe(process_rss_bytes() as usize);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let sw = Stopwatch::start();
    let prepare = execute_scan_streaming_with_store(
        task,
        Some(metrics.clone()),
        ctx,
        store,
        None,
        None,
        None,
        false,
    )
    .await;

    let mut failure_reason = None;
    let mut rows = 0usize;
    let mut bytes_decoded = 0u64;

    match prepare {
        Ok((_schema, mut stream)) => {
            // Slow consumer: pause between batches so the producer fills the
            // 16-slot channel. This is the Flight slow-client analogue without
            // requiring a full gRPC stack for the Phase 0 baseline.
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        rows += batch.num_rows();
                        bytes_decoded += batch.get_array_memory_size() as u64;
                        peak_queue.observe(metrics.scan_queue_resident_bytes.get() as usize);
                        peak_tracked.observe(pool.reserved());
                        // Simulate a slow Flight client / downstream stage.
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Some(Err(e)) => {
                        failure_reason = Some(e.to_string());
                        break;
                    }
                    None => break,
                }
            }
        }
        Err(e) => {
            failure_reason = Some(e.to_string());
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    let path = record_baseline_case(BaselineCase {
        name: "slow_consumer_wide_rows".to_string(),
        wall_time_ms: sw.elapsed_ms(),
        bytes_input: fixture.file_size_bytes,
        bytes_decoded_or_buffered: bytes_decoded,
        peak_rss_bytes: peak_rss.get() as u64,
        peak_tracked_bytes: peak_tracked.get() as u64,
        failure_reason: failure_reason.clone(),
        notes: format!(
            "rows={rows}, peak_queue={}, pause_ms=25, limit={}",
            peak_queue.get(),
            WORKER_MEMORY_LIMIT_BYTES
        ),
    })
    .expect("write baseline");
    eprintln!(
        "slow_consumer peak_queue={} peak_tracked={} failure={:?} baseline={}",
        peak_queue.get(),
        peak_tracked.get(),
        failure_reason,
        path.display()
    );

    // Phase 0: with cumulative reservation + wide rows, either we exhaust or
    // the queue-resident peak is material. Both outcomes document the boundary.
    assert!(
        failure_reason.is_some() || peak_queue.get() > 0 || rows > 0,
        "slow consumer reproducer produced no signal"
    );
}

/// Future-green (Phase 1): pausing the client for 30s caps additional
/// fetched+decoded bytes at scan_budget + flight_budget.
#[tokio::test]
#[ignore = "phase-0 red gate: turns green in Phase 1 (byte backpressure, no further GETs when full)"]
async fn slow_consumer_caps_bytes_when_client_pauses() {
    panic!(
        "Phase 1 must implement byte-admitted scan+Flight budgets so a 30s \
         client pause caps additional fetched+decoded bytes at \
         scan_budget + flight_budget (+ one in-flight batch)"
    );
}
