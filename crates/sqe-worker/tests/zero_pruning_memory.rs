//! Phase 0 red gates: zero-pruning scan memory, wide-batch queue bounds, and
//! shuffle batch-count bounds.
//!
//! Plan: `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`
//! (Phase 0).
//!
//! Run ignored future-green tests after Phase 1/4 land:
//! ```text
//! cargo test -p sqe-worker --test zero_pruning_memory -- --ignored
//! ```

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use sqe_worker::executor::{
    execute_scan_streaming_with_store, SCAN_CHANNEL_ITEM_CAPACITY,
};
use sqe_worker::shuffle::{ShuffleReceiver, DEFAULT_CHANNEL_CAPACITY};

use common::*;

/// Phase 0 gate: a no-filter projected scan whose decoded volume is ≥ 20x the
/// 64 MiB worker limit fails with `ResourcesExhausted` from the cumulative
/// fragment reservation (`executor.rs` try_grow on every batch, never shrink).
///
/// This test is **not** ignored: it documents current unsafe behaviour and
/// writes the baseline JSON artifact. Phase 1 inverts the assertion (scan
/// completes within budget) and retires this reproducer.
#[tokio::test]
async fn phase0_reproducer_zero_pruning_hits_resources_exhausted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fixture = generate_large_parquet(
        tmp.path(),
        "data/zero_pruning.parquet",
        ZERO_PRUNING_TARGET_DECODED_BYTES,
    )
    .expect("generate parquet");

    assert!(
        fixture.decoded_bytes_estimate as usize >= ZERO_PRUNING_TARGET_DECODED_BYTES,
        "fixture decoded estimate {} < target {}",
        fixture.decoded_bytes_estimate,
        ZERO_PRUNING_TARGET_DECODED_BYTES
    );

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
    let stop = Arc::new(AtomicBool::new(false));
    let sample_metrics = metrics.clone();
    let sample_pool = pool.clone();
    let sample_peak = peak_tracked.clone();
    let sample_rss = peak_rss.clone();
    let sample_stop = stop.clone();
    let sampler = tokio::spawn(async move {
        sample_peaks(
            sample_metrics,
            move || sample_pool.reserved(),
            sample_peak,
            sample_rss,
            sample_stop,
        )
        .await;
    });

    let sw = Stopwatch::start();
    let result = execute_scan_streaming_with_store(
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

    let failure_reason;
    let mut bytes_decoded = 0u64;

    match result {
        Ok((_schema, mut stream)) => {
            let mut err: Option<String> = None;
            while let Some(item) = stream.next().await {
                match item {
                    Ok(batch) => {
                        bytes_decoded += batch.get_array_memory_size() as u64;
                        peak_tracked.observe(pool.reserved());
                        peak_tracked
                            .observe(metrics.scan_decode_resident_bytes.get() as usize);
                    }
                    Err(e) => {
                        err = Some(e.to_string());
                        break;
                    }
                }
            }
            failure_reason = err;
        }
        Err(e) => {
            failure_reason = Some(e.to_string());
        }
    }

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;
    peak_tracked.observe(pool.reserved());
    peak_rss.observe(process_rss_bytes() as usize);

    let reason = failure_reason.expect(
        "Phase 0 gate: zero-pruning scan of ≥20x worker memory must fail; \
         if this passes, Phase 1 may already have landed",
    );
    let reason_l = reason.to_lowercase();
    assert!(
        reason_l.contains("resource")
            || reason_l.contains("memory")
            || reason_l.contains("exhaust"),
        "expected ResourcesExhausted-style failure from cumulative reservation, got: {reason}"
    );

    let path = record_baseline_case(BaselineCase {
        name: "zero_pruning_scan_resources_exhausted".to_string(),
        wall_time_ms: sw.elapsed_ms(),
        bytes_input: fixture.file_size_bytes,
        bytes_decoded_or_buffered: bytes_decoded
            .max(metrics.scan_decode_resident_bytes.get() as u64),
        peak_rss_bytes: peak_rss.get() as u64,
        peak_tracked_bytes: peak_tracked.get() as u64,
        failure_reason: Some(reason),
        notes: format!(
            "decoded_estimate={}, batches={}, rows={}, limit={}",
            fixture.decoded_bytes_estimate,
            fixture.num_batches,
            fixture.num_rows,
            WORKER_MEMORY_LIMIT_BYTES
        ),
    })
    .expect("write baseline");
    eprintln!("wrote baseline case to {}", path.display());
}

/// Future-green (Phase 1): a ≥20x scan must complete with peak tracked
/// scan+queue bytes within budget. Ignored until Phase 1 lands.
///
/// ```text
/// cargo test -p sqe-worker --test zero_pruning_memory \
///   zero_pruning_scan_completes_under_byte_budget -- --ignored
/// ```
#[tokio::test]
#[ignore = "phase-0 red gate: turns green in Phase 1 (byte-budgeted scan ownership)"]
async fn zero_pruning_scan_completes_under_byte_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let fixture = generate_large_parquet(
        tmp.path(),
        "data/zero_pruning_green.parquet",
        ZERO_PRUNING_TARGET_DECODED_BYTES,
    )
    .expect("generate parquet");

    let ctx = session_with_memory_limit(WORKER_MEMORY_LIMIT_BYTES);
    let metrics = worker_metrics();
    let store = local_store(tmp.path());
    let task = local_scan_task(
        vec![fixture.object_key.clone()],
        vec![fixture.file_size_bytes],
    );

    let (_schema, mut stream) = execute_scan_streaming_with_store(
        task,
        Some(metrics.clone()),
        ctx,
        store,
        None,
        None,
        None,
        false,
    )
    .await
    .expect("Phase 1: scan setup must succeed under byte budgets");

    let mut rows = 0usize;
    while let Some(item) = stream.next().await {
        let batch = item.expect("Phase 1: scan must not ResourcesExhaust");
        rows += batch.num_rows();
        let queue = metrics.scan_queue_resident_bytes.get() as usize;
        let decode = metrics.scan_decode_resident_bytes.get() as usize;
        // Allow one accounting unit of headroom once Phase 1 uses 64 KiB units.
        let unit = 64 * 1024;
        assert!(
            queue + decode <= WORKER_MEMORY_LIMIT_BYTES + unit,
            "peak scan residency {} exceeded limit {}",
            queue + decode,
            WORKER_MEMORY_LIMIT_BYTES
        );
    }
    assert!(rows > 0, "scan must return rows");
}

/// Phase 0 gate: at the same 16-batch item bound, wide variable-length batches
/// hold ≥ 4x the resident bytes of narrow batches. Proves batch-count bounds
/// do not bound bytes.
#[tokio::test]
async fn phase0_reproducer_wide_batch_queue_exceeds_narrow() {
    // Synthesize 16 batches of each shape and measure sum of array memory sizes
    // — the same quantity a full channel would hold under SCAN_CHANNEL_ITEM_CAPACITY.
    const BATCHES: usize = SCAN_CHANNEL_ITEM_CAPACITY;
    const ROWS: usize = 1_024;
    const WIDE_PAYLOAD: usize = 4_096;

    let narrow_schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]);
    let mut narrow_bytes = 0usize;
    for i in 0..BATCHES {
        let ids: Vec<i64> = (0..ROWS as i64).map(|r| r + (i as i64) * 1000).collect();
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(narrow_schema.clone()),
            vec![Arc::new(arrow_array::Int64Array::from(ids))],
        )
        .unwrap();
        narrow_bytes += batch.get_array_memory_size();
    }

    let mut wide_bytes = 0usize;
    for _ in 0..BATCHES {
        let batch = wide_batch(ROWS, WIDE_PAYLOAD);
        wide_bytes += batch.get_array_memory_size();
    }

    let ratio = wide_bytes as f64 / narrow_bytes as f64;
    assert!(
        ratio >= 4.0,
        "wide/narrow queue-resident ratio {ratio:.2} < 4.0 \
         (wide={wide_bytes}, narrow={narrow_bytes}, cap={BATCHES})"
    );

    let path = record_baseline_case(BaselineCase {
        name: "wide_batch_queue_vs_narrow".to_string(),
        wall_time_ms: 0,
        bytes_input: 0,
        bytes_decoded_or_buffered: wide_bytes as u64,
        peak_rss_bytes: process_rss_bytes(),
        peak_tracked_bytes: wide_bytes as u64,
        failure_reason: None,
        notes: format!(
            "narrow_bytes={narrow_bytes}, wide_bytes={wide_bytes}, ratio={ratio:.2}, \
             item_cap={BATCHES}"
        ),
    })
    .expect("write baseline");
    eprintln!("wrote baseline case to {}", path.display());
}

/// Phase 0 gate: filling shuffle channels with wide batches can hold ≥ 10x a
/// configured shuffle memory budget while still only using the item-count cap.
#[tokio::test]
async fn phase0_reproducer_shuffle_exceeds_byte_budget() {
    let metrics = worker_metrics();
    let schema = wide_batch(1, 16).schema();
    let receiver = ShuffleReceiver::new_with_metrics(
        1,
        schema,
        DEFAULT_CHANNEL_CAPACITY,
        Some(metrics.clone()),
    );

    // Each batch ~1 MiB of Utf8 payload so 64 buffered batches ≈ 64 MiB,
    // well above a 16 MiB shuffle budget.
    const ROWS: usize = 256;
    const PAYLOAD: usize = 4_096; // 256 * 4 KiB = 1 MiB payload alone
    let mut sent = 0usize;
    let mut buffered_bytes = 0usize;

    for _ in 0..DEFAULT_CHANNEL_CAPACITY {
        let batch = wide_batch(ROWS, PAYLOAD);
        let sz = batch.get_array_memory_size();
        receiver
            .send_batch(0, batch)
            .await
            .expect("channel should accept up to capacity");
        sent += 1;
        buffered_bytes += sz;
    }

    let resident = receiver.resident_bytes();
    assert_eq!(sent, DEFAULT_CHANNEL_CAPACITY);
    assert_eq!(resident, buffered_bytes);
    assert!(
        resident >= 10 * SHUFFLE_MEMORY_BUDGET_BYTES,
        "expected shuffle resident bytes ({resident}) ≥ 10x budget ({}), \
         got buffered={buffered_bytes}",
        SHUFFLE_MEMORY_BUDGET_BYTES
    );

    let path = record_baseline_case(BaselineCase {
        name: "shuffle_exceeds_byte_budget".to_string(),
        wall_time_ms: 0,
        bytes_input: buffered_bytes as u64,
        bytes_decoded_or_buffered: resident as u64,
        peak_rss_bytes: process_rss_bytes(),
        peak_tracked_bytes: metrics.shuffle_resident_bytes.get() as u64,
        failure_reason: Some(format!(
            "item-bounded shuffle held {resident} bytes at capacity {DEFAULT_CHANNEL_CAPACITY} \
             against budget {}",
            SHUFFLE_MEMORY_BUDGET_BYTES
        )),
        notes: format!(
            "capacity={DEFAULT_CHANNEL_CAPACITY}, batch≈1MiB, budget={}",
            SHUFFLE_MEMORY_BUDGET_BYTES
        ),
    })
    .expect("write baseline");
    eprintln!("wrote baseline case to {}", path.display());
}

/// Future-green (Phase 4): shuffle of ≥10x budget completes with resident
/// bytes within `shuffle_memory_budget`.
#[tokio::test]
#[ignore = "phase-0 red gate: turns green in Phase 4 (spillable shuffle)"]
async fn shuffle_ten_x_budget_completes_with_spill() {
    panic!(
        "Phase 4 must implement SpillablePartitionBuffer and turn this test \
         into a real ≥10x-budget exchange that stays within shuffle_memory_budget"
    );
}
