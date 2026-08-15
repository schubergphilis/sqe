//! Phase 0/1 memory gates: zero-pruning scan, wide-batch queue bounds, shuffle.
//!
//! Plan: `docs/superpowers/plans/2026-07-25-bounded-memory-spill-execution.md`
//!
//! Phase 1 turns the zero-pruning scan green: ownership-based `ByteBudget`
//! admits decoded batches under a 64 MiB pool while streaming ≥20x that volume.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use sqe_spill::ByteBudget;
use sqe_worker::executor::{execute_scan_streaming_with_store, SCAN_CHANNEL_ITEM_CAPACITY};
use sqe_worker::shuffle::{ShuffleReceiver, DEFAULT_CHANNEL_CAPACITY};

use common::*;

fn session_and_scan_budget(memory_limit: usize) -> (SessionContext, ByteBudget) {
    let pool = Arc::new(FairSpillPool::new(memory_limit));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .expect("runtime"),
    );
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    // Scan budget = full worker limit so a pure-scan workload can use it all;
    // ownership accounting keeps residency bounded by live permits only.
    let scan_budget = ByteBudget::new("scan", memory_limit, Some(pool));
    (ctx, scan_budget)
}

/// Phase 1 gate: a no-filter scan of ≥20x worker memory completes; peak
/// tracked scan residency stays within the budget (+ one accounting unit).
#[tokio::test]
async fn zero_pruning_scan_completes_under_byte_budget() {
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

    let (ctx, scan_budget) = session_and_scan_budget(WORKER_MEMORY_LIMIT_BYTES);
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
    let (schema, mut stream) = execute_scan_streaming_with_store(
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
    .expect("Phase 1: scan setup must succeed under byte budgets");
    assert!(!schema.fields().is_empty());

    let mut rows = 0usize;
    let mut bytes_decoded = 0u64;
    let unit = scan_budget.unit_bytes();

    while let Some(item) = stream.next().await {
        let accounted = item.expect("Phase 1: scan must not ResourcesExhaust");
        rows += accounted.get().num_rows();
        bytes_decoded += accounted.logical_bytes() as u64;
        let used = scan_budget.used_bytes();
        let pool_used = pool.reserved();
        peak_tracked.observe(used.max(pool_used));
        // Live ownership must stay within budget + one unit (rounding headroom).
        assert!(
            used <= WORKER_MEMORY_LIMIT_BYTES + unit,
            "scan budget used {used} exceeded limit {} + unit {unit}",
            WORKER_MEMORY_LIMIT_BYTES
        );
        // Drop accounted at end of loop iteration → permit released.
    }

    stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    assert!(rows > 0, "scan must return rows");
    assert!(
        bytes_decoded as usize >= ZERO_PRUNING_TARGET_DECODED_BYTES / 2,
        "expected substantial decoded volume, got {bytes_decoded}"
    );
    // After full drain, permits return.
    assert_eq!(
        scan_budget.used_bytes(),
        0,
        "all scan permits must release after drain"
    );
    assert_eq!(pool.reserved(), 0, "pool must return to zero after drain");

    let path = record_baseline_case(BaselineCase {
        name: "zero_pruning_scan_completes_under_byte_budget".to_string(),
        wall_time_ms: sw.elapsed_ms(),
        bytes_input: fixture.file_size_bytes,
        bytes_decoded_or_buffered: bytes_decoded,
        peak_rss_bytes: peak_rss.get() as u64,
        peak_tracked_bytes: peak_tracked.get() as u64,
        failure_reason: None,
        notes: format!(
            "phase1 green: rows={rows}, decoded_estimate={}, peak_tracked={}, limit={}",
            fixture.decoded_bytes_estimate,
            peak_tracked.get(),
            WORKER_MEMORY_LIMIT_BYTES
        ),
    })
    .expect("write baseline");
    eprintln!("phase1 zero_pruning green; baseline={}", path.display());
}

/// Phase 0 gate retained: at the same 16-batch item bound, wide batches hold
/// ≥ 4x the resident bytes of narrow batches.
#[tokio::test]
async fn phase0_reproducer_wide_batch_queue_exceeds_narrow() {
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

/// Phase 0 gate: item-bounded shuffle can hold ≥10x a configured budget.
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

    const ROWS: usize = 256;
    const PAYLOAD: usize = 4_096;
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
        "expected shuffle resident bytes ({resident}) ≥ 10x budget ({})",
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

/// Phase 4 gate: ≥10x shuffle budget completes with spill and bounded residency.
///
/// Covered end-to-end at the buffer + DoExchange intake logic by
/// `spill_buffer::tests::do_exchange_style_ten_x_intake_stays_bounded` and
/// `ten_x_budget_completes_via_spill`. Flight-level round-trip stays in
/// worker unit/integration suites that spin a real Flight server.
#[tokio::test]
async fn shuffle_ten_x_budget_completes_with_spill() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::FairSpillPool;
    use futures::StreamExt;
    use sqe_spill::{LocalSegmentStore, SpillManager, SpillScope};
    use sqe_worker::spill_buffer::SpillablePartitionBuffer;
    use std::sync::Arc;

    let budget_bytes = SHUFFLE_MEMORY_BUDGET_BYTES; // 4 MiB
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalSegmentStore::open(tmp.path(), 1 << 30, 0, 4, 4).unwrap());
    let manager = Arc::new(SpillManager::new(store, std::time::Duration::from_secs(0)));
    let pool = Arc::new(FairSpillPool::new(budget_bytes.max(1024 * 1024)));
    let budget = ByteBudget::new("shuffle-10x", budget_bytes, Some(pool));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let scope = SpillScope::new("q-10x-gate", "s0", "do_exchange", 0, 0);
    let mut buf = SpillablePartitionBuffer::new(manager, scope, schema.clone(), budget, None);

    // ~40 MiB of i64 batches (10x the 4 MiB budget).
    let rows_per_batch = 64 * 1024; // 512 KiB of i64 + overhead
    let n_batches = 80;
    let mut appended = 0usize;
    let mut peak = 0usize;
    for i in 0..n_batches {
        let vals: Vec<i64> = (0..rows_per_batch as i64)
            .map(|r| i as i64 * 1_000_000 + r)
            .collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap();
        appended += batch.get_array_memory_size();
        buf.append(batch).await.unwrap();
        peak = peak.max(buf.resident_bytes());
        assert!(
            buf.resident_bytes() <= budget_bytes + 512 * 1024,
            "resident {} exceeded 4 MiB budget headroom",
            buf.resident_bytes()
        );
    }
    assert!(
        appended >= 10 * budget_bytes,
        "appended {appended} < 10x budget {budget_bytes}"
    );

    let manifest = buf.finish().await.unwrap();
    let mut drain = buf.into_drain_stream().await.unwrap();
    let mut rows = 0usize;
    while let Some(item) = drain.next().await {
        rows += item.unwrap().num_rows();
    }
    assert_eq!(rows as u64, manifest.rows);
    assert_eq!(rows, n_batches * rows_per_batch);
    assert!(
        peak <= budget_bytes + 512 * 1024,
        "peak resident {peak} must stay near shuffle budget {budget_bytes}"
    );
}
