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
use sqe_worker::executor::{execute_scan_streaming_with_store, ScanBudget};

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
    let scan_budget = ScanBudget::new(WORKER_MEMORY_LIMIT_BYTES, Some(pool.clone()));
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

/// `ObjectStore` wrapper that counts data GETs (every `get_opts` that is not
/// a HEAD) and delegates everything else. `get_range` / `get_ranges` lower
/// to `get_opts` through the trait's default methods, so a parquet reader's
/// row-group reads land here too.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gets: Arc<std::sync::atomic::AtomicUsize>,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if !options.head {
            self.gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// A parquet file whose bytes on disk are close to its decoded size: every
/// payload is distinct, so neither dictionary encoding nor a codec can fold
/// it (the harness's `generate_wide_parquet` repeats one string and lands at
/// 80 KiB per 2 MiB decoded). Returns the file size in bytes.
fn write_incompressible_parquet(
    root: &std::path::Path,
    object_key: &str,
    num_batches: usize,
    rows_per_batch: usize,
    payload_len: usize,
) -> anyhow::Result<u64> {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let file_path = root.join(object_key);
    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file_path)?,
        Arc::clone(&schema),
        Some(props),
    )?;
    let mut next_id: i64 = 0;
    for _ in 0..num_batches {
        let ids: Vec<i64> = (next_id..next_id + rows_per_batch as i64).collect();
        let payloads: Vec<String> = ids
            .iter()
            .map(|id| format!("{id:0>width$x}", width = payload_len))
            .collect();
        next_id += rows_per_batch as i64;
        writer.write(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?)?;
    }
    writer.close()?;
    Ok(std::fs::metadata(&file_path)?.len())
}

/// #407: once the scan budget is full, a paused client must stop the worker
/// from fetching more of the object store, not just from decoding it.
///
/// Eight uncompressed files of about 2 MiB each under a 4 MiB scan budget.
/// The fetch lane is a quarter of that (1 MiB) and one file's charge is
/// capped at half the lane, so at most two files are open at a time; the
/// decode lane (3 MiB) holds about five of the 550 KiB batches. The client
/// takes two batches and pauses. With the decode lane full the producer
/// blocks in `acquire`, the open files cannot drain, their fetch permits are
/// never released, and no third file can be opened: the GET counter must
/// not move for the rest of the pause. Afterwards the scan drains to the end
/// and both lanes release to zero.
#[tokio::test]
async fn slow_consumer_caps_bytes_when_client_pauses() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut keys = Vec::new();
    let mut sizes = Vec::new();
    for i in 0..8 {
        let key = format!("data/paused_{i}.parquet");
        let size = write_incompressible_parquet(tmp.path(), &key, 4, 2048, 256)
            .expect("generate parquet");
        keys.push(key);
        sizes.push(size);
    }
    assert!(
        sizes.iter().all(|s| *s > 1024 * 1024),
        "each file must exceed the per-file fetch cap so the lane admits few files: {sizes:?}"
    );

    const SCAN_BUDGET: usize = 4 * 1024 * 1024;
    let pool = Arc::new(FairSpillPool::new(WORKER_MEMORY_LIMIT_BYTES));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .expect("runtime"),
    );
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let scan_budget = ScanBudget::new(SCAN_BUDGET, Some(pool.clone()));
    let metrics = worker_metrics();
    let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(CountingStore {
        inner: local_store(tmp.path()),
        gets: gets.clone(),
    });

    let (_schema, mut stream) = execute_scan_streaming_with_store(
        local_scan_task(keys, sizes),
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
    for _ in 0..2 {
        let accounted = stream
            .next()
            .await
            .expect("two batches before the pause")
            .expect("batch");
        rows += accounted.get().num_rows();
    }

    // Let whatever was already in flight land, then freeze the counter.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let gets_at_pause = gets.load(Ordering::SeqCst);
    assert!(gets_at_pause > 0, "the first two batches needed at least one GET");
    let used_at_pause = scan_budget.used_bytes();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        gets.load(Ordering::SeqCst),
        gets_at_pause,
        "the worker fetched more of the store while the client was paused with a full budget \
         (scan budget used at pause: {used_at_pause} of {SCAN_BUDGET})"
    );
    assert!(
        scan_budget.used_bytes() <= SCAN_BUDGET + scan_budget.unit_bytes(),
        "live scan ownership {} exceeded the budget during the pause",
        scan_budget.used_bytes()
    );

    while let Some(item) = stream.next().await {
        rows += item.expect("drain after the pause").get().num_rows();
    }
    assert_eq!(rows, 8 * 4 * 2048, "every row of every file arrives after the pause");
    // The freeze was backpressure, not "nothing left to fetch": the files the
    // fetch lane refused during the pause were fetched once the client drained.
    assert!(
        gets.load(Ordering::SeqCst) > gets_at_pause,
        "no GETs after the pause means the lane had already admitted every file \
         and the frozen counter proved nothing (gets at pause: {gets_at_pause})"
    );
    assert_eq!(scan_budget.used_bytes(), 0);
    assert_eq!(pool.reserved(), 0);
}
