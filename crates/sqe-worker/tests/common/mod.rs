//! Shared helpers for Phase 0 bounded-memory red gates.
//!
//! Laptop-runnable: local temp-dir Parquet + LocalFileSystem, no Polaris, no
//! S3, no N-times-RAM host. The "larger than memory" illusion comes from a
//! 64 MiB configured worker limit, not from exhausting real host RAM.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sqe_metrics::WorkerMetricsRegistry;
use sqe_planner::ScanTask;

/// Hard worker memory limit used by Phase 0 scan reproducers.
pub const WORKER_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Target decoded volume for the zero-pruning scan fixture: 20x the limit.
pub const ZERO_PRUNING_TARGET_DECODED_BYTES: usize = 20 * WORKER_MEMORY_LIMIT_BYTES;

/// Default shuffle memory budget used by the shuffle red gate.
///
/// Chosen so a full item-bounded channel of ~1 MiB batches
/// (`DEFAULT_CHANNEL_CAPACITY` = 64) is ≥ 10x this budget without requiring
/// multi-GB fixtures.
pub const SHUFFLE_MEMORY_BUDGET_BYTES: usize = 4 * 1024 * 1024; // 4 MiB

/// Baseline artifact path relative to the workspace (sqlengine) root.
pub const BASELINE_RELATIVE_PATH: &str =
    "benchmarks/results/bounded-memory-phase0-baseline.json";

/// One Phase 0 reproducer measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineCase {
    pub name: String,
    pub wall_time_ms: u64,
    pub bytes_input: u64,
    pub bytes_decoded_or_buffered: u64,
    pub peak_rss_bytes: u64,
    pub peak_tracked_bytes: u64,
    pub failure_reason: Option<String>,
    pub notes: String,
}

/// Full Phase 0 baseline document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineDocument {
    pub plan: String,
    pub phase: String,
    pub generated_at_rfc3339: String,
    pub worker_memory_limit_bytes: u64,
    pub cases: Vec<BaselineCase>,
}

/// Build a SessionContext backed by a FairSpillPool of `memory_limit` bytes.
pub fn session_with_memory_limit(memory_limit: usize) -> SessionContext {
    let pool = Arc::new(FairSpillPool::new(memory_limit));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build()
            .expect("runtime env"),
    );
    SessionContext::new_with_config_rt(SessionConfig::new(), runtime)
}

/// Fresh worker metrics registry for a single reproducer.
pub fn worker_metrics() -> Arc<WorkerMetricsRegistry> {
    Arc::new(WorkerMetricsRegistry::new().expect("worker metrics"))
}

/// ScanTask pointing at `s3://test-bucket/{object_key}` for LocalFileSystem
/// fixtures that store the same relative key under a temp root.
pub fn local_scan_task(object_keys: Vec<String>, file_sizes: Vec<u64>) -> ScanTask {
    let data_file_paths = object_keys
        .into_iter()
        .map(|k| format!("s3://test-bucket/{k}"))
        .collect();
    ScanTask {
            version: 1,
            morsel_id: None,
            row_group_start: None,
            row_group_end: None,
            start_byte: None,
            end_byte: None,
        fragment_id: "phase0-frag".to_string(),
        data_file_paths,
        file_sizes_bytes: file_sizes,
        projected_columns: vec![],
        projected_field_ids: vec![],
        s3_endpoint: String::new(),
        s3_region: "us-east-1".to_string(),
        s3_access_key: String::new(),
        s3_secret_key: String::new(),
        s3_session_token: String::new(),
        s3_path_style: true,
        s3_allow_http: true,
        predicate_proto: None,
        limit: None,
    }
}

/// LocalFileSystem store rooted at `root` (no path prefix stripping).
pub fn local_store(root: &Path) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(root).expect("local store"))
}

/// Result of generating a large Parquet fixture.
pub struct ParquetFixture {
    pub root: PathBuf,
    pub object_key: String,
    pub file_path: PathBuf,
    pub file_size_bytes: u64,
    /// Sum of per-batch `get_array_memory_size()` written (decoded estimate).
    pub decoded_bytes_estimate: u64,
    pub num_rows: u64,
    pub num_batches: u64,
}

/// Generate a Parquet file whose decoded Arrow volume is at least
/// `target_decoded_bytes`, writing incrementally so generation itself stays
/// well under the 64 MiB worker limit.
///
/// Layout: `root/{object_key}` with a single Int64 id column plus a fixed-width
/// Utf8 payload column. Compression keeps the on-disk footprint far smaller
/// than the decoded size so laptops do not need 20x-RAM free disk.
pub fn generate_large_parquet(
    root: &Path,
    object_key: &str,
    target_decoded_bytes: usize,
) -> anyhow::Result<ParquetFixture> {
    let file_path = root.join(object_key);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        // 256-byte payload per row: wide enough that batch-count bounds fail
        // to control bytes, compressible enough that disk stays small.
        Field::new("payload", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_row_count(Some(64 * 1024))
        .build();

    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    // ~4k rows * (~8 + ~256 + overhead) ≈ ~1.2 MiB decoded per batch.
    const ROWS_PER_BATCH: usize = 4_096;
    let payload = "x".repeat(256);

    let mut decoded_bytes: usize = 0;
    let mut num_rows: u64 = 0;
    let mut num_batches: u64 = 0;
    let mut next_id: i64 = 0;

    while decoded_bytes < target_decoded_bytes {
        let ids: Vec<i64> = (next_id..next_id + ROWS_PER_BATCH as i64).collect();
        next_id += ROWS_PER_BATCH as i64;
        let payloads: Vec<&str> = std::iter::repeat(payload.as_str())
            .take(ROWS_PER_BATCH)
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?;
        decoded_bytes += batch.get_array_memory_size();
        num_rows += batch.num_rows() as u64;
        num_batches += 1;
        writer.write(&batch)?;
    }

    writer.close()?;
    let file_size_bytes = fs::metadata(&file_path)?.len();

    Ok(ParquetFixture {
        root: root.to_path_buf(),
        object_key: object_key.to_string(),
        file_path,
        file_size_bytes,
        decoded_bytes_estimate: decoded_bytes as u64,
        num_rows,
        num_batches,
    })
}

/// Generate a narrow (Int64-only) Parquet file for the wide-vs-narrow comparison.
pub fn generate_narrow_parquet(
    root: &Path,
    object_key: &str,
    num_batches: usize,
    rows_per_batch: usize,
) -> anyhow::Result<(ParquetFixture, Vec<usize>)> {
    let file_path = root.join(object_key);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut batch_sizes = Vec::with_capacity(num_batches);
    let mut decoded_bytes: usize = 0;
    let mut num_rows: u64 = 0;
    let mut next_id: i64 = 0;

    for _ in 0..num_batches {
        let ids: Vec<i64> = (next_id..next_id + rows_per_batch as i64).collect();
        next_id += rows_per_batch as i64;
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])?;
        batch_sizes.push(batch.get_array_memory_size());
        decoded_bytes += batch.get_array_memory_size();
        num_rows += batch.num_rows() as u64;
        writer.write(&batch)?;
    }
    writer.close()?;
    let file_size_bytes = fs::metadata(&file_path)?.len();
    Ok((
        ParquetFixture {
            root: root.to_path_buf(),
            object_key: object_key.to_string(),
            file_path,
            file_size_bytes,
            decoded_bytes_estimate: decoded_bytes as u64,
            num_rows,
            num_batches: num_batches as u64,
        },
        batch_sizes,
    ))
}

/// Generate a wide Utf8 Parquet file (same row counts as the narrow companion).
pub fn generate_wide_parquet(
    root: &Path,
    object_key: &str,
    num_batches: usize,
    rows_per_batch: usize,
    payload_len: usize,
) -> anyhow::Result<(ParquetFixture, Vec<usize>)> {
    let file_path = root.join(object_key);
    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let file = fs::File::create(&file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let payload = "W".repeat(payload_len);
    let mut batch_sizes = Vec::with_capacity(num_batches);
    let mut decoded_bytes: usize = 0;
    let mut num_rows: u64 = 0;
    let mut next_id: i64 = 0;

    for _ in 0..num_batches {
        let ids: Vec<i64> = (next_id..next_id + rows_per_batch as i64).collect();
        next_id += rows_per_batch as i64;
        let payloads: Vec<&str> = std::iter::repeat(payload.as_str())
            .take(rows_per_batch)
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(payloads)),
            ],
        )?;
        batch_sizes.push(batch.get_array_memory_size());
        decoded_bytes += batch.get_array_memory_size();
        num_rows += batch.num_rows() as u64;
        writer.write(&batch)?;
    }
    writer.close()?;
    let file_size_bytes = fs::metadata(&file_path)?.len();
    Ok((
        ParquetFixture {
            root: root.to_path_buf(),
            object_key: object_key.to_string(),
            file_path,
            file_size_bytes,
            decoded_bytes_estimate: decoded_bytes as u64,
            num_rows,
            num_batches: num_batches as u64,
        },
        batch_sizes,
    ))
}

/// Build a single in-memory RecordBatch with `rows` rows and a Utf8 payload of
/// `payload_len` bytes each. Used by the shuffle red gate.
pub fn wide_batch(rows: usize, payload_len: usize) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let payload = "S".repeat(payload_len);
    let ids: Vec<i64> = (0..rows as i64).collect();
    let payloads: Vec<&str> = std::iter::repeat(payload.as_str()).take(rows).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .expect("wide batch")
}

/// Best-effort process RSS in bytes. Linux uses `/proc/self/status`; macOS
/// shells out to `ps`. Returns 0 when unavailable so baselines stay complete.
pub fn process_rss_bytes() -> u64 {
    if let Some(v) = process_rss_bytes_inner() {
        v
    } else {
        0
    }
}

fn process_rss_bytes_inner() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let status = fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
        let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        return Some(kb * 1024);
    }
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id().to_string();
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .output()
            .ok()?;
        let s = String::from_utf8(output.stdout).ok()?;
        let kb: u64 = s.trim().parse().ok()?;
        return Some(kb * 1024);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// Tracks the high-water mark of a metric sampler while a reproducer runs.
pub struct PeakTracker {
    peak: AtomicUsize,
}

impl PeakTracker {
    pub fn new() -> Self {
        Self {
            peak: AtomicUsize::new(0),
        }
    }

    pub fn observe(&self, value: usize) {
        self.peak.fetch_max(value, Ordering::Relaxed);
    }

    pub fn get(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

impl Default for PeakTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Sample pool reserved + metric gauges into a peak tracker for `duration`.
pub async fn sample_peaks(
    metrics: Arc<WorkerMetricsRegistry>,
    pool_reserved: impl Fn() -> usize + Send + 'static,
    peak_tracked: Arc<PeakTracker>,
    peak_rss: Arc<PeakTracker>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let tracked = pool_reserved()
            .max(metrics.scan_decode_resident_bytes.get() as usize)
            .max(metrics.scan_queue_resident_bytes.get() as usize)
            .max(metrics.shuffle_resident_bytes.get() as usize)
            .max(metrics.flight_encode_resident_bytes.get() as usize);
        peak_tracked.observe(tracked);
        peak_rss.observe(process_rss_bytes() as usize);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Locate the sqlengine workspace root (directory containing `Cargo.toml`
/// workspace and `benchmarks/`).
pub fn workspace_root() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // crates/sqe-worker -> crates -> workspace root
    dir.pop();
    dir.pop();
    dir
}

/// Merge-or-create the Phase 0 baseline JSON with one case result.
pub fn record_baseline_case(case: BaselineCase) -> anyhow::Result<PathBuf> {
    let path = workspace_root().join(BASELINE_RELATIVE_PATH);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut doc = if path.exists() {
        let raw = fs::read_to_string(&path)?;
        serde_json::from_str::<BaselineDocument>(&raw).unwrap_or_else(|_| BaselineDocument {
            plan: "2026-07-25-bounded-memory-spill-execution".to_string(),
            phase: "0".to_string(),
            generated_at_rfc3339: chrono_like_now(),
            worker_memory_limit_bytes: WORKER_MEMORY_LIMIT_BYTES as u64,
            cases: vec![],
        })
    } else {
        BaselineDocument {
            plan: "2026-07-25-bounded-memory-spill-execution".to_string(),
            phase: "0".to_string(),
            generated_at_rfc3339: chrono_like_now(),
            worker_memory_limit_bytes: WORKER_MEMORY_LIMIT_BYTES as u64,
            cases: vec![],
        }
    };

    doc.generated_at_rfc3339 = chrono_like_now();
    doc.cases.retain(|c| c.name != case.name);
    doc.cases.push(case);
    doc.cases.sort_by(|a, b| a.name.cmp(&b.name));

    let pretty = serde_json::to_string_pretty(&doc)?;
    fs::write(&path, pretty)?;
    Ok(path)
}

fn chrono_like_now() -> String {
    // Avoid pulling chrono into dev-deps: RFC3339-ish UTC via system time.
    use std::time::SystemTime;
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{secs}Z")
}

/// Wall-clock helper.
pub struct Stopwatch {
    start: Instant,
}

impl Stopwatch {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}
