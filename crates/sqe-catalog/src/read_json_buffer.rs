//! Buffer path for `read_json`: reads a whole JSON document into memory,
//! decompresses (gzip/zip), reshapes a top-level array into NDJSON, decodes
//! with `arrow::json`, and serves the rows as a `MemTable`. Used for
//! `format=array` and `compression=zip`, which DataFusion's streaming JSON
//! listing path cannot handle.
//!
//! Every representation the path holds (downloaded bytes, decompressed bytes,
//! reshaped NDJSON, decoded Arrow batches) is capped at `max_bytes` and charged
//! to the session memory pool through a [`BufferBudget`]. The cap is enforced
//! while bytes arrive, not against a HEAD size that a compressed or replaced
//! object can defeat (security/scale review 906).

use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

use sqe_core::config::StorageConfig;

use crate::file_tvf_common::{
    register_azure_store_if_needed, register_gcs_store_if_needed, register_http_store_if_needed,
    register_s3_store_if_needed, FileTvfArgs,
};
use crate::read_json::{JsonCompression, JsonFraming};

const FN_NAME: &str = "read_json";

/// Chunk size for capped decompression reads. Small enough that the pool sees
/// growth before a hostile stream gets far past the cap.
const READ_CHUNK: usize = 64 * 1024;

// Buffer processing is CPU-heavy. Keep its concurrency independent of Tokio
// worker count and hold permits until blocking work actually finishes.
static BUFFER_PROCESSING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Schema inference and Arrow decoding use BufRead. Check cancellation on
/// each read so their full-document inference pass can stop between lines.
struct CancellableReader<R> {
    inner: R,
    cancelled: Arc<AtomicBool>,
}

impl<R> CancellableReader<R> {
    fn check(&self) -> std::io::Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err(std::io::Error::other(
                "read_json: buffer processing cancelled",
            ))
        } else {
            Ok(())
        }
    }
}

impl<R: std::io::Read> std::io::Read for CancellableReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.check()?;
        self.inner.read(out)
    }
}

impl<R: std::io::BufRead> std::io::BufRead for CancellableReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.check()?;
        self.inner.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}

/// Byte cap plus pool reservation for the buffered JSON path.
///
/// `max_bytes` bounds every single representation (download, decompressed
/// output, reshaped NDJSON, cumulative ZIP output). The reservation charges the
/// live bytes to the shared pool so a denied grow fails this one query with a
/// `ResourcesExhausted` error instead of taking the node down. The reservation
/// stays alive inside the returned table so the decoded batches remain visible
/// to the pool for as long as the table is registered.
pub(crate) struct BufferBudget {
    max_bytes: usize,
    reservation: MemoryReservation,
    cancelled: Arc<AtomicBool>,
}

impl BufferBudget {
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>, max_bytes: usize) -> Self {
        Self {
            max_bytes,
            reservation: MemoryConsumer::new(format!("{FN_NAME}-buffer")).register(pool),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Reject a representation that would exceed the byte cap.
    fn check_cap(&self, bytes: usize, what: &str) -> DFResult<()> {
        self.check_cancelled()?;
        if bytes > self.max_bytes {
            return Err(DataFusionError::Plan(format!(
                "{FN_NAME}: {what} exceeds max_buffer_bytes {} (raise max_buffer_bytes to read it)",
                self.max_bytes
            )));
        }
        Ok(())
    }

    /// Charge `bytes` more to the pool.
    fn charge(&mut self, bytes: usize, what: &str) -> DFResult<()> {
        self.check_cancelled()?;
        self.reservation.try_grow(bytes).map_err(|e| {
            DataFusionError::ResourcesExhausted(format!(
                "{FN_NAME}: {what} needs {bytes} more bytes than the memory pool allows: {e}"
            ))
        })
    }

    /// Return `bytes` to the pool after a representation is dropped.
    fn release(&mut self, bytes: usize) {
        self.reservation.shrink(bytes.min(self.reservation.size()));
    }

    /// Bytes currently reserved against the pool.
    pub(crate) fn reserved(&self) -> usize {
        self.reservation.size()
    }

    fn check_cancelled(&self) -> DFResult<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            Err(DataFusionError::Execution(format!(
                "{FN_NAME}: buffer processing cancelled"
            )))
        } else {
            Ok(())
        }
    }
}

/// A `MemTable` that keeps its decoded batches charged to the pool.
struct ReservedMemTable {
    inner: MemTable,
    _budget: BufferBudget,
}

impl std::fmt::Debug for ReservedMemTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReservedMemTable")
            .field("reserved_bytes", &self._budget.reserved())
            .finish()
    }
}

#[async_trait]
impl TableProvider for ReservedMemTable {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    // Written in `async_trait`'s desugared form on purpose (compile time,
    // see docs/internal/plans/2026-09-07-compile-time-async-hubs.md): an
    // `async fn` here would own a coroutine that captures `filters: &[Expr]`,
    // and proving that coroutine `Send` walks DataFusion's whole `Expr` /
    // `LogicalPlan` type graph once more under this method's own parameter
    // environment (about 4 s of the crate's build). Handing the inner
    // table's already-boxed future straight through owns no coroutine at
    // all, and the behaviour is identical.
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> futures::future::BoxFuture<'async_trait, DFResult<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.scan(state, projection, filters, limit)
    }
}

/// Fetch the whole object, decompress, reshape to NDJSON, decode to a MemTable.
/// The caller MUST have already run `enforce_tvf_path_policy` on `args` — this
/// function fetches bytes directly via `object_store`, bypassing ListingTable,
/// so the gate is the only thing preventing arbitrary local-path reads
/// (`/etc/shadow`) or SSRF against link-local/metadata endpoints.
pub(crate) async fn build_buffer_table(
    args: &FileTvfArgs,
    framing: JsonFraming,
    codec: JsonCompression,
    max_bytes: usize,
    storage: &StorageConfig,
    runtime_env: Option<&RuntimeEnv>,
) -> DFResult<Arc<dyn TableProvider>> {
    let mut args = args.clone();
    crate::file_tvf_common::rewrite_hf_path_in_place(FN_NAME, &mut args)?;

    let listing_url = ListingTableUrl::parse(&args.path)?;

    let tmp_ctx = crate::scoped_table::inference_context(runtime_env)?;
    register_s3_store_if_needed(FN_NAME, &tmp_ctx, &args, storage, None)?;
    register_azure_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_gcs_store_if_needed(FN_NAME, &tmp_ctx, &args, storage)?;
    register_http_store_if_needed(FN_NAME, &tmp_ctx, &args.path)?;

    let store: Arc<dyn ObjectStore> = tmp_ctx.state().runtime_env().object_store(&listing_url)?;
    let path = listing_url.prefix().clone();

    // The inference runtime shares the session's memory pool (or a fresh one
    // when planning outside a session), so every byte below is governed.
    let mut budget = BufferBudget::new(&tmp_ctx.runtime_env().memory_pool, max_bytes);

    // Cheap early rejection. Not a security boundary: the object can change
    // between HEAD and GET, and compressed size says nothing about expansion,
    // so the streaming cap below re-checks every representation.
    let meta = store.head(&path).await.map_err(|e| {
        DataFusionError::Plan(format!("{FN_NAME}: cannot stat '{}': {e}", args.path))
    })?;
    if meta.size as usize > max_bytes {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: '{}' is {} bytes, exceeds max_buffer_bytes {} (raise max_buffer_bytes to read it)",
            args.path, meta.size, max_bytes
        )));
    }

    let raw = fetch_capped(store.as_ref(), &path, &args.path, &mut budget).await?;

    process_buffer(raw, framing, codec, budget).await
}

async fn process_buffer(
    raw: Vec<u8>,
    framing: JsonFraming,
    codec: JsonCompression,
    budget: BufferBudget,
) -> DFResult<Arc<dyn TableProvider>> {
    let _cancel = CancelOnDrop(Arc::clone(&budget.cancelled));
    let permit = BUFFER_PROCESSING.acquire().await.map_err(|_| {
        DataFusionError::Execution(format!("{FN_NAME}: processing semaphore closed"))
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        budget.check_cancelled()?;
        process_buffer_sync(raw, framing, codec, budget)
    })
    .await
    .map_err(|e| DataFusionError::Execution(format!("{FN_NAME}: buffer task failed: {e}")))?
}

fn process_buffer_sync(
    raw: Vec<u8>,
    framing: JsonFraming,
    codec: JsonCompression,
    mut budget: BufferBudget,
) -> DFResult<Arc<dyn TableProvider>> {
    let ndjson = match codec {
        JsonCompression::Zip => decompress_zip_to_ndjson(raw, framing, &mut budget)?,
        other => {
            let inner = decompress(raw, other, &mut budget)?;
            let resolved = match framing {
                JsonFraming::Auto => detect_framing(&inner),
                f => f,
            };
            match resolved {
                JsonFraming::Array => {
                    let out = reshape_array_to_ndjson(&inner, &mut budget)?;
                    budget.release(inner.len());
                    out
                }
                _ => inner,
            }
        }
    };

    decode_ndjson_to_memtable(&ndjson, budget)
}

/// Download the object while enforcing the cap on bytes actually received.
async fn fetch_capped(
    store: &dyn ObjectStore,
    path: &object_store::path::Path,
    display: &str,
    budget: &mut BufferBudget,
) -> DFResult<Vec<u8>> {
    let mut stream = store
        .get(path)
        .await
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: fetch '{display}' failed: {e}")))?
        .into_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| {
            DataFusionError::Plan(format!("{FN_NAME}: read '{display}' failed: {e}"))
        })?;
        budget.check_cap(out.len() + chunk.len(), "downloaded object")?;
        budget.charge(chunk.len(), "downloaded object")?;
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// Drain `reader` in chunks, rejecting once `base + produced` passes the cap
/// and charging every produced byte to the pool as it is produced.
fn read_capped<R: Read>(
    mut reader: R,
    base: usize,
    budget: &mut BufferBudget,
    what: &str,
) -> DFResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut chunk = vec![0u8; READ_CHUNK];
    loop {
        let n = reader
            .read(&mut chunk)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: {what} failed: {e}")))?;
        if n == 0 {
            break;
        }
        budget.check_cap(base + out.len() + n, what)?;
        budget.charge(n, what)?;
        out.extend_from_slice(&chunk[..n]);
    }
    Ok(out)
}

fn detect_framing(bytes: &[u8]) -> JsonFraming {
    crate::read_json::detect_framing_from_bytes(bytes)
}

/// Parse a top-level JSON value. An array becomes one line per element; a
/// bare object becomes a single line. Anything else is an error.
///
/// Elements are visited one at a time, so only the current element and the
/// growing NDJSON output are resident; the output is capped and charged as it
/// grows. Elements are re-serialized compactly because the arrow schema
/// inference reads line by line.
pub(crate) fn reshape_array_to_ndjson(
    bytes: &[u8],
    budget: &mut BufferBudget,
) -> DFResult<Vec<u8>> {
    use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
    use serde::Deserialize;

    struct Sink<'b> {
        out: Vec<u8>,
        budget: &'b mut BufferBudget,
        failure: &'b mut Option<DataFusionError>,
    }

    impl Sink<'_> {
        fn push<E: serde::de::Error>(&mut self, item: &serde_json::Value) -> Result<(), E> {
            let start = self.out.len();
            serde_json::to_writer(&mut self.out, item)
                .map_err(|e| E::custom(format!("re-serialize failed: {e}")))?;
            self.out.push(b'\n');
            let grown = self.out.len() - start;
            let charged = self
                .budget
                .check_cap(self.out.len(), "reshaped JSON")
                .and_then(|_| self.budget.charge(grown, "reshaped JSON"));
            if let Err(e) = charged {
                *self.failure = Some(e);
                return Err(E::custom("buffer limit reached"));
            }
            Ok(())
        }
    }

    impl<'de> Visitor<'de> for Sink<'_> {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a JSON object or array of objects at top level")
        }

        fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            while let Some(item) = seq.next_element::<serde_json::Value>()? {
                self.push(&item)?;
            }
            Ok(self.out)
        }

        fn visit_map<A: MapAccess<'de>>(mut self, map: A) -> Result<Vec<u8>, A::Error> {
            let item =
                serde_json::Value::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
            self.push(&item)?;
            Ok(self.out)
        }
    }

    impl<'de> DeserializeSeed<'de> for Sink<'_> {
        type Value = Vec<u8>;
        fn deserialize<D: Deserializer<'de>>(self, de: D) -> Result<Vec<u8>, D::Error> {
            de.deserialize_any(self)
        }
    }

    let mut failure = None;
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let sink = Sink {
        out: Vec::new(),
        budget,
        failure: &mut failure,
    };
    let result = sink
        .deserialize(&mut de)
        .and_then(|out| de.end().map(|_| out));
    match result {
        Ok(out) => Ok(out),
        Err(_) if failure.is_some() => Err(failure.take().expect("checked")),
        Err(e) => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: failed to parse JSON document (line {}, col {}): {e}",
            e.line(),
            e.column()
        ))),
    }
}

pub(crate) fn decompress(
    bytes: Vec<u8>,
    codec: JsonCompression,
    budget: &mut BufferBudget,
) -> DFResult<Vec<u8>> {
    match codec {
        JsonCompression::None => Ok(bytes),
        JsonCompression::Gzip => {
            let dec = flate2::read::GzDecoder::new(&bytes[..]);
            let out = read_capped(dec, 0, budget, "gzip decode")?;
            budget.release(bytes.len());
            Ok(out)
        }
        JsonCompression::Zip => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip archives are decoded via decompress_zip_to_ndjson, not decompress"
        ))),
        JsonCompression::Zstd | JsonCompression::Bz2 | JsonCompression::Xz => {
            Err(DataFusionError::Plan(format!(
                "{FN_NAME}: compression {codec:?} is not supported with format=array or zip (only gzip, zip, or none)"
            )))
        }
    }
}

/// Read every JSON-extension entry from a zip archive, reshape each per the
/// resolved framing, and concatenate into one NDJSON byte stream. Entries
/// whose name does not end in .json/.jsonl/.ndjson are ignored. Zero JSON
/// entries is an error. The cap applies to the cumulative decompressed output
/// across entries, not to any single entry.
pub(crate) fn decompress_zip_to_ndjson(
    bytes: Vec<u8>,
    framing: JsonFraming,
    budget: &mut BufferBudget,
) -> DFResult<Vec<u8>> {
    use std::io::Cursor;
    let archive_len = bytes.len();
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: not a valid zip: {e}")))?;

    let mut out = Vec::new();
    let mut json_entries = 0usize;
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry read failed: {e}")))?;
        if entry.is_dir() {
            continue;
        }
        // zip 9 returns an error for an entry name that is not valid UTF-8.
        let name = entry
            .name()
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry name: {e}")))?
            .to_ascii_lowercase();
        if !(name.ends_with(".json") || name.ends_with(".jsonl") || name.ends_with(".ndjson")) {
            continue;
        }
        let raw = read_capped(entry, out.len(), budget, "zip entry decompress")?;

        // Resolve framing per entry when Auto.
        let entry_framing = match framing {
            JsonFraming::Auto => crate::read_json::detect_framing_from_bytes(&raw),
            other => other,
        };
        let nd = match entry_framing {
            JsonFraming::Array => {
                // The reshaped copy is charged as it grows; the raw entry is
                // released once the copy exists.
                let nd = reshape_array_to_ndjson(&raw, budget)?;
                budget.release(raw.len());
                nd
            }
            _ => {
                // Ensure trailing newline so concatenation stays line-safe.
                let mut v = raw;
                if !v.ends_with(b"\n") {
                    budget.check_cap(out.len() + v.len() + 1, "zip entry decompress")?;
                    budget.charge(1, "zip entry decompress")?;
                    v.push(b'\n');
                }
                v
            }
        };
        budget.check_cap(out.len() + nd.len(), "zip archive output")?;
        out.extend_from_slice(&nd);
        json_entries += 1;
    }
    drop(archive);
    budget.release(archive_len);

    if json_entries == 0 {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip archive contains no .json/.jsonl/.ndjson entries"
        )));
    }
    Ok(out)
}

pub(crate) fn decode_ndjson_to_memtable(
    ndjson: &[u8],
    mut budget: BufferBudget,
) -> DFResult<Arc<dyn TableProvider>> {
    use arrow::json::reader::infer_json_schema;
    use arrow::json::ReaderBuilder;
    use std::io::Cursor;

    budget.check_cancelled()?;
    let cancelled = Arc::clone(&budget.cancelled);
    let reader = || CancellableReader {
        inner: Cursor::new(ndjson),
        cancelled: Arc::clone(&cancelled),
    };
    let (schema, _) = infer_json_schema(&mut reader(), None).map_err(|e| {
        DataFusionError::Plan(format!("{FN_NAME}: JSON schema inference failed: {e}"))
    })?;
    let schema = Arc::new(schema);

    let reader = ReaderBuilder::new(schema.clone())
        .build(reader())
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: JSON reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for b in reader {
        let b =
            b.map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: JSON decode failed: {e}")))?;
        budget.charge(b.get_array_memory_size(), "decoded Arrow batches")?;
        batches.push(b);
    }
    // The NDJSON bytes are dropped by the caller right after this returns;
    // the reservation now tracks only the decoded batches.
    budget.release(ndjson.len());

    let inner = MemTable::try_new(schema, vec![batches])?;
    Ok(Arc::new(ReservedMemTable {
        inner,
        _budget: budget,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    #[tokio::test(flavor = "current_thread")]
    async fn buffered_processing_leaves_runtime_responsive() {
        let raw = b"{\"a\":1}\n".repeat(100_000);
        let mut budget = unbounded();
        budget.charge(raw.len(), "test input").unwrap();
        let started = std::time::Instant::now();
        let (table, delay) = tokio::join!(
            process_buffer(
                raw,
                JsonFraming::NewlineDelimited,
                JsonCompression::None,
                budget
            ),
            async {
                tokio::task::yield_now().await;
                started.elapsed()
            },
        );
        assert!(table.is_ok());
        assert!(
            delay < std::time::Duration::from_millis(100),
            "runtime stalled for {delay:?}"
        );
    }

    #[test]
    fn cancelled_buffer_work_releases_pool_bytes() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut budget = BufferBudget::new(&pool, 1 << 20);
        let raw = b"{\"a\":1}\n".to_vec();
        budget.charge(raw.len(), "test input").unwrap();
        budget.cancelled.store(true, Ordering::Relaxed);
        let error = decode_ndjson_to_memtable(&raw, budget).unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(pool.reserved(), 0);
    }

    fn unbounded() -> BufferBudget {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        BufferBudget::new(&pool, usize::MAX)
    }

    fn capped(max_bytes: usize) -> BufferBudget {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        BufferBudget::new(&pool, max_bytes)
    }

    fn gzip(payload: &[u8]) -> Vec<u8> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(payload).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn reshape_array_yields_one_line_per_element() {
        let out = reshape_array_to_ndjson(b"[{\"a\":1},{\"a\":2}]", &mut unbounded()).unwrap();
        let s = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "{\"a\":1}");
        assert_eq!(lines[1], "{\"a\":2}");
    }

    #[test]
    fn reshape_accepts_single_object() {
        let out = reshape_array_to_ndjson(b"{\"a\":1}", &mut unbounded()).unwrap();
        assert_eq!(String::from_utf8(out).unwrap().lines().count(), 1);
    }

    #[test]
    fn reshape_rejects_malformed() {
        assert!(reshape_array_to_ndjson(b"[not json", &mut unbounded()).is_err());
    }

    #[test]
    fn decompress_gzip_roundtrips() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"{\"a\":1}\n").unwrap();
        let gz = enc.finish().unwrap();
        let out = decompress(gz, JsonCompression::Gzip, &mut unbounded()).unwrap();
        assert_eq!(out, b"{\"a\":1}\n");
    }

    #[tokio::test]
    async fn decode_ndjson_builds_two_row_table() {
        use datafusion::prelude::SessionContext;

        let table = decode_ndjson_to_memtable(b"{\"a\":1}\n{\"a\":2}\n", unbounded()).unwrap();
        assert_eq!(table.schema().fields().len(), 1);

        let ctx = SessionContext::new();
        ctx.register_table("t", table).unwrap();
        let n = ctx
            .sql("SELECT count(*) AS n FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[test]
    fn reshape_rejects_top_level_scalar() {
        assert!(reshape_array_to_ndjson(b"42", &mut unbounded()).is_err());
        assert!(reshape_array_to_ndjson(b"\"hello\"", &mut unbounded()).is_err());
    }

    #[test]
    fn decompress_zip_and_other_codecs_error() {
        use crate::read_json::JsonCompression;
        assert!(decompress(vec![], JsonCompression::Zip, &mut unbounded()).is_err());
        assert!(decompress(vec![], JsonCompression::Zstd, &mut unbounded()).is_err());
    }

    #[test]
    fn zip_concatenates_json_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts = SimpleFileOptions::default();
            zw.start_file("a.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":1}\n").unwrap();
            zw.start_file("README.txt", opts).unwrap(); // ignored (non-json)
            zw.write_all(b"ignore me").unwrap();
            zw.start_file("b.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":2}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd =
            decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited, &mut unbounded()).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 2);
    }

    #[test]
    fn zip_with_no_json_entry_errors() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("README.txt", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"nope").unwrap();
            zw.finish().unwrap();
        }
        assert!(decompress_zip_to_ndjson(buf, JsonFraming::Auto, &mut unbounded()).is_err());
    }

    #[test]
    fn zip_auto_mixes_array_and_ndjson_entries() {
        use crate::read_json::JsonFraming;
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("a.json", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"[{\"a\":1},{\"a\":2}]").unwrap();
            zw.start_file("b.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":3}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::Auto, &mut unbounded()).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 3);
    }

    #[test]
    fn zip_appends_missing_trailing_newline_between_entries() {
        use crate::read_json::JsonFraming;
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("a.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":1}").unwrap(); // no trailing newline
            zw.start_file("b.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":2}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd =
            decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited, &mut unbounded()).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 2);
    }

    #[tokio::test]
    async fn buffer_size_guard_rejects_oversized() {
        // Exercises the `head()`-based size guard in `build_buffer_table`
        // directly (bypassing `enforce_tvf_path_policy`, which is the
        // caller's job — see the security tests in `read_json.rs`). A tiny
        // `max_bytes` cap must reject the file before any `get()` fetch.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.json");
        std::fs::write(&path, b"[{\"a\":1},{\"a\":2}]").unwrap();

        let args = FileTvfArgs {
            path: path.display().to_string(),
            ..Default::default()
        };
        let err = build_buffer_table(
            &args,
            JsonFraming::Array,
            JsonCompression::None,
            4, // smaller than the file
            &StorageConfig::default(),
            None,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("exceeds max_buffer_bytes"),
            "expected a size-guard rejection, got: {err}"
        );
    }

    #[test]
    fn zip_skips_directory_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.add_directory("data/", SimpleFileOptions::default())
                .unwrap();
            zw.start_file("data/a.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":1}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd =
            decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited, &mut unbounded()).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 1);
    }

    // ---- 906: decompression and decoding are capped and pool-charged ------

    #[test]
    fn gzip_expansion_is_rejected_by_the_decoded_byte_cap() {
        // ~8 KiB of gzip expands to 8 MiB of NDJSON; a 1 MiB cap must stop
        // the decode well before the whole expansion is resident.
        let line =
            b"{\"a\":\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"}\n";
        let payload: Vec<u8> = line.iter().copied().cycle().take(8 * 1024 * 1024).collect();
        let gz = gzip(&payload);
        assert!(
            gz.len() < 64 * 1024,
            "compressed input is small: {}",
            gz.len()
        );
        let mut budget = capped(1024 * 1024);
        let err = decompress(gz, JsonCompression::Gzip, &mut budget)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds max_buffer_bytes"), "{err}");
        assert!(
            budget.reserved() <= 1024 * 1024 + READ_CHUNK,
            "{}",
            budget.reserved()
        );
    }

    #[test]
    fn gzip_decode_is_denied_by_a_small_memory_pool() {
        let payload: Vec<u8> = b"{\"a\":1}\n"
            .iter()
            .copied()
            .cycle()
            .take(512 * 1024)
            .collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
        let mut budget = BufferBudget::new(&pool, usize::MAX);
        let err = decompress(gzip(&payload), JsonCompression::Gzip, &mut budget).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "pool denial must be a typed resource error: {err}"
        );
        drop(budget);
        assert_eq!(pool.reserved(), 0, "reservation released on drop");
    }

    #[test]
    fn zip_cap_applies_to_cumulative_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let entry: Vec<u8> = b"{\"a\":1}\n".iter().copied().cycle().take(600).collect();
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            for name in ["a.jsonl", "b.jsonl", "c.jsonl"] {
                zw.start_file(name, SimpleFileOptions::default()).unwrap();
                zw.write_all(&entry).unwrap();
            }
            zw.finish().unwrap();
        }
        // Each entry fits on its own; three together do not.
        assert!(decompress_zip_to_ndjson(
            buf.clone(),
            JsonFraming::NewlineDelimited,
            &mut capped(700)
        )
        .is_err());
        let ok = decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited, &mut capped(2000))
            .unwrap();
        assert_eq!(ok.len(), 1800);
    }

    #[test]
    fn reshape_streams_elements_and_honours_the_cap() {
        let mut doc = b"[".to_vec();
        for i in 0..1000 {
            if i > 0 {
                doc.push(b',');
            }
            doc.extend_from_slice(format!("{{\"a\":{i}}}").as_bytes());
        }
        doc.push(b']');
        let out = reshape_array_to_ndjson(&doc, &mut unbounded()).unwrap();
        assert_eq!(out.iter().filter(|b| **b == b'\n').count(), 1000);
        let err = reshape_array_to_ndjson(&doc, &mut capped(100))
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds max_buffer_bytes"), "{err}");
        // Pretty-printed elements are compacted to one line each.
        let pretty = reshape_array_to_ndjson(
            b"[\n  {\n    \"a\": 1\n  },\n  {\n    \"a\": 2\n  }\n]",
            &mut unbounded(),
        )
        .unwrap();
        assert_eq!(String::from_utf8(pretty).unwrap(), "{\"a\":1}\n{\"a\":2}\n");
        // Trailing garbage after the document is still a parse error.
        assert!(reshape_array_to_ndjson(b"[{\"a\":1}] trailing", &mut unbounded()).is_err());
    }

    #[test]
    fn decoded_batches_stay_charged_to_the_pool_while_the_table_lives() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let table = decode_ndjson_to_memtable(
            b"{\"a\":1}\n{\"a\":2}\n",
            BufferBudget::new(&pool, usize::MAX),
        )
        .unwrap();
        assert!(
            pool.reserved() > 0,
            "decoded Arrow memory is visible to the pool"
        );
        drop(table);
        assert_eq!(pool.reserved(), 0, "dropping the table releases it");
    }

    #[tokio::test]
    async fn download_is_capped_on_received_bytes_not_head_size() {
        // The cap is enforced on the bytes actually streamed, so an object
        // that grew (or lied) after HEAD cannot slip past the size check.
        use object_store::memory::InMemory;
        let store = InMemory::new();
        let path = object_store::path::Path::from("big.json");
        store.put(&path, vec![b'x'; 100].into()).await.unwrap();
        let err = fetch_capped(&store, &path, "mem://big.json", &mut capped(64))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds max_buffer_bytes"), "{err}");
        let ok = fetch_capped(&store, &path, "mem://big.json", &mut capped(100))
            .await
            .unwrap();
        assert_eq!(ok.len(), 100);
    }

    #[tokio::test]
    async fn buffer_path_end_to_end_keeps_only_decoded_batches_reserved() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arr.json");
        std::fs::write(&path, b"[{\"a\":1},{\"a\":2},{\"a\":3}]").unwrap();
        let args = FileTvfArgs {
            path: path.display().to_string(),
            ..Default::default()
        };
        let table = build_buffer_table(
            &args,
            JsonFraming::Array,
            JsonCompression::None,
            1024,
            &StorageConfig::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(table.schema().fields().len(), 1);
        // Raw and reshaped copies were released; only Arrow memory remains.
        let debug = format!("{table:?}");
        assert!(debug.starts_with("ReservedMemTable"), "{debug}");
        assert!(!debug.contains("reserved_bytes: 0"), "{debug}");
    }
}
