use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::Serializeable;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use sqe_catalog::late_materialize;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use url::Url;

use sqe_catalog::FooterCache;
use sqe_metrics::{MetricsRegistry, WorkerMetricsRegistry};
use sqe_planner::ScanTask;

use crate::credential_channel::RefreshableCredentials;

/// Stream of `RecordBatch`es produced by [`execute_scan_streaming`].
pub type ScanBatchStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<RecordBatch>> + Send + 'static>>;

/// Per-file Parquet batch stream returned by [`open_parquet_stream`].
type ParquetBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, parquet::errors::ParquetError>> + Send>>;

/// Result of opening one Parquet file: its batch stream, schema, and byte size.
type OpenedParquet = anyhow::Result<(ParquetBatchStream, SchemaRef, u64)>;

/// A boxed, in-flight open of one Parquet file (#234 depth-1 prefetch).
type PendingOpen = Pin<Box<dyn std::future::Future<Output = OpenedParquet> + Send>>;

/// Decode the coordinator's pushed-down predicate (#233) from its serialized
/// `Expr` bytes. Returns `None` when no predicate was pushed or when decoding
/// fails. A decode failure is non-fatal: the worker simply ships every
/// projected row and the coordinator's authoritative `FilterExec` filters them.
fn decode_pushed_predicate(task: &ScanTask) -> Option<Expr> {
    let bytes = task.predicate_proto.as_ref()?;
    match Expr::from_bytes(bytes) {
        Ok(expr) => Some(expr),
        Err(e) => {
            warn!(
                fragment_id = %task.fragment_id,
                error = %e,
                "Failed to decode pushed-down predicate; ignoring it (coordinator \
                 still filters authoritatively)"
            );
            None
        }
    }
}

/// Build a `PhysicalExpr` for `predicate` against the full parquet `file_schema`
/// (before projection), so a predicate column outside the projection still
/// resolves. Returns `None` on any planning error (non-fatal: skip worker-side
/// filtering for this file).
fn build_physical_predicate(
    predicate: &Expr,
    file_schema: &SchemaRef,
    fragment_id: &str,
) -> Option<Arc<dyn PhysicalExpr>> {
    let df_schema = match DFSchema::try_from(file_schema.as_ref().clone()) {
        Ok(s) => s,
        Err(e) => {
            warn!(fragment_id = %fragment_id, error = %e, "DFSchema build failed for predicate");
            return None;
        }
    };
    let ctx = SessionContext::new();
    match create_physical_expr(predicate, &df_schema, ctx.state().execution_props()) {
        Ok(expr) => Some(expr),
        Err(e) => {
            warn!(fragment_id = %fragment_id, error = %e, "Physical predicate build failed");
            None
        }
    }
}

/// Execute a scan task by reading Parquet files from S3 and returning Arrow RecordBatches.
///
/// The `session_ctx` parameter supplies the DataFusion [`SessionContext`] whose
/// `RuntimeEnv` carries the configured memory pool. Each batch read is
/// accounted against that pool via a [`MemoryConsumer`] reservation so the
/// worker respects its memory limit.
///
/// When `metrics` is provided, the function records:
/// - `sqe_worker_fragments_executed_total` (incremented by 1)
/// - `sqe_worker_rows_scanned_total` (incremented by total rows read)
/// - `sqe_worker_bytes_read_total` (incremented by storage bytes read)
/// - `sqe_worker_fragment_duration_seconds` (observed elapsed wall time)
///
/// When `credential_rx` is provided, the executor checks for refreshed credentials
/// before each file read. If new credentials are available, the S3 object store is
/// rebuilt with the updated credentials so that long-running scans survive credential
/// expiry.
///
/// When `footer_cache` is provided, parsed Parquet footers (metadata) are cached
/// across files and queries using an LRU cache. On cache hit the footer is not
/// re-fetched from S3, reducing latency for repeated scans of the same files.
///
/// When `filter_expr` is provided and late materialization is beneficial
/// (predicate columns are a proper subset of projected columns), builds an
/// arrow-rs `RowFilter` for two-phase Parquet scans: Phase 1 reads only
/// predicate columns and evaluates the filter; Phase 2 reads remaining
/// projection columns only for surviving rows. This can reduce S3 I/O by
/// 10-50x for selective queries on wide tables.
/// Streaming variant of [`execute_scan`]: returns the schema synchronously
/// and yields `RecordBatch`es from the in-flight Parquet streams without
/// materialising the full scan in memory.
///
/// The first Parquet file is opened in this call so the caller can return
/// the schema before the result stream is polled. A background task chains
/// the per-file streams into an mpsc channel; if the consumer drops the
/// returned stream, the channel closes and the task exits.
pub async fn execute_scan_streaming(
    task: ScanTask,
    metrics: Option<Arc<WorkerMetricsRegistry>>,
    session_ctx: SessionContext,
    credential_rx: Option<watch::Receiver<Option<RefreshableCredentials>>>,
    footer_cache: Option<Arc<FooterCache>>,
    coordinator_metrics: Option<Arc<MetricsRegistry>>,
) -> anyhow::Result<(SchemaRef, ScanBatchStream)> {
    if task.data_file_paths.is_empty() {
        anyhow::bail!("ScanTask has no data files");
    }

    let start = std::time::Instant::now();
    let pool = session_ctx.runtime_env().memory_pool.clone();
    let consumer = MemoryConsumer::new(format!("scan:{}", task.fragment_id));
    let reservation = consumer.register(&pool);

    // Decode the coordinator's pushed-down predicate once (#233). The
    // PhysicalExpr is rebuilt per-file inside open_parquet_stream because it
    // must bind to that file's schema.
    let predicate = decode_pushed_predicate(&task);
    // Per-fragment LIMIT hint (#233): stop emitting once this many rows have
    // shipped. The coordinator's GlobalLimitExec still enforces the global
    // limit, so an over-count here is harmless.
    let row_limit = task.limit;

    // Open the first file synchronously so we know the schema before
    // returning the stream.
    let first_path = task.data_file_paths[0].clone();
    let initial_store: Arc<dyn ObjectStore> = Arc::new(build_object_store_with_creds(
        &task,
        &task.s3_access_key,
        &task.s3_secret_key,
        &task.s3_session_token,
    )?);

    let (mut first_stream, first_schema, first_bytes) = open_parquet_stream(
        &task,
        &first_path,
        initial_store.clone(),
        footer_cache.as_deref(),
        predicate.as_ref(),
        task.file_sizes_bytes.first().copied(),
        coordinator_metrics.as_deref(),
    )
    .await?;

    let (tx, rx) = mpsc::channel::<anyhow::Result<RecordBatch>>(16);

    // Share the task and the per-file open inputs by Arc so each prefetch
    // future (below) can own its inputs without borrowing producer locals.
    let task = Arc::new(task);
    let task_for_producer = task.clone();
    let metrics_clone = metrics.clone();
    let coord_metrics_clone = coordinator_metrics.clone();
    let footer_cache_clone = footer_cache.clone();
    let predicate_clone = predicate.clone();
    let session_ctx_clone = session_ctx.clone();
    let credential_rx_clone = credential_rx.clone();

    tokio::spawn(async move {
        let task = task_for_producer;
        let metrics = metrics_clone;
        let coordinator_metrics = coord_metrics_clone;
        let footer_cache = footer_cache_clone;
        let predicate = predicate_clone;
        let _session_ctx = session_ctx_clone;
        let credential_rx = credential_rx_clone;

        let initial_store: Arc<dyn ObjectStore> = initial_store;
        let mut total_rows: usize = 0;
        let mut total_bytes: u64 = first_bytes;

        // Drain the first file's batches.
        let mut first_file_bytes: usize = 0;
        while let Some(batch_res) = first_stream.next().await {
            match batch_res {
                Ok(batch) => {
                    total_rows += batch.num_rows();
                    first_file_bytes += batch.get_array_memory_size();
                    if let Err(e) = reservation.try_grow(batch.get_array_memory_size()) {
                        let _ = tx.send(Err(anyhow::Error::new(e))).await;
                        return;
                    }
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(anyhow::Error::new(e))).await;
                    return;
                }
            }
            // Early-stop once the per-fragment limit hint is satisfied.
            if let Some(limit) = row_limit {
                if total_rows >= limit {
                    debug!(
                        fragment_id = %task.fragment_id,
                        total_rows,
                        limit,
                        "Per-fragment limit reached; stopping scan early"
                    );
                    return;
                }
            }
        }
        debug!(file = %first_path, rows = first_file_bytes, "first-file batches drained");

        // Pipeline subsequent files (#234) with depth-1 prefetch: while the
        // current file's batches drain into the channel, the next file's footer
        // is already being fetched. Credentials and the object store advance
        // sequentially (single owner of `credential_rx`, best-effort as before).
        // The mpsc(16) backpressure bounds in-flight memory.
        let mut store: Arc<dyn ObjectStore> = initial_store;
        let mut credential_rx = credential_rx;
        let mut idx = 1usize; // file 0 was opened synchronously above

        // Helper closure to build the open future for file `idx` using the
        // current store, so the next file's footer fetch overlaps the current
        // file's drain.
        let start_next_open = |idx: usize, store: Arc<dyn ObjectStore>| -> PendingOpen {
            let task = task.clone();
            let footer_cache = footer_cache.clone();
            let predicate = predicate.clone();
            let coordinator_metrics = coordinator_metrics.clone();
            Box::pin(async move {
                let path = task.data_file_paths[idx].clone();
                let size = task.file_sizes_bytes.get(idx).copied();
                open_parquet_stream(
                    &task,
                    &path,
                    store,
                    footer_cache.as_deref(),
                    predicate.as_ref(),
                    size,
                    coordinator_metrics.as_deref(),
                )
                .await
            })
        };

        // Kick off the first prefetch (file index 1) if it exists.
        let mut pending = if idx < task.data_file_paths.len() {
            let fut = start_next_open(idx, store.clone());
            idx += 1;
            Some(fut)
        } else {
            None
        };

        'files: while let Some(open_fut) = pending.take() {
            // Await the file we prefetched while the previous file drained.
            let (mut file_stream, _schema, bytes) = match open_fut.await {
                Ok(t) => t,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            total_bytes += bytes;

            // Advance credentials, then kick off the NEXT file's open so its
            // footer fetch overlaps with this file's drain below.
            if idx < task.data_file_paths.len() {
                if let Some(ref mut rx) = credential_rx {
                    if rx.has_changed().unwrap_or(false) {
                        let new_creds = rx.borrow_and_update().clone();
                        if let Some(creds) = new_creds {
                            info!(
                                fragment_id = %task.fragment_id,
                                expiry = %creds.expiry,
                                "Applying refreshed credentials for next file read"
                            );
                            match build_object_store_with_creds(
                                &task,
                                &creds.access_key_id,
                                &creds.secret_access_key,
                                &creds.session_token,
                            ) {
                                Ok(s) => store = Arc::new(s),
                                Err(e) => warn!(
                                    fragment_id = %task.fragment_id,
                                    error = %e,
                                    "Failed to rebuild object store with refreshed credentials"
                                ),
                            }
                        }
                    }
                }
                pending = Some(start_next_open(idx, store.clone()));
                idx += 1;
            }

            while let Some(batch_res) = file_stream.next().await {
                match batch_res {
                    Ok(batch) => {
                        total_rows += batch.num_rows();
                        if let Err(e) = reservation.try_grow(batch.get_array_memory_size()) {
                            let _ = tx.send(Err(anyhow::Error::new(e))).await;
                            return;
                        }
                        if tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(anyhow::Error::new(e))).await;
                        return;
                    }
                }
            }

            // Early-stop once the per-fragment limit hint is satisfied.
            // Dropping `pending` cancels the in-flight prefetch open.
            if let Some(limit) = row_limit {
                if total_rows >= limit {
                    debug!(
                        fragment_id = %task.fragment_id,
                        total_rows,
                        limit,
                        "Per-fragment limit reached; stopping scan early"
                    );
                    break 'files;
                }
            }
        }

        let elapsed = start.elapsed();
        info!(
            fragment_id = %task.fragment_id,
            total_rows = total_rows,
            total_bytes = total_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "Streaming scan complete"
        );
        if let Some(ref m) = metrics {
            m.fragments_executed.inc();
            m.rows_scanned.inc_by(total_rows as f64);
            m.bytes_read.inc_by(total_bytes as f64);
            m.fragment_duration.observe(elapsed.as_secs_f64());
        }
    });

    let out_stream: ScanBatchStream = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    Ok((first_schema, out_stream))
}

/// Open a single Parquet file and return its batch stream plus its byte size.
///
/// When `known_size` is `Some` (the ScanTask carried the file size from
/// manifest metadata, #234), the per-file `store.head()` request is skipped
/// entirely: the size is fed straight to `ParquetObjectReader::with_file_size`.
/// When it is `None` (size missing for this file), the function falls back to a
/// single `head()` to learn the size.
async fn open_parquet_stream(
    task: &ScanTask,
    file_path: &str,
    store: Arc<dyn ObjectStore>,
    footer_cache: Option<&FooterCache>,
    predicate: Option<&Expr>,
    known_size: Option<u64>,
    coordinator_metrics: Option<&MetricsRegistry>,
) -> OpenedParquet {
    debug!(file = %file_path, "Reading Parquet file");
    let object_key = s3_url_to_key(file_path)?;
    let path = ObjectPath::from(object_key.as_str());

    let s3_start = std::time::Instant::now();
    // Skip the per-file HEAD when the coordinator already supplied the size
    // (#234). Only HEAD when the size is unknown for this specific file.
    let bytes_total: u64 = match known_size {
        Some(size) => size,
        None => {
            let meta = store.head(&path).await?;
            if let Some(cm) = coordinator_metrics {
                cm.s3_requests_total
                    .with_label_values(&["head", "success"])
                    .inc();
            }
            meta.size as u64
        }
    };
    let reader = ParquetObjectReader::new(store.clone(), path.clone()).with_file_size(bytes_total);

    let mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader> = if let Some(cache) =
        footer_cache
    {
        let cache_key = file_path.to_string();
        let store_for_fetch = store.clone();
        let path_for_fetch = path.clone();
        let fetch_size = bytes_total;
        let cached_meta = cache
            .get_or_fetch(&cache_key, || {
                let s = store_for_fetch;
                let p = path_for_fetch;
                async move {
                    let fetch_reader = ParquetObjectReader::new(s, p).with_file_size(fetch_size);
                    let tmp_builder = ParquetRecordBatchStreamBuilder::new(fetch_reader).await?;
                    Ok::<ParquetMetaData, parquet::errors::ParquetError>(
                        tmp_builder.metadata().as_ref().clone(),
                    )
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("Footer cache error: {e}"))?;
        let reader_opts = ArrowReaderOptions::new()
            .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
        let arrow_meta = ArrowReaderMetadata::try_new(cached_meta, reader_opts)?;
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta)
    } else {
        let reader_opts = ArrowReaderOptions::new()
            .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
        ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_opts).await?
    };

    if !task.projected_columns.is_empty() {
        let parquet_schema = builder.schema().clone();
        let indices: Vec<usize> = task
            .projected_columns
            .iter()
            .filter_map(|name| {
                parquet_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
            })
            .collect();
        if !indices.is_empty() {
            let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }
    }

    // Pushed-down predicate (#233): build a PhysicalExpr against the full file
    // schema (so a predicate column outside the projection still resolves) and,
    // when late materialization is beneficial, install a two-phase RowFilter so
    // the worker decodes predicate columns first and skips non-matching rows.
    // Skipping this is always safe: the coordinator keeps the authoritative
    // FilterExec above the distributed scan.
    if let Some(predicate) = predicate {
        let file_schema = builder.schema().clone();
        if let Some(filter) = build_physical_predicate(predicate, &file_schema, &task.fragment_id) {
            if late_materialize::is_late_materialization_beneficial(
                Some(filter.as_ref()),
                &task.projected_columns,
            ) {
                let classification =
                    late_materialize::classify_columns(filter.as_ref(), &task.projected_columns);
                let predicate_schema =
                    late_materialize::build_predicate_schema(&classification, &file_schema);
                match late_materialize::remap_predicate_columns(&filter, &predicate_schema) {
                    Ok(remapped_predicate) => {
                        let row_filter = late_materialize::build_row_filter(
                            remapped_predicate,
                            &predicate_schema,
                            builder.parquet_schema(),
                        );
                        builder = builder.with_row_filter(row_filter);
                    }
                    Err(e) => warn!(
                        fragment_id = %task.fragment_id,
                        error = %e,
                        "Failed to remap predicate columns for late materialization"
                    ),
                }
            }
        }
    }

    // Take the schema from the BUILT stream, not the builder:
    // `builder.schema()` is always the full parquet file schema, while the
    // stream's schema reflects the applied ProjectionMask. The caller hands
    // this schema to the Flight encoder; advertising the full schema while
    // shipping projected batches made every projected distributed scan fail
    // on the coordinator with "number of columns(N) must match number of
    // fields(M)" during Flight decode (the !327 regression, introduced when
    // the streaming path replaced the buffering path that took the schema
    // from `batches[0].schema()`).
    let stream = builder.build()?;
    let schema = stream.schema().clone();

    if let Some(cm) = coordinator_metrics {
        let s3_elapsed = s3_start.elapsed();
        cm.s3_requests_total
            .with_label_values(&["get", "success"])
            .inc();
        cm.s3_bytes_read_total.inc_by(bytes_total);
        cm.s3_request_duration_seconds
            .observe(s3_elapsed.as_secs_f64());
    }

    Ok((Box::pin(stream), schema, bytes_total))
}

#[tracing::instrument(skip(task, metrics, session_ctx, credential_rx, footer_cache, filter_expr, coordinator_metrics), fields(fragment_id = %task.fragment_id, file_count = task.data_file_paths.len()))]
pub async fn execute_scan(
    task: &ScanTask,
    metrics: Option<&Arc<WorkerMetricsRegistry>>,
    session_ctx: &SessionContext,
    credential_rx: Option<watch::Receiver<Option<RefreshableCredentials>>>,
    footer_cache: Option<&Arc<FooterCache>>,
    filter_expr: Option<Arc<dyn PhysicalExpr>>,
    coordinator_metrics: Option<&Arc<MetricsRegistry>>,
) -> anyhow::Result<(SchemaRef, Vec<RecordBatch>)> {
    let start = std::time::Instant::now();

    info!(
        fragment_id = %task.fragment_id,
        file_count = task.data_file_paths.len(),
        "Executing scan task"
    );

    if task.data_file_paths.is_empty() {
        anyhow::bail!("ScanTask has no data files");
    }

    // Mutable credential state — may be updated between files
    let mut current_access_key = task.s3_access_key.clone();
    let mut current_secret_key = task.s3_secret_key.clone();
    let mut current_session_token = task.s3_session_token.clone();
    let mut credential_rx = credential_rx;

    let store = build_object_store_with_creds(
        task,
        &current_access_key,
        &current_secret_key,
        &current_session_token,
    )?;
    let mut store: Arc<dyn ObjectStore> = Arc::new(store);

    // Register a memory consumer with the session's pool so that batch
    // allocations are tracked and the worker memory limit is enforced.
    let pool = session_ctx.runtime_env().memory_pool.clone();
    let consumer = MemoryConsumer::new(format!("scan:{}", task.fragment_id));
    let reservation = consumer.register(&pool);

    let mut all_batches = Vec::new();
    let mut result_schema: Option<SchemaRef> = None;
    let mut total_bytes: u64 = 0;

    for file_path in &task.data_file_paths {
        // Check for credential refresh before each file read
        if let Some(ref mut rx) = credential_rx {
            if rx.has_changed().unwrap_or(false) {
                let new_creds = rx.borrow_and_update().clone();
                if let Some(creds) = new_creds {
                    info!(
                        fragment_id = %task.fragment_id,
                        expiry = %creds.expiry,
                        "Applying refreshed credentials for next file read"
                    );
                    current_access_key = creds.access_key_id;
                    current_secret_key = creds.secret_access_key;
                    current_session_token = creds.session_token;

                    match build_object_store_with_creds(
                        task,
                        &current_access_key,
                        &current_secret_key,
                        &current_session_token,
                    ) {
                        Ok(new_store) => {
                            store = Arc::new(new_store);
                        }
                        Err(e) => {
                            warn!(
                                fragment_id = %task.fragment_id,
                                error = %e,
                                "Failed to rebuild object store with refreshed credentials, \
                                 continuing with previous credentials"
                            );
                        }
                    }
                }
            }
        }

        debug!(file = %file_path, "Reading Parquet file");

        let object_key = s3_url_to_key(file_path)?;
        let path = ObjectPath::from(object_key.as_str());

        // Use head() to get ObjectMeta (includes size) for bounded range requests
        let s3_start = std::time::Instant::now();
        let meta = store.head(&path).await?;
        // Record S3 HEAD request
        if let Some(cm) = coordinator_metrics {
            cm.s3_requests_total
                .with_label_values(&["head", "success"])
                .inc();
        }
        total_bytes += meta.size as u64;
        let reader =
            ParquetObjectReader::new(store.clone(), meta.location).with_file_size(meta.size);

        // Use the footer cache if available: get_or_fetch returns cached
        // metadata or fetches it via a temporary reader and caches the result.
        let mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader> = if let Some(cache) =
            footer_cache
        {
            let cache_key = file_path.clone();
            let store_for_fetch = store.clone();
            let path_for_fetch = path.clone();
            let file_size = meta.size;

            let cached_meta = cache
                .get_or_fetch(&cache_key, || {
                    let s = store_for_fetch;
                    let p = path_for_fetch;
                    async move {
                        let fetch_reader = ParquetObjectReader::new(s, p).with_file_size(file_size);
                        let tmp_builder =
                            ParquetRecordBatchStreamBuilder::new(fetch_reader).await?;
                        Ok::<ParquetMetaData, parquet::errors::ParquetError>(
                            tmp_builder.metadata().as_ref().clone(),
                        )
                    }
                })
                .await
                .map_err(|e| anyhow::anyhow!("Footer cache error: {e}"))?;

            // Enable page-level min/max pruning via PageIndex.
            // This lets the Parquet reader skip individual data pages
            // within row groups whose min/max don't satisfy the predicate.
            //
            // NOTE: sqe_pages_pruned_index_total remains at 0 because arrow-rs
            // does not expose a page-skip counter from its internal PageIndex
            // pruning. Instrumenting this requires upstream changes in arrow-rs
            // or a custom ParquetExec wrapper. Tracked for future work.
            let reader_opts = ArrowReaderOptions::new()
                .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
            let arrow_meta = ArrowReaderMetadata::try_new(cached_meta, reader_opts)?;
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta)
        } else {
            // Enable page-level min/max pruning for direct reads too
            let reader_opts = ArrowReaderOptions::new()
                .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
            ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_opts).await?
        };

        // Apply column projection. Prefer field-ID-based projection (#43): the
        // coordinator sends `projected_field_ids` parallel to `projected_columns`,
        // and Iceberg writes a `PARQUET:field_id` metadata key on every parquet
        // column. Resolving by ID survives RENAME COLUMN (storage name unchanged,
        // catalog name updated) and ADD COLUMN (new field absent from old files).
        // Fall back to name-based projection when the coordinator did not supply
        // IDs (older sender, name-mapping path) or the parquet file has no IDs
        // (Hive-written files predating Iceberg's metadata stamp).
        if !task.projected_columns.is_empty() {
            let parquet_schema = builder.schema().clone();
            let projected_by_id = project_by_field_id(&parquet_schema, &task.projected_field_ids);
            let indices: Vec<usize> = match projected_by_id {
                Some(ids) => ids,
                None => task
                    .projected_columns
                    .iter()
                    .filter_map(|name| {
                        parquet_schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == name)
                    })
                    .collect(),
            };

            if !indices.is_empty() {
                let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), indices);
                builder = builder.with_projection(mask);
            }
        }

        // Late materialization: apply RowFilter for two-phase Parquet scan
        // when a filter expression is provided and there are projection-only
        // columns that can be deferred to Phase 2.
        if let Some(ref filter) = filter_expr {
            if late_materialize::is_late_materialization_beneficial(
                Some(filter.as_ref()),
                &task.projected_columns,
            ) {
                let classification =
                    late_materialize::classify_columns(filter.as_ref(), &task.projected_columns);

                debug!(
                    fragment_id = %task.fragment_id,
                    predicate_columns = ?classification.predicate_columns,
                    projection_only_columns = ?classification.projection_only_columns,
                    "Applying late materialization RowFilter"
                );

                // Build the predicate schema from the full file schema
                let file_schema = builder.schema().clone();
                let predicate_schema =
                    late_materialize::build_predicate_schema(&classification, &file_schema);

                // Remap predicate column indices to the predicate-only schema
                match late_materialize::remap_predicate_columns(filter, &predicate_schema) {
                    Ok(remapped_predicate) => {
                        let row_filter = late_materialize::build_row_filter(
                            remapped_predicate,
                            &predicate_schema,
                            builder.parquet_schema(),
                        );
                        builder = builder.with_row_filter(row_filter);
                    }
                    Err(e) => {
                        warn!(
                            fragment_id = %task.fragment_id,
                            error = %e,
                            "Failed to remap predicate columns for late materialization, \
                             falling back to full scan"
                        );
                    }
                }
            }
        }

        let stream = builder.build()?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        // Record S3 GET metrics for this file read
        if let Some(cm) = coordinator_metrics {
            let s3_elapsed = s3_start.elapsed();
            cm.s3_requests_total
                .with_label_values(&["get", "success"])
                .inc();
            cm.s3_bytes_read_total.inc_by(meta.size as u64);
            cm.s3_request_duration_seconds
                .observe(s3_elapsed.as_secs_f64());

            // Late materialization metrics: track bytes and selectivity when
            // a RowFilter was applied. Approximate predicate bytes as the file
            // size (full file was fetched from S3), and projection bytes as the
            // in-memory batch size (which reflects only surviving/projected data).
            if filter_expr.is_some() {
                let batch_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let batch_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
                cm.late_mat_bytes_predicate.inc_by(meta.size as f64);
                cm.late_mat_bytes_projection.inc_by(batch_bytes as f64);
                // Estimate selectivity from metadata: file_record_count vs surviving rows.
                // We approximate the pre-filter row count from metadata row group info.
                // For a simple estimate, use file_size ratio as a proxy.
                if meta.size > 0 && batch_rows > 0 {
                    let selectivity = batch_bytes as f64 / meta.size as f64;
                    cm.late_mat_selectivity.observe(selectivity.min(1.0));
                }
            }
        }

        if result_schema.is_none() && !batches.is_empty() {
            result_schema = Some(batches[0].schema());
        }

        // Account for the in-memory size of the Arrow batches against the
        // memory pool.  `try_grow` will return an error if the limit is
        // exceeded, propagating back-pressure to the caller.
        let batch_mem: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        reservation.try_grow(batch_mem)?;

        debug!(
            file = %file_path,
            batch_count = batches.len(),
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            batch_memory_bytes = batch_mem,
            "Read Parquet file"
        );

        all_batches.extend(batches);
    }

    let schema = result_schema.unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    let elapsed = start.elapsed();

    info!(
        fragment_id = %task.fragment_id,
        total_batches = all_batches.len(),
        total_rows = total_rows,
        total_bytes = total_bytes,
        elapsed_ms = elapsed.as_millis() as u64,
        "Scan task complete"
    );

    // Record worker metrics
    if let Some(m) = metrics {
        m.fragments_executed.inc();
        m.rows_scanned.inc_by(total_rows as f64);
        m.bytes_read.inc_by(total_bytes as f64);
        m.fragment_duration.observe(elapsed.as_secs_f64());
    }

    Ok((schema, all_batches))
}

/// Build an S3 ObjectStore from ScanTask config with explicit credentials.
///
/// This is separated from `build_object_store` so that the executor can rebuild
/// the store when refreshed credentials arrive without re-reading the ScanTask's
/// original (possibly expired) credentials.
fn build_object_store_with_creds(
    task: &ScanTask,
    access_key: &str,
    secret_key: &str,
    session_token: &str,
) -> anyhow::Result<impl ObjectStore> {
    let mut builder = AmazonS3Builder::new();

    if !task.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&task.s3_endpoint);
    }
    if !task.s3_region.is_empty() {
        builder = builder.with_region(&task.s3_region);
    }
    if !access_key.is_empty() {
        builder = builder.with_access_key_id(access_key);
    }
    if !secret_key.is_empty() {
        builder = builder.with_secret_access_key(secret_key);
    }
    if !session_token.is_empty() {
        builder = builder.with_token(session_token);
    }
    if task.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    // Allow HTTP only when explicitly configured (dev/test with S3-compatible endpoints like MinIO).
    // Defaults to false (HTTPS required) to prevent plaintext S3 traffic in production.
    builder = builder.with_allow_http(task.s3_allow_http);

    // Extract bucket from the first file path
    let bucket = s3_url_to_bucket(
        task.data_file_paths
            .first()
            .ok_or_else(|| anyhow::anyhow!("ScanTask has no data files"))?,
    )?;
    builder = builder.with_bucket_name(&bucket);

    Ok(builder.build()?)
}

/// Extract the bucket name from an S3 URL like `s3://bucket/key/path`.
fn s3_url_to_bucket(url: &str) -> anyhow::Result<String> {
    let parsed = Url::parse(url)?;
    parsed
        .host_str()
        .map(|h| h.to_string())
        .ok_or_else(|| anyhow::anyhow!("No bucket in S3 URL: {url}"))
}

/// Extract the object key from an S3 URL like `s3://bucket/key/path`.
fn s3_url_to_key(url: &str) -> anyhow::Result<String> {
    let parsed = Url::parse(url)?;
    let path = parsed.path();
    Ok(path.trim_start_matches('/').to_string())
}

/// Iceberg writes a `PARQUET:field_id` metadata entry on every parquet column.
/// The reader resolves a projection by ID when the parquet file actually
/// stamped IDs on every projected field and the coordinator supplied IDs.
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// Build a parquet-column-index list for `projected_field_ids` by reading the
/// `PARQUET:field_id` metadata key on each parquet field (#43).
///
/// Returns `None` when the projection cannot be resolved entirely by ID
/// (caller-supplied IDs missing, parquet file missing IDs, or one of the
/// requested IDs is absent from this file). The caller falls back to the
/// existing name-based projection in that case so old files and old
/// coordinators continue to work.
///
/// Top-level only: matches the existing name-based projection's
/// `parquet_schema.fields().position(...)` granularity. Nested field-id
/// projection is iceberg-rust's job (see arrow/reader.rs).
fn project_by_field_id(
    parquet_schema: &arrow_schema::Schema,
    projected_field_ids: &[i32],
) -> Option<Vec<usize>> {
    if projected_field_ids.is_empty() {
        return None;
    }

    let mut id_to_index: std::collections::HashMap<i32, usize> =
        std::collections::HashMap::with_capacity(parquet_schema.fields().len());
    for (idx, field) in parquet_schema.fields().iter().enumerate() {
        let Some(id_str) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
            // At least one parquet field lacks an ID. Treat the file as
            // pre-Iceberg-stamp and abandon field-ID projection.
            return None;
        };
        let Ok(id) = id_str.parse::<i32>() else {
            return None;
        };
        id_to_index.insert(id, idx);
    }

    let mut indices = Vec::with_capacity(projected_field_ids.len());
    for fid in projected_field_ids {
        match id_to_index.get(fid) {
            Some(idx) => indices.push(*idx),
            None => {
                // The projected field id is absent from this file. This is
                // expected for ADD COLUMN against pre-evolution files. The
                // name-based fallback would also miss it, but returning None
                // here lets the existing path log and continue. A future
                // refinement can return the partial mask and tell the caller
                // to fill the rest with NULL (see iceberg-rust's
                // RecordBatchTransformer).
                return None;
            }
        }
    }
    Some(indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_url_to_bucket() {
        assert_eq!(
            s3_url_to_bucket("s3://my-bucket/path/to/file.parquet").unwrap(),
            "my-bucket"
        );
    }

    #[test]
    fn test_s3_url_to_key() {
        assert_eq!(
            s3_url_to_key("s3://my-bucket/path/to/file.parquet").unwrap(),
            "path/to/file.parquet"
        );
    }

    #[test]
    fn test_s3_url_to_key_nested() {
        assert_eq!(
            s3_url_to_key("s3://bucket/warehouse/db/table/data/00001.parquet").unwrap(),
            "warehouse/db/table/data/00001.parquet"
        );
    }

    // -------------------------------------------------------------------------
    // Field-ID projection (#43) — covers the RENAME / ADD COLUMN survival paths.
    // -------------------------------------------------------------------------

    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;

    fn stamped(name: &str, id: i32) -> Field {
        let mut md = HashMap::new();
        md.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
        Field::new(name, DataType::Int64, false).with_metadata(md)
    }

    #[test]
    fn project_by_field_id_rename_survives() {
        // Parquet was written when the column was called "b". The Iceberg
        // catalog has since renamed it to "c". The worker is asked to project
        // [field_id = 2] and must resolve to parquet position 1 even though
        // the file's column name is "b".
        let schema = Schema::new(vec![stamped("id", 1), stamped("b", 2)]);
        let indices = project_by_field_id(&schema, &[1, 2]).unwrap();
        assert_eq!(indices, vec![0, 1]);
    }

    #[test]
    fn project_by_field_id_returns_none_when_id_absent_in_file() {
        // Post-rename file has no column with the old field id.
        let schema = Schema::new(vec![stamped("id", 1), stamped("c", 2)]);
        assert!(project_by_field_id(&schema, &[1, 99]).is_none());
    }

    #[test]
    fn project_by_field_id_returns_none_when_file_lacks_field_ids() {
        // Hive-written file with no PARQUET:field_id metadata.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        assert!(project_by_field_id(&schema, &[1, 2]).is_none());
    }

    #[test]
    fn project_by_field_id_empty_caller_returns_none() {
        let schema = Schema::new(vec![stamped("id", 1)]);
        assert!(project_by_field_id(&schema, &[]).is_none());
    }

    #[test]
    fn project_by_field_id_reorders_to_match_caller_order() {
        // Caller asks for [b, id] (ids [2, 1]). The mask must follow the
        // caller's order, not the parquet schema's order.
        let schema = Schema::new(vec![stamped("id", 1), stamped("b", 2)]);
        let indices = project_by_field_id(&schema, &[2, 1]).unwrap();
        assert_eq!(indices, vec![1, 0]);
    }

    // -------------------------------------------------------------------------
    // Pushed-down predicate (#233) — decode + physical-expr build.
    // -------------------------------------------------------------------------

    use datafusion::logical_expr::{col, lit};
    use datafusion_proto::bytes::Serializeable;

    fn task_with_predicate(predicate_proto: Option<Vec<u8>>, limit: Option<usize>) -> ScanTask {
        ScanTask {
            fragment_id: "frag-pred".to_string(),
            data_file_paths: vec!["s3://bucket/f.parquet".to_string()],
            file_sizes_bytes: vec![1024],
            projected_columns: vec!["a".to_string(), "b".to_string()],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: false,
            s3_allow_http: false,
            predicate_proto,
            limit,
        }
    }

    #[test]
    fn decode_pushed_predicate_none_when_absent() {
        let task = task_with_predicate(None, None);
        assert!(decode_pushed_predicate(&task).is_none());
    }

    #[test]
    fn decode_pushed_predicate_roundtrips() {
        let expr = col("a").gt(lit(5i64));
        let bytes = expr.to_bytes().unwrap().to_vec();
        let task = task_with_predicate(Some(bytes), None);
        let decoded = decode_pushed_predicate(&task).expect("decode");
        assert_eq!(decoded, expr);
    }

    #[test]
    fn decode_pushed_predicate_ignores_garbage() {
        // A corrupt predicate must not abort the scan: decode returns None and
        // the worker ships every row (coordinator filters authoritatively).
        let task = task_with_predicate(Some(vec![0xde, 0xad, 0xbe, 0xef]), None);
        assert!(decode_pushed_predicate(&task).is_none());
    }

    #[test]
    fn build_physical_predicate_resolves_against_file_schema() {
        // The predicate references column "a" which is present in the file
        // schema (even when it is not in the projection set), so the physical
        // expr must build successfully.
        let expr = col("a").gt(lit(5i64));
        let file_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let physical = build_physical_predicate(&expr, &file_schema, "frag-test");
        assert!(physical.is_some(), "predicate should bind to file schema");
    }

    #[test]
    fn build_physical_predicate_fails_for_unknown_column() {
        // A column absent from the file schema cannot bind; build returns None
        // (non-fatal: scan proceeds unfiltered, coordinator still filters).
        let expr = col("does_not_exist").gt(lit(5i64));
        let file_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        assert!(build_physical_predicate(&expr, &file_schema, "frag-test").is_none());
    }

    // -------------------------------------------------------------------------
    // HEAD-skip (#234) — open_parquet_stream must not call store.head() when
    // the ScanTask supplied the file size, and must fall back to head() once
    // when the size is unknown.
    // -------------------------------------------------------------------------

    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as OsPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// ObjectStore wrapper that counts HEAD-style metadata fetches, delegating
    /// everything else to an inner store. In object_store 0.13 `head()` is an
    /// extension method that lowers to `get_opts` with `options.head == true`,
    /// so that is what we count to assert the HEAD-skip path (#234).
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        head_calls: Arc<AtomicUsize>,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({:?})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OsPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.head_calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OsPath>>,
        ) -> BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            opts: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// Write a tiny single-column Parquet blob to a fresh InMemory store and
    /// return the counting store plus its byte size.
    async fn write_parquet_blob() -> (Arc<CountingStore>, Arc<AtomicUsize>, u64) {
        use arrow_array::Int64Array;
        use parquet::arrow::ArrowWriter;

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let size = buf.len() as u64;

        let inner = Arc::new(InMemory::new());
        let path = OsPath::from("data/f.parquet");
        inner.put(&path, PutPayload::from(buf)).await.unwrap();

        let head_calls = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(CountingStore {
            inner,
            head_calls: head_calls.clone(),
        });
        (store, head_calls, size)
    }

    fn scan_task_for_blob() -> ScanTask {
        // The bucket/path machinery uses s3:// URLs; the object key resolves to
        // "data/f.parquet" matching the blob written above.
        ScanTask {
            fragment_id: "frag-head".to_string(),
            data_file_paths: vec!["s3://bucket/data/f.parquet".to_string()],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            projected_field_ids: vec![],
            s3_endpoint: String::new(),
            s3_region: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_session_token: String::new(),
            s3_path_style: true,
            s3_allow_http: false,
            predicate_proto: None,
            limit: None,
        }
    }

    #[tokio::test]
    async fn open_parquet_stream_skips_head_when_size_known() {
        let (store, head_calls, size) = write_parquet_blob().await;
        let task = scan_task_for_blob();
        let (stream, _schema, bytes) = open_parquet_stream(
            &task,
            &task.data_file_paths[0],
            store.clone(),
            None,
            None,
            Some(size),
            None,
        )
        .await
        .expect("open should succeed");
        // Drain to be sure no lazy head fires during reads.
        let _: Vec<_> = stream.collect().await;
        assert_eq!(
            head_calls.load(Ordering::SeqCst),
            0,
            "no HEAD must be issued when the size is known"
        );
        assert_eq!(bytes, size);
    }

    #[tokio::test]
    async fn open_parquet_stream_heads_once_when_size_unknown() {
        let (store, head_calls, _size) = write_parquet_blob().await;
        let task = scan_task_for_blob();
        let (stream, _schema, _bytes) = open_parquet_stream(
            &task,
            &task.data_file_paths[0],
            store.clone(),
            None,
            None,
            None, // size unknown -> fall back to head() exactly once
            None,
        )
        .await
        .expect("open should succeed");
        let _: Vec<_> = stream.collect().await;
        assert_eq!(
            head_calls.load(Ordering::SeqCst),
            1,
            "exactly one HEAD must be issued when the size is unknown"
        );
    }

    /// End-to-end (#233): a pushed-down predicate installs a late-materialization
    /// RowFilter and the file emits only matching rows. The projection includes a
    /// column NOT in the predicate (b), so `is_late_materialization_beneficial`
    /// is true and the RowFilter actually runs. Exact-count assertion proves the
    /// filter ran rather than the file simply being short.
    #[tokio::test]
    async fn open_parquet_stream_predicate_reduces_rows() {
        use arrow_array::Int64Array;
        use datafusion::logical_expr::{col, lit};
        use parquet::arrow::ArrowWriter;

        // Two columns: a (predicate column) and b (projection-only). a = 1..=10.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let a: Vec<i64> = (1..=10).collect();
        let b: Vec<i64> = a.iter().map(|v| v * 100).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let size = buf.len() as u64;
        let inner = Arc::new(InMemory::new());
        inner
            .put(&OsPath::from("data/f.parquet"), PutPayload::from(buf))
            .await
            .unwrap();
        let store: Arc<dyn ObjectStore> = inner;

        let mut task = scan_task_for_blob();
        task.projected_columns = vec!["a".to_string(), "b".to_string()];

        // Predicate a > 5 -> rows a=6..=10 survive -> exactly 5 rows.
        let predicate = col("a").gt(lit(5i64));

        let (stream, _schema, _bytes) = open_parquet_stream(
            &task,
            &task.data_file_paths[0],
            store,
            None,
            Some(&predicate),
            Some(size),
            None,
        )
        .await
        .expect("open should succeed");

        let batches: Vec<_> = stream.collect().await;
        let rows: usize = batches
            .into_iter()
            .map(|r| r.expect("batch ok").num_rows())
            .sum();
        assert_eq!(rows, 5, "RowFilter should keep only rows where a > 5");
    }

    /// Regression test for the projected-distributed-scan failure (!327):
    /// `open_parquet_stream` must return the PROJECTED schema -- the schema of
    /// the batches it actually emits -- not the full parquet file schema.
    /// Before the fix it returned `builder.schema()` (full file width), the
    /// Flight encoder advertised that full schema, and the coordinator's
    /// Flight decode failed with "number of columns(N) must match number of
    /// fields(M)" on every projected distributed scan.
    #[tokio::test]
    async fn open_parquet_stream_returns_projected_schema_matching_batches() {
        use arrow_array::Int64Array;
        use parquet::arrow::ArrowWriter;

        // Three columns in the file; the task projects two of them.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
                Arc::new(Int64Array::from(vec![100i64, 200])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let size = buf.len() as u64;
        let inner = Arc::new(InMemory::new());
        inner
            .put(&OsPath::from("data/f.parquet"), PutPayload::from(buf))
            .await
            .unwrap();
        let store: Arc<dyn ObjectStore> = inner;

        let mut task = scan_task_for_blob();
        task.projected_columns = vec!["c".to_string(), "a".to_string()];

        let (stream, advertised_schema, _bytes) = open_parquet_stream(
            &task,
            &task.data_file_paths[0],
            store,
            None,
            None,
            Some(size),
            None,
        )
        .await
        .expect("open should succeed");

        let batches: Vec<RecordBatch> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.expect("batch ok"))
            .collect();
        assert!(!batches.is_empty(), "projected scan yields batches");

        // The advertised schema must be exactly what the batches carry.
        assert_eq!(
            advertised_schema.fields().len(),
            2,
            "advertised schema must be projected (2 of 3 columns)"
        );
        for b in &batches {
            assert_eq!(
                b.schema().fields(),
                advertised_schema.fields(),
                "every emitted batch must match the advertised schema"
            );
        }
        // ProjectionMask is a mask: output follows FILE order, not request order.
        let names: Vec<&str> = advertised_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec!["a", "c"],
            "projected columns come back in file order"
        );
    }
}
