use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, ObjectStoreExt};
use object_store::path::Path as ObjectPath;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use futures::{Stream, StreamExt, TryStreamExt};
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
    filter_expr: Option<Arc<dyn PhysicalExpr>>,
    coordinator_metrics: Option<Arc<MetricsRegistry>>,
) -> anyhow::Result<(SchemaRef, ScanBatchStream)> {
    if task.data_file_paths.is_empty() {
        anyhow::bail!("ScanTask has no data files");
    }

    let start = std::time::Instant::now();
    let pool = session_ctx.runtime_env().memory_pool.clone();
    let consumer = MemoryConsumer::new(format!("scan:{}", task.fragment_id));
    let reservation = consumer.register(&pool);

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
        filter_expr.as_ref(),
        coordinator_metrics.as_deref(),
    )
    .await?;

    let (tx, rx) =
        mpsc::channel::<anyhow::Result<RecordBatch>>(16);

    let task_for_producer = task.clone();
    let metrics_clone = metrics.clone();
    let coord_metrics_clone = coordinator_metrics.clone();
    let footer_cache_clone = footer_cache.clone();
    let filter_expr_clone = filter_expr.clone();
    let session_ctx_clone = session_ctx.clone();
    let credential_rx_clone = credential_rx.clone();

    tokio::spawn(async move {
        let task = task_for_producer;
        let metrics = metrics_clone;
        let coordinator_metrics = coord_metrics_clone;
        let footer_cache = footer_cache_clone;
        let filter_expr = filter_expr_clone;
        let _session_ctx = session_ctx_clone;
        let mut credential_rx = credential_rx_clone;

        let mut store: Arc<dyn ObjectStore> = initial_store;
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
        }
        debug!(file = %first_path, rows = first_file_bytes, "first-file batches drained");

        // Stream subsequent files.
        for file_path in task.data_file_paths.iter().skip(1) {
            // Apply any refreshed credentials before opening the next file.
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

            let opened = open_parquet_stream(
                &task,
                file_path,
                store.clone(),
                footer_cache.as_deref(),
                filter_expr.as_ref(),
                coordinator_metrics.as_deref(),
            )
            .await;
            let (mut file_stream, _schema, bytes) = match opened {
                Ok(t) => t,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            total_bytes += bytes;

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

    let out_stream: ScanBatchStream =
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    Ok((first_schema, out_stream))
}

/// Open a single Parquet file and return its batch stream plus the recorded
/// byte size (from `head()`). Shared helper for [`execute_scan_streaming`]
/// and the buffered [`execute_scan`] wrapper.
#[allow(clippy::type_complexity)]
async fn open_parquet_stream(
    task: &ScanTask,
    file_path: &str,
    store: Arc<dyn ObjectStore>,
    footer_cache: Option<&FooterCache>,
    filter_expr: Option<&Arc<dyn PhysicalExpr>>,
    coordinator_metrics: Option<&MetricsRegistry>,
) -> anyhow::Result<(
    Pin<Box<dyn Stream<Item = Result<RecordBatch, parquet::errors::ParquetError>> + Send>>,
    SchemaRef,
    u64,
)> {
    debug!(file = %file_path, "Reading Parquet file");
    let object_key = s3_url_to_key(file_path)?;
    let path = ObjectPath::from(object_key.as_str());

    let s3_start = std::time::Instant::now();
    let meta = store.head(&path).await?;
    if let Some(cm) = coordinator_metrics {
        cm.s3_requests_total
            .with_label_values(&["head", "success"])
            .inc();
    }
    let bytes_total = meta.size as u64;
    let reader = ParquetObjectReader::new(store.clone(), meta.location).with_file_size(meta.size);

    let mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader> = if let Some(cache) =
        footer_cache
    {
        let cache_key = file_path.to_string();
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

    if let Some(filter) = filter_expr {
        if late_materialize::is_late_materialization_beneficial(
            Some(filter.as_ref()),
            &task.projected_columns,
        ) {
            let classification =
                late_materialize::classify_columns(filter.as_ref(), &task.projected_columns);
            let file_schema = builder.schema().clone();
            let predicate_schema =
                late_materialize::build_predicate_schema(&classification, &file_schema);
            match late_materialize::remap_predicate_columns(filter, &predicate_schema) {
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

    let schema = builder.schema().clone();
    let stream = builder.build()?;

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
            cm.s3_requests_total.with_label_values(&["head", "success"]).inc();
        }
        total_bytes += meta.size as u64;
        let reader = ParquetObjectReader::new(store.clone(), meta.location)
            .with_file_size(meta.size);

        // Use the footer cache if available: get_or_fetch returns cached
        // metadata or fetches it via a temporary reader and caches the result.
        let mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader> =
            if let Some(cache) = footer_cache {
                let cache_key = file_path.clone();
                let store_for_fetch = store.clone();
                let path_for_fetch = path.clone();
                let file_size = meta.size;

                let cached_meta = cache
                    .get_or_fetch(&cache_key, || {
                        let s = store_for_fetch;
                        let p = path_for_fetch;
                        async move {
                            let fetch_reader = ParquetObjectReader::new(s, p)
                                .with_file_size(file_size);
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
                let reader_opts = ArrowReaderOptions::new().with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
                let arrow_meta = ArrowReaderMetadata::try_new(
                    cached_meta,
                    reader_opts,
                )?;
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_meta)
            } else {
                // Enable page-level min/max pruning for direct reads too
                let reader_opts = ArrowReaderOptions::new().with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Required);
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
            let projected_by_id = project_by_field_id(
                &parquet_schema,
                &task.projected_field_ids,
            );
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
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    indices,
                );
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
            cm.s3_requests_total.with_label_values(&["get", "success"]).inc();
            cm.s3_bytes_read_total.inc_by(meta.size as u64);
            cm.s3_request_duration_seconds.observe(s3_elapsed.as_secs_f64());

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
        let batch_mem: usize = batches
            .iter()
            .map(|b| b.get_array_memory_size())
            .sum();
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

    use std::collections::HashMap;
    use arrow_schema::{DataType, Field, Schema};

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
        let schema = Schema::new(vec![
            stamped("id", 1),
            stamped("b", 2),
        ]);
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
}
