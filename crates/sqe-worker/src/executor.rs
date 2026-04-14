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
use futures::TryStreamExt;
use sqe_catalog::late_materialize;
use tokio::sync::watch;
use tracing::{debug, info, warn};
use url::Url;

use sqe_catalog::FooterCache;
use sqe_metrics::{MetricsRegistry, WorkerMetricsRegistry};
use sqe_planner::ScanTask;

use crate::credential_channel::RefreshableCredentials;

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

        // Apply column projection if specified
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
}
