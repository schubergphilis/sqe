use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::prelude::SessionContext;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures::TryStreamExt;
use tokio::sync::watch;
use tracing::{debug, info, warn};
use url::Url;

use sqe_metrics::WorkerMetricsRegistry;
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
#[tracing::instrument(skip(task, metrics, session_ctx, credential_rx), fields(fragment_id = %task.fragment_id, file_count = task.data_file_paths.len()))]
pub async fn execute_scan(
    task: &ScanTask,
    metrics: Option<&Arc<WorkerMetricsRegistry>>,
    session_ctx: &SessionContext,
    credential_rx: Option<watch::Receiver<Option<RefreshableCredentials>>>,
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
    let mut reservation = consumer.register(&pool);

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
        let meta = store.head(&path).await?;
        total_bytes += meta.size as u64;
        let reader = ParquetObjectReader::new(store.clone(), meta.location)
            .with_file_size(meta.size);
        let mut builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader> =
            ParquetRecordBatchStreamBuilder::new(reader).await?;

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

        let stream = builder.build()?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

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
