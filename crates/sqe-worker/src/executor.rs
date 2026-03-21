use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use futures::TryStreamExt;
use tracing::{debug, info};
use url::Url;

use sqe_metrics::WorkerMetricsRegistry;
use sqe_planner::ScanTask;

/// Execute a scan task by reading Parquet files from S3 and returning Arrow RecordBatches.
///
/// When `metrics` is provided, the function records:
/// - `sqe_worker_fragments_executed_total` (incremented by 1)
/// - `sqe_worker_rows_scanned_total` (incremented by total rows read)
/// - `sqe_worker_bytes_read_total` (incremented by storage bytes read)
/// - `sqe_worker_fragment_duration_seconds` (observed elapsed wall time)
#[tracing::instrument(skip(task, metrics), fields(fragment_id = %task.fragment_id, file_count = task.data_file_paths.len()))]
pub async fn execute_scan(
    task: &ScanTask,
    metrics: Option<&Arc<WorkerMetricsRegistry>>,
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

    let store = build_object_store(task)?;
    let store = Arc::new(store);

    let mut all_batches = Vec::new();
    let mut result_schema: Option<SchemaRef> = None;
    let mut total_bytes: u64 = 0;

    for file_path in &task.data_file_paths {
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

        debug!(
            file = %file_path,
            batch_count = batches.len(),
            rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
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

/// Build an S3 ObjectStore from ScanTask credentials.
fn build_object_store(task: &ScanTask) -> anyhow::Result<impl ObjectStore> {
    let mut builder = AmazonS3Builder::new();

    if !task.s3_endpoint.is_empty() {
        builder = builder.with_endpoint(&task.s3_endpoint);
    }
    if !task.s3_region.is_empty() {
        builder = builder.with_region(&task.s3_region);
    }
    if !task.s3_access_key.is_empty() {
        builder = builder.with_access_key_id(&task.s3_access_key);
    }
    if !task.s3_secret_key.is_empty() {
        builder = builder.with_secret_access_key(&task.s3_secret_key);
    }
    if !task.s3_session_token.is_empty() {
        builder = builder.with_token(&task.s3_session_token);
    }
    if task.s3_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    // Allow HTTP for dev (MinIO)
    builder = builder.with_allow_http(true);

    // Extract bucket from the first file path
    let bucket = s3_url_to_bucket(&task.data_file_paths[0])?;
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
