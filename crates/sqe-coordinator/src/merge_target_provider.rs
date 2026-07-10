//! Streaming table provider over a pinned set of Iceberg data files, used as
//! the target relation of a copy-on-write MERGE (write-path memory safety,
//! Layer B phase B2).
//!
//! The default MERGE path reads the entire target table into a
//! `Vec<RecordBatch>` and registers it as a `MemTable`. That Vec is bounded by
//! the shared pool (Layer A `merge-target-buffer`), but it still materialises
//! the whole target. This provider instead scans exactly the captured
//! `old_data_files` lazily, one file at a time, so the target flows through the
//! merge join as governed DataFusion operator memory (pool-tracked, spillable
//! under subsystem A) instead of an invisible Vec.
//!
//! It reuses the target table's own `FileIO` (the same `file_io().read()` call
//! the buffered path uses via `read_parquet_via_table`), so no object-store or
//! credential re-wiring is involved. Each decoded batch is normalised to the
//! table's canonical Arrow schema so the declared provider schema and every
//! yielded batch are identical (`StreamingTableExec` validates per batch).
//!
//! Invariant: it scans the *captured* `old_data_files`, never a live snapshot,
//! so the set the join reads equals the set the rewrite deletes.
//!
//! See `docs/internal/specs/2026-07-02-write-path-memory-safety-design.md`.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::StreamExt;
use iceberg::io::FileIO;
use iceberg::spec::DataFile;
use iceberg::table::Table as IcebergTable;
use sqe_core::SqeError;

/// One partition streaming a pinned list of parquet data files through the
/// target table's `FileIO`, normalising each batch to `schema`.
pub struct MergeTargetPartition {
    schema: SchemaRef,
    file_io: FileIO,
    file_paths: Vec<String>,
}

impl fmt::Debug for MergeTargetPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeTargetPartition")
            .field("files", &self.file_paths.len())
            .field("schema", &self.schema)
            .finish()
    }
}

impl MergeTargetPartition {
    /// Build a partition over `file_paths`, reading through `file_io` and
    /// normalising every batch to `schema`.
    pub fn new(schema: SchemaRef, file_io: FileIO, file_paths: Vec<String>) -> Self {
        Self {
            schema,
            file_io,
            file_paths,
        }
    }
}

impl PartitionStream for MergeTargetPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = self.schema.clone();
        let file_io = self.file_io.clone();
        let paths = self.file_paths.clone();

        // Read one file at a time (O(one file) resident, not the whole target),
        // flattening its normalised batches into the output stream before
        // moving to the next file.
        let adapter_schema = schema.clone();
        let s = futures::stream::iter(paths)
            .then(move |path| {
                let file_io = file_io.clone();
                let schema = schema.clone();
                async move { read_file_normalised(file_io, path, schema).await }
            })
            .map(|res: Result<Vec<RecordBatch>, DataFusionError>| {
                let items: Vec<Result<RecordBatch, DataFusionError>> = match res {
                    Ok(batches) => batches.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                };
                futures::stream::iter(items)
            })
            .flatten();

        Box::pin(RecordBatchStreamAdapter::new(adapter_schema, s))
    }
}

/// Build a [`StreamingTable`] over `old_data_files` for use as the MERGE target
/// relation. `schema` is the table's canonical Arrow schema; every batch is
/// normalised to it.
pub fn merge_target_table(
    table: &IcebergTable,
    old_data_files: &[DataFile],
    schema: SchemaRef,
) -> Result<StreamingTable, SqeError> {
    let file_paths: Vec<String> = old_data_files
        .iter()
        .map(|d| d.file_path().to_string())
        .collect();
    let partition = MergeTargetPartition::new(schema.clone(), table.file_io().clone(), file_paths);
    StreamingTable::try_new(schema, vec![Arc::new(partition)])
        .map_err(|e| SqeError::Execution(format!("Failed to build MERGE target provider: {e}")))
}

fn to_df(e: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Read one data file and normalise every batch to `schema`.
async fn read_file_normalised(
    file_io: FileIO,
    path: String,
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let input = file_io.new_input(&path).map_err(to_df)?;
    let bytes = input.read().await.map_err(to_df)?;
    let reader = parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(bytes)
        .map_err(to_df)?
        .build()
        .map_err(to_df)?;
    let mut out = Vec::new();
    for item in reader {
        let batch = item.map_err(to_df)?;
        out.push(normalise_batch(batch, &schema)?);
    }
    Ok(out)
}

/// Re-key a decoded file batch onto the canonical target `schema`: cast any
/// column whose type differs by position, then rebuild the batch with the
/// exact `schema` Arc so the yielded `batch.schema()` matches the declared
/// provider schema (what `StreamingTableExec` validates against).
///
/// A column-count mismatch errors (the buffered MemTable path has the same
/// limitation), so schema evolution beyond type promotion is out of scope here.
fn normalise_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, DataFusionError> {
    let want = schema.fields().len();
    let got = batch.num_columns();
    if got != want {
        return Err(DataFusionError::Execution(format!(
            "MERGE target file column count {got} does not match table schema {want}"
        )));
    }
    let mut columns = Vec::with_capacity(want);
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        if col.data_type() == field.data_type() {
            columns.push(col.clone());
        } else {
            columns.push(arrow::compute::cast(col, field.data_type()).map_err(to_df)?);
        }
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(to_df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]))
    }

    fn batch(ids: &[i32], regions: &[&str]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(regions.to_vec())),
        ])
        .unwrap()
    }

    /// Write `batches` to `path` as a parquet file (local fs).
    fn write_parquet(path: &std::path::Path, batches: &[RecordBatch]) {
        let file = std::fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema(), Some(WriterProperties::builder().build()))
            .unwrap();
        for b in batches {
            w.write(b).unwrap();
        }
        w.close().unwrap();
    }

    #[test]
    fn normalise_batch_rebuilds_with_target_schema() {
        let b = batch(&[1, 2], &["US", "EU"]);
        let s = schema();
        let out = normalise_batch(b, &s).unwrap();
        // Same Arc → StreamingTableExec's per-batch validation is satisfied.
        assert!(Arc::ptr_eq(&out.schema(), &s));
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn normalise_batch_rejects_column_count_mismatch() {
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let err = normalise_batch(wrong, &schema()).unwrap_err();
        assert!(err.to_string().contains("column count"), "got: {err}");
    }

    #[tokio::test]
    async fn partition_streams_pinned_files_in_order() {
        let dir = TempDir::new().unwrap();
        let p1 = dir.path().join("f1.parquet");
        let p2 = dir.path().join("f2.parquet");
        write_parquet(&p1, &[batch(&[1, 2], &["US", "US"])]);
        write_parquet(&p2, &[batch(&[3], &["EU"])]);

        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        let part = MergeTargetPartition::new(
            schema(),
            file_io,
            vec![
                p1.to_str().unwrap().to_string(),
                p2.to_str().unwrap().to_string(),
            ],
        );

        let ctx = Arc::new(TaskContext::default());
        let mut stream = part.execute(ctx);
        let mut rows = 0;
        let mut ids = Vec::new();
        while let Some(b) = stream.next().await {
            let b = b.unwrap();
            // Every yielded batch carries the declared schema.
            assert_eq!(b.schema().fields().len(), 2);
            rows += b.num_rows();
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..col.len() {
                ids.push(col.value(i));
            }
        }
        assert_eq!(rows, 3, "all rows across the pinned files stream through");
        assert_eq!(ids, vec![1, 2, 3], "files stream in pinned order");
    }

    #[tokio::test]
    async fn empty_file_set_yields_nothing() {
        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        let part = MergeTargetPartition::new(schema(), file_io, vec![]);
        let mut stream = part.execute(Arc::new(TaskContext::default()));
        assert!(stream.next().await.is_none(), "no files → empty stream");
    }
}
