//! Streaming Iceberg data-file writer bridge shared by compaction rewrite
//! (`crate::rewrite::rewrite_group`) and, in `sqe-coordinator`, the ingest
//! write paths (`write_handler.rs`, `maintenance_log.rs`).
//!
//! Moved verbatim from `sqe-coordinator/src/writer.rs` (Phase 4c Task 1):
//! only the streaming write function and its direct helpers came along --
//! the non-streaming `write_data_files`/`write_data_files_with_metrics` and
//! the position/equality delete-file writers stayed in the coordinator
//! because nothing on the compaction path needs them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::compute::{cast, cast_with_options, CastOptions};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionKey, Schema as IcebergSchema, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, ZstdLevel};
use sqe_core::{SqeError, SqeErrorCode};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::write_memory::WriteReservation;

/// Shared list of every parquet file location the rolling writer has handed out
/// during a write. Used by [`WriteCleanupGuard`] to delete orphans when the
/// outer query future is cancelled mid-write before the Iceberg commit.
///
/// The vector is wrapped in `Arc<Mutex<>>` because the wrapping location
/// generator must be `Clone + Send + Sync + 'static` (the iceberg-rust trait
/// bound) and is cloned into multiple per-partition rolling writers. Each
/// `generate_location` call appends one entry; the guard drains the vector on
/// commit success or on drop without success.
pub type UploadedPaths = Arc<Mutex<Vec<String>>>;

/// Wraps [`DefaultLocationGenerator`] so every generated parquet path is
/// recorded in a shared [`UploadedPaths`] tracker. The rolling writer asks the
/// generator for a path before each file opens, so the tracker captures every
/// file that could possibly exist on S3 after a mid-write cancellation.
///
/// See [`WriteCleanupGuard`] for the orphan-cleanup semantics this enables.
#[derive(Clone, Debug)]
pub struct TrackingLocationGenerator {
    inner: DefaultLocationGenerator,
    tracker: UploadedPaths,
}

impl TrackingLocationGenerator {
    pub fn new(inner: DefaultLocationGenerator, tracker: UploadedPaths) -> Self {
        Self { inner, tracker }
    }
}

impl LocationGenerator for TrackingLocationGenerator {
    fn generate_location(&self, partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        let path = self.inner.generate_location(partition_key, file_name);
        if let Ok(mut paths) = self.tracker.lock() {
            paths.push(path.clone());
        }
        path
    }
}

/// Drop guard that deletes any parquet file the writer pushed to S3 if the
/// surrounding write future never reaches commit. Closes the gap left by
/// `tokio::time::timeout(write_future).await` and any other `Drop` of the
/// outer write task.
///
/// Usage: the caller creates the guard with the table's `FileIO` and the
/// shared `UploadedPaths` tracker, runs the streaming writer, and calls
/// [`Self::mark_committed`] once the Iceberg commit succeeds. Drop without
/// `mark_committed` spawns a best-effort tokio task that deletes every
/// recorded path so S3 is not littered with orphan parquet files. The cleanup
/// is best-effort because Drop cannot be async; we log when files cannot be
/// removed.
pub struct WriteCleanupGuard {
    file_io: FileIO,
    tracker: UploadedPaths,
    committed: AtomicBool,
    op: &'static str,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
}

impl WriteCleanupGuard {
    pub fn new(file_io: FileIO, tracker: UploadedPaths, op: &'static str) -> Self {
        Self {
            file_io,
            tracker,
            committed: AtomicBool::new(false),
            op,
            metrics: None,
        }
    }

    /// Attach the metrics registry so orphan cleanup emits
    /// `sqe_write_orphan_files_total{op,outcome}` (COORD-06). A `None` registry
    /// (e.g. the maintenance path) leaves the counter unset; the error-level
    /// log line remains the alerting signal in that case.
    #[must_use = "with_metrics returns the guard; bind the returned value"]
    pub fn with_metrics(mut self, metrics: Option<Arc<sqe_metrics::MetricsRegistry>>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Signal that the Iceberg commit has succeeded; the guard's Drop becomes
    /// a no-op. Must be called once the catalog commit lands; otherwise the
    /// guard treats the write as cancelled and removes the parquet files.
    pub fn mark_committed(&self) {
        self.committed.store(true, Ordering::Release);
    }
}

impl Drop for WriteCleanupGuard {
    fn drop(&mut self) {
        if self.committed.load(Ordering::Acquire) {
            return;
        }
        let paths: Vec<String> = match self.tracker.lock() {
            Ok(mut g) => std::mem::take(&mut *g),
            Err(_) => return,
        };
        if paths.is_empty() {
            return;
        }
        let file_io = self.file_io.clone();
        let op = self.op;
        let count = paths.len();
        let metrics = self.metrics.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                warn!(
                    op,
                    orphan_count = count,
                    "write cancelled before commit; deleting orphan parquet files"
                );
                // COORD-06: count delete failures so operators can detect S3
                // orphan accumulation (paid-for, never-queried storage). The
                // per-file warns are easy to lose; the error-level summary with
                // a stable `leaked` count is alertable, and the
                // `sqe_write_orphan_files_total{op,outcome}` counter lets a
                // periodic `remove_orphan_files` procedure reconcile leaks.
                let mut leaked = 0usize;
                for p in paths {
                    if let Err(e) = file_io.delete(&p).await {
                        warn!(op, path = %p, error = %e, "orphan cleanup: delete failed");
                        leaked += 1;
                    }
                }
                if let Some(m) = &metrics {
                    let deleted = count - leaked;
                    if deleted > 0 {
                        m.write_orphan_files_total
                            .with_label_values(&[op, "deleted"])
                            .inc_by(deleted as u64);
                    }
                    if leaked > 0 {
                        m.write_orphan_files_total
                            .with_label_values(&[op, "leaked"])
                            .inc_by(leaked as u64);
                    }
                }
                if leaked > 0 {
                    error!(
                        op,
                        leaked,
                        attempted = count,
                        "orphan cleanup: {leaked} parquet file(s) could not be deleted and \
                         remain on S3 as uncommitted orphans; reconcile via remove_orphan_files"
                    );
                }
            });
        } else {
            // No runtime at drop -> every file is left behind. Surface at error
            // level (COORD-06): these are guaranteed orphans, not best-effort.
            if let Some(m) = &metrics {
                m.write_orphan_files_total
                    .with_label_values(&[op, "leaked"])
                    .inc_by(count as u64);
            }
            error!(
                op,
                orphan_count = paths.len(),
                "write cancelled outside tokio runtime; orphan parquet files left on S3 \
                 (reconcile via remove_orphan_files)"
            );
        }
    }
}

/// When a table is logically unpartitioned (no fields, or all-Void) but its
/// default partition spec is not the original `spec_id == 0`, the data files
/// must still be stamped with the current default spec id so the catalog
/// commit succeeds. Returns `None` for the canonical `spec_id == 0` case so
/// the writer keeps its previous fast-path behaviour. For evolved unpartitioned
/// specs, returns a synthetic `PartitionKey` with empty struct data and the
/// current spec attached.
pub fn unpartitioned_spec_key(
    table: &Table,
    partition_spec: &iceberg::spec::PartitionSpecRef,
) -> Option<PartitionKey> {
    if partition_spec.spec_id() == 0 {
        return None;
    }
    Some(PartitionKey::new(
        (**partition_spec).clone(),
        table.metadata().current_schema().clone(),
        Struct::empty(),
    ))
}

/// Parse a compression config string into a Parquet `Compression` value.
///
/// Supported values (case-insensitive): `"zstd"`, `"lz4"`, `"snappy"`, `"none"`.
/// Defaults to ZSTD(3) if the value is unrecognised.
pub fn parse_parquet_compression(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
        "lz4" => Compression::LZ4_RAW,
        "snappy" => Compression::SNAPPY,
        "none" => Compression::UNCOMPRESSED,
        _ => {
            tracing::warn!(
                compression = s,
                "Unknown parquet compression '{}', defaulting to ZSTD(3)",
                s
            );
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        }
    }
}

/// Write data files from a [`SendableRecordBatchStream`] (streaming, constant memory).
///
/// Instead of buffering all batches in memory, writes each batch to the Parquet
/// writer as it arrives from the DataFusion execution stream. This is critical
/// for large CTAS / INSERT loads (millions of rows) where `df.collect()` would OOM.
///
/// The caller must supply the Iceberg schema upfront (from `df.schema()`) so the
/// table can be created and the Parquet writer initialised before the first batch
/// arrives. Field IDs and type casts are applied per-batch.
///
/// Returns `(data_files, total_rows)` on success. The caller registers the
/// `tracker`'s paths with a [`WriteCleanupGuard`] so a cancellation between
/// the last batch and the Iceberg commit deletes the parquet files instead of
/// orphaning them on S3 (#58).
///
/// `progress`, when `Some`, is ticked once per batch via
/// [`ProgressReporter::on_batch`] with the cumulative row count so far. This
/// is the only fine-grained hook into an otherwise single `.await`-per-batch
/// loop; `sqe-worker`'s `compact_file_group` action uses it to emit
/// `CompactGroupFrame::Progress` frames mid-rewrite so the coordinator's
/// `group_heartbeat_timeout` bounds a stalled read/write, not just frame
/// delivery. Every other caller (the coordinator's own ingest/CTAS/rewrite
/// paths) passes `None` and is byte-for-byte unaffected.
#[allow(clippy::too_many_arguments)]
pub async fn write_data_files_streaming(
    table: &Table,
    mut stream: SendableRecordBatchStream,
    file_prefix: &str,
    compression: Compression,
    tracker: UploadedPaths,
    fanout: FanoutLimits,
    target_file_size: Option<u64>,
    mut progress: Option<crate::progress::ProgressReporter>,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let inner_loc = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Location generator error: {e}")))?;
    let location_generator = TrackingLocationGenerator::new(inner_loc, tracker);

    let _ = file_prefix; // kept for parity with non-streaming API; not used in file names
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    // V3 row lineage: a rewrite that read `_row_id` / `_last_updated_sequence_number`
    // (see the delete-aware read plan) must persist them, so the data file's
    // schema is the table schema plus the two reserved lineage fields. Inserts
    // carry neither and write the table schema unchanged.
    let lineage = carries_row_lineage(table, &stream.schema());
    let write_schema = if lineage {
        schema_with_row_lineage(table.metadata().current_schema())?
    } else {
        table.metadata().current_schema().clone()
    };

    // Byte-identical to the coordinator's `writer_props_for_table` wrapper
    // (a 1-line forwarder): call the shared helper directly rather than
    // duplicating that wrapper across the crate boundary.
    let parquet_writer_builder = ParquetWriterBuilder::new(
        crate::parquet_writer_config::writer_props_for_table(table, compression),
        write_schema.clone(),
    );

    // Roll output at the caller's requested size when given (compaction pins
    // this to `target_file_size_bytes` so a globally-sorted stream is cut into
    // multiple files with disjoint key ranges). Otherwise fall back to the
    // Iceberg default target size (the ingest paths take this branch).
    let rolling_writer_builder = match target_file_size {
        Some(size) => RollingFileWriterBuilder::new(
            parquet_writer_builder,
            size as usize,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        ),
        None => RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        ),
    };

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // Build the stamped schema once from the Iceberg schema so it can be reused
    // for every batch without re-deriving it on each iteration.
    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let stamped_schema = build_stamped_schema(&write_schema)?;
    let partition_spec = metadata.default_partition_spec().clone();
    // Stamping is positional: put the lineage columns last, in schema order.
    let stamp = |batch: RecordBatch| -> sqe_core::Result<RecordBatch> {
        let batch = if lineage { lineage_columns_last(batch)? } else { batch };
        let batch = apply_stamped_schema(batch, &stamped_schema)?;
        check_required_columns(&batch, &write_schema)?;
        Ok(batch)
    };

    let mut total_rows = 0usize;

    let data_files = if partition_spec.is_unpartitioned() {
        // Fast path: unpartitioned tables stream straight into a
        // DataFileWriter. See `write_data_files` (coordinator) for why we
        // still need a synthetic empty PartitionKey when `spec_id != 0`.
        let partition_key = unpartitioned_spec_key(table, &partition_spec);
        let mut writer = data_file_writer_builder
            .build(partition_key)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build data file writer: {e}")))?;

        while let Some(batch_result) = stream.next().await {
            let batch =
                batch_result.map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Stream error: {e}")))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let stamped = stamp(batch)?;
            total_rows += stamped.num_rows();
            writer
                .write(stamped)
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Write error: {e}")))?;
            if let Some(p) = progress.as_mut() {
                p.on_batch(total_rows as u64);
            }
        }

        if total_rows == 0 {
            // Propagate close errors even on the empty-write path: a failed
            // close can signal a file build/flush problem we must not mask.
            writer.close().await.map_err(|e| {
                SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close writer error (empty write): {e}"))
            })?;
            return Ok((vec![], 0));
        }

        writer
            .close()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close writer error: {e}")))?
    } else {
        // Partitioned streaming. The splitter computes partition values from
        // source columns at runtime so the input stream does not need a
        // pre-stamped `_partition` column.
        use iceberg::arrow::RecordBatchPartitionSplitter;
        use iceberg::writer::task_writer::TaskWriter;
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            iceberg_schema.clone(),
            partition_spec.clone(),
        )
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build partition splitter: {e}")))?;

        if fanout.is_bounded() {
            // Memory-bounded path: SQE's BoundedFanoutWriter caps open writers
            // and buffered bytes, cutting over least-recently-written writers.
            // This is the default for coordinator writes (caps auto-derived
            // from the pool when unset); only `write_buffer_tracking = false`
            // and compaction rewrites take the unbounded TaskWriter path below.
            let reservation = fanout
                .pool
                .as_ref()
                .map(|pool| WriteReservation::new(pool, "fanout-buffer"));
            let mut writer = BoundedFanoutWriter::new(
                data_file_writer_builder,
                splitter,
                fanout.max_open,
                fanout.byte_budget,
                reservation,
            );
            while let Some(batch_result) = stream.next().await {
                let batch =
                    batch_result.map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Stream error: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let stamped = stamp(batch)?;
                total_rows += stamped.num_rows();
                writer.write(stamped).await?;
                if let Some(p) = progress.as_mut() {
                    p.on_batch(total_rows as u64);
                }
            }
            let cutovers = writer.cutovers();
            let data_files = writer.close().await?;
            if cutovers > 0 {
                info!(
                    cutovers,
                    file_count = data_files.len(),
                    "Bounded fanout cut over open writers (small-file debt; \
                     repair with system.rewrite_data_files)"
                );
            }
            data_files
        } else {
            // Default unbounded path: TaskWriter with fanout enabled so
            // unsorted streams work, one open writer per partition value.
            let mut writer = TaskWriter::new_with_partition_splitter(
                data_file_writer_builder,
                true,
                iceberg_schema.clone(),
                partition_spec,
                Some(splitter),
            );

            while let Some(batch_result) = stream.next().await {
                let batch =
                    batch_result.map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Stream error: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let stamped = stamp(batch)?;
                total_rows += stamped.num_rows();
                writer
                    .write(stamped)
                    .await
                    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Partitioned write error: {e}")))?;
                if let Some(p) = progress.as_mut() {
                    p.on_batch(total_rows as u64);
                }
            }

            if total_rows == 0 {
                // Propagate close errors even on the empty-write path: a failed
                // close can signal a file build/flush problem we must not mask.
                writer.close().await.map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "Close partitioned writer error (empty write): {e}"
                    ))
                })?;
                return Ok((vec![], 0));
            }

            writer
                .close()
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close partitioned writer error: {e}")))?
        }
    };

    info!(
        file_count = data_files.len(),
        total_rows, file_prefix, "Data files written successfully (streaming)"
    );

    Ok((data_files, total_rows))
}

/// Whether a write into `table` must persist V3 row lineage: the table is V3
/// and the incoming batches carry `_row_id` (a rewrite read selected it). A
/// table column that happens to use the reserved name is not lineage.
pub fn carries_row_lineage(table: &Table, input: &ArrowSchema) -> bool {
    use iceberg::metadata_columns::RESERVED_COL_NAME_ROW_ID;
    table.metadata().format_version() >= iceberg::spec::FormatVersion::V3
        && input.column_with_name(RESERVED_COL_NAME_ROW_ID).is_some()
        && table
            .metadata()
            .current_schema()
            .field_by_name(RESERVED_COL_NAME_ROW_ID)
            .is_none()
}

/// `schema` plus the two reserved V3 row-lineage fields, for a data file's
/// Parquet schema only (the table's schema never lists them).
pub fn schema_with_row_lineage(
    schema: &iceberg::spec::SchemaRef,
) -> sqe_core::Result<iceberg::spec::SchemaRef> {
    use iceberg::metadata_columns::{last_updated_sequence_number_field, row_id_field};
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .cloned()
        .chain([
            row_id_field().clone(),
            last_updated_sequence_number_field().clone(),
        ]);
    IcebergSchema::builder()
        .with_schema_id(schema.schema_id())
        .with_identifier_field_ids(schema.identifier_field_ids())
        .with_fields(fields)
        .build()
        .map(Arc::new)
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build row-lineage write schema: {e}"),
            )
        })
}

/// Decodes the row-lineage columns of a rewrite read into plain `Int64`.
///
/// The reader materializes `_last_updated_sequence_number` for files that do
/// not store it as a per-file constant, run-end encoded. DataFusion expressions
/// (UPDATE / MERGE CASE projections) and the writer's stamping expect a plain
/// long, so rewrite reads decode it once, right after the scan.
pub fn decode_lineage_columns(batch: RecordBatch) -> sqe_core::Result<RecordBatch> {
    use iceberg::metadata_columns::{
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    };
    let schema = batch.schema();
    let needs_decode = schema.fields().iter().any(|f| {
        (f.name() == RESERVED_COL_NAME_ROW_ID
            || f.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)
            && f.data_type() != &arrow_schema::DataType::Int64
    });
    if !needs_decode {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        let is_lineage = field.name() == RESERVED_COL_NAME_ROW_ID
            || field.name() == RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER;
        if is_lineage && field.data_type() != &arrow_schema::DataType::Int64 {
            let decoded = cast(batch.column(idx), &arrow_schema::DataType::Int64).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to decode row lineage column {}: {e}", field.name()),
                )
            })?;
            fields.push(Arc::new(
                arrow_schema::Field::new(field.name(), arrow_schema::DataType::Int64, true)
                    .with_metadata(field.metadata().clone()),
            ));
            columns.push(decoded);
        } else {
            fields.push(field.clone());
            columns.push(batch.column(idx).clone());
        }
    }
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new_with_metadata(fields, schema.metadata().clone())),
        columns,
    )
    .map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Failed to decode row lineage columns: {e}"),
        )
    })
}

/// Reorders a batch so the lineage columns come last as `_row_id`,
/// `_last_updated_sequence_number`, matching [`schema_with_row_lineage`]. A
/// missing lineage column is filled with nulls (new rows: inheritance assigns
/// both at commit).
pub fn lineage_columns_last(batch: RecordBatch) -> sqe_core::Result<RecordBatch> {
    use arrow_array::{ArrayRef, Int64Array};
    use iceberg::metadata_columns::{
        RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, RESERVED_COL_NAME_ROW_ID,
    };
    let schema = batch.schema();
    let lineage_col = |name: &str| -> sqe_core::Result<ArrayRef> {
        match schema.index_of(name) {
            Ok(idx) => cast(batch.column(idx), &arrow_schema::DataType::Int64).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("row lineage column {name} is not a long: {e}"),
                )
            }),
            Err(_) => Ok(Arc::new(Int64Array::new_null(batch.num_rows()))),
        }
    };
    let row_id = lineage_col(RESERVED_COL_NAME_ROW_ID)?;
    let last_updated = lineage_col(RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER)?;
    let mut fields = Vec::with_capacity(schema.fields().len() + 2);
    let mut columns = Vec::with_capacity(schema.fields().len() + 2);
    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().as_str();
        if name != RESERVED_COL_NAME_ROW_ID && name != RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER
        {
            fields.push(field.clone());
            columns.push(batch.column(idx).clone());
        }
    }
    for (name, col) in [
        (RESERVED_COL_NAME_ROW_ID, row_id),
        (RESERVED_COL_NAME_LAST_UPDATED_SEQUENCE_NUMBER, last_updated),
    ] {
        fields.push(Arc::new(arrow_schema::Field::new(
            name,
            arrow_schema::DataType::Int64,
            true,
        )));
        columns.push(col);
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).map_err(|e| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Failed to reorder row lineage columns: {e}"),
        )
    })
}

/// Allocate a fresh tracker for a streaming write. Bundled here so callers
/// don't need to know the inner representation.
pub fn new_upload_tracker() -> UploadedPaths {
    Arc::new(Mutex::new(Vec::new()))
}

/// Build a stamped Arrow schema from an Iceberg schema.
///
/// The one field-id stamping implementation (#600): derive the target schema
/// once, then apply it to every batch via [`apply_stamped_schema`] or
/// [`stamp_batches`]. Every column is nullable (the safe default for
/// Iceberg, and it needs no cross-batch null scan) and types come from the
/// Iceberg schema, so a type-mapping fix lands here once for the streaming
/// writes, the buffered writes, the compaction rewrite and the delete key
/// files alike.
pub fn build_stamped_schema(
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Arc<ArrowSchema>> {
    let expected_arrow_schema = schema_to_arrow_schema(iceberg_schema)
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to derive expected Arrow schema: {e}")))?;

    let iceberg_fields = iceberg_schema.as_struct().fields();
    let new_fields: Vec<Arc<arrow_schema::Field>> = expected_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, arrow_field)| {
            // `expected_arrow_schema` is derived from `iceberg_schema`, so this
            // lookup cannot miss. It used to fall back to a fabricated
            // `(i + 1)` id, which would stamp a parquet column with an id that
            // belongs to no Iceberg field (505). A missing target field is an
            // error, never an invented id.
            let field_id = iceberg_fields.get(i).map(|f| f.id).ok_or_else(|| {
                SqeError::Execution(format!(
                    "internal error: stamped schema column {i} ('{}') has no \
                     Iceberg field (schema has {} fields)",
                    arrow_field.name(),
                    iceberg_fields.len()
                ))
            })?;
            let mut meta = arrow_field.metadata().clone();
            meta.insert("PARQUET:field_id".to_string(), field_id.to_string());
            // Mark all columns nullable. Safe default; avoids cross-batch null scan.
            Ok(Arc::new(
                arrow_schema::Field::new(arrow_field.name(), arrow_field.data_type().clone(), true)
                    .with_metadata(meta),
            ))
        })
        .collect::<sqe_core::Result<Vec<_>>>()?;

    Ok(Arc::new(ArrowSchema::new(new_fields)))
}

/// Apply a pre-built stamped schema to a single [`RecordBatch`].
///
/// Casts columns whose type differs from the target schema (e.g. Timestamp(ns)
/// -> Timestamp(µs)) and re-wraps the batch with the stamped schema.
///
/// Arity is checked first (505). `ArrowSchema::field(i)` is `&self.fields[i]`
/// and panics past the end, so a batch wider than the target used to abort the
/// write task mid-stream — the client saw a generic internal error and the log
/// showed a panic instead of a SQL-level message. A mismatch on either side is
/// a user error (`INSERT INTO t2 SELECT a, b, c` against a two-column `t2`) and
/// is reported as one.
pub fn apply_stamped_schema(
    batch: RecordBatch,
    stamped_schema: &Arc<ArrowSchema>,
) -> sqe_core::Result<RecordBatch> {
    if batch.num_columns() != stamped_schema.fields().len() {
        return Err(SqeError::Execution(format!(
            "INSERT has {} columns but target has {}",
            batch.num_columns(),
            stamped_schema.fields().len()
        )));
    }
    let new_columns: Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let target = stamped_schema.field(i).data_type();
            if col.data_type() == target {
                Ok(col.clone())
            } else {
                // Checked, not safe (#834): a safe cast turns 'abc' -> Int64
                // into NULL, so the INSERT succeeded and stored a value the
                // user never wrote.
                cast_with_options(col, target, &CastOptions { safe: false, ..Default::default() }).map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
                        "Failed to cast column '{}' from {:?} to {:?}: {e}",
                        stamped_schema.field(i).name(),
                        col.data_type(),
                        target,
                    ))
                })
            }
        })
        .collect();
    RecordBatch::try_new(stamped_schema.clone(), new_columns?)
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to apply stamped schema: {e}")))
}

/// Stamp a whole batch list against `iceberg_schema` (#600): the buffered
/// callers' form of [`apply_stamped_schema`]. An empty list is returned as is.
pub fn stamp_batches(
    batches: Vec<RecordBatch>,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let stamped_schema = build_stamped_schema(iceberg_schema)?;
    batches
        .into_iter()
        .map(|batch| {
            let batch = apply_stamped_schema(batch, &stamped_schema)?;
            check_required_columns(&batch, iceberg_schema)?;
            Ok(batch)
        })
        .collect()
}

/// Refuse a NULL in a required top-level column (#834). The stamped schema
/// is all-nullable, and the parquet writer stores the zeroed value slot of a
/// NULL in a required column, so `INSERT ... VALUES (NULL)` into a NOT NULL
/// column used to succeed and read back as 0. `null_count` is cached on the
/// array, so the check costs nothing per row.
pub fn check_required_columns(
    batch: &RecordBatch,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<()> {
    for (field, col) in iceberg_schema.as_struct().fields().iter().zip(batch.columns()) {
        if field.required && col.null_count() > 0 {
            return Err(SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("NULL value not allowed for NOT NULL column: {}", field.name),
            ));
        }
    }
    Ok(())
}

/// Resolved fanout limits for a partitioned streaming write. Both `0` means
/// unbounded: the write takes the vendored `TaskWriter` path, byte-for-byte
/// unchanged. A non-zero value in either field switches the write to
/// [`BoundedFanoutWriter`]. A `0` in one field while the other is set means
/// that particular limit is disabled (e.g. cap the open-writer count but impose
/// no byte budget).
///
/// `pool`, when set, makes the bounded writer register a `fanout-buffer`
/// reservation so the buffered partition state is charged to the shared
/// memory pool alongside operator memory (security/scale review 905). A
/// denied reservation first cuts over open writers and, if nothing is left to
/// evict, fails the write with a typed resource error.
#[derive(Clone)]
pub struct FanoutLimits {
    /// Max concurrently open partition writers (`0` = no cap).
    pub max_open: usize,
    /// Total buffered-byte budget across open writers (`0` = no budget).
    pub byte_budget: usize,
    /// Shared pool to charge buffered fanout bytes to (`None` = untracked).
    pub pool: Option<Arc<dyn MemoryPool>>,
}

impl std::fmt::Debug for FanoutLimits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutLimits")
            .field("max_open", &self.max_open)
            .field("byte_budget", &self.byte_budget)
            .field("pool", &self.pool.is_some())
            .finish()
    }
}

impl FanoutLimits {
    /// No limits, unbounded `TaskWriter` path (compaction rewrites and the
    /// `write_buffer_tracking = false` escape hatch).
    pub const fn unbounded() -> Self {
        Self {
            max_open: 0,
            byte_budget: 0,
            pool: None,
        }
    }

    /// Charge buffered fanout bytes to `pool` (bounded mode only).
    pub fn with_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    /// True when at least one limit is active (selects [`BoundedFanoutWriter`]).
    pub fn is_bounded(&self) -> bool {
        self.max_open > 0 || self.byte_budget > 0
    }
}

/// Rough per-open-writer resident estimate: one parquet row group buffered
/// before it flushes to the multipart upload. Used only to auto-derive the
/// fanout caps from the pool size. Provisional (spec open decision #4, pending
/// measurement of real row-group residency).
const FANOUT_PER_WRITER_EST_BYTES: usize = 128 * 1024 * 1024;

/// Auto-derive `(max_open, byte_budget)` from the node memory pool size, used
/// to fill whichever fanout knob an operator left at 0 once bounded mode is
/// active. `max_open` is one writer per estimated row group the pool holds
/// (floor 8, ceiling 1024) and the budget is a pool fraction floored at one
/// writer. See spec open decision #4.
///
/// The ceiling was 64, on the reasoning that it "sits above any realistic
/// partition count". It does not: TPC-H's monthly partitions are 84, and an
/// UNSORTED partitioned CTAS touches every partition in every batch, so a
/// 64-writer cap cut over constantly. TPC-H SF0.1 lineitem (600k rows) was
/// written as 3,769 files instead of ~84 and full scans of it ran 2-3x
/// slower (2026-09-27). Memory is already bounded by `byte_budget`, which
/// flushes writers least-recently-written first; the count cap only has to
/// stay within what the pool can hold, not below the partition count.
pub fn auto_fanout_caps(pool_bytes: usize) -> (usize, usize) {
    let max_open = (pool_bytes / FANOUT_PER_WRITER_EST_BYTES).clamp(8, 1024);
    let byte_budget = (pool_bytes / 8).max(FANOUT_PER_WRITER_EST_BYTES);
    (max_open, byte_budget)
}

/// A memory-bounded partitioned writer (write-path memory safety, Layer B).
///
/// Like the vendored [`FanoutWriter`](iceberg::writer::partitioning::fanout_writer)
/// it keeps one open rolling writer per distinct partition value, but with two
/// limits:
///
/// - `max_open`: cap on concurrently open partition writers.
/// - `byte_budget`: total estimated buffered bytes across open writers.
///
/// When either limit would be exceeded, the least-recently-written open writer
/// is closed and flushed first ("cutover"), its `DataFile`s collected. A
/// partition that receives more rows after its writer was cut simply gets a
/// fresh writer and another file. Passing `0` for a limit disables it.
///
/// Cutover is correct by construction: Iceberg permits any number of data files
/// per partition. It trades bounded memory for small-file debt, repaired by the
/// existing `system.rewrite_data_files` procedure. The response to a
/// cutover-heavy write is a counter (`cutovers()`), not a failure.
///
/// The byte estimate accumulates [`RecordBatch::get_array_memory_size`] per
/// partition since that partition's writer was (re)opened. Arrow size exceeds
/// encoded parquet size, so it overstates, which is the safe direction for a
/// budget. When a `fanout-buffer` [`WriteReservation`] is supplied the estimate
/// is reserved on the shared pool before each partition write: a denial cuts
/// over other open writers first and fails the write once nothing is left to
/// evict, so fanout memory is bounded by the pool, not only by `byte_budget`.
///
/// Wired into the streaming write path ([`write_data_files_streaming`]) behind
/// the [`FanoutLimits`] gate: the coordinator's default for partitioned writes
/// (caps auto-derived from the pool when the `fanout_*` knobs are 0); only
/// `write_buffer_tracking = false` selects the unbounded path. The file-level
/// behaviour (splitting, cutover, `DataFile` completeness) is covered by the
/// local `fs_io` tests in this module; the end-to-end Iceberg commit of cutover
/// output still needs a live catalog to validate.
pub struct BoundedFanoutWriter<B: IcebergWriterBuilder> {
    inner_builder: B,
    splitter: iceberg::arrow::RecordBatchPartitionSplitter,
    open: HashMap<Struct, B::R>,
    est_bytes: HashMap<Struct, usize>,
    /// Monotonic "last written" tick per open partition, for LRW eviction.
    last_written: HashMap<Struct, u64>,
    tick: u64,
    output: Vec<DataFile>,
    max_open: usize,
    byte_budget: usize,
    total_bytes: usize,
    cutovers: u64,
    reservation: Option<WriteReservation>,
}

impl<B: IcebergWriterBuilder> BoundedFanoutWriter<B> {
    /// Create a bounded fanout writer. `max_open` / `byte_budget` of `0`
    /// disable the respective limit. `reservation`, when supplied, is resized
    /// to the running byte estimate for pool visibility (best-effort).
    pub fn new(
        inner_builder: B,
        splitter: iceberg::arrow::RecordBatchPartitionSplitter,
        max_open: usize,
        byte_budget: usize,
        reservation: Option<WriteReservation>,
    ) -> Self {
        Self {
            inner_builder,
            splitter,
            open: HashMap::new(),
            est_bytes: HashMap::new(),
            last_written: HashMap::new(),
            tick: 0,
            output: Vec::new(),
            max_open,
            byte_budget,
            total_bytes: 0,
            cutovers: 0,
            reservation,
        }
    }

    /// Number of cutovers performed (extra files beyond one-per-partition).
    pub fn cutovers(&self) -> u64 {
        self.cutovers
    }

    /// Split `batch` per partition and route each sub-batch to its partition
    /// writer, cutting over least-recently-written writers as needed to honour
    /// `max_open` and `byte_budget`.
    pub async fn write(&mut self, batch: RecordBatch) -> sqe_core::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let partitioned = self
            .splitter
            .split(&batch)
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Partition split error: {e}")))?;

        for (partition_key, part_batch) in partitioned {
            if part_batch.num_rows() == 0 {
                continue;
            }
            let key = partition_key.data().clone();
            let bytes = part_batch.get_array_memory_size();

            // Cap: opening a new partition when the map is full evicts the
            // least-recently-written writer(s) first.
            if self.max_open > 0 && !self.open.contains_key(&key) {
                while self.open.len() >= self.max_open {
                    if !self.cut_over_lrw(Some(&key)).await? {
                        break;
                    }
                }
            }

            // Budget: evict other partitions until this write fits. Never evict
            // the partition being written (it would only reopen). If it is the
            // sole open partition, accept the overflow: one writer at full
            // row-group size must always fit.
            if self.byte_budget > 0 {
                while self.total_bytes + bytes > self.byte_budget {
                    if !self.cut_over_lrw(Some(&key)).await? {
                        break;
                    }
                }
            }

            // Shared pool: the estimate after this write must fit alongside
            // everything else the pool governs. A denial evicts other open
            // writers first; when only this partition remains the write fails
            // with a typed resource error rather than growing untracked.
            loop {
                let attempt = self
                    .reservation
                    .as_mut()
                    .map(|res| res.try_resize(self.total_bytes + bytes));
                match attempt {
                    None | Some(Ok(())) => break,
                    Some(Err(denied)) => {
                        if !self.cut_over_lrw(Some(&key)).await? {
                            return Err(denied);
                        }
                    }
                }
            }

            self.get_or_open(&partition_key)
                .await?
                .write(part_batch)
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Partitioned write error: {e}")))?;

            self.total_bytes += bytes;
            *self.est_bytes.entry(key.clone()).or_insert(0) += bytes;
            self.tick += 1;
            self.last_written.insert(key, self.tick);
        }
        Ok(())
    }

    /// Close every open writer and return all `DataFile`s (from cutovers and
    /// the final flush).
    pub async fn close(mut self) -> sqe_core::Result<Vec<DataFile>> {
        let keys: Vec<Struct> = self.open.keys().cloned().collect();
        for key in keys {
            if let Some(mut writer) = self.open.remove(&key) {
                let files = writer.close().await.map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close partition writer error: {e}"))
                })?;
                self.output.extend(files);
            }
        }
        Ok(self.output)
    }

    /// Get the open writer for `partition_key`, building a fresh one if this
    /// partition has no open writer (first write, or after a cutover).
    async fn get_or_open(&mut self, partition_key: &PartitionKey) -> sqe_core::Result<&mut B::R> {
        let key = partition_key.data().clone();
        if !self.open.contains_key(&key) {
            let writer = self
                .inner_builder
                .build(Some(partition_key.clone()))
                .await
                .map_err(|e| {
                    SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build partition writer: {e}"))
                })?;
            self.open.insert(key.clone(), writer);
        }
        self.open
            .get_mut(&key)
            .ok_or_else(|| SqeError::with_code(SqeErrorCode::ExecutionFailed, "partition writer missing after build"))
    }

    /// Close and flush the least-recently-written open writer (never `protect`),
    /// collecting its `DataFile`s. Returns `false` when there is nothing left to
    /// evict (empty, or only `protect` remains).
    async fn cut_over_lrw(&mut self, protect: Option<&Struct>) -> sqe_core::Result<bool> {
        let victim = self
            .open
            .keys()
            .filter(|k| protect != Some(*k))
            .min_by_key(|k| self.last_written.get(*k).copied().unwrap_or(0))
            .cloned();
        let Some(key) = victim else {
            return Ok(false);
        };
        if let Some(mut writer) = self.open.remove(&key) {
            let files = writer
                .close()
                .await
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Cutover close error: {e}")))?;
            self.output.extend(files);
        }
        let freed = self.est_bytes.remove(&key).unwrap_or(0);
        self.total_bytes = self.total_bytes.saturating_sub(freed);
        self.last_written.remove(&key);
        self.cutovers += 1;
        if let Some(res) = self.reservation.as_mut() {
            // Shrinking never fails; keep the pool in step with the estimate.
            let excess = res.size().saturating_sub(self.total_bytes);
            res.shrink(excess);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reader emits `_last_updated_sequence_number` as a run-end encoded
    /// constant; the rewrite paths need plain longs, placed last for stamping.
    #[test]
    fn lineage_columns_decode_and_move_last() {
        use arrow_array::{Array, Int32Array, Int64Array, RunArray};
        let ree = RunArray::try_new(&Int32Array::from(vec![3]), &Int64Array::from(vec![7])).unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow_schema::Field::new("_last_updated_sequence_number", ree.data_type().clone(), true),
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            arrow_schema::Field::new("_row_id", arrow_schema::DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ree),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(12)])),
            ],
        )
        .unwrap();
        let out = lineage_columns_last(decode_lineage_columns(batch).unwrap()).unwrap();
        let out_schema = out.schema();
        let names: Vec<&str> = out_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "_row_id", "_last_updated_sequence_number"]);
        let seq = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(seq.values(), &[7, 7, 7]);
        assert!(out.column(1).is_null(1), "a null _row_id stays null (inherited at commit)");
    }
    use std::sync::Arc;

    // ---- apply_stamped_schema arity (505) ------------------------------
    //
    // `ArrowSchema::field(i)` is `&self.fields[i]` and panics past the end, so
    // a batch wider than the target used to abort the write task from inside a
    // tokio task: the client saw a generic internal error, the log showed a
    // panic, and the `WriteCleanupGuard` ran from `Drop` during unwinding.

    fn arity_batch(cols: usize) -> RecordBatch {
        let fields: Vec<arrow_schema::Field> = (0..cols)
            .map(|i| arrow_schema::Field::new(format!("c{i}"), arrow_schema::DataType::Int32, true))
            .collect();
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        let columns: Vec<arrow_array::ArrayRef> = (0..cols)
            .map(|i| {
                Arc::new(arrow_array::Int32Array::from(vec![i as i32])) as arrow_array::ArrayRef
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn arity_schema(cols: usize) -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(
            (0..cols)
                .map(|i| {
                    arrow_schema::Field::new(format!("c{i}"), arrow_schema::DataType::Int32, true)
                })
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn apply_stamped_schema_rejects_wider_batch() {
        let err = apply_stamped_schema(arity_batch(3), &arity_schema(2))
            .expect_err("a 3-column batch against a 2-field schema must be an error, not a panic");
        assert!(
            format!("{err}").contains("INSERT has 3 columns but target has 2"),
            "err: {err}"
        );
    }

    #[test]
    fn apply_stamped_schema_rejects_narrower_batch() {
        let err = apply_stamped_schema(arity_batch(1), &arity_schema(2)).expect_err("narrower");
        assert!(
            format!("{err}").contains("INSERT has 1 columns but target has 2"),
            "err: {err}"
        );
    }

    #[test]
    fn apply_stamped_schema_accepts_matching_arity() {
        let out = apply_stamped_schema(arity_batch(2), &arity_schema(2)).expect("matching arity");
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.num_rows(), 1);
    }

    /// #834: a value the target type cannot hold fails the write. A safe cast
    /// turned 'abc' into NULL, which a required column then stored as 0.
    #[test]
    fn apply_stamped_schema_rejects_unparseable_value() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
                "n",
                arrow_schema::DataType::Utf8,
                false,
            )])),
            vec![Arc::new(arrow_array::StringArray::from(vec!["7", "abc"]))],
        )
        .unwrap();
        for nullable in [false, true] {
            let target = Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
                "n",
                arrow_schema::DataType::Int64,
                nullable,
            )]));
            let err = apply_stamped_schema(batch.clone(), &target)
                .expect_err("'abc' is not an Int64");
            assert!(format!("{err}").contains("Failed to cast column 'n'"), "err: {err}");
        }
    }

    /// The checked cast still converts what fits, and a NULL stays NULL.
    #[test]
    fn apply_stamped_schema_casts_valid_values() {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
                "n",
                arrow_schema::DataType::Utf8,
                true,
            )])),
            vec![Arc::new(arrow_array::StringArray::from(vec![Some("7"), None]))],
        )
        .unwrap();
        let target = Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
            "n",
            arrow_schema::DataType::Int64,
            true,
        )]));
        use arrow_array::Array;
        let out = apply_stamped_schema(batch, &target).expect("castable");
        let col = out.column(0).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(col.value(0), 7);
        assert!(col.is_null(1));
    }

    /// #834: a NULL in a required column is refused instead of stored as 0;
    /// the same NULL in an optional column passes.
    #[test]
    fn stamp_batches_refuses_null_in_required_column() {
        let ice = |required: bool| {
            let f = if required { NestedField::required } else { NestedField::optional };
            IceSchema::builder()
                .with_fields(vec![f(1, "n", IceType::Primitive(PrimitiveType::Long)).into()])
                .build()
                .unwrap()
        };
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![arrow_schema::Field::new(
                "n",
                arrow_schema::DataType::Int64,
                true,
            )])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let err = stamp_batches(vec![batch.clone()], &ice(true)).expect_err("NULL in NOT NULL");
        assert!(
            format!("{err}").contains("NULL value not allowed for NOT NULL column: n"),
            "err: {err}"
        );
        assert_eq!(stamp_batches(vec![batch], &ice(false)).unwrap()[0].num_rows(), 2);
    }

    /// `parse_parquet_compression` maps "lz4" to the modern `LZ4_RAW` frame,
    /// not the deprecated Hadoop-framed `Compression::LZ4`. The distinction
    /// matters: `LZ4_RAW` is the only LZ4 variant readers reliably interop on.
    #[test]
    fn lz4_string_maps_to_lz4_raw() {
        assert_eq!(parse_parquet_compression("lz4"), Compression::LZ4_RAW);
        assert_eq!(parse_parquet_compression("LZ4"), Compression::LZ4_RAW);
    }

    // ---- BoundedFanoutWriter -------------------------------------------
    //
    // These exercise the file-level behaviour (splitting, cutover order, cap,
    // budget, DataFile completeness) against a local fs FileIO + TempDir, the
    // same rig the vendored fanout tests use. They validate everything short of
    // the Iceberg commit, which needs a live catalog.

    use arrow_array::{Int32Array, StringArray};
    use iceberg::spec::{
        Literal, NestedField, PartitionSpec, PrimitiveType, Schema as IceSchema, Transform,
        Type as IceType,
    };
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    fn fanout_schema() -> Arc<IceSchema> {
        Arc::new(
            IceSchema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", IceType::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "region", IceType::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
    }

    fn fanout_splitter(schema: &Arc<IceSchema>) -> iceberg::arrow::RecordBatchPartitionSplitter {
        let spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("region", "region", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            spec,
        )
        .unwrap()
    }

    fn fanout_builder(dir: &TempDir, schema: Arc<IceSchema>) -> impl IcebergWriterBuilder {
        let file_io = iceberg::io::FileIO::new_with_fs();
        let loc =
            DefaultLocationGenerator::with_data_location(dir.path().to_str().unwrap().to_string());
        let name = DefaultFileNameGenerator::new(
            "test".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pqb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rwb = RollingFileWriterBuilder::new_with_default_file_size(pqb, file_io, loc, name);
        DataFileWriterBuilder::new(rwb)
    }

    fn region_batch(ids: &[i32], regions: &[&str]) -> RecordBatch {
        let id_field =
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
            );
        let region_field =
            arrow_schema::Field::new("region", arrow_schema::DataType::Utf8, false).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string())]),
            );
        let schema = Arc::new(arrow_schema::Schema::new(vec![id_field, region_field]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(regions.to_vec())),
            ],
        )
        .unwrap()
    }

    fn region_struct(region: &str) -> Struct {
        Struct::from_iter([Some(Literal::string(region))])
    }

    /// Group the returned data files into (file_count, total_rows) per partition.
    fn by_partition(files: &[DataFile]) -> HashMap<Struct, (usize, u64)> {
        let mut m: HashMap<Struct, (usize, u64)> = HashMap::new();
        for f in files {
            let e = m.entry(f.partition().clone()).or_insert((0, 0));
            e.0 += 1;
            e.1 += f.record_count();
        }
        m
    }

    #[test]
    fn auto_fanout_caps_are_generous_and_clamped() {
        let mb = 1024 * 1024;
        let gb = 1024 * mb;
        // 8GB pool: 8GB/128MB = 64 open, budget = pool/8 = 1GB.
        let (max_open, budget) = auto_fanout_caps(8 * gb);
        assert_eq!(max_open, 64);
        assert_eq!(budget, gb);
        // 64GB pool (the test stack): 512 open, above TPC-H's 84 monthly
        // partitions, so an unsorted partitioned CTAS does not cut over.
        let (max_open, _) = auto_fanout_caps(64 * gb);
        assert_eq!(max_open, 512, "no 64 ceiling below the pool's capacity");
        // Huge pool: clamps to the 1024 ceiling.
        let (max_open, _) = auto_fanout_caps(1024 * gb);
        assert_eq!(max_open, 1024, "huge pool clamps to 1024");
        // Tiny 256MB pool: cap floors at 8, budget floors at one row-group est.
        let (max_open, budget) = auto_fanout_caps(256 * mb);
        assert_eq!(max_open, 8, "tiny pool floors at 8");
        assert_eq!(
            budget, FANOUT_PER_WRITER_EST_BYTES,
            "budget floors at one writer"
        );
        // Mid 4GB pool: 4GB/128MB = 32 open, 512MB budget.
        let (max_open, budget) = auto_fanout_caps(4 * gb);
        assert_eq!(max_open, 32);
        assert_eq!(budget, 512 * mb);
    }

    #[test]
    fn fanout_limits_gating() {
        assert!(
            !FanoutLimits::unbounded().is_bounded(),
            "default is unbounded"
        );
        assert!(!FanoutLimits {
            max_open: 0,
            byte_budget: 0,
            pool: None,
        }
        .is_bounded());
        assert!(
            FanoutLimits {
                max_open: 8,
                byte_budget: 0,
                pool: None,
            }
            .is_bounded(),
            "cap only"
        );
        assert!(
            FanoutLimits {
                max_open: 0,
                byte_budget: 1 << 20,
                pool: None,
            }
            .is_bounded(),
            "budget only"
        );
        assert!(FanoutLimits {
            max_open: 8,
            byte_budget: 1 << 20,
            pool: None,
        }
        .is_bounded());
    }

    #[tokio::test]
    async fn bounded_fanout_unbounded_writes_one_file_per_partition() {
        let dir = TempDir::new().unwrap();
        let schema = fanout_schema();
        let mut w = BoundedFanoutWriter::new(
            fanout_builder(&dir, schema.clone()),
            fanout_splitter(&schema),
            0, // no cap
            0, // no byte budget
            None,
        );
        w.write(region_batch(&[1, 2, 3, 4], &["US", "US", "EU", "ASIA"]))
            .await
            .unwrap();
        assert_eq!(w.cutovers(), 0, "no limits => no cutover");
        let files = w.close().await.unwrap();
        let per = by_partition(&files);
        assert_eq!(per.len(), 3, "one entry per distinct partition");
        assert_eq!(per[&region_struct("US")], (1, 2));
        assert_eq!(per[&region_struct("EU")], (1, 1));
        assert_eq!(per[&region_struct("ASIA")], (1, 1));
    }

    #[tokio::test]
    async fn bounded_fanout_cap_one_reopens_partition_and_preserves_rows() {
        let dir = TempDir::new().unwrap();
        let schema = fanout_schema();
        let mut w = BoundedFanoutWriter::new(
            fanout_builder(&dir, schema.clone()),
            fanout_splitter(&schema),
            1, // only one open writer at a time
            0,
            None,
        );
        // Separate writes so the sequence is deterministic: US, EU, US.
        w.write(region_batch(&[1], &["US"])).await.unwrap();
        w.write(region_batch(&[2], &["EU"])).await.unwrap(); // evicts US
        w.write(region_batch(&[3], &["US"])).await.unwrap(); // evicts EU, reopens US
        assert_eq!(w.cutovers(), 2, "each new partition evicts the open one");
        let files = w.close().await.unwrap();
        let per = by_partition(&files);
        assert_eq!(per[&region_struct("US")], (2, 2), "US reopened => 2 files");
        assert_eq!(per[&region_struct("EU")], (1, 1));
        let total: u64 = files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total, 3, "cutover preserves every row");
    }

    #[tokio::test]
    async fn bounded_fanout_byte_budget_forces_cutover() {
        let dir = TempDir::new().unwrap();
        let schema = fanout_schema();
        let mut w = BoundedFanoutWriter::new(
            fanout_builder(&dir, schema.clone()),
            fanout_splitter(&schema),
            0,
            1, // 1-byte budget: any second open partition trips it
            None,
        );
        w.write(region_batch(&[1, 2, 3], &["US", "EU", "ASIA"]))
            .await
            .unwrap();
        assert!(w.cutovers() >= 1, "tiny budget must cut over");
        let files = w.close().await.unwrap();
        let total: u64 = files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total, 3, "budget cutover preserves every row");
        let per = by_partition(&files);
        assert!(per.contains_key(&region_struct("US")));
        assert!(per.contains_key(&region_struct("EU")));
        assert!(per.contains_key(&region_struct("ASIA")));
    }

    #[tokio::test]
    async fn bounded_fanout_pool_denial_cuts_over_then_fails_typed() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let dir = TempDir::new().unwrap();
        let schema = fanout_schema();
        let one = region_batch(&[1], &["US"]).get_array_memory_size();
        // Room for roughly one and a half partition batches: the second
        // partition must evict the first instead of growing untracked.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(one + one / 2));
        let mut w = BoundedFanoutWriter::new(
            fanout_builder(&dir, schema.clone()),
            fanout_splitter(&schema),
            0,
            0,
            Some(WriteReservation::new(&pool, "fanout-buffer")),
        );
        w.write(region_batch(&[1], &["US"])).await.unwrap();
        assert_eq!(
            pool.reserved(),
            one,
            "first partition is charged to the pool"
        );
        w.write(region_batch(&[2], &["EU"])).await.unwrap();
        assert_eq!(
            w.cutovers(),
            1,
            "pool denial evicts the least-recently-written writer"
        );
        assert_eq!(
            pool.reserved(),
            one,
            "evicted bytes are returned to the pool"
        );

        // A batch that can never fit, even with every other writer evicted,
        // fails with a typed resource error and leaves the pool consistent.
        // Arrow rounds small buffers up to the same allocation size, so the
        // oversized batch needs enough rows to be measurably larger.
        let ids: Vec<i32> = (3..515).collect();
        let regions = vec!["ASIA"; ids.len()];
        assert!(region_batch(&ids, &regions).get_array_memory_size() > one + one / 2);
        let err = w
            .write(region_batch(&ids, &regions))
            .await
            .expect_err("oversized batch must be denied");
        assert_eq!(err.error_code(), sqe_core::SqeErrorCode::ResourceExhausted);
        assert!(err.to_string().contains("fanout-buffer"), "{err}");
        assert_eq!(w.cutovers(), 2, "EU was evicted while trying to make room");
        assert_eq!(pool.reserved(), 0);

        let files = w.close().await.unwrap();
        let total: u64 = files.iter().map(|f| f.record_count()).sum();
        assert_eq!(
            total, 2,
            "rows accepted before the denial are all committed"
        );
        drop(files);
        drop(pool);
    }

    #[tokio::test]
    async fn bounded_fanout_evicts_least_recently_written_first() {
        let dir = TempDir::new().unwrap();
        let schema = fanout_schema();
        let mut w = BoundedFanoutWriter::new(
            fanout_builder(&dir, schema.clone()),
            fanout_splitter(&schema),
            2, // two open writers
            0,
            None,
        );
        // Sequence: US, EU, US, ASIA, EU.
        //  - after US,EU: open {US,EU}
        //  - US touched => EU is now least-recently-written
        //  - ASIA (cap full) evicts EU (LRW), not US
        //  - EU (cap full: US,ASIA) evicts US (now LRW)
        // So EU is written to two files, US and ASIA to one each.
        w.write(region_batch(&[1], &["US"])).await.unwrap();
        w.write(region_batch(&[2], &["EU"])).await.unwrap();
        w.write(region_batch(&[3], &["US"])).await.unwrap();
        w.write(region_batch(&[4], &["ASIA"])).await.unwrap();
        w.write(region_batch(&[5], &["EU"])).await.unwrap();
        assert_eq!(w.cutovers(), 2);
        let files = w.close().await.unwrap();
        let per = by_partition(&files);
        assert_eq!(
            per[&region_struct("US")],
            (1, 2),
            "US never evicted mid-run => 1 file, 2 rows"
        );
        assert_eq!(
            per[&region_struct("EU")],
            (2, 2),
            "EU evicted then reopened => 2 files"
        );
        assert_eq!(per[&region_struct("ASIA")], (1, 1));
        let total: u64 = files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total, 5);
    }

    // ---- write_data_files_streaming progress wiring --------------------
    //
    // Exercises the actual batch loop `write_data_files_streaming` hooks
    // (unlike `progress.rs`'s tests, which drive `ProgressReporter` in
    // isolation): a real local-fs Table, a real multi-batch DataFusion
    // stream, and a real `RollingFileWriterBuilder`/`ParquetWriterBuilder`
    // write. No S3 involved, so this stays a plain `cargo test` (not
    // `#[ignore]`d): the streaming writer never talks to a catalog or an
    // object store here, just the local filesystem.

    /// Minimal unpartitioned, snapshot-less local `Table`: just enough
    /// metadata for `write_data_files_streaming` to derive a schema, a
    /// partition spec, and a `DefaultLocationGenerator` rooted at `dir`.
    /// `current_snapshot_id`/`snapshots` are omitted entirely (the write
    /// path never reads them); only a real catalog commit would need them.
    fn local_write_table(dir: &TempDir) -> Table {
        let json = format!(
            r#"{{
              "format-version": 2,
              "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
              "location": "{}",
              "last-sequence-number": 1,
              "last-updated-ms": 1602638573590,
              "last-column-id": 1,
              "current-schema-id": 0,
              "schemas": [
                {{"type": "struct", "schema-id": 0, "fields": [
                  {{"id": 1, "name": "id", "required": true, "type": "int"}}
                ]}}
              ],
              "default-spec-id": 0,
              "partition-specs": [{{"spec-id": 0, "fields": []}}],
              "last-partition-id": 999,
              "default-sort-order-id": 0,
              "sort-orders": [{{"order-id": 0, "fields": []}}],
              "properties": {{}}
            }}"#,
            dir.path().to_str().unwrap()
        );
        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&json).unwrap();
        let file_io = iceberg::io::FileIO::new_with_fs();
        Table::builder()
            .runtime(iceberg::Runtime::current())
            .metadata(metadata)
            .identifier(iceberg::TableIdent::from_strs(["ns", "tbl"]).unwrap())
            .file_io(file_io)
            .build()
            .unwrap()
    }

    /// One single-column `id` batch of `n` rows starting at `start`, with the
    /// Parquet field-id metadata `apply_stamped_schema` needs to re-stamp are
    /// not required on the input side: `write_data_files_streaming` derives
    /// its own stamped schema from the Iceberg schema and casts every batch
    /// into it, so a plain unstamped Arrow batch is enough here.
    fn id_batch(start: i32, n: i32) -> RecordBatch {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(
                (start..start + n).collect::<Vec<_>>(),
            ))],
        )
        .unwrap()
    }

    /// Adapt a fixed list of batches into the `SendableRecordBatchStream`
    /// `write_data_files_streaming` consumes, mirroring how
    /// `rewrite_group` adapts its Iceberg scan stream.
    fn batches_to_stream(
        schema: arrow_schema::SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> SendableRecordBatchStream {
        let s = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<RecordBatch, datafusion::error::DataFusionError>),
        );
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, s))
    }

    #[tokio::test]
    async fn streaming_write_reports_progress_every_interval_batches() {
        let dir = TempDir::new().unwrap();
        let table = local_write_table(&dir);
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]));

        // 10 batches of 3 rows each = 30 rows total; interval of 4 batches
        // should fire at batch 4 (12 rows) and batch 8 (24 rows), matching
        // `ProgressReporter`'s own interval semantics (see progress.rs).
        let batches: Vec<RecordBatch> = (0..10).map(|i| id_batch(i * 3, 3)).collect();
        let stream = batches_to_stream(arrow_schema, batches);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_cb = seen.clone();
        let progress = crate::progress::ProgressReporter::new(
            4,
            Box::new(move |rows| seen_for_cb.lock().unwrap().push(rows)),
        );

        let tracker = new_upload_tracker();
        let (data_files, total_rows) = write_data_files_streaming(
            &table,
            stream,
            "test",
            Compression::UNCOMPRESSED,
            tracker,
            FanoutLimits::unbounded(),
            None,
            Some(progress),
        )
        .await
        .unwrap();

        assert_eq!(
            total_rows, 30,
            "progress hooking must not change the write itself"
        );
        assert!(!data_files.is_empty());
        let total_written: u64 = data_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total_written, 30);

        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            vec![12, 24],
            "must fire at batch 4 (12 rows) and batch 8 (24 rows)"
        );
        assert!(
            seen.windows(2).all(|w| w[1] > w[0]),
            "rows_read must be monotonically increasing"
        );
    }

    #[tokio::test]
    async fn streaming_write_without_progress_is_unaffected() {
        // `progress: None` (every existing caller other than the worker's
        // compaction path) must produce byte-identical output to the same
        // write with a progress reporter attached, just without any
        // callback firing.
        let dir = TempDir::new().unwrap();
        let table = local_write_table(&dir);
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let batches: Vec<RecordBatch> = (0..5).map(|i| id_batch(i * 2, 2)).collect();
        let stream = batches_to_stream(arrow_schema, batches);

        let tracker = new_upload_tracker();
        let (data_files, total_rows) = write_data_files_streaming(
            &table,
            stream,
            "test",
            Compression::UNCOMPRESSED,
            tracker,
            FanoutLimits::unbounded(),
            None,
            None,
        )
        .await
        .unwrap();

        assert_eq!(total_rows, 10);
        let total_written: u64 = data_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total_written, 10);
    }
}
