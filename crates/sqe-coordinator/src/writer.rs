use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::compute::cast;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{DataFile, PartitionKey, Schema as IcebergSchema, Struct};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_file_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteInput, POSITION_DELETE_SCHEMA,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sqe_catalog::parquet_writer_config::{self, writer_props_for_table as shared_writer_props_for_table};
use sqe_core::SqeError;

use crate::write_memory::WriteReservation;
use tracing::{error, info, instrument, warn};
use uuid::Uuid;

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
    fn generate_location(
        &self,
        partition_key: Option<&PartitionKey>,
        file_name: &str,
    ) -> String {
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
fn unpartitioned_spec_key(
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

/// Iceberg table property that lists columns to get Parquet bloom filters.
///
/// Re-exported from [`sqe_catalog::parquet_writer_config`] so the coordinator
/// and worker share the same property name. Value is a comma-separated list of
/// column names (case-sensitive, matched against top-level schema fields).
pub use parquet_writer_config::PROP_BLOOM_FILTER_COLUMNS;

/// Iceberg table property for the bloom filter false-positive probability.
///
/// Re-exported; defaults to [`DEFAULT_BLOOM_FILTER_FPP`] when absent.
pub use parquet_writer_config::PROP_BLOOM_FILTER_FPP;

/// Default bloom filter FPP when the table property is absent.
pub use parquet_writer_config::DEFAULT_BLOOM_FILTER_FPP;

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

/// Resolve the effective Parquet write compression.
///
/// A per-session codec (`session_codec`, from the Trino `iceberg.compression_codec`
/// session property, #353) wins over the static `config_codec` default. Both go
/// through [`parse_parquet_compression`], so an unknown value falls back to ZSTD.
pub fn resolve_write_compression(session_codec: Option<&str>, config_codec: &str) -> Compression {
    parse_parquet_compression(session_codec.unwrap_or(config_codec))
}

/// Build `WriterProperties` with the given compression codec.
///
/// Used by the position-delete writer and other paths that do not carry an
/// Iceberg table context. Data-file writes go through
/// [`writer_props_for_table`] so that per-table bloom filter settings apply.
fn writer_props(compression: Compression) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression)
        .build()
}

/// Build `WriterProperties` honouring the table's bloom filter properties.
///
/// Thin wrapper around [`sqe_catalog::parquet_writer_config::writer_props_for_table`]
/// so both coordinator and worker see identical behaviour for
/// `write.parquet.bloom-filter-columns` and `write.parquet.bloom-filter-fpp`.
///
/// Absence of the bloom filter columns property leaves the writer with no
/// bloom filters (matching Iceberg spec default).
pub fn writer_props_for_table(
    table: &Table,
    compression: Compression,
) -> WriterProperties {
    shared_writer_props_for_table(table, compression)
}

/// Write RecordBatches as Parquet data files for an Iceberg table.
///
/// Uses iceberg-rust's writer infrastructure to produce properly formatted
/// Iceberg data files with correct metadata (file path, size, record count, etc.)
///
/// `compression` controls the Parquet compression codec. Use [`parse_parquet_compression`]
/// to convert a config string (e.g. `"zstd"`) into a [`Compression`] value.
///
/// Returns the DataFile descriptors needed for Iceberg transaction commits.
///
/// `tracker` collects parquet paths the rolling writer creates so the caller's
/// [`WriteCleanupGuard`] can remove them on cancellation before the Iceberg
/// commit (#58).
#[instrument(skip(table, batches, compression, tracker), fields(table = %table.identifier(), file_prefix, total_rows))]
pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    file_prefix: &str,
    compression: Compression,
    tracker: UploadedPaths,
) -> sqe_core::Result<Vec<DataFile>> {
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok(vec![]);
    }

    info!(total_rows, file_prefix, "Writing data files for Iceberg table");

    // DataFusion-produced RecordBatches have no Iceberg field-ID metadata on their
    // Arrow fields. The Parquet writer requires "PARQUET:field_id" in each field's
    // metadata to map columns to the Iceberg schema. Stamp the IDs from the table's
    // current schema onto the batch schema before writing.
    let batches = stamp_field_ids(batches, table.metadata().current_schema())?;

    let inner_loc = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;
    let location_generator = TrackingLocationGenerator::new(inner_loc, tracker);

    // Generate a unique write ID for this operation. File names follow the
    // Iceberg convention: {write_uuid}-{counter}.parquet — no operation label.
    // This matches Spark/Trino behavior and prevents collisions across writes.
    let _ = file_prefix; // kept for logging; not used in file names
    let write_id = Uuid::now_v7();
    let unique_prefix = format!("{write_id}");

    let file_name_generator = DefaultFileNameGenerator::new(
        unique_prefix,
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_props_for_table(table, compression),
        table.metadata().current_schema().clone(),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let metadata = table.metadata();
    let partition_spec = metadata.default_partition_spec().clone();
    let data_files = if partition_spec.is_unpartitioned() {
        // Fast path: unpartitioned tables use the data-file writer directly.
        // Even on the unpartitioned path the data file must record the
        // table's current default spec id. Tables that have evolved
        // their partition spec (ALTER TABLE DROP/REPLACE PARTITION FIELD)
        // can be unpartitioned with `spec_id != 0`, and the catalog
        // rejects the commit with "Data file partition spec id does not
        // match table default partition spec id" when the file is
        // stamped with the iceberg-rust default of 0.
        let partition_key = unpartitioned_spec_key(table, &partition_spec);
        let mut writer = data_file_writer_builder
            .build(partition_key)
            .await
            .map_err(|e| {
                SqeError::Execution(format!("Failed to build data file writer: {e}"))
            })?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                writer
                    .write(batch.clone())
                    .await
                    .map_err(|e| SqeError::Execution(format!("Write error: {e}")))?;
            }
        }

        writer
            .close()
            .await
            .map_err(|e| SqeError::Execution(format!("Close writer error: {e}")))?
    } else {
        // Partitioned path: TaskWriter routes per-row to per-partition
        // writers, emitting one DataFile per partition with the right
        // partition struct attached. We pass a partition splitter that
        // COMPUTES partition values from source columns at runtime
        // (`try_new_with_computed_values`), so callers do not need to
        // pre-stamp a `_partition` column on the incoming RecordBatch.
        // Fanout writer enabled so unsorted INSERTs work without a
        // pre-clustering step.
        use iceberg::arrow::RecordBatchPartitionSplitter;
        use iceberg::writer::task_writer::TaskWriter;
        let schema = metadata.current_schema().clone();
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec.clone(),
        )
        .map_err(|e| {
            SqeError::Execution(format!(
                "Failed to build partition splitter: {e}"
            ))
        })?;
        let mut writer = TaskWriter::new_with_partition_splitter(
            data_file_writer_builder,
            true,
            schema,
            partition_spec,
            Some(splitter),
        );
        for batch in &batches {
            if batch.num_rows() > 0 {
                writer
                    .write(batch.clone())
                    .await
                    .map_err(|e| {
                        SqeError::Execution(format!(
                            "Partitioned write error: {e}"
                        ))
                    })?;
            }
        }
        writer
            .close()
            .await
            .map_err(|e| {
                SqeError::Execution(format!("Close partitioned writer error: {e}"))
            })?
    };

    info!(
        file_count = data_files.len(),
        total_rows,
        "Data files written successfully"
    );

    Ok(data_files)
}

/// Write data files and record S3 write metrics.
///
/// Delegates to [`write_data_files`] and, when `metrics` is provided, increments
/// `sqe_s3_bytes_written_total` and `sqe_s3_requests_total{operation="put"}` based
/// on the sizes reported in the returned `DataFile` descriptors.
pub async fn write_data_files_with_metrics(
    table: &Table,
    batches: Vec<RecordBatch>,
    file_prefix: &str,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    compression: Compression,
    tracker: UploadedPaths,
) -> sqe_core::Result<Vec<DataFile>> {
    let data_files = write_data_files(table, batches, file_prefix, compression, tracker).await?;

    if let Some(m) = metrics {
        let total_bytes: u64 = data_files.iter().map(|df| df.file_size_in_bytes()).sum();
        let file_count = data_files.len() as u64;
        if total_bytes > 0 {
            m.s3_bytes_written_total.inc_by(total_bytes);
        }
        if file_count > 0 {
            m.s3_requests_total
                .with_label_values(&["put", "success"])
                .inc_by(file_count);
        }
    }

    Ok(data_files)
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
/// Returns `(data_files, total_rows, uploaded_paths)` on success. The
/// `uploaded_paths` tracker is registered with a [`WriteCleanupGuard`] by the
/// caller so a cancellation between the last batch and the Iceberg commit
/// deletes the parquet files instead of orphaning them on S3 (#58).
pub async fn write_data_files_streaming(
    table: &Table,
    mut stream: SendableRecordBatchStream,
    file_prefix: &str,
    compression: Compression,
    tracker: UploadedPaths,
    fanout: FanoutLimits,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let inner_loc = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;
    let location_generator = TrackingLocationGenerator::new(inner_loc, tracker);

    let _ = file_prefix; // kept for parity with non-streaming API; not used in file names
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_props_for_table(table, compression),
        table.metadata().current_schema().clone(),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // Build the stamped schema once from the Iceberg schema so it can be reused
    // for every batch without re-deriving it on each iteration.
    let metadata = table.metadata();
    let iceberg_schema = metadata.current_schema();
    let stamped_schema = build_stamped_schema(iceberg_schema)?;
    let partition_spec = metadata.default_partition_spec().clone();

    let mut total_rows = 0usize;

    let data_files = if partition_spec.is_unpartitioned() {
        // Fast path: unpartitioned tables stream straight into a
        // DataFileWriter. See `write_data_files` for why we still need
        // a synthetic empty PartitionKey when `spec_id != 0`.
        let partition_key = unpartitioned_spec_key(table, &partition_spec);
        let mut writer = data_file_writer_builder
            .build(partition_key)
            .await
            .map_err(|e| {
                SqeError::Execution(format!(
                    "Failed to build data file writer: {e}"
                ))
            })?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| SqeError::Execution(format!("Stream error: {e}")))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let stamped = apply_stamped_schema(batch, &stamped_schema)?;
            total_rows += stamped.num_rows();
            writer
                .write(stamped)
                .await
                .map_err(|e| SqeError::Execution(format!("Write error: {e}")))?;
        }

        if total_rows == 0 {
            // Propagate close errors even on the empty-write path: a failed
            // close can signal a file build/flush problem we must not mask.
            writer.close().await.map_err(|e| {
                SqeError::Execution(format!("Close writer error (empty write): {e}"))
            })?;
            return Ok((vec![], 0));
        }

        writer
            .close()
            .await
            .map_err(|e| SqeError::Execution(format!("Close writer error: {e}")))?
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
        .map_err(|e| {
            SqeError::Execution(format!(
                "Failed to build partition splitter: {e}"
            ))
        })?;

        if fanout.is_bounded() {
            // Memory-bounded path: SQE's BoundedFanoutWriter caps open writers
            // and buffered bytes, cutting over least-recently-written writers.
            // Only reached when an operator sets `fanout_max_open_writers` or
            // `fanout_buffer_budget`; the default leaves both 0 and takes the
            // unbounded TaskWriter path below (byte-for-byte unchanged).
            let mut writer = BoundedFanoutWriter::new(
                data_file_writer_builder,
                splitter,
                fanout.max_open,
                fanout.byte_budget,
                None,
            );
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result
                    .map_err(|e| SqeError::Execution(format!("Stream error: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let stamped = apply_stamped_schema(batch, &stamped_schema)?;
                total_rows += stamped.num_rows();
                writer.write(stamped).await?;
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
                let batch = batch_result
                    .map_err(|e| SqeError::Execution(format!("Stream error: {e}")))?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let stamped = apply_stamped_schema(batch, &stamped_schema)?;
                total_rows += stamped.num_rows();
                writer.write(stamped).await.map_err(|e| {
                    SqeError::Execution(format!("Partitioned write error: {e}"))
                })?;
            }

            if total_rows == 0 {
                // Propagate close errors even on the empty-write path: a failed
                // close can signal a file build/flush problem we must not mask.
                writer.close().await.map_err(|e| {
                    SqeError::Execution(format!(
                        "Close partitioned writer error (empty write): {e}"
                    ))
                })?;
                return Ok((vec![], 0));
            }

            writer
                .close()
                .await
                .map_err(|e| SqeError::Execution(format!(
                    "Close partitioned writer error: {e}"
                )))?
        }
    };

    info!(
        file_count = data_files.len(),
        total_rows,
        file_prefix,
        "Data files written successfully (streaming)"
    );

    Ok((data_files, total_rows))
}

/// Write streaming data files and record S3 write metrics.
///
/// Delegates to [`write_data_files_streaming`] and, when `metrics` is provided,
/// increments `sqe_s3_bytes_written_total` and `sqe_s3_requests_total{operation="put"}`.
///
/// `tracker` collects every parquet file location handed out by the rolling
/// writer so the caller's [`WriteCleanupGuard`] can delete those files if the
/// surrounding future is cancelled before commit.
pub async fn write_data_files_streaming_with_metrics(
    table: &Table,
    stream: SendableRecordBatchStream,
    file_prefix: &str,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    compression: Compression,
    tracker: UploadedPaths,
    fanout: FanoutLimits,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let (data_files, total_rows) =
        write_data_files_streaming(table, stream, file_prefix, compression, tracker, fanout)
            .await?;

    if let Some(m) = metrics {
        let total_bytes: u64 = data_files.iter().map(|df| df.file_size_in_bytes()).sum();
        let file_count = data_files.len() as u64;
        if total_bytes > 0 {
            m.s3_bytes_written_total.inc_by(total_bytes);
        }
        if file_count > 0 {
            m.s3_requests_total
                .with_label_values(&["put", "success"])
                .inc_by(file_count);
        }
    }

    Ok((data_files, total_rows))
}

/// Allocate a fresh tracker for a streaming write. Bundled here so callers
/// don't need to know the inner representation.
pub fn new_upload_tracker() -> UploadedPaths {
    Arc::new(Mutex::new(Vec::new()))
}

/// Write position delete files for an Iceberg table.
///
/// Takes a list of `(file_path, row_position)` pairs and writes them as Iceberg
/// position delete Parquet files. Inputs are sorted by `(file_path, pos)` before
/// writing, as required by the Iceberg specification.
///
/// Returns `DataFile` descriptors with `content_type = PositionDeletes`, ready to
/// be passed to `FastAppendAction::add_data_files()` which auto-routes them into the
/// delete manifest.
pub async fn write_position_delete_files(
    table: &Table,
    deletes: Vec<(String, i64)>,
    compression: Compression,
) -> sqe_core::Result<Vec<DataFile>> {
    if deletes.is_empty() {
        return Ok(vec![]);
    }

    info!(
        table = %table.identifier(),
        delete_count = deletes.len(),
        "Writing position delete files"
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;

    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}-delete"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    // ParquetWriterBuilder for position delete files uses the fixed position-delete
    // schema (file_path, pos), not the table's data schema.
    let parquet_writer_builder = ParquetWriterBuilder::new(
        writer_props(compression),
        Arc::new(POSITION_DELETE_SCHEMA.clone()),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let pos_delete_builder = PositionDeleteFileWriterBuilder::new(rolling_writer_builder);

    let mut writer = pos_delete_builder
        .build(None)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to build position delete writer: {e}")))?;

    // Convert to PositionDeleteInput and sort by (file_path, pos) as required by spec.
    let mut inputs: Vec<PositionDeleteInput> = deletes
        .into_iter()
        .map(|(path, pos)| PositionDeleteInput::new(Arc::from(path.as_str()), pos))
        .collect();
    inputs.sort();

    writer
        .write(inputs)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to write position deletes: {e}")))?;

    let delete_files = writer
        .close()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to close position delete writer: {e}")))?;

    info!(
        table = %table.identifier(),
        delete_file_count = delete_files.len(),
        "Position delete files written"
    );

    Ok(delete_files)
}

/// Write equality-delete files for an Iceberg table (Phase E, task 6.7).
///
/// Each row in `key_batches` represents one logical row to delete. The writer
/// projects `equality_ids` out of the table's full schema and records them as
/// the equality keys. Compared to position deletes this is snapshot-stable
/// (new data files matching the same equality keys are also deleted) and avoids
/// per-row scan cost for the writer.
///
/// `equality_ids` defaults to the table's `identifier-field-ids` when empty;
/// callers typically pass the declared primary key.
///
/// Returns `DataFile` descriptors with `content_type = EqualityDeletes`, ready
/// to be passed to `RowDeltaAction::add_delete_files()`.
pub async fn write_equality_delete_files(
    table: &Table,
    key_batches: Vec<RecordBatch>,
    equality_ids: Vec<i32>,
    compression: Compression,
) -> sqe_core::Result<Vec<DataFile>> {
    let total_rows: usize = key_batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok(vec![]);
    }

    let iceberg_schema = table.metadata().current_schema();

    // Resolve equality ids: fall back to declared identifier-field-ids when
    // caller passes an empty vec. DELETE on a table without declared PK or
    // explicit equality columns is an error.
    let resolved_ids: Vec<i32> = if equality_ids.is_empty() {
        iceberg_schema.identifier_field_ids().collect()
    } else {
        equality_ids
    };
    if resolved_ids.is_empty() {
        return Err(SqeError::Execution(
            "equality delete requires identifier-field-ids on the table or explicit equality_ids"
                .to_string(),
        ));
    }

    info!(
        table = %table.identifier(),
        total_rows,
        equality_ids = ?resolved_ids,
        "Writing equality delete files"
    );

    // Stamp field-ids on the Arrow schema so the projector inside the writer
    // can match PARQUET:field_id metadata against `resolved_ids`.
    let stamped = stamp_field_ids(key_batches, iceberg_schema.as_ref())?;

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;

    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}-eq-delete"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    // The Parquet writer takes the Iceberg schema; the equality-delete writer
    // then projects keys from it via `EqualityDeleteWriterConfig`.
    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_props(compression), iceberg_schema.clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let config = EqualityDeleteWriterConfig::new(resolved_ids, iceberg_schema.clone())
        .map_err(|e| SqeError::Execution(format!("Equality delete config error: {e}")))?;

    let eq_delete_builder = EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config);

    let mut writer = eq_delete_builder
        .build(None)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to build equality delete writer: {e}")))?;

    for batch in stamped {
        writer
            .write(batch)
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to write equality deletes: {e}")))?;
    }

    let delete_files = writer
        .close()
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to close equality delete writer: {e}")))?;

    info!(
        table = %table.identifier(),
        delete_file_count = delete_files.len(),
        "Equality delete files written"
    );

    Ok(delete_files)
}

/// Add Iceberg field IDs to each Arrow field's metadata so the Parquet writer
/// can map columns to the Iceberg schema, and cast columns to the Iceberg-expected
/// Arrow types (e.g. Timestamp(ns) → Timestamp(µs)).
///
/// DataFusion produces `Timestamp(Nanosecond, None)` for CURRENT_TIMESTAMP and
/// timestamp literals, but Iceberg stores timestamps as `Timestamp(Microsecond, None)`.
/// The Parquet writer rejects type mismatches, so we cast here before writing.
fn stamp_field_ids(
    batches: Vec<RecordBatch>,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(batches);
    };

    // Build the canonical Arrow schema from the Iceberg schema so we know the
    // expected Arrow data type for each column (e.g. Timestamp(µs) not Timestamp(ns)).
    let expected_arrow_schema =
        schema_to_arrow_schema(iceberg_schema).map_err(|e| {
            SqeError::Execution(format!("Failed to derive expected Arrow schema: {e}"))
        })?;

    let iceberg_fields = iceberg_schema.as_struct().fields();
    let new_fields: Vec<Arc<arrow_schema::Field>> = first
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, arrow_field)| {
            let field_id = iceberg_fields
                .get(i)
                .map(|f| f.id)
                .unwrap_or((i + 1) as i32);
            let mut meta = arrow_field.metadata().clone();
            meta.insert("PARQUET:field_id".to_string(), field_id.to_string());
            // DataFusion sometimes marks a field as non-nullable even when the column
            // contains nulls (e.g. CAST(NULL AS T) in UNION ALL). Check across ALL batches
            // because the null value may appear in any batch, not just the first one.
            let has_nulls = batches.iter().any(|b| b.column(i).null_count() > 0);
            let nullable = arrow_field.is_nullable() || has_nulls;
            // Use the Iceberg-expected Arrow data type (may differ, e.g. Timestamp precision).
            let target_type = expected_arrow_schema
                .fields()
                .get(i)
                .map(|f| f.data_type().clone())
                .unwrap_or_else(|| arrow_field.data_type().clone());
            Arc::new(
                arrow_schema::Field::new(arrow_field.name(), target_type, nullable)
                    .with_metadata(meta),
            )
        })
        .collect();

    let new_schema = Arc::new(ArrowSchema::new(new_fields));

    batches
        .into_iter()
        .map(|batch| {
            // Cast any columns whose type changed (e.g. Timestamp(ns) → Timestamp(µs)).
            let new_columns: Result<Vec<_>, _> = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let target = new_schema.field(i).data_type();
                    if col.data_type() == target {
                        Ok(col.clone())
                    } else {
                        cast(col, target).map_err(|e| {
                            SqeError::Execution(format!(
                                "Failed to cast column '{}' from {:?} to {:?}: {e}",
                                new_schema.field(i).name(),
                                col.data_type(),
                                target,
                            ))
                        })
                    }
                })
                .collect();
            RecordBatch::try_new(new_schema.clone(), new_columns?)
                .map_err(|e| SqeError::Execution(format!("Failed to stamp field IDs: {e}")))
        })
        .collect()
}

/// Build a stamped Arrow schema from an Iceberg schema.
///
/// Used by the streaming write path: derive the target schema once, then reuse
/// it for every batch via [`apply_stamped_schema`]. Unlike [`stamp_field_ids`]
/// this function does not require all batches to be present — it marks all
/// columns as nullable (the safe default for Iceberg) and takes types from the
/// Iceberg schema directly.
fn build_stamped_schema(iceberg_schema: &IcebergSchema) -> sqe_core::Result<Arc<ArrowSchema>> {
    let expected_arrow_schema =
        schema_to_arrow_schema(iceberg_schema).map_err(|e| {
            SqeError::Execution(format!("Failed to derive expected Arrow schema: {e}"))
        })?;

    let iceberg_fields = iceberg_schema.as_struct().fields();
    let new_fields: Vec<Arc<arrow_schema::Field>> = expected_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, arrow_field)| {
            let field_id = iceberg_fields
                .get(i)
                .map(|f| f.id)
                .unwrap_or((i + 1) as i32);
            let mut meta = arrow_field.metadata().clone();
            meta.insert("PARQUET:field_id".to_string(), field_id.to_string());
            // Mark all columns nullable — safe default; avoids cross-batch null scan.
            Arc::new(
                arrow_schema::Field::new(arrow_field.name(), arrow_field.data_type().clone(), true)
                    .with_metadata(meta),
            )
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new(new_fields)))
}

/// Apply a pre-built stamped schema to a single [`RecordBatch`].
///
/// Casts columns whose type differs from the target schema (e.g. Timestamp(ns)
/// → Timestamp(µs)) and re-wraps the batch with the stamped schema.
fn apply_stamped_schema(
    batch: RecordBatch,
    stamped_schema: &Arc<ArrowSchema>,
) -> sqe_core::Result<RecordBatch> {
    let new_columns: Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let target = stamped_schema.field(i).data_type();
            if col.data_type() == target {
                Ok(col.clone())
            } else {
                cast(col, target).map_err(|e| {
                    SqeError::Execution(format!(
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
        .map_err(|e| SqeError::Execution(format!("Failed to apply stamped schema: {e}")))
}

/// Resolved fanout limits for a partitioned streaming write. Both `0` means
/// unbounded (the default): the write takes the vendored `TaskWriter` path,
/// byte-for-byte unchanged. A non-zero value in either field switches the write
/// to [`BoundedFanoutWriter`]. A `0` in one field while the other is set means
/// that particular limit is disabled (e.g. cap the open-writer count but impose
/// no byte budget).
#[derive(Debug, Clone, Copy)]
pub struct FanoutLimits {
    /// Max concurrently open partition writers (`0` = no cap).
    pub max_open: usize,
    /// Total buffered-byte budget across open writers (`0` = no budget).
    pub byte_budget: usize,
}

impl FanoutLimits {
    /// The default: no limits, unbounded `TaskWriter` path.
    pub const fn unbounded() -> Self {
        Self {
            max_open: 0,
            byte_budget: 0,
        }
    }

    /// True when at least one limit is active (selects [`BoundedFanoutWriter`]).
    pub fn is_bounded(&self) -> bool {
        self.max_open > 0 || self.byte_budget > 0
    }
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
/// is mirrored onto the shared pool (best-effort) so the governor sees the
/// fanout memory; the hard bound remains `byte_budget`.
///
/// Wired into the streaming write path ([`write_data_files_streaming`]) behind
/// the [`FanoutLimits`] gate: reached only when an operator sets
/// `fanout_max_open_writers` or `fanout_buffer_budget` (the default leaves both
/// 0 and takes the unbounded `TaskWriter` path unchanged). The file-level
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
            .map_err(|e| SqeError::Execution(format!("Partition split error: {e}")))?;

        for (partition_key, part_batch, _row_ids) in partitioned {
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

            self.get_or_open(&partition_key)
                .await?
                .write(part_batch)
                .await
                .map_err(|e| SqeError::Execution(format!("Partitioned write error: {e}")))?;

            self.total_bytes += bytes;
            *self.est_bytes.entry(key.clone()).or_insert(0) += bytes;
            self.tick += 1;
            self.last_written.insert(key, self.tick);

            // Pool visibility (best-effort): the hard bound is `byte_budget`.
            if let Some(res) = self.reservation.as_mut() {
                let _ = res.try_resize(self.total_bytes);
            }
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
                    SqeError::Execution(format!("Close partition writer error: {e}"))
                })?;
                self.output.extend(files);
            }
        }
        Ok(self.output)
    }

    /// Get the open writer for `partition_key`, building a fresh one if this
    /// partition has no open writer (first write, or after a cutover).
    async fn get_or_open(
        &mut self,
        partition_key: &PartitionKey,
    ) -> sqe_core::Result<&mut B::R> {
        let key = partition_key.data().clone();
        if !self.open.contains_key(&key) {
            let writer = self
                .inner_builder
                .build(Some(partition_key.clone()))
                .await
                .map_err(|e| {
                    SqeError::Execution(format!("Failed to build partition writer: {e}"))
                })?;
            self.open.insert(key.clone(), writer);
        }
        self.open
            .get_mut(&key)
            .ok_or_else(|| SqeError::Execution("partition writer missing after build".into()))
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
                .map_err(|e| SqeError::Execution(format!("Cutover close error: {e}")))?;
            self.output.extend(files);
        }
        let freed = self.est_bytes.remove(&key).unwrap_or(0);
        self.total_bytes = self.total_bytes.saturating_sub(freed);
        self.last_written.remove(&key);
        self.cutovers += 1;
        if let Some(res) = self.reservation.as_mut() {
            let _ = res.try_resize(self.total_bytes);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    // Bloom filter unit tests live next to their implementation in
    // `sqe_catalog::parquet_writer_config`. The coordinator writer is a
    // thin wrapper; end-to-end coverage runs in
    // `tests/bloom_distributed_write.rs`.

    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    /// `parse_parquet_compression` maps "lz4" to the modern `LZ4_RAW` frame,
    /// not the deprecated Hadoop-framed `Compression::LZ4`. The distinction
    /// matters: `LZ4_RAW` is the only LZ4 variant readers reliably interop on.
    #[test]
    fn lz4_string_maps_to_lz4_raw() {
        assert_eq!(parse_parquet_compression("lz4"), Compression::LZ4_RAW);
        assert_eq!(parse_parquet_compression("LZ4"), Compression::LZ4_RAW);
    }

    #[test]
    fn resolve_write_compression_session_wins_over_config() {
        // Session override takes precedence.
        assert_eq!(
            resolve_write_compression(Some("snappy"), "zstd"),
            Compression::SNAPPY
        );
        // No session override -> config default.
        assert_eq!(
            resolve_write_compression(None, "snappy"),
            Compression::SNAPPY
        );
        // Unknown session codec -> ZSTD fallback (no panic), matching config leniency.
        assert_eq!(
            resolve_write_compression(Some("garbage"), "snappy"),
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap())
        );
    }

    /// Empirical proof that an `LZ4_RAW`-compressed Parquet file written with
    /// the same `WriterProperties` SQE feeds iceberg-rust round-trips in this
    /// build (the `lz4` parquet feature is compiled). This is the ground-truth
    /// check behind #332: LZ4 is supported for Parquet data files. If the
    /// feature ever gets dropped from the build, this test fails at write.
    #[test]
    fn lz4_raw_parquet_roundtrips() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        // Same property shape the write path uses: set_compression(LZ4_RAW).
        let props = writer_props(Compression::LZ4_RAW);

        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        assert!(!buf.is_empty(), "LZ4_RAW write produced no bytes");

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(buf))
            .unwrap()
            .build()
            .unwrap();
        let mut rows = 0;
        for rb in reader {
            rows += rb.unwrap().num_rows();
        }
        assert_eq!(rows, 3, "LZ4_RAW read-back lost rows");
    }

    // ---- BoundedFanoutWriter -------------------------------------------
    //
    // These exercise the file-level behaviour (splitting, cutover order, cap,
    // budget, DataFile completeness) against a local fs FileIO + TempDir, the
    // same rig the vendored fanout tests use. They validate everything short of
    // the Iceberg commit, which needs a live catalog.

    use iceberg::spec::{
        Literal, NestedField, PartitionSpec, PrimitiveType, Schema as IceSchema, Transform,
        Type as IceType,
    };
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
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

    fn fanout_splitter(
        schema: &Arc<IceSchema>,
    ) -> iceberg::arrow::RecordBatchPartitionSplitter {
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
        let file_io = iceberg::io::FileIOBuilder::new_fs_io().build().unwrap();
        let loc = DefaultLocationGenerator::with_data_location(
            dir.path().to_str().unwrap().to_string(),
        );
        let name = DefaultFileNameGenerator::new(
            "test".to_string(),
            None,
            iceberg::spec::DataFileFormat::Parquet,
        );
        let pqb = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rwb =
            RollingFileWriterBuilder::new_with_default_file_size(pqb, file_io, loc, name);
        DataFileWriterBuilder::new(rwb)
    }

    fn region_batch(ids: &[i32], regions: &[&str]) -> RecordBatch {
        let id_field = arrow_schema::Field::new("id", DataType::Int32, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())]),
        );
        let region_field = arrow_schema::Field::new("region", DataType::Utf8, false)
            .with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )]));
        let schema = Arc::new(arrow_schema::Schema::new(vec![id_field, region_field]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(regions.to_vec())),
        ])
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
    fn fanout_limits_gating() {
        assert!(!FanoutLimits::unbounded().is_bounded(), "default is unbounded");
        assert!(!FanoutLimits { max_open: 0, byte_budget: 0 }.is_bounded());
        assert!(FanoutLimits { max_open: 8, byte_budget: 0 }.is_bounded(), "cap only");
        assert!(
            FanoutLimits { max_open: 0, byte_budget: 1 << 20 }.is_bounded(),
            "budget only"
        );
        assert!(FanoutLimits { max_open: 8, byte_budget: 1 << 20 }.is_bounded());
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
        assert_eq!(per[&region_struct("US")], (1, 2), "US never evicted mid-run => 1 file, 2 rows");
        assert_eq!(per[&region_struct("EU")], (2, 2), "EU evicted then reopened => 2 files");
        assert_eq!(per[&region_struct("ASIA")], (1, 1));
        let total: u64 = files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total, 5);
    }
}
