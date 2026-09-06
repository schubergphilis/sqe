use std::sync::Arc;

use arrow::compute::cast;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_file_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteInput, POSITION_DELETE_SCHEMA,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sqe_catalog::parquet_writer_config::{
    self, writer_props_for_table as shared_writer_props_for_table,
};
use sqe_core::SqeError;

use tracing::{info, instrument};
use uuid::Uuid;

/// The streaming-writer bridge (`write_data_files_streaming` and its direct
/// helpers) moved to `sqe-compaction` (Phase 4c Task 1) so the compaction
/// rewrite path (`crate::maintenance::rewrite_group`, itself now in
/// `sqe-compaction::rewrite`) and, later, the worker-side `compact_file_group`
/// action can share it without depending on this crate. Re-exported under
/// their original paths so every existing `crate::writer::X` call site
/// (`maintenance.rs`, `maintenance_log.rs`, `write_handler.rs`, and the
/// external `sqe_coordinator::writer::` integration tests) compiles unchanged.
pub use sqe_compaction::writer::{
    auto_fanout_caps, new_upload_tracker, parse_parquet_compression, write_data_files_streaming,
    FanoutLimits, TrackingLocationGenerator, UploadedPaths, WriteCleanupGuard,
};
// Internal-only helper: never referenced outside this module either before or
// after the move, so a plain (non-`pub`) import is enough.
use sqe_compaction::writer::unpartitioned_spec_key;
use sqe_core::SqeErrorCode;

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
pub fn writer_props_for_table(table: &Table, compression: Compression) -> WriterProperties {
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

    info!(
        total_rows,
        file_prefix, "Writing data files for Iceberg table"
    );

    // DataFusion-produced RecordBatches have no Iceberg field-ID metadata on their
    // Arrow fields. The Parquet writer requires "PARQUET:field_id" in each field's
    // metadata to map columns to the Iceberg schema. Stamp the IDs from the table's
    // current schema onto the batch schema before writing.
    let batches = stamp_field_ids(batches, table.metadata().current_schema())?;

    let inner_loc = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Location generator error: {e}")))?;
    let location_generator = TrackingLocationGenerator::new(inner_loc, tracker);

    // Generate a unique write ID for this operation. File names follow the
    // Iceberg convention: {write_uuid}-{counter}.parquet — no operation label.
    // This matches Spark/Trino behavior and prevents collisions across writes.
    let _ = file_prefix; // kept for logging; not used in file names
    let write_id = Uuid::now_v7();
    let unique_prefix = format!("{write_id}");

    let file_name_generator =
        DefaultFileNameGenerator::new(unique_prefix, None, iceberg::spec::DataFileFormat::Parquet);

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
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build data file writer: {e}")))?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                writer
                    .write(batch.clone())
                    .await
                    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Write error: {e}")))?;
            }
        }

        writer
            .close()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close writer error: {e}")))?
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build partition splitter: {e}")))?;
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
                    .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Partitioned write error: {e}")))?;
            }
        }
        writer
            .close()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Close partitioned writer error: {e}")))?
    };

    info!(
        file_count = data_files.len(),
        total_rows, "Data files written successfully"
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
    stream: datafusion::execution::SendableRecordBatchStream,
    file_prefix: &str,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
    compression: Compression,
    tracker: UploadedPaths,
    fanout: FanoutLimits,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let (data_files, total_rows) = write_data_files_streaming(
        table,
        stream,
        file_prefix,
        compression,
        tracker,
        fanout,
        None,
        None,
    )
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

    // Convert to PositionDeleteInput and sort by (file_path, pos) as required by spec.
    let mut inputs: Vec<PositionDeleteInput> = deletes
        .into_iter()
        .map(|(path, pos)| PositionDeleteInput::new(Arc::from(path.as_str()), pos))
        .collect();
    inputs.sort();

    let mut writer = PositionDeleteWriter::open(table, compression).await?;
    writer.write_sorted(inputs).await?;
    writer.close().await
}

/// Incremental position-delete writer (architecture review 504).
///
/// The MoR DELETE path used to collect every `(file_path, pos)` pair for the
/// whole statement into one `Vec<(String, i64)>` -- one heap `String` holding
/// the full object-store path per deleted row -- and only then hand it to the
/// writer. This handle lets the caller flush per data file instead: one
/// `Arc<str>` per file, positions written as they are found, and nothing but
/// the rolling parquet writer's in-progress row group resident between files.
///
/// The Iceberg spec requires rows inside a delete file to be sorted by
/// `(file_path, pos)`. Callers must therefore feed files in ascending path
/// order and positions in ascending order within each file; [`write_file`]
/// (Self::write_file) sorts the positions it is given, the path order is the
/// caller's contract.
pub struct PositionDeleteWriter {
    inner: Box<dyn IcebergWriter<Vec<PositionDeleteInput>>>,
    written: usize,
    table_ident: String,
}

impl PositionDeleteWriter {
    /// Open a rolling position-delete writer for `table`.
    pub async fn open(table: &Table, compression: Compression) -> sqe_core::Result<Self> {
        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Location generator error: {e}"),
                )
            })?;

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

        let writer = pos_delete_builder
            .build(None)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build position delete writer: {e}")))?;

        Ok(Self {
            inner: Box::new(writer),
            written: 0,
            table_ident: table.identifier().to_string(),
        })
    }

    /// Write the deleted positions of one data file. `positions` is sorted
    /// here; `path` is shared (one allocation) across every row of the file.
    pub async fn write_file(
        &mut self,
        path: Arc<str>,
        mut positions: Vec<i64>,
    ) -> sqe_core::Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        positions.sort_unstable();
        let inputs: Vec<PositionDeleteInput> = positions
            .into_iter()
            .map(|pos| PositionDeleteInput::new(Arc::clone(&path), pos))
            .collect();
        self.write_sorted(inputs).await
    }

    async fn write_sorted(&mut self, inputs: Vec<PositionDeleteInput>) -> sqe_core::Result<()> {
        self.written += inputs.len();
        self.inner.write(inputs).await.map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to write position deletes: {e}"),
            )
        })
    }

    /// Number of positions written so far.
    pub fn written(&self) -> usize {
        self.written
    }

    /// Close the writer and return the delete `DataFile`s. Returns an empty
    /// set (and writes nothing) when no position was ever written.
    pub async fn close(mut self) -> sqe_core::Result<Vec<DataFile>> {
        if self.written == 0 {
            return Ok(vec![]);
        }
        let delete_files = self
            .inner
            .close()
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to close position delete writer: {e}")))?;

        info!(
            table = %self.table_ident,
            delete_count = self.written,
            delete_file_count = delete_files.len(),
            "Position delete files written"
        );

        Ok(delete_files)
    }
}

/// Project `batch` down to the table's identifier (primary-key) columns, in
/// `identifier_field_ids` order (architecture review 504, item 3).
///
/// The MoR DELETE / UPDATE / MERGE paths buffer the keys of every matched row
/// until commit. Buffering the full-width row when only the PK bytes are
/// needed multiplies that residency by the table width; project first so the
/// tracked key buffer holds only what the equality-delete file will contain.
/// Pair with [`write_equality_delete_key_files`], which expects exactly this
/// shape.
pub fn project_to_identifier_columns(
    batch: &RecordBatch,
    iceberg_schema: &IcebergSchema,
    identifier_field_ids: &[i32],
) -> sqe_core::Result<RecordBatch> {
    let indices = identifier_field_ids
        .iter()
        .map(|id| {
            let field = iceberg_schema.field_by_id(*id).ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("identifier field id {id} is not in the table schema"),
                )
            })?;
            batch.schema().index_of(&field.name).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("identifier column '{}' missing from batch: {e}", field.name),
                )
            })
        })
        .collect::<sqe_core::Result<Vec<usize>>>()?;
    batch.project(&indices).map_err(|e| {
        SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("failed to project identifier columns: {e}"))
    })
}

/// Write equality-delete files from batches that hold only the identifier
/// columns, in `identifier_field_ids` order, as produced by
/// [`project_to_identifier_columns`].
///
/// Same output as [`write_equality_delete_files`]; the difference is that the
/// key-only batches are stamped against a schema projected to the identifier
/// fields, so the caller can buffer PK bytes instead of full rows.
pub async fn write_equality_delete_key_files(
    table: &Table,
    key_batches: Vec<RecordBatch>,
    identifier_field_ids: Vec<i32>,
    compression: Compression,
) -> sqe_core::Result<Vec<DataFile>> {
    let total_rows: usize = key_batches.iter().map(|b| b.num_rows()).sum();
    if total_rows == 0 {
        return Ok(vec![]);
    }
    if identifier_field_ids.is_empty() {
        return Err(SqeError::Execution(
            "equality delete requires identifier-field-ids on the table".to_string(),
        ));
    }

    let full_schema = table.metadata().current_schema();
    let key_fields = identifier_field_ids
        .iter()
        .map(|id| {
            full_schema.field_by_id(*id).cloned().ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("identifier field id {id} is not in the table schema"),
                )
            })
        })
        .collect::<sqe_core::Result<Vec<_>>>()?;
    let key_schema = IcebergSchema::builder()
        .with_schema_id(full_schema.schema_id())
        .with_identifier_field_ids(identifier_field_ids.iter().copied())
        .with_fields(key_fields)
        .build()
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build key schema: {e}"),
            )
        })?;

    info!(
        table = %table.identifier(),
        total_rows,
        equality_ids = ?identifier_field_ids,
        "Writing equality delete files (key-projected)"
    );

    let stamped = stamp_field_ids(key_batches, &key_schema)?;

    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Location generator error: {e}"),
            )
        })?;
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}-eq-delete"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let key_schema = Arc::new(key_schema);
    let parquet_writer_builder =
        ParquetWriterBuilder::new(writer_props(compression), key_schema.clone());
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let config =
        EqualityDeleteWriterConfig::new(identifier_field_ids, key_schema).map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Equality delete config error: {e}"),
            )
        })?;
    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config)
        .build(None)
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build equality delete writer: {e}")))?;

    for batch in stamped {
        writer
            .write(batch)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to write equality deletes: {e}")))?;
    }
    let delete_files = writer
        .close()
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to close equality delete writer: {e}")))?;

    info!(
        table = %table.identifier(),
        delete_file_count = delete_files.len(),
        "Equality delete files written"
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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Location generator error: {e}")))?;

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
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Equality delete config error: {e}")))?;

    let eq_delete_builder = EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config);

    let mut writer = eq_delete_builder
        .build(None)
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to build equality delete writer: {e}")))?;

    for batch in stamped {
        writer
            .write(batch)
            .await
            .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to write equality deletes: {e}")))?;
    }

    let delete_files = writer
        .close()
        .await
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to close equality delete writer: {e}")))?;

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
    let expected_arrow_schema = schema_to_arrow_schema(iceberg_schema)
        .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to derive expected Arrow schema: {e}")))?;

    let iceberg_fields = iceberg_schema.as_struct().fields();
    // A batch wider than the target has no field to stamp its extra columns
    // against. This used to fabricate a `(i + 1)` field id and an
    // Arrow-type fallback, writing a parquet column carrying an Iceberg field
    // id that belongs to no field (505). Reject it with the same message the
    // streaming path uses.
    if first.schema().fields().len() > iceberg_fields.len() {
        return Err(SqeError::Execution(format!(
            "INSERT has {} columns but target has {}",
            first.schema().fields().len(),
            iceberg_fields.len()
        )));
    }
    let new_fields: Vec<Arc<arrow_schema::Field>> = first
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, arrow_field)| {
            let field_id = iceberg_fields.get(i).map(|f| f.id).ok_or_else(|| {
                SqeError::Execution(format!(
                    "internal error: batch column {i} ('{}') has no Iceberg field \
                     (schema has {} fields)",
                    arrow_field.name(),
                    iceberg_fields.len()
                ))
            })?;
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
                .ok_or_else(|| {
                    SqeError::Execution(format!(
                        "internal error: batch column {i} ('{}') has no Arrow field \
                         in the target schema",
                        arrow_field.name()
                    ))
                })?;
            Ok(Arc::new(
                arrow_schema::Field::new(arrow_field.name(), target_type, nullable)
                    .with_metadata(meta),
            ))
        })
        .collect::<sqe_core::Result<Vec<_>>>()?;

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
                            SqeError::with_code(SqeErrorCode::ExecutionFailed, format!(
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
                .map_err(|e| SqeError::with_code(SqeErrorCode::ExecutionFailed, format!("Failed to stamp field IDs: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    // Bloom filter unit tests live next to their implementation in
    // `sqe_catalog::parquet_writer_config`. The coordinator writer is a
    // thin wrapper; end-to-end coverage runs in
    // `tests/bloom_distributed_write.rs`.
    //
    // `parse_parquet_compression`'s own unit test
    // (`lz4_string_maps_to_lz4_raw`) and the `BoundedFanoutWriter`/
    // `FanoutLimits`/`auto_fanout_caps` tests moved to
    // `sqe-compaction/src/writer.rs` along with the code they cover
    // (Phase 4c Task 1).

    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::ZstdLevel;

    /// 504: key batches are projected to the identifier columns, in
    /// identifier-field order, before they are buffered.
    #[test]
    fn project_to_identifier_columns_keeps_only_pk_in_id_order() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let iceberg_schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![3, 1])
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "payload",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "tenant",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
            Field::new("tenant", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
                Arc::new(StringArray::from(vec!["t1", "t2"])),
            ],
        )
        .unwrap();

        let keys = project_to_identifier_columns(&batch, &iceberg_schema, &[3, 1]).unwrap();
        let key_schema = keys.schema();
        let names: Vec<&str> = key_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["tenant", "id"]);
        assert_eq!(keys.num_rows(), 2);
        assert!(
            keys.get_array_memory_size() < batch.get_array_memory_size(),
            "projected keys must be smaller than the full row"
        );

        let err = project_to_identifier_columns(&batch, &iceberg_schema, &[99]).unwrap_err();
        assert!(err.to_string().contains("99"), "{err}");
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
        let schema = Arc::new(arrow_schema::Schema::new(vec![
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
}
