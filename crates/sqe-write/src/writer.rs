use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use iceberg::spec::{DataContentType, DataFile, ManifestStatus, Schema as IcebergSchema};
use iceberg::table::Table;
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
use sqe_core::SqeError;

use tracing::{info, instrument};
use uuid::Uuid;

/// The streaming-writer bridge (`write_data_files_streaming` and its direct
/// helpers) moved to `sqe-compaction` (Phase 4c Task 1) so the compaction
/// rewrite path (`sqe_compaction::rewrite::rewrite_group`, reached from
/// `maintenance.rs`) and, later, the worker-side `compact_file_group`
/// action can share it without depending on this crate. Re-exported under
/// their original paths so every existing `crate::writer::X` call site
/// (`maintenance.rs`, `maintenance_log.rs`, `write_handler.rs`, and the
/// external `sqe_coordinator::writer::` integration tests) compiles unchanged.
pub use sqe_compaction::writer::{
    apply_stamped_schema, auto_fanout_caps, build_stamped_schema, new_upload_tracker,
    parse_parquet_compression, stamp_batches, write_data_files_streaming, FanoutLimits,
    TrackingLocationGenerator, UploadedPaths, WriteCleanupGuard,
};

/// Collect the live data files of the current snapshot. Mirrors the helper
/// in `WriteHandler` but does not need access to the compression config.
/// `pub` for the coordinator's maintenance procedures (re-exported from
/// its `maintenance.rs`) and the shared sqlite test rig
/// (`test_support::sqlite_rig::SqliteRig::live_files`, review 513).
pub async fn collect_live_data_files(table: &Table) -> sqe_core::Result<Vec<DataFile>> {
    use futures::{StreamExt, TryStreamExt};

    let metadata_ref = table.metadata_ref();
    let snapshot = match metadata_ref.current_snapshot() {
        Some(s) => s,
        None => return Ok(vec![]),
    };

    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to load manifest list: {e}"),
            )
        })?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to load manifest: {e}"),
                )
            })?;

    let data_files = manifests
        .into_iter()
        .flat_map(|manifest| {
            manifest
                .entries()
                .iter()
                .filter(|entry| {
                    entry.status() != ManifestStatus::Deleted
                        && entry.data_file().content_type() == DataContentType::Data
                })
                .map(|entry| entry.data_file().clone())
                .collect::<Vec<_>>()
        })
        .collect();

    Ok(data_files)
}
use sqe_core::SqeErrorCode;

pub use sqe_compaction::parquet_writer_config::{
    DEFAULT_BLOOM_FILTER_FPP, PROP_BLOOM_FILTER_COLUMNS, PROP_BLOOM_FILTER_FPP,
};

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
/// `sqe_compaction::parquet_writer_config::writer_props_for_table` so that
/// per-table bloom filter settings apply.
fn writer_props(compression: Compression) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression)
        .build()
}

/// Write already-materialised RecordBatches as Parquet data files for an
/// Iceberg table.
///
/// A thin adapter over [`write_data_files_streaming`] (#600): the batches
/// become a one-shot stream and take exactly the path CTAS, INSERT and the
/// compaction rewrite take, including its field-id stamping, its
/// partitioned fan-out and its file naming. This used to be a second writer
/// with its own stamping (`stamp_field_ids`) that scanned every batch for
/// nulls to decide nullability and cast per batch; the two disagreed, and a
/// type-mapping fix had to land twice. Callers that hold a `Vec` (the lease
/// and ledger rows, the CoW per-file rewrite) keep this signature.
///
/// `compression` controls the Parquet compression codec. Use
/// [`parse_parquet_compression`] to convert a config string (e.g. `"zstd"`)
/// into a [`Compression`] value.
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
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        return Ok(vec![]);
    };

    info!(
        total_rows,
        file_prefix, "Writing data files for Iceberg table"
    );

    let stream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(batches.into_iter().map(Ok)),
    ));
    // Unbounded fan-out keeps the partitioned path on the vendored
    // `TaskWriter`, which is what this writer used directly before; the
    // batches are already resident, so there is nothing for a bounded
    // fan-out to protect.
    let (data_files, _rows) = write_data_files_streaming(
        table,
        stream,
        file_prefix,
        compression,
        tracker,
        FanoutLimits::unbounded(),
        None,
        None,
    )
    .await?;

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

    let stamped = stamp_batches(key_batches, &key_schema)?;

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

#[cfg(test)]
mod tests {
    use arrow_schema::Schema as ArrowSchema;

    // Bloom filter unit tests live next to their implementation in
    // `sqe_compaction::parquet_writer_config`. The coordinator writer is a
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
