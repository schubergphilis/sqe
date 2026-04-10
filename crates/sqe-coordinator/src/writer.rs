use std::sync::Arc;

use arrow::compute::cast;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFile, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::position_delete_file_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteInput, POSITION_DELETE_SCHEMA,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use sqe_core::SqeError;
use tracing::{info, instrument};
use uuid::Uuid;

/// Write RecordBatches as Parquet data files for an Iceberg table.
///
/// Uses iceberg-rust's writer infrastructure to produce properly formatted
/// Iceberg data files with correct metadata (file path, size, record count, etc.)
///
/// Returns the DataFile descriptors needed for Iceberg transaction commits.
#[instrument(skip(table, batches), fields(table = %table.identifier(), file_prefix, total_rows))]
pub async fn write_data_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    file_prefix: &str,
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

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;

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
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut writer = data_file_writer_builder
        .build(None)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to build data file writer: {e}")))?;

    for batch in &batches {
        if batch.num_rows() > 0 {
            writer
                .write(batch.clone())
                .await
                .map_err(|e| SqeError::Execution(format!("Write error: {e}")))?;
        }
    }

    let data_files = writer
        .close()
        .await
        .map_err(|e| SqeError::Execution(format!("Close writer error: {e}")))?;

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
) -> sqe_core::Result<Vec<DataFile>> {
    let data_files = write_data_files(table, batches, file_prefix).await?;

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
/// Returns `(data_files, total_rows)` on success.
pub async fn write_data_files_streaming(
    table: &Table,
    mut stream: SendableRecordBatchStream,
    file_prefix: &str,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| SqeError::Execution(format!("Location generator error: {e}")))?;

    let _ = file_prefix; // kept for parity with non-streaming API; not used in file names
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(
        format!("{write_id}"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    let mut writer = data_file_writer_builder
        .build(None)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to build data file writer: {e}")))?;

    // Build the stamped schema once from the Iceberg schema so it can be reused
    // for every batch without re-deriving it on each iteration.
    let iceberg_schema = table.metadata().current_schema();
    let stamped_schema = build_stamped_schema(iceberg_schema)?;

    let mut total_rows = 0usize;

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
        // No rows streamed — close the writer cleanly and return empty.
        let _ = writer.close().await;
        return Ok((vec![], 0));
    }

    let data_files = writer
        .close()
        .await
        .map_err(|e| SqeError::Execution(format!("Close writer error: {e}")))?;

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
pub async fn write_data_files_streaming_with_metrics(
    table: &Table,
    stream: SendableRecordBatchStream,
    file_prefix: &str,
    metrics: Option<&Arc<sqe_metrics::MetricsRegistry>>,
) -> sqe_core::Result<(Vec<DataFile>, usize)> {
    let (data_files, total_rows) =
        write_data_files_streaming(table, stream, file_prefix).await?;

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
        WriterProperties::builder().build(),
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
