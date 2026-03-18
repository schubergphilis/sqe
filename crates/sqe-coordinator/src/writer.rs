use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use iceberg::spec::{DataFile, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;
use sqe_core::SqeError;
use tracing::info;

/// Write RecordBatches as Parquet data files for an Iceberg table.
///
/// Uses iceberg-rust's writer infrastructure to produce properly formatted
/// Iceberg data files with correct metadata (file path, size, record count, etc.)
///
/// Returns the DataFile descriptors needed for Iceberg transaction commits.
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

    let file_name_generator = DefaultFileNameGenerator::new(
        file_prefix.to_string(),
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

/// Add Iceberg field IDs to each Arrow field's metadata so the Parquet writer
/// can map columns to the Iceberg schema. The mapping is positional: field N in
/// the batch corresponds to field N in the Iceberg schema.
fn stamp_field_ids(
    batches: Vec<RecordBatch>,
    iceberg_schema: &IcebergSchema,
) -> sqe_core::Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(batches);
    };

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
            Arc::new(
                arrow_schema::Field::new(arrow_field.name(), arrow_field.data_type().clone(), nullable)
                    .with_metadata(meta),
            )
        })
        .collect();

    let new_schema = Arc::new(ArrowSchema::new(new_fields));
    batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(new_schema.clone(), batch.columns().to_vec())
                .map_err(|e| SqeError::Execution(format!("Failed to stamp field IDs: {e}")))
        })
        .collect()
}
