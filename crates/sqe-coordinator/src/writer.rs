use arrow_array::RecordBatch;
use iceberg::spec::DataFile;
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
