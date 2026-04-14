// The writer and its constant will be called from the generator implementations
// added in Task 7; allow dead_code for now so clippy stays clean.
#![allow(dead_code)]

use std::fs;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const MAX_FILE_BYTES: usize = 128 * 1024 * 1024; // 128 MB

/// Write `batches` to one or more Parquet files under `{output_dir}/{table_name}/`.
///
/// Files are split when the estimated in-memory size of the accumulated batches
/// exceeds `MAX_FILE_BYTES`.  Returns `(file_count, total_compressed_bytes)`.
pub fn write_parquet_files(
    batches: &[RecordBatch],
    schema: SchemaRef,
    output_dir: &str,
    table_name: &str,
) -> anyhow::Result<(usize, u64)> {
    let dir = format!("{output_dir}/{table_name}");
    fs::create_dir_all(&dir)?;

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .build();

    let mut file_idx = 0usize;
    let mut total_bytes = 0u64;
    let mut writer: Option<ArrowWriter<fs::File>> = None;
    let mut current_bytes = 0usize;

    for batch in batches {
        if writer.is_none() || current_bytes >= MAX_FILE_BYTES {
            if let Some(w) = writer.take() {
                total_bytes += w.bytes_written() as u64;
                w.close()?;
            }
            let path = format!("{dir}/{file_idx:05}.parquet");
            let file = fs::File::create(&path)?;
            writer = Some(ArrowWriter::try_new(
                file,
                schema.clone(),
                Some(props.clone()),
            )?);
            file_idx += 1;
            current_bytes = 0;
        }
        if let Some(ref mut w) = writer {
            w.write(batch)?;
            current_bytes += batch.get_array_memory_size();
        }
    }

    if let Some(w) = writer.take() {
        total_bytes += w.bytes_written() as u64;
        w.close()?;
    }

    Ok((file_idx, total_bytes))
}
