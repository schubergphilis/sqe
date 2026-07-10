// The streaming writer is the primary API; `write_parquet_files` remains
// as a thin wrapper so generators that haven't been migrated to iterators
// yet can still call in. Allow `dead_code` on helpers that the still-in-flux
// generator modules may not be consuming yet.
#![allow(dead_code)]

use std::fs;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::config::{CompressionKind, GenerateConfig};

const MAX_FILE_BYTES: usize = 128 * 1024 * 1024; // 128 MB

/// Write `batches` to one or more Parquet files under `{output_dir}/{table_name}/`.
///
/// Files rotate when the estimated in-memory size exceeds `MAX_FILE_BYTES`.
/// Returns `(file_count, total_compressed_bytes)`.
///
/// Retained for backward compatibility with generators that still produce
/// `Vec<RecordBatch>`. New code should prefer [`write_parquet_stream`].
pub fn write_parquet_files(
    batches: &[RecordBatch],
    schema: SchemaRef,
    output_dir: &str,
    table_name: &str,
) -> anyhow::Result<(usize, u64)> {
    let config = GenerateConfig {
        compression: CompressionKind::Zstd3,
        ..GenerateConfig::default()
    };
    write_parquet_stream(
        batches.iter().cloned(),
        schema,
        output_dir,
        table_name,
        "",
        &config,
    )
}

/// Stream `batches` into one or more Parquet files. Files rotate when the
/// accumulated in-memory batch size crosses `MAX_FILE_BYTES`.
///
/// The `file_prefix` is prepended to the numeric file index, giving each
/// parallel worker a disjoint namespace. A caller passing `"00"` produces
/// `00{00000..}.parquet`; another worker passing `"01"` produces
/// `01{00000..}.parquet`. Pass an empty string for unpartitioned output
/// (matches the pre-parallel file layout).
///
/// Returns `(file_count, total_compressed_bytes)`.
pub fn write_parquet_stream<I>(
    batches: I,
    schema: SchemaRef,
    output_dir: &str,
    table_name: &str,
    file_prefix: &str,
    config: &GenerateConfig,
) -> anyhow::Result<(usize, u64)>
where
    I: IntoIterator<Item = RecordBatch>,
{
    let dir = format!("{output_dir}/{table_name}");
    fs::create_dir_all(&dir)?;

    let mut props_builder =
        WriterProperties::builder().set_compression(config.compression.to_parquet());
    if let Some(rgs) = config.row_group_size {
        props_builder = props_builder.set_max_row_group_row_count(Some(rgs));
    }
    let props = props_builder.build();

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
            let path = format!("{dir}/{file_prefix}{file_idx:05}.parquet");
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
            current_bytes += batch.get_array_memory_size();
            w.write(&batch)?;
        }
    }

    if let Some(w) = writer.take() {
        total_bytes += w.bytes_written() as u64;
        w.close()?;
    }

    Ok((file_idx, total_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn small_batch(n: usize) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let arr = Int32Array::from_iter_values((0..n as i32).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
        (schema, batch)
    }

    #[test]
    fn stream_writes_single_file_for_small_input() {
        let tmp = tempdir_for("stream_single");
        let (schema, batch) = small_batch(100);
        let config = GenerateConfig::default();

        let (files, bytes) = write_parquet_stream(
            std::iter::once(batch),
            schema,
            tmp.to_str().unwrap(),
            "mytbl",
            "",
            &config,
        )
        .unwrap();

        assert_eq!(files, 1);
        assert!(bytes > 0);
        let out = std::fs::read_dir(tmp.join("mytbl")).unwrap().count();
        assert_eq!(out, 1);
    }

    #[test]
    fn stream_applies_file_prefix_for_partitioned_output() {
        let tmp = tempdir_for("stream_prefix");
        let (schema, batch) = small_batch(10);
        let config = GenerateConfig::default();

        write_parquet_stream(
            std::iter::once(batch.clone()),
            schema.clone(),
            tmp.to_str().unwrap(),
            "t",
            "00",
            &config,
        )
        .unwrap();
        write_parquet_stream(
            std::iter::once(batch),
            schema,
            tmp.to_str().unwrap(),
            "t",
            "01",
            &config,
        )
        .unwrap();

        let mut names: Vec<String> = std::fs::read_dir(tmp.join("t"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, vec!["0000000.parquet", "0100000.parquet"]);
    }

    #[test]
    fn stream_honors_compression_choice() {
        let tmp = tempdir_for("stream_uncompressed");
        let (schema, batch) = small_batch(10);
        let config = GenerateConfig {
            compression: CompressionKind::None,
            ..GenerateConfig::default()
        };

        let (files, _bytes) = write_parquet_stream(
            std::iter::once(batch),
            schema,
            tmp.to_str().unwrap(),
            "t",
            "",
            &config,
        )
        .unwrap();
        assert_eq!(files, 1);
    }

    fn tempdir_for(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sqe-bench-parquet-writer-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
