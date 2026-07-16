//! Buffer path for `read_json`: reads a whole JSON document into memory,
//! decompresses (gzip/zip), reshapes a top-level array into NDJSON, decodes
//! with `arrow::json`, and serves the rows as a `MemTable`. Used for
//! `format=array` and `compression=zip`, which DataFusion's streaming JSON
//! listing path cannot handle.

use std::io::Read;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};

use crate::read_json::{JsonCompression, JsonFraming};

const FN_NAME: &str = "read_json";

/// Parse a top-level JSON value. An array becomes one line per element; a
/// bare object becomes a single line. Anything else is an error.
pub(crate) fn reshape_array_to_ndjson(bytes: &[u8]) -> DFResult<Vec<u8>> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
        DataFusionError::Plan(format!(
            "{FN_NAME}: failed to parse JSON document (line {}, col {}): {e}",
            e.line(),
            e.column()
        ))
    })?;

    let mut out = Vec::with_capacity(bytes.len());
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                serde_json::to_writer(&mut out, &item).map_err(|e| {
                    DataFusionError::Plan(format!("{FN_NAME}: re-serialize failed: {e}"))
                })?;
                out.push(b'\n');
            }
        }
        obj @ serde_json::Value::Object(_) => {
            serde_json::to_writer(&mut out, &obj).map_err(|e| {
                DataFusionError::Plan(format!("{FN_NAME}: re-serialize failed: {e}"))
            })?;
            out.push(b'\n');
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "{FN_NAME}: expected a JSON object or array of objects at top level"
            )))
        }
    }
    Ok(out)
}

pub(crate) fn decompress(bytes: Vec<u8>, codec: JsonCompression) -> DFResult<Vec<u8>> {
    match codec {
        JsonCompression::None => Ok(bytes),
        JsonCompression::Gzip => {
            let mut dec = flate2::read::GzDecoder::new(&bytes[..]);
            let mut out = Vec::new();
            dec.read_to_end(&mut out).map_err(|e| {
                DataFusionError::Plan(format!("{FN_NAME}: gzip decode failed: {e}"))
            })?;
            Ok(out)
        }
        JsonCompression::Zip => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip handled by decompress_zip_to_ndjson (see Task 5)"
        ))),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: codec {other:?} is not valid on the buffer path"
        ))),
    }
}

/// Read every JSON-extension entry from a zip archive, reshape each per the
/// resolved framing, and concatenate into one NDJSON byte stream. Entries
/// whose name does not end in .json/.jsonl/.ndjson are ignored. Zero JSON
/// entries is an error.
pub(crate) fn decompress_zip_to_ndjson(
    bytes: Vec<u8>,
    framing: JsonFraming,
) -> DFResult<Vec<u8>> {
    use std::io::Cursor;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: not a valid zip: {e}")))?;

    let mut out = Vec::new();
    let mut json_entries = 0usize;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry read failed: {e}")))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry.name().to_ascii_lowercase();
        if !(name.ends_with(".json") || name.ends_with(".jsonl") || name.ends_with(".ndjson")) {
            continue;
        }
        let mut raw = Vec::new();
        entry
            .read_to_end(&mut raw)
            .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: zip entry decompress failed: {e}")))?;

        // Resolve framing per entry when Auto.
        let entry_framing = match framing {
            JsonFraming::Auto => crate::read_json::detect_framing_from_bytes(&raw),
            other => other,
        };
        let nd = match entry_framing {
            JsonFraming::Array => reshape_array_to_ndjson(&raw)?,
            _ => {
                // Ensure trailing newline so concatenation stays line-safe.
                let mut v = raw;
                if !v.ends_with(b"\n") {
                    v.push(b'\n');
                }
                v
            }
        };
        out.extend_from_slice(&nd);
        json_entries += 1;
    }

    if json_entries == 0 {
        return Err(DataFusionError::Plan(format!(
            "{FN_NAME}: zip archive contains no .json/.jsonl/.ndjson entries"
        )));
    }
    Ok(out)
}

pub(crate) fn decode_ndjson_to_memtable(ndjson: &[u8]) -> DFResult<Arc<dyn TableProvider>> {
    use arrow::json::reader::infer_json_schema;
    use arrow::json::ReaderBuilder;
    use std::io::Cursor;

    let (schema, _) = infer_json_schema(&mut Cursor::new(ndjson), None).map_err(|e| {
        DataFusionError::Plan(format!("{FN_NAME}: JSON schema inference failed: {e}"))
    })?;
    let schema = Arc::new(schema);

    let reader = ReaderBuilder::new(schema.clone())
        .build(Cursor::new(ndjson))
        .map_err(|e| DataFusionError::Plan(format!("{FN_NAME}: JSON reader build failed: {e}")))?;

    let mut batches = Vec::new();
    for b in reader {
        batches.push(b.map_err(|e| {
            DataFusionError::Plan(format!("{FN_NAME}: JSON decode failed: {e}"))
        })?);
    }

    let table = MemTable::try_new(schema, vec![batches])?;
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reshape_array_yields_one_line_per_element() {
        let out = reshape_array_to_ndjson(b"[{\"a\":1},{\"a\":2}]").unwrap();
        let s = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "{\"a\":1}");
        assert_eq!(lines[1], "{\"a\":2}");
    }

    #[test]
    fn reshape_accepts_single_object() {
        let out = reshape_array_to_ndjson(b"{\"a\":1}").unwrap();
        assert_eq!(String::from_utf8(out).unwrap().lines().count(), 1);
    }

    #[test]
    fn reshape_rejects_malformed() {
        assert!(reshape_array_to_ndjson(b"[not json").is_err());
    }

    #[test]
    fn decompress_gzip_roundtrips() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"{\"a\":1}\n").unwrap();
        let gz = enc.finish().unwrap();
        let out = decompress(gz, JsonCompression::Gzip).unwrap();
        assert_eq!(out, b"{\"a\":1}\n");
    }

    #[tokio::test]
    async fn decode_ndjson_builds_two_row_table() {
        use datafusion::prelude::SessionContext;

        let table = decode_ndjson_to_memtable(b"{\"a\":1}\n{\"a\":2}\n").unwrap();
        assert_eq!(table.schema().fields().len(), 1);

        let ctx = SessionContext::new();
        ctx.register_table("t", table).unwrap();
        let n = ctx
            .sql("SELECT count(*) AS n FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[test]
    fn reshape_rejects_top_level_scalar() {
        assert!(reshape_array_to_ndjson(b"42").is_err());
        assert!(reshape_array_to_ndjson(b"\"hello\"").is_err());
    }

    #[test]
    fn decompress_zip_and_other_codecs_error() {
        use crate::read_json::JsonCompression;
        assert!(decompress(vec![], JsonCompression::Zip).is_err());
        assert!(decompress(vec![], JsonCompression::Zstd).is_err());
    }

    #[test]
    fn zip_concatenates_json_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            let opts = SimpleFileOptions::default();
            zw.start_file("a.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":1}\n").unwrap();
            zw.start_file("README.txt", opts).unwrap(); // ignored (non-json)
            zw.write_all(b"ignore me").unwrap();
            zw.start_file("b.jsonl", opts).unwrap();
            zw.write_all(b"{\"a\":2}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 2);
    }

    #[test]
    fn zip_with_no_json_entry_errors() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("README.txt", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"nope").unwrap();
            zw.finish().unwrap();
        }
        assert!(decompress_zip_to_ndjson(buf, JsonFraming::Auto).is_err());
    }

    #[test]
    fn zip_auto_mixes_array_and_ndjson_entries() {
        use crate::read_json::JsonFraming;
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("a.json", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"[{\"a\":1},{\"a\":2}]").unwrap();
            zw.start_file("b.jsonl", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"{\"a\":3}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::Auto).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 3);
    }

    #[test]
    fn zip_appends_missing_trailing_newline_between_entries() {
        use crate::read_json::JsonFraming;
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.start_file("a.jsonl", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"{\"a\":1}").unwrap(); // no trailing newline
            zw.start_file("b.jsonl", SimpleFileOptions::default()).unwrap();
            zw.write_all(b"{\"a\":2}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 2);
    }

    #[test]
    fn zip_skips_directory_entries() {
        use std::io::Write;
        use zip::write::SimpleFileOptions;
        let mut buf = Vec::new();
        {
            let mut zw = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            zw.add_directory("data/", SimpleFileOptions::default())
                .unwrap();
            zw.start_file("data/a.jsonl", SimpleFileOptions::default())
                .unwrap();
            zw.write_all(b"{\"a\":1}\n").unwrap();
            zw.finish().unwrap();
        }
        let nd = decompress_zip_to_ndjson(buf, JsonFraming::NewlineDelimited).unwrap();
        assert_eq!(String::from_utf8(nd).unwrap().lines().count(), 1);
    }
}
