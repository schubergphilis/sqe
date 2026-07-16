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
            "{FN_NAME}: zip handled by decompress_zip (see Task 5)"
        ))),
        other => Err(DataFusionError::Plan(format!(
            "{FN_NAME}: codec {other:?} is not valid on the buffer path"
        ))),
    }
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

    #[test]
    fn decode_ndjson_builds_two_row_table() {
        let table = decode_ndjson_to_memtable(b"{\"a\":1}\n{\"a\":2}\n").unwrap();
        assert_eq!(table.schema().fields().len(), 1);
    }
}
