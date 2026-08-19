//! SerDe and `STORED AS` allow-list, and the `ListingOptions` it produces.
//!
//! No SerDe is implemented. Each allow-listed class maps onto one of the three
//! formats DataFusion already reads. Anything else is rejected by name, because
//! a silently ignored SerDe property changes query results and a wrong answer
//! is worse than a rejected DDL.

use std::collections::BTreeMap;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingOptions;

use super::manifest::HiveManifest;
use super::HiveError;

const OPEN_CSV: &str = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
const LAZY_SIMPLE: &str = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
const OPENX_JSON: &str = "org.openx.data.jsonserde.JsonSerDe";
const HCATALOG_JSON: &str = "org.apache.hive.hcatalog.data.JsonSerDe";
const PARQUET_SERDE: &str = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

const TEXT_INPUT_FORMAT: &str = "org.apache.hadoop.mapred.TextInputFormat";
const PARQUET_INPUT_FORMAT: &str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";

/// Hive's `LazySimpleSerDe` default field delimiter: ctrl-A, not comma.
/// Athena behaves the same way, so a table declared without a delimiter over
/// comma-separated data is a one-column table in both engines.
const HIVE_DEFAULT_DELIM: u8 = 0x01;

/// The three file formats DataFusion already reads. No SerDe is implemented;
/// this only records which reader a resolved table maps onto.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveFileFormat {
    Csv,
    Json,
    Parquet,
}

/// The result of resolving a manifest's SerDe / `STORED AS` declaration onto
/// a DataFusion-readable format, plus the CSV dialect settings that matter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFormat {
    pub kind: HiveFileFormat,
    pub delimiter: u8,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
    pub has_header: bool,
    pub file_extension: String,
}

/// Resolve a manifest's format, SerDe class taking precedence over
/// `STORED AS` when both are present, as Hive does.
pub fn resolve_format(m: &HiveManifest) -> Result<ResolvedFormat, HiveError> {
    let class = m
        .storage_descriptor
        .serde_info
        .serialization_library
        .as_deref();
    let props = &m.storage_descriptor.serde_info.parameters;

    let kind = match class {
        Some(OPEN_CSV) | Some(LAZY_SIMPLE) => HiveFileFormat::Csv,
        Some(OPENX_JSON) | Some(HCATALOG_JSON) => HiveFileFormat::Json,
        Some(PARQUET_SERDE) => HiveFileFormat::Parquet,
        Some(other) => {
            return Err(HiveError::Validation(format!(
                "SerDe `{other}` is not supported. Supported: {OPEN_CSV}, {LAZY_SIMPLE}, \
                 {OPENX_JSON}, {HCATALOG_JSON}, {PARQUET_SERDE}"
            )))
        }
        None => kind_from_input_format(m)?,
    };

    // Delimiter precedence: OpenCSVSerde's separatorChar, then Hive's
    // field.delim / serialization.format, then ctrl-A.
    let delimiter = single_byte(
        props,
        &["separatorChar", "field.delim", "serialization.format"],
    )?
    .unwrap_or(HIVE_DEFAULT_DELIM);

    let has_header = match m
        .parameters
        .get("skip.header.line.count")
        .map(String::as_str)
    {
        None | Some("0") => false,
        Some("1") => true,
        Some(other) => {
            return Err(HiveError::Validation(format!(
                "skip.header.line.count = {other} is not supported: DataFusion expresses the \
                 setting as a boolean, so only 0 or 1 can be honored. Strip the extra header \
                 lines from the data, or use 1."
            )))
        }
    };

    Ok(ResolvedFormat {
        kind,
        delimiter,
        quote: single_byte(props, &["quoteChar"])?,
        escape: single_byte(props, &["escapeChar", "escape.delim"])?,
        has_header,
        file_extension: String::new(),
    })
}

/// A human-readable keyword for a Hadoop input format class this release
/// does not read, so a rejection can name both what was recorded and what a
/// user would call it. `manifest.rs`'s `formats_from_stored_as` is what
/// attaches these classes for `STORED AS ORC`/`AVRO`/`SEQUENCEFILE`/`RCFILE`;
/// this function never invents a class of its own, it only recognizes ones
/// that are already genuine Hadoop/Hive classes.
fn friendly_keyword_for_input_format(class: &str) -> Option<&'static str> {
    match class {
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" => Some("ORC"),
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" => Some("Avro"),
        "org.apache.hadoop.mapred.SequenceFileInputFormat" => Some("SequenceFile"),
        "org.apache.hadoop.hive.ql.io.RCFileInputFormat" => Some("RCFile"),
        _ => None,
    }
}

/// Fallback when no `ROW FORMAT SERDE` was given: reads
/// `storage_descriptor.input_format`, set from `STORED AS`/`INPUTFORMAT` by
/// [`HiveManifest::from_spec`]. Only `TextInputFormat` (csv, or json when
/// `parameters["classification"] = "json"`, the way Glue records a JSON
/// table) and `MapredParquetInputFormat` (parquet) are accepted; anything
/// else, including an ORC or Avro input format, is rejected by the class
/// name `from_spec` recorded, not by a class this function invents.
fn kind_from_input_format(m: &HiveManifest) -> Result<HiveFileFormat, HiveError> {
    match m.storage_descriptor.input_format.as_deref() {
        Some(TEXT_INPUT_FORMAT) => {
            if m.parameters.get("classification").map(String::as_str) == Some("json") {
                Ok(HiveFileFormat::Json)
            } else {
                Ok(HiveFileFormat::Csv)
            }
        }
        Some(PARQUET_INPUT_FORMAT) => Ok(HiveFileFormat::Parquet),
        Some(other) => {
            let keyword = friendly_keyword_for_input_format(other)
                .map(|k| format!(" ({k})"))
                .unwrap_or_default();
            Err(HiveError::Validation(format!(
                "input format `{other}`{keyword} is not supported. Supported: \
                 {TEXT_INPUT_FORMAT}, {PARQUET_INPUT_FORMAT}"
            )))
        }
        None => Err(HiveError::Validation(
            "no ROW FORMAT SERDE and no STORED AS/INPUTFORMAT this release recognizes \
             identified a readable format"
                .to_string(),
        )),
    }
}

/// Reads the first present key in `keys` from `props` and requires its
/// value to be exactly one byte.
///
/// No escape decoding happens here: `sqe_sql::hive_ddl` already resolved
/// any Hive/Java-style backslash escape (`\\`, `\t`, octal `\NNN`, etc.)
/// before the value ever reached the manifest, so by the time it is stored
/// here it already holds the byte Hive or Glue itself would. Doing that
/// decoding at consumption time instead would mean the manifest on disk
/// held the sqlparser-rs transport artifact (two literal backslash
/// characters for `escapeChar = '\\'`, say) rather than the one byte Hive
/// means, which breaks the same field-mapping promise the ORC/Avro/etc.
/// class-name fidelity in `manifest.rs` protects. A value that is not
/// exactly one byte here is therefore a real problem, and is rejected
/// naming the key and the value, not silently reinterpreted.
fn single_byte(props: &BTreeMap<String, String>, keys: &[&str]) -> Result<Option<u8>, HiveError> {
    let Some((key, value)) = keys
        .iter()
        .find_map(|k| props.get(*k).map(|v| (*k, v.as_str())))
    else {
        return Ok(None);
    };

    let bytes = value.as_bytes();
    let byte = match bytes {
        [b] => *b,
        _ => {
            return Err(HiveError::Validation(format!(
                "{key} = `{value}` must be exactly one byte"
            )))
        }
    };
    Ok(Some(byte))
}

/// Hive scalar types onto Arrow. Nested types (`array<...>`, `struct<...>`,
/// `map<...>`) are cycle-2 work and rejected by name here, mirroring
/// `sqe_sql::hive_ddl`'s own `'<'` rejection at parse time.
pub fn arrow_type(sql_type: &str) -> Result<DataType, HiveError> {
    let t = sql_type.trim().to_ascii_lowercase();

    if let Some(rest) = t.strip_prefix("decimal") {
        let rest = rest.trim();
        if rest.is_empty() {
            // Hive and Athena both default a bare `decimal` (no precision or
            // scale given) to `decimal(10,0)`. Athena parity is the point of
            // this feature, so this defaults rather than errors.
            return Ok(DataType::Decimal128(10, 0));
        }
        let inner = rest.trim_start_matches('(').trim_end_matches(')');
        let (p, s) = inner
            .split_once(',')
            .ok_or_else(|| HiveError::Validation(format!("type `{sql_type}` is not supported")))?;
        let precision: u8 = p
            .trim()
            .parse()
            .map_err(|_| HiveError::Validation(format!("type `{sql_type}` is not supported")))?;
        let scale: i8 = s
            .trim()
            .parse()
            .map_err(|_| HiveError::Validation(format!("type `{sql_type}` is not supported")))?;
        return Ok(DataType::Decimal128(precision, scale));
    }

    if t.contains('<') {
        return Err(HiveError::Validation(format!(
            "type `{sql_type}` is a nested type, which is not supported in this release"
        )));
    }

    // varchar/char carry an optional length, e.g. `varchar(255)`; that length
    // does not affect the Arrow mapping, so it is stripped rather than parsed.
    let base = t.split('(').next().unwrap_or(&t).trim();

    match base {
        "tinyint" => Ok(DataType::Int8),
        "smallint" => Ok(DataType::Int16),
        "int" | "integer" => Ok(DataType::Int32),
        "bigint" => Ok(DataType::Int64),
        "float" | "real" => Ok(DataType::Float32),
        "double" => Ok(DataType::Float64),
        "boolean" => Ok(DataType::Boolean),
        "string" | "varchar" | "char" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        _ => Err(HiveError::Validation(format!(
            "type `{sql_type}` is not supported"
        ))),
    }
}

/// Builds the `ListingOptions` a resolved format reads with.
///
/// `with_collect_stat(false)`: collecting real statistics reads a Parquet
/// footer per file at plan time, which is the exact latency this design
/// avoids. A later task supplies size-derived statistics instead.
pub fn listing_options(
    r: &ResolvedFormat,
    partition_cols: Vec<(String, DataType)>,
) -> Result<ListingOptions, HiveError> {
    let format: std::sync::Arc<dyn FileFormat> = match r.kind {
        HiveFileFormat::Csv => {
            let mut csv = CsvFormat::default()
                .with_delimiter(r.delimiter)
                .with_has_header(r.has_header)
                .with_escape(r.escape);
            if let Some(q) = r.quote {
                csv = csv.with_quote(q);
            }
            std::sync::Arc::new(csv)
        }
        HiveFileFormat::Json => std::sync::Arc::new(JsonFormat::default()),
        HiveFileFormat::Parquet => std::sync::Arc::new(ParquetFormat::default()),
    };

    Ok(ListingOptions::new(format)
        .with_file_extension(&r.file_extension)
        .with_table_partition_cols(partition_cols)
        .with_collect_stat(false))
}
