//! Trino-compatibility layer over DataFusion's built-in `information_schema`.
//!
//! We keep DataFusion's standard built-in `information_schema` (it serves every
//! catalog uniformly and survives without live Iceberg connectivity). It has
//! two traits that break BI tools speaking the Trino protocol, which this layer
//! corrects on the HTTP result boundary only:
//!
//!  1. `data_type` is rendered with Arrow display names (`Utf8`, `Int64`,
//!     `Timestamp(µs)`) -- Trino clients parse these as Trino SQL type names
//!     and fall back to "unknown", breaking schema sync. We translate them.
//!  2. The built-in is global: it merges every catalog, including SQE's
//!     internal `system`/`datafusion` catalogs, so a BI tool filtering only by
//!     `table_schema` sees engine internals. We scope the listing to the
//!     session catalog (or, absent one, drop the internal catalogs).
//!
//! This runs only for metadata queries (`is_metadata_query`) on the Trino path;
//! Flight SQL metadata RPCs build Arrow directly and are untouched.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};

/// Internal catalogs the built-in `information_schema` exposes that a Trino BI
/// client should never see.
const INTERNAL_CATALOGS: [&str; 2] = ["system", "datafusion"];

/// Does this SQL read table metadata (so its result should be Trino-normalized)?
pub fn is_metadata_query(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_lowercase();
    s.contains("information_schema")
        || s.starts_with("show columns")
        || s.starts_with("describe ")
        || s.starts_with("desc ")
}

/// Map a DataFusion Arrow type *display string* (as emitted by the built-in
/// `information_schema.data_type` column) to a Trino SQL type name. Unknown
/// inputs pass through unchanged rather than being mangled.
pub fn arrow_display_to_trino_type(s: &str) -> String {
    let t = s.trim();
    if let Some(rest) = t
        .strip_prefix("Decimal128(")
        .or_else(|| t.strip_prefix("Decimal256("))
    {
        return format_decimal(rest);
    }
    if let Some(inner) = t.strip_prefix("Timestamp(") {
        // A tz argument shows up as a second, comma-separated element.
        return if inner.contains(',') {
            "timestamp with time zone".to_string()
        } else {
            "timestamp".to_string()
        };
    }
    if t.starts_with("Time32(") || t.starts_with("Time64(") {
        return "time".to_string();
    }
    if t.starts_with("FixedSizeBinary") {
        return "varbinary".to_string();
    }
    if t.starts_with("List") || t.starts_with("LargeList") || t.starts_with("FixedSizeList") {
        return "array".to_string();
    }
    if t.starts_with("Struct") {
        return "row".to_string();
    }
    if t.starts_with("Map") {
        return "map".to_string();
    }
    let mapped = match t {
        "Boolean" => "boolean",
        "Int8" => "tinyint",
        "Int16" => "smallint",
        "Int32" => "integer",
        "Int64" => "bigint",
        "UInt8" => "smallint",
        "UInt16" => "integer",
        "UInt32" => "bigint",
        "UInt64" => "decimal(20,0)",
        "Float16" | "Float32" => "real",
        "Float64" => "double",
        "Utf8" | "LargeUtf8" | "Utf8View" => "varchar",
        "Binary" | "LargeBinary" | "BinaryView" => "varbinary",
        "Date32" | "Date64" => "date",
        "Null" => "unknown",
        // Unknown: pass through rather than mangle a type we don't recognize.
        other => return other.to_string(),
    };
    mapped.to_string()
}

/// Render `Decimal128(10, 2)` / `Decimal256(p, s)` tails as `decimal(p,s)`.
fn format_decimal(rest: &str) -> String {
    let inner = rest.trim_end_matches(')');
    let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
    if parts.len() == 2 {
        format!("decimal({},{})", parts[0], parts[1])
    } else {
        "decimal".to_string()
    }
}

/// Normalize an `information_schema` result for Trino clients: translate the
/// `data_type` column to Trino type names and scope the catalog listing.
///
/// `session_catalog` is the catalog on the connection (`X-Trino-Catalog`); when
/// present, only rows for that catalog are kept. When absent, internal catalogs
/// are dropped but real catalogs remain.
pub fn apply_info_schema_compat(
    batches: Vec<RecordBatch>,
    session_catalog: Option<&str>,
) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .map(|b| scope_catalog(translate_data_type(b), session_catalog))
        .collect()
}

/// Replace the Arrow display strings in a `data_type` column with Trino names.
fn translate_data_type(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let idx = match schema.index_of("data_type") {
        Ok(i) => i,
        Err(_) => return batch,
    };
    let col = match batch.column(idx).as_any().downcast_ref::<StringArray>() {
        Some(c) => c,
        None => return batch,
    };
    let translated: StringArray = col
        .iter()
        .map(|v| v.map(arrow_display_to_trino_type))
        .collect();
    let mut cols = batch.columns().to_vec();
    cols[idx] = Arc::new(translated);
    RecordBatch::try_new(schema, cols).unwrap_or(batch)
}

/// Drop catalogs a Trino client should not see: scope to the session catalog
/// when set, otherwise hide the engine-internal catalogs.
fn scope_catalog(batch: RecordBatch, session_catalog: Option<&str>) -> RecordBatch {
    let schema = batch.schema();
    let cat_idx = schema
        .index_of("table_catalog")
        .or_else(|_| schema.index_of("catalog_name"));
    let cat_idx = match cat_idx {
        Ok(i) => i,
        Err(_) => return batch,
    };
    let col = match batch.column(cat_idx).as_any().downcast_ref::<StringArray>() {
        Some(c) => c,
        None => return batch,
    };
    let mask: BooleanArray = (0..col.len())
        .map(|i| {
            if col.is_null(i) {
                return Some(true);
            }
            let v = col.value(i);
            Some(match session_catalog {
                Some(sc) => v == sc,
                None => !INTERNAL_CATALOGS.contains(&v),
            })
        })
        .collect();
    match arrow::compute::filter_record_batch(&batch, &mask) {
        Ok(filtered) => filtered,
        Err(_) => batch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};

    // ── type mapping (ground truth from the live stack discriminator) ──────
    #[test]
    fn maps_known_arrow_display_strings() {
        let cases = [
            ("Boolean", "boolean"),
            ("Int8", "tinyint"),
            ("Int16", "smallint"),
            ("Int32", "integer"),
            ("Int64", "bigint"),
            ("Float32", "real"),
            ("Float64", "double"),
            ("Utf8", "varchar"),
            ("LargeUtf8", "varchar"),
            ("Date32", "date"),
            ("LargeBinary", "varbinary"),
            ("Decimal128(10, 2)", "decimal(10,2)"),
            ("Time64(µs)", "time"),
            ("Timestamp(µs)", "timestamp"),
            ("Timestamp(µs, \"+00:00\")", "timestamp with time zone"),
        ];
        for (input, want) in cases {
            assert_eq!(arrow_display_to_trino_type(input), want, "input {input}");
        }
    }

    #[test]
    fn unknown_type_passes_through() {
        assert_eq!(arrow_display_to_trino_type("SomeFutureType"), "SomeFutureType");
    }

    #[test]
    fn composite_types_map_to_trino_kinds() {
        assert_eq!(arrow_display_to_trino_type("Struct([Field ...])"), "row");
        assert_eq!(arrow_display_to_trino_type("List(Field ...)"), "array");
        assert_eq!(arrow_display_to_trino_type("Map(Field ...)"), "map");
    }

    // ── result normalization ───────────────────────────────────────────────
    fn columns_batch(names: &[&str], types: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(types.to_vec())),
            ],
        )
        .unwrap()
    }

    fn data_type_values(b: &RecordBatch) -> Vec<String> {
        let idx = b.schema().index_of("data_type").unwrap();
        let col = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
        (0..col.len()).map(|i| col.value(i).to_string()).collect()
    }

    #[test]
    fn translates_data_type_column() {
        let b = columns_batch(&["a", "b"], &["Int64", "Utf8"]);
        let out = apply_info_schema_compat(vec![b], None);
        assert_eq!(data_type_values(&out[0]), vec!["bigint", "varchar"]);
    }

    fn tables_batch(catalogs: &[&str], schemas: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(catalogs.to_vec())),
                Arc::new(StringArray::from(schemas.to_vec())),
            ],
        )
        .unwrap()
    }

    fn catalog_values(b: &RecordBatch) -> Vec<String> {
        let idx = b.schema().index_of("table_catalog").unwrap();
        let col = b.column(idx).as_any().downcast_ref::<StringArray>().unwrap();
        (0..col.len()).map(|i| col.value(i).to_string()).collect()
    }

    #[test]
    fn scopes_to_session_catalog_when_set() {
        let b = tables_batch(
            &["iceberg", "iceberg", "system", "datafusion"],
            &["default", "sales", "jdbc", "information_schema"],
        );
        let out = apply_info_schema_compat(vec![b], Some("iceberg"));
        assert_eq!(catalog_values(&out[0]), vec!["iceberg", "iceberg"]);
    }

    #[test]
    fn drops_internal_catalogs_when_no_session_catalog() {
        let b = tables_batch(
            &["iceberg", "system", "datafusion"],
            &["default", "jdbc", "information_schema"],
        );
        let out = apply_info_schema_compat(vec![b], None);
        assert_eq!(catalog_values(&out[0]), vec!["iceberg"]);
    }

    #[test]
    fn passthrough_when_no_relevant_columns() {
        // A normal query result (no data_type / table_catalog) is unchanged.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let out = apply_info_schema_compat(vec![b], Some("iceberg"));
        assert_eq!(out[0].num_rows(), 3);
    }
}
