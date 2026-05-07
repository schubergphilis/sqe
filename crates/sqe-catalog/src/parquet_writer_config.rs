//! Shared Parquet `WriterProperties` builder for Iceberg data-file writes.
//!
//! Reads Iceberg table properties and produces `WriterProperties` with
//! bloom filters enabled per column. Used by both the coordinator's batch
//! and streaming write paths, and by any future worker-side writer so that
//! distributed writes honour the same bloom filter configuration.
//!
//! ## Properties consumed
//!
//! - `write.parquet.bloom-filter-columns` (comma-separated top-level column
//!   names). Explicit override; when set, only these columns get blooms.
//! - `write.parquet.bloom-filter-auto` (`true` / `false`). Default `true`.
//!   When `bloom-filter-columns` is absent, auto-detects FK-shaped
//!   integer columns (names ending in `_sk` or `_id`) and enables blooms
//!   for them. Set to `false` to opt out entirely.
//! - `write.parquet.bloom-filter-fpp` (float, 0 < fpp < 1). Default 0.01.
//!
//! ## Auto-detection rationale
//!
//! Star-schema fact tables join high-cardinality FK columns (cs_item_sk,
//! ss_store_sk, etc.) against filtered dimension tables. Without bloom
//! filters, DataFusion's dynamic filter from the dim build side prunes at
//! the row level only — it cannot skip Parquet row groups whose min/max
//! covers the full FK range. Bloom filters give exact-membership skipping
//! per row group at a tiny storage cost (~0.5-3 KB per file per column at
//! the default 1% FPP). The `_sk` and `_id` suffixes are the standard FK
//! convention in TPC-DS, TPC-H, SSB, and most Kimball-style schemas, so
//! the heuristic is conservative and false positives only cost extra
//! bloom-filter bytes, not correctness.
//!
//! ## Design
//!
//! The helper is intentionally thin. It owns only the property parsing and
//! the `WriterProperties` builder mutations; compression codec selection
//! stays with the caller because the position-delete writer, which does not
//! honour bloom filters, reuses the compression argument.
//!
//! The function signature takes a raw `&HashMap<String, String>` plus an
//! Iceberg schema so callers can stand up tests without a live `Table`.
//! Call sites that hold a `Table` use [`writer_props_for_table`].

use std::collections::HashMap;

use iceberg::spec::{PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::table::Table;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// Iceberg table property that lists columns to get Parquet bloom filters.
///
/// Value is a comma-separated list of column names (case-sensitive, matched
/// against the top-level schema field names). When set, this list is the
/// authoritative choice and auto-detection is skipped.
pub const PROP_BLOOM_FILTER_COLUMNS: &str = "write.parquet.bloom-filter-columns";

/// Iceberg table property to opt out of FK auto-detection.
///
/// Default `true`. When `false` and `write.parquet.bloom-filter-columns`
/// is unset, no bloom filters are written. Operators with storage-cost
/// concerns or external readers that don't consume Parquet blooms can
/// disable per-table.
pub const PROP_BLOOM_FILTER_AUTO: &str = "write.parquet.bloom-filter-auto";

/// Iceberg table property for the bloom filter false-positive probability.
///
/// Defaults to [`DEFAULT_BLOOM_FILTER_FPP`] (1%) when absent or unparseable.
pub const PROP_BLOOM_FILTER_FPP: &str = "write.parquet.bloom-filter-fpp";

/// Default bloom filter FPP when the table property is absent or invalid.
pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;

/// Default for `write.parquet.bloom-filter-auto`.
///
/// True means we auto-bloom FK-shaped integer columns when no explicit
/// list is given. The cost is small (a few KB per file per column) and
/// the win on multi-way star-schema joins is large.
pub const DEFAULT_BLOOM_FILTER_AUTO: bool = true;

/// Build `WriterProperties` honouring the table's bloom filter properties.
///
/// Reads `write.parquet.bloom-filter-columns` (comma-separated explicit
/// column list) and optional `write.parquet.bloom-filter-fpp` (float) and
/// enables per-column bloom filters on the returned [`WriterProperties`].
/// When `bloom-filter-columns` is unset and `bloom-filter-auto` is true
/// (default), auto-detects FK-shaped integer columns; see
/// [`auto_detect_fk_columns`].
pub fn writer_props_for_table(
    table: &Table,
    compression: Compression,
) -> WriterProperties {
    build_writer_props(
        table.metadata().properties(),
        table.metadata().current_schema(),
        compression,
    )
}

/// Pure helper used by [`writer_props_for_table`] and unit tests.
///
/// Decouples property parsing from the iceberg-rust [`Table`] so callers
/// without a live catalog can still exercise every branch.
pub fn build_writer_props(
    properties: &HashMap<String, String>,
    schema: &IcebergSchema,
    compression: Compression,
) -> WriterProperties {
    let mut builder = WriterProperties::builder().set_compression(compression);

    // Resolve which columns to bloom: explicit list wins; otherwise fall
    // back to auto-detection unless the operator opted out via
    // `write.parquet.bloom-filter-auto = false`.
    let explicit = parse_bloom_filter_columns(properties);
    let columns = if !explicit.is_empty() {
        explicit
    } else if parse_bloom_filter_auto(properties) {
        auto_detect_fk_columns(schema)
    } else {
        Vec::new()
    };

    if columns.is_empty() {
        return builder.build();
    }

    let fpp = parse_bloom_filter_fpp(properties);
    let schema_fields = schema.as_struct().fields();
    let valid_names: Vec<&str> = schema_fields.iter().map(|f| f.name.as_str()).collect();

    for col in &columns {
        if valid_names.contains(&col.as_str()) {
            let path = ColumnPath::new(vec![col.clone()]);
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path, fpp);
        } else {
            tracing::warn!(
                column = %col,
                "write.parquet.bloom-filter-columns references unknown column; skipping"
            );
        }
    }

    builder.build()
}

/// Pick FK-shaped integer columns from the schema for default bloom filtering.
///
/// A column is treated as a foreign-key candidate when its name ends in
/// `_sk` or `_id` (case-sensitive, matching the TPC-DS / Kimball
/// convention) and its primitive type is `Int` or `Long`. Other types
/// (strings, decimals, dates, floats) are skipped: bloom filters on
/// strings work but most string FKs are low-cardinality codes where
/// dictionary encoding already covers skipping; floats and decimals
/// rarely participate in equi-joins.
///
/// Returns column names in schema order, deduplicated. The caller still
/// validates each name against the schema so unknown columns log a
/// warning rather than panic.
pub fn auto_detect_fk_columns(schema: &IcebergSchema) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for field in schema.as_struct().fields() {
        if !is_fk_name(&field.name) {
            continue;
        }
        if !is_integer_primitive(&field.field_type) {
            continue;
        }
        if !out.contains(&field.name) {
            out.push(field.name.clone());
        }
    }
    out
}

fn is_fk_name(name: &str) -> bool {
    name.ends_with("_sk") || name.ends_with("_id")
}

fn is_integer_primitive(ty: &Type) -> bool {
    matches!(
        ty,
        Type::Primitive(PrimitiveType::Int) | Type::Primitive(PrimitiveType::Long)
    )
}

/// Parse `write.parquet.bloom-filter-auto` (true/false), default true.
pub fn parse_bloom_filter_auto(properties: &HashMap<String, String>) -> bool {
    properties
        .get(PROP_BLOOM_FILTER_AUTO)
        .map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(DEFAULT_BLOOM_FILTER_AUTO)
}

/// Parse `write.parquet.bloom-filter-columns` into a deduplicated list.
///
/// Values are comma-separated, trimmed, and compared case-sensitively
/// against the schema. Duplicate names fold silently so typos in the
/// property do not blow up the writer.
pub fn parse_bloom_filter_columns(properties: &HashMap<String, String>) -> Vec<String> {
    let Some(raw) = properties.get(PROP_BLOOM_FILTER_COLUMNS) else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let name = trimmed.to_string();
        if !out.contains(&name) {
            out.push(name);
        }
    }
    out
}

/// Parse `write.parquet.bloom-filter-fpp` or fall back to the default.
pub fn parse_bloom_filter_fpp(properties: &HashMap<String, String>) -> f64 {
    properties
        .get(PROP_BLOOM_FILTER_FPP)
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|f| *f > 0.0 && *f < 1.0)
        .unwrap_or(DEFAULT_BLOOM_FILTER_FPP)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};

    fn schema_id_name() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    /// Schema modelling a small TPC-DS-style fact table with both FK and
    /// non-FK columns of varied types — used to exercise the auto-detect
    /// heuristic.
    fn schema_fact_table() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "ss_item_sk", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(2, "ss_store_sk", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(3, "ss_promo_sk", Type::Primitive(PrimitiveType::Long))
                    .into(),
                NestedField::required(4, "i_item_id", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::optional(5, "ss_quantity", Type::Primitive(PrimitiveType::Int))
                    .into(),
                NestedField::optional(
                    6,
                    "ss_sales_price",
                    Type::Primitive(PrimitiveType::Double),
                )
                .into(),
            ])
            .build()
            .expect("schema")
    }

    #[test]
    fn bloom_filter_auto_detects_integer_fk_columns_by_default() {
        // No properties at all means default: auto-detect FK-shaped
        // integer columns. The string `i_item_id` is rejected (wrong
        // type) and `ss_quantity`/`ss_sales_price` are rejected (no FK
        // suffix or wrong type).
        let props = HashMap::new();
        let schema = schema_fact_table();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        for fk in ["ss_item_sk", "ss_store_sk", "ss_promo_sk"] {
            assert!(
                w.bloom_filter_properties(&ColumnPath::new(vec![fk.to_string()]))
                    .is_some(),
                "{fk} should be auto-bloomed (integer FK)"
            );
        }
        for non_fk in ["i_item_id", "ss_quantity", "ss_sales_price"] {
            assert!(
                w.bloom_filter_properties(&ColumnPath::new(vec![non_fk.to_string()]))
                    .is_none(),
                "{non_fk} should NOT be auto-bloomed"
            );
        }
    }

    #[test]
    fn bloom_filter_auto_off_via_property() {
        // Operator opts out: even FK-shaped columns get no bloom.
        let mut props = HashMap::new();
        props.insert(PROP_BLOOM_FILTER_AUTO.to_string(), "false".to_string());
        let schema = schema_fact_table();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        for col in ["ss_item_sk", "ss_store_sk", "ss_promo_sk"] {
            assert!(
                w.bloom_filter_properties(&ColumnPath::new(vec![col.to_string()]))
                    .is_none(),
                "{col} should NOT be bloomed when auto=false"
            );
        }
    }

    #[test]
    fn explicit_bloom_columns_override_auto_detection() {
        // Explicit list trumps auto-detection: only `ss_promo_sk` is
        // bloomed, even though other FK-shaped columns exist.
        let mut props = HashMap::new();
        props.insert(
            PROP_BLOOM_FILTER_COLUMNS.to_string(),
            "ss_promo_sk".to_string(),
        );
        let schema = schema_fact_table();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        assert!(
            w.bloom_filter_properties(&ColumnPath::new(vec!["ss_promo_sk".to_string()]))
                .is_some()
        );
        for col in ["ss_item_sk", "ss_store_sk"] {
            assert!(
                w.bloom_filter_properties(&ColumnPath::new(vec![col.to_string()]))
                    .is_none(),
                "{col} should NOT be bloomed when an explicit list excludes it"
            );
        }
    }

    #[test]
    fn auto_detect_skips_string_fk() {
        // String columns are skipped even with FK suffixes — most string
        // FKs are low-cardinality codes where dictionary encoding already
        // gives row-group skipping. See module docs.
        let schema = schema_fact_table();
        let cols = auto_detect_fk_columns(&schema);
        assert!(!cols.contains(&"i_item_id".to_string()));
        assert_eq!(cols, vec!["ss_item_sk", "ss_store_sk", "ss_promo_sk"]);
    }

    #[test]
    fn legacy_simple_schema_yields_no_auto_blooms() {
        // The original `id` / `name` schema: `id` is a Long but doesn't
        // end in `_sk` or `_id` suffix so no auto detection. Explicit
        // bloom path still works.
        let props = HashMap::new();
        let schema = schema_id_name();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);
        assert!(
            w.bloom_filter_properties(&ColumnPath::new(vec!["id".to_string()]))
                .is_none(),
            "bare 'id' is not auto-bloomed (no _sk/_id suffix)"
        );
    }

    #[test]
    fn bloom_filter_columns_single_column_enables_bloom() {
        let mut props = HashMap::new();
        props.insert(PROP_BLOOM_FILTER_COLUMNS.to_string(), "id".to_string());
        let schema = schema_id_name();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        let bf = w
            .bloom_filter_properties(&ColumnPath::new(vec!["id".to_string()]))
            .expect("id should have bloom filter");
        assert!((bf.fpp - DEFAULT_BLOOM_FILTER_FPP).abs() < f64::EPSILON);

        assert!(
            w.bloom_filter_properties(&ColumnPath::new(vec!["name".to_string()]))
                .is_none(),
            "name column should not have bloom filter"
        );
    }

    #[test]
    fn bloom_filter_fpp_honours_property() {
        let mut props = HashMap::new();
        props.insert(PROP_BLOOM_FILTER_COLUMNS.to_string(), "id".to_string());
        props.insert(PROP_BLOOM_FILTER_FPP.to_string(), "0.05".to_string());
        let schema = schema_id_name();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        let bf = w
            .bloom_filter_properties(&ColumnPath::new(vec!["id".to_string()]))
            .expect("id should have bloom filter");
        assert!((bf.fpp - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn bloom_filter_fpp_invalid_falls_back_to_default() {
        let mut props = HashMap::new();
        props.insert(PROP_BLOOM_FILTER_COLUMNS.to_string(), "id".to_string());
        props.insert(PROP_BLOOM_FILTER_FPP.to_string(), "garbage".to_string());
        let schema = schema_id_name();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);
        let bf = w
            .bloom_filter_properties(&ColumnPath::new(vec!["id".to_string()]))
            .expect("id should have bloom filter");
        assert!((bf.fpp - DEFAULT_BLOOM_FILTER_FPP).abs() < f64::EPSILON);
    }

    #[test]
    fn bloom_filter_unknown_column_is_skipped() {
        let mut props = HashMap::new();
        props.insert(
            PROP_BLOOM_FILTER_COLUMNS.to_string(),
            "id,does_not_exist".to_string(),
        );
        let schema = schema_id_name();
        let w = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);
        assert!(
            w.bloom_filter_properties(&ColumnPath::new(vec!["id".to_string()]))
                .is_some()
        );
        assert!(
            w.bloom_filter_properties(&ColumnPath::new(vec!["does_not_exist".to_string()]))
                .is_none()
        );
    }

    #[test]
    fn parse_bloom_filter_columns_trims_and_dedups() {
        let mut props = HashMap::new();
        props.insert(
            PROP_BLOOM_FILTER_COLUMNS.to_string(),
            " id , name ,id,, ".to_string(),
        );
        let cols = parse_bloom_filter_columns(&props);
        assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
    }

    /// End-to-end footer inspection: build `WriterProperties` from
    /// table props, write a tiny parquet file with them, then re-read
    /// the file's metadata and assert that the bloomed column carries a
    /// bloom filter offset. Closes the gap in the matrix evidence for
    /// `sqe:bloom-filters:v2/v3`: the previous tests proved property
    /// parsing and the v3 e2e test proved property round-trip through
    /// the catalog, but neither inspected the resulting file's parquet
    /// footer. This test does, without needing the docker-compose
    /// stack or any S3 plumbing.
    #[test]
    fn writer_props_emit_bloom_filter_in_parquet_footer() {
        use std::sync::Arc;

        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use bytes::Bytes;
        use parquet::arrow::ArrowWriter;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        // Build the same WriterProperties production uses, with bloom
        // filters on `id` only. `name` should NOT get a bloom.
        let mut props = HashMap::new();
        props.insert(PROP_BLOOM_FILTER_COLUMNS.to_string(), "id".to_string());
        let schema = schema_id_name();
        let writer_props = build_writer_props(&props, &schema, Compression::UNCOMPRESSED);

        // Build a 4-row record batch matching the iceberg schema. The
        // bloom filter is sized to the per-page row count; a single
        // batch is enough to populate it.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .expect("record batch");

        // Write to an in-memory buffer so the test stays self-contained.
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, Some(writer_props))
                .expect("ArrowWriter");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        // Re-read the file and assert bloom filter offsets.
        let reader = SerializedFileReader::new(Bytes::from(buf)).expect("reader");
        let metadata = reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1, "expected single row group");
        let rg = metadata.row_group(0);

        // Column ordering matches the arrow schema: 0 = id, 1 = name.
        let id_col = rg.column(0);
        let name_col = rg.column(1);

        assert!(
            id_col.bloom_filter_offset().is_some(),
            "id column should carry a bloom filter offset; metadata: {id_col:?}"
        );
        assert!(
            name_col.bloom_filter_offset().is_none(),
            "name column should NOT carry a bloom filter offset; metadata: {name_col:?}"
        );
    }
}
