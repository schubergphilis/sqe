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
//! This runs only for statements the SQL front door classified as metadata
//! reads (`sqe_sql::NormalizedSql::is_metadata_query`, decided from the AST:
//! an `information_schema` relation, `SHOW COLUMNS` or `DESCRIBE <table>`) on
//! the Trino path; Flight SQL metadata RPCs build Arrow directly and are
//! untouched. Qualifying an unqualified `information_schema.<x>` with the
//! session catalog (#2) is likewise an AST pass in the front door
//! (`sqe_sql::qualify_information_schema`), so literals and comments that
//! mention `information_schema` are never touched (finding 704).

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};

/// Internal catalogs the built-in `information_schema` exposes that a Trino BI
/// client should never see.
const INTERNAL_CATALOGS: [&str; 2] = ["system", "datafusion"];

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

/// Reshape a SHOW COLUMNS / DESCRIBE result from SQE's
/// `[column_name, data_type, is_nullable]` to Trino's
/// `[Column, Type, Extra, Comment]`. The `data_type` values must already be
/// Trino type names (run [`apply_info_schema_compat`] first). Batches whose
/// shape doesn't match are returned unchanged. The server gates the call on
/// the statement being `SHOW COLUMNS` or `DESCRIBE <table>` in the AST, so
/// plain `information_schema.columns` selects keep their native column names.
pub fn reshape_describe_to_trino(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    batches.into_iter().map(reshape_describe_batch).collect()
}

fn reshape_describe_batch(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let (Ok(name_idx), Ok(type_idx)) =
        (schema.index_of("column_name"), schema.index_of("data_type"))
    else {
        return batch;
    };
    let n = batch.num_rows();
    let empty: arrow_array::ArrayRef = Arc::new(StringArray::from(vec![""; n]));
    let out_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("Column", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("Type", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("Extra", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("Comment", arrow_schema::DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        out_schema,
        vec![
            batch.column(name_idx).clone(),
            batch.column(type_idx).clone(),
            empty.clone(),
            empty,
        ],
    )
    .unwrap_or(batch)
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

    #[test]
    fn describe_reshape_to_trino_columns() {
        // [column_name, data_type, is_nullable] -> [Column, Type, Extra, Comment]
        let schema = Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("is_nullable", DataType::Utf8, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["id", "name"])),
                Arc::new(StringArray::from(vec!["bigint", "varchar"])),
                Arc::new(StringArray::from(vec!["NO", "YES"])),
            ],
        )
        .unwrap();
        let out = reshape_describe_to_trino(vec![b]);
        let s = out[0].schema();
        assert_eq!(
            s.fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["Column", "Type", "Extra", "Comment"]
        );
        assert_eq!(out[0].num_rows(), 2);
    }

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
        assert_eq!(
            arrow_display_to_trino_type("SomeFutureType"),
            "SomeFutureType"
        );
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
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
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
        let col = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
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

    // ── information_schema qualification on the Trino path (#2, 704) ───────
    //
    // The qualification itself is `sqe_sql::qualify_information_schema`, an
    // AST pass the SQL front door runs when the session has a catalog. These
    // tests drive it exactly the way the Trino server does, so they pin the
    // behaviour the wire sees: relations are qualified, literals, comments and
    // identifier substrings are not.

    use sqe_sql::{SqlFrontDoor, WireDialect};

    fn qualify(sql: &str, session_catalog: Option<&str>) -> String {
        SqlFrontDoor::new(WireDialect::Trino)
            .with_session_catalog(session_catalog)
            .normalize(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .sql()
            .to_string()
    }

    fn qualifier(catalog: &str) -> String {
        format!("\"{}\".information_schema", catalog.replace('"', "\"\""))
    }

    #[test]
    fn qualifies_unqualified_information_schema() {
        let out = qualify(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='gold'",
            Some("ws_energy_co"),
        );
        assert_eq!(
            out,
            "SELECT table_name FROM \"ws_energy_co\".information_schema.tables WHERE table_schema = 'gold'"
        );
    }

    #[test]
    fn leaves_already_qualified_unchanged() {
        let out = qualify(
            "SELECT 1 FROM ws_energy_co.information_schema.columns",
            Some("ws_energy_co"),
        );
        assert!(!out.contains(&qualifier("ws_energy_co")), "{out}");
        assert!(
            out.contains("FROM ws_energy_co.information_schema.columns"),
            "{out}"
        );
    }

    #[test]
    fn leaves_string_literal_unchanged() {
        let out = qualify(
            "SELECT 1 WHERE table_schema = 'information_schema'",
            Some("ws_energy_co"),
        );
        assert!(!out.contains(&qualifier("ws_energy_co")), "{out}");
        assert!(out.contains("'information_schema'"), "{out}");
    }

    #[test]
    fn leaves_identifier_substring_unchanged() {
        let sql = "SELECT * FROM my_information_schema.t";
        assert_eq!(qualify(sql, Some("ws_energy_co")), sql);
    }

    #[test]
    fn no_session_catalog_is_noop() {
        let sql = "SELECT * FROM information_schema.tables";
        assert_eq!(qualify(sql, None), sql);
    }

    #[test]
    fn case_insensitive_preserves_original_case() {
        let out = qualify("SELECT * FROM INFORMATION_SCHEMA.TABLES", Some("ws"));
        assert_eq!(out, "SELECT * FROM \"ws\".INFORMATION_SCHEMA.TABLES");
    }

    #[test]
    fn malicious_session_catalog_cannot_break_out_of_quoted_identifier() {
        // The X-Trino-Catalog header is attacker-controlled. A double-quote in
        // the value must be doubled so it cannot close the quoted identifier
        // and inject SQL into the rewritten statement.
        let out = qualify(
            "SELECT * FROM information_schema.tables",
            Some("evil\".oops--"),
        );
        assert_eq!(
            out,
            "SELECT * FROM \"evil\"\".oops--\".information_schema.tables"
        );
        // The raw (un-doubled) breakout sequence must not appear.
        assert!(!out.contains("evil\".oops"), "breakout present: {out}");
    }

    #[test]
    fn qualifies_multiple_occurrences() {
        let out = qualify(
            "SELECT * FROM information_schema.tables t JOIN information_schema.columns c ON true",
            Some("ws"),
        );
        assert_eq!(out.matches(&qualifier("ws")).count(), 2, "{out}");
        assert!(out.contains("\"ws\".information_schema.tables"), "{out}");
        assert!(out.contains("\"ws\".information_schema.columns"), "{out}");
    }

    // ── Adversarial inputs: comments, quoted identifiers, literals (#630) ──

    #[test]
    fn quoted_information_schema_identifier_is_qualified_too() {
        // `"information_schema".tables` names the same relation as the bare
        // form; the AST pass sees the identifier value, not the quoting, so
        // it is qualified as well (the text scanner used to skip it).
        let out = qualify("SELECT * FROM \"information_schema\".tables", Some("ws"));
        assert_eq!(out, "SELECT * FROM \"ws\".\"information_schema\".tables");
    }

    #[test]
    fn leading_comment_does_not_block_qualification() {
        // A leading comment used to defeat prefix detection. It is dropped
        // when the AST is re-serialized, which is the documented cost of any
        // rewrite firing.
        let out = qualify(
            "-- list tables )\nSELECT * FROM information_schema.tables",
            Some("ws"),
        );
        assert_eq!(out, "SELECT * FROM \"ws\".information_schema.tables");
    }

    /// Finding 704 acceptance: `'information_schema.tables'` in a WHERE clause
    /// is a value the query compares against, not a relation, and keeps its
    /// bytes.
    #[test]
    fn information_schema_dot_inside_literal_is_untouched() {
        let out = qualify(
            "SELECT 1 WHERE table_schema = 'information_schema.tables'",
            Some("ws"),
        );
        assert!(!out.contains(&qualifier("ws")), "{out}");
        assert!(out.contains("= 'information_schema.tables'"), "{out}");

        // Returned verbatim when nothing else rewrites the statement.
        let sql = "SELECT * FROM t WHERE note LIKE '%information_schema.%'";
        assert_eq!(qualify(sql, Some("ws")), sql);
    }

    #[test]
    fn information_schema_dot_inside_comment_is_untouched() {
        let sql = "SELECT * FROM t -- see information_schema.tables";
        assert_eq!(qualify(sql, Some("ws")), sql);

        let out = qualify("-- see information_schema.tables\nSELECT 1", Some("ws"));
        assert!(!out.contains(&qualifier("ws")), "{out}");
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        /// Scanner-hostile bytes with no quotes and no `*`, plus the needle
        /// itself in both cases and followed by a dot.
        fn payload() -> impl Strategy<Value = String> {
            prop::collection::vec(
                prop_oneof![
                    Just(")".to_string()),
                    Just(".".to_string()),
                    Just(" information_schema".to_string()),
                    Just(" INFORMATION_SCHEMA.".to_string()),
                    Just("information_schema.tables".to_string()),
                    Just("x_information_schema.".to_string()),
                    Just(" -- ".to_string()),
                    Just(" /* ".to_string()),
                    "[a-z0-9 ]{1,4}",
                ],
                1..6,
            )
            .prop_map(|parts| parts.concat())
        }

        fn catalog() -> impl Strategy<Value = String> {
            prop_oneof![
                "[a-z][a-z0-9_]{0,8}",
                Just("evil\".oops--".to_string()),
                Just("ws energy".to_string()),
            ]
        }

        fn literal() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("'{p}'"))
        }

        fn line_comment() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("--{p}\n"))
        }

        fn block_comment() -> impl Strategy<Value = String> {
            payload().prop_map(|p| format!("/*{}*/", p.replace("/*", "/ *")))
        }

        proptest! {
            /// (a) A literal's bytes survive the rewrite untouched, and exactly
            /// the one relation is qualified, however hostile the literal and
            /// the surrounding comments look to a text scanner.
            #[test]
            fn literal_bytes_survive_the_rewrite(
                l in literal(),
                lc in line_comment(),
                bc in block_comment(),
                cat in catalog(),
            ) {
                let sql = format!("{lc}SELECT {l} AS v FROM information_schema.tables {bc}");
                let out = qualify(&sql, Some(&cat));
                prop_assert!(out.contains(&l), "literal changed: {out}");
                prop_assert_eq!(out.matches(&qualifier(&cat)).count(), 1, "{}", out);
            }

            /// (b) With no relation to qualify, comments and literals that
            /// mention the needle leave the statement byte-identical.
            #[test]
            fn needle_in_literal_or_comment_is_identity(
                l in literal(),
                lc in line_comment(),
                bc in block_comment(),
                cat in catalog(),
            ) {
                let sql = format!("{lc}SELECT * FROM t WHERE x = {l} {bc}");
                prop_assert_eq!(qualify(&sql, Some(&cat)), sql);
            }
        }
    }
}
