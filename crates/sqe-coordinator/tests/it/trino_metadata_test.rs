//! Trino-wire metadata compatibility: what `information_schema` actually serves.
//!
//! BI tools on the Trino protocol (Superset, Metabase, DBeaver, the Trino
//! JDBC driver) reflect schema through UNQUALIFIED `information_schema`
//! queries and parse `data_type` as Trino SQL type names. This binary pins
//! down two open questions on the real engine path:
//!   1. Does DataFusion's built-in `information_schema` (Arrow type names)
//!      shadow SQE's custom per-catalog provider (SQL type names)?
//!   2. Does an unqualified `information_schema` reference resolve to the
//!      session catalog, or to the config-derived default catalog?
//!
//! `#[ignore]` because it needs the live test stack:
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored --nocapture \
//!     trino_metadata_test
//! ```

use arrow_array::{Array, StringArray};

fn print_str_cols(label: &str, batches: &[arrow_array::RecordBatch]) {
    eprintln!("---- {label} ----");
    for b in batches {
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        eprintln!("    columns: {names:?}");
        for row in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let col = b.column(c);
                    col.as_any()
                        .downcast_ref::<StringArray>()
                        .map(|s| {
                            if s.is_null(row) {
                                "NULL".into()
                            } else {
                                s.value(row).to_string()
                            }
                        })
                        .unwrap_or_else(|| format!("<{}>", col.data_type()))
                })
                .collect();
            eprintln!("    {cells:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn information_schema_reveals_provider_and_catalog() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "trino_meta_types";
    let fq = format!("{ns}.{name}");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    handler
        .execute(
            &session,
            &format!(
                "CREATE TABLE {fq} (\
                 c_int INT, c_big BIGINT, c_str VARCHAR, c_dbl DOUBLE, \
                 c_dec DECIMAL(10,2), c_date DATE, c_ts TIMESTAMP, c_bool BOOLEAN)"
            ),
            None,
        )
        .await
        .expect("create table");

    // (A) THE discriminator: unqualified information_schema.columns data_type.
    // Arrow names (Utf8/Int64/Date32) => built-in shadows custom provider.
    // SQL names (varchar/bigint/date)  => custom provider wins.
    let cols = handler
        .execute(
            &session,
            &format!(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_name = '{name}' ORDER BY ordinal_position"
            ),
            None,
        )
        .await
        .expect("info_schema.columns query");
    print_str_cols("(A) unqualified information_schema.columns", &cols);

    // (B) What catalogs/schemas does unqualified information_schema.tables see?
    // Reveals whether the default catalog is the user catalog or the system one.
    let tbls = handler
        .execute(
            &session,
            "SELECT table_catalog, table_schema, count(*) AS n \
             FROM information_schema.tables GROUP BY table_catalog, table_schema \
             ORDER BY table_catalog, table_schema",
            None,
        )
        .await;
    match tbls {
        Ok(b) => {
            eprintln!("---- (B) information_schema.tables grouped ----");
            for batch in &b {
                let schema = batch.schema();
                eprintln!(
                    "    columns: {:?}",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                );
                for row in 0..batch.num_rows() {
                    let cat = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|s| s.value(row).to_string())
                        .unwrap_or_default();
                    let sch = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|s| s.value(row).to_string())
                        .unwrap_or_default();
                    eprintln!("    catalog={cat} schema={sch}");
                }
            }
        }
        Err(e) => eprintln!("---- (B) information_schema.tables ERROR: {e}"),
    }

    // (C) SHOW COLUMNS path (rewrites to information_schema.columns).
    match handler
        .execute(&session, &format!("SHOW COLUMNS FROM {fq}"), None)
        .await
    {
        Ok(b) => print_str_cols("(C) SHOW COLUMNS", &b),
        Err(e) => eprintln!("---- (C) SHOW COLUMNS ERROR: {e}"),
    }

    // (D) Catalog-qualified columns, to compare against the unqualified (A).
    match handler
        .execute(
            &session,
            &format!(
                "SELECT column_name, data_type FROM iceberg.information_schema.columns \
                 WHERE table_name = '{name}' ORDER BY ordinal_position"
            ),
            None,
        )
        .await
    {
        Ok(b) => print_str_cols("(D) iceberg.information_schema.columns", &b),
        Err(e) => eprintln!("---- (D) catalog-qualified columns ERROR: {e}"),
    }

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// #4: DESCRIBE <table> must be supported (aliased to SHOW COLUMNS) instead of
// failing with "Statement type not supported". Returns the same projection.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn describe_table_aliases_to_show_columns() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "describe_probe";
    let fq = format!("{ns}.{name}");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    handler
        .execute(
            &session,
            &format!("CREATE TABLE {fq} (a INT, b VARCHAR)"),
            None,
        )
        .await
        .expect("create table");

    let rows = handler
        .execute(&session, &format!("DESCRIBE {fq}"), None)
        .await
        .expect("DESCRIBE must be supported (aliased to SHOW COLUMNS)");

    assert!(!rows.is_empty());
    let schema = rows[0].schema();
    assert_eq!(schema.field(0).name(), "column_name");
    assert_eq!(schema.field(1).name(), "data_type");

    let col = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("column_name is Utf8");
    let names: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();
    assert!(
        names.contains(&"a") && names.contains(&"b"),
        "got columns: {names:?}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// Security guard: a table name containing a single quote must be escaped into
// the information_schema query (SHOW COLUMNS / DESCRIBE), not break it. Without
// escaping, `WHERE table_name = 'weird'name'` is a SQL error (or injection);
// with escaping it is a valid query that simply matches no table.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn describe_with_quote_in_name_is_escaped_not_injected() {
    let (session, handler) = crate::common::setup_handler().await;
    let r = handler
        .execute(&session, "DESCRIBE \"weird'name\"", None)
        .await;
    assert!(
        r.is_ok(),
        "escaped name must produce valid SQL (no injection/parse error), got: {r:?}"
    );
}

/// Every value of the first column as text.
fn first_column(batches: &[arrow_array::RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            out.push(arrow::util::display::array_value_to_string(b.column(0), row).unwrap());
        }
    }
    out
}

// DBeaver over the Trino driver lists a schema's tables through
// `TrinoDatabaseMetaData`, which reads 13 `system.jdbc` tables. SQE served 5:
// `getTableTypes()` failed on the missing `table_types`, and views were never
// listed. These are the driver's own statements (master), sent the way it
// sends them: unquoted upper-case names, `LIKE ... ESCAPE '\'`, and
// `LIMIT 0` on the tables it only needs the shape of.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn trino_jdbc_driver_metadata_lists_tables_and_views() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "jdbc_meta";
    for sql in [
        format!("DROP VIEW IF EXISTS {ns}.v"),
        format!("DROP TABLE IF EXISTS {ns}.t"),
    ] {
        let _ = handler.execute(&session, &sql, None).await;
    }
    for sql in [
        format!("CREATE SCHEMA IF NOT EXISTS {ns}"),
        format!("CREATE TABLE {ns}.t (id BIGINT, name VARCHAR)"),
        format!("CREATE VIEW {ns}.v AS SELECT id FROM {ns}.t"),
    ] {
        handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let types_sql = "SELECT TABLE_TYPE FROM system.jdbc.table_types ORDER BY TABLE_TYPE";
    let types = handler.execute(&session, types_sql, None).await.unwrap();
    assert_eq!(first_column(&types), ["TABLE", "VIEW"]);

    let tables_sql = format!(
        "SELECT TABLE_NAME, TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, REMARKS, TYPE_CAT, \
         TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION \
         FROM system.jdbc.tables WHERE TABLE_SCHEM LIKE '{ns}' ESCAPE '\\' \
         AND TABLE_NAME LIKE '%' ESCAPE '\\' AND TABLE_TYPE IN ('TABLE', 'VIEW') \
         ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME"
    );
    let tables = handler.execute(&session, &tables_sql, None).await.unwrap();
    let names = first_column(&tables);
    let kinds: Vec<String> = tables
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(|r| arrow::util::display::array_value_to_string(b.column(1), r).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, ["t", "v"], "kinds: {kinds:?}");
    assert_eq!(kinds, ["TABLE", "VIEW"]);

    let shape_only = [
        "SELECT PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, null, null, null, REMARKS, \
         PROCEDURE_TYPE, SPECIFIC_NAME FROM system.jdbc.procedures",
        "SELECT PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, COLUMN_NAME, COLUMN_TYPE, \
         DATA_TYPE, TYPE_NAME, PRECISION, LENGTH, SCALE, RADIX, NULLABLE, REMARKS, COLUMN_DEF, \
         SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, \
         SPECIFIC_NAME FROM system.jdbc.procedure_columns",
        "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME, CLASS_NAME, DATA_TYPE, REMARKS, BASE_TYPE \
         FROM system.jdbc.udts",
        "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SUPERTYPE_CAT, SUPERTYPE_SCHEM, SUPERTYPE_NAME \
         FROM system.jdbc.super_types",
        "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, SUPERTABLE_NAME FROM system.jdbc.super_tables",
        "SELECT TYPE_CAT, TYPE_SCHEM, TYPE_NAME, ATTR_NAME, DATA_TYPE, ATTR_TYPE_NAME, ATTR_SIZE, \
         DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE, REMARKS, ATTR_DEF, SQL_DATA_TYPE, \
         SQL_DATETIME_SUB, CHAR_OCTET_LENGTH, ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, \
         SCOPE_SCHEMA, SCOPE_TABLE, SOURCE_DATA_TYPE FROM system.jdbc.attributes",
        "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_SIZE, \
         DECIMAL_DIGITS, NUM_PREC_RADIX, COLUMN_USAGE, REMARKS, CHAR_OCTET_LENGTH, IS_NULLABLE \
         FROM system.jdbc.pseudo_columns",
        "SELECT CAST(NULL AS varchar) TABLE_CAT, CAST(NULL AS varchar) TABLE_SCHEM, \
         CAST(NULL AS varchar) TABLE_NAME, CAST(NULL AS varchar) COLUMN_NAME, \
         CAST(NULL AS smallint) KEY_SEQ, CAST(NULL AS varchar) PK_NAME WHERE false",
    ];
    for sql in shape_only {
        let sql = format!("{sql} LIMIT 0");
        handler
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let columns_sql = format!(
        "SELECT COLUMN_NAME FROM system.jdbc.columns WHERE TABLE_SCHEM LIKE '{ns}' ESCAPE '\\' \
         AND TABLE_NAME LIKE 't' ESCAPE '\\' ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, \
         ORDINAL_POSITION"
    );
    let columns = handler.execute(&session, &columns_sql, None).await.unwrap();
    assert_eq!(first_column(&columns), ["id", "name"]);

    for sql in [
        format!("DROP VIEW IF EXISTS {ns}.v"),
        format!("DROP TABLE IF EXISTS {ns}.t"),
        format!("DROP SCHEMA IF EXISTS {ns}"),
    ] {
        let _ = handler.execute(&session, &sql, None).await;
    }
}
