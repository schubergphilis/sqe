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
                        .map(|s| if s.is_null(row) { "NULL".into() } else { s.value(row).to_string() })
                        .unwrap_or_else(|| format!("<{}>", col.data_type()))
                })
                .collect();
            eprintln!("    {cells:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
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
        None)
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
                    let cat = batch.column(0).as_any().downcast_ref::<StringArray>().map(|s| s.value(row).to_string()).unwrap_or_default();
                    let sch = batch.column(1).as_any().downcast_ref::<StringArray>().map(|s| s.value(row).to_string()).unwrap_or_default();
                    eprintln!("    catalog={cat} schema={sch}");
                }
            }
        }
        Err(e) => eprintln!("---- (B) information_schema.tables ERROR: {e}"),
    }

    // (C) SHOW COLUMNS path (rewrites to information_schema.columns).
    match handler.execute(&session, &format!("SHOW COLUMNS FROM {fq}"), None).await {
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
#[ignore]
async fn describe_table_aliases_to_show_columns() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "describe_probe";
    let fq = format!("{ns}.{name}");

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None).await;
    handler
        .execute(&session, &format!("CREATE TABLE {fq} (a INT, b VARCHAR)"), None)
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
    assert!(names.contains(&"a") && names.contains(&"b"), "got columns: {names:?}");

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None).await;
}

// Security guard: a table name containing a single quote must be escaped into
// the information_schema query (SHOW COLUMNS / DESCRIBE), not break it. Without
// escaping, `WHERE table_name = 'weird'name'` is a SQL error (or injection);
// with escaping it is a valid query that simply matches no table.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
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
