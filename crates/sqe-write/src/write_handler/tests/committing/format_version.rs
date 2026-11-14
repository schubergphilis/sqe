//! #897: the format version a CTAS or an `ALTER TABLE ... SET PROPERTIES`
//! asks for, on a SQLite catalog. dbt-trino builds a table with
//! `CREATE TABLE ... WITH (format_version = 3) AS (...)` and the table came
//! out at the server default.

use super::*;
use crate::catalog_ops::CatalogOps;
use iceberg::spec::FormatVersion;

/// A handler whose server default is V2, so only the option can make V3.
fn v2_handler(rig: &SqliteRig) -> WriteHandler {
    let mut config = rig.config.clone();
    config.catalog.default_table_format_version = 2;
    WriteHandler::new(config)
}

async fn alter(rig: &SqliteRig, sql: &str) -> sqe_core::Result<()> {
    let dialect = sqlparser::dialect::GenericDialect {};
    let stmt = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .expect("parse")
        .remove(0);
    CatalogOps::new(rig.config.clone())
        .set_table_properties(&rig.session(), &stmt)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctas_honours_the_format_version_option() {
    let rig = SqliteRig::new().await;
    let h = v2_handler(&rig);
    for (name, with, want) in [
        ("t_default", "", FormatVersion::V2),
        ("t_trino", "WITH (format_version = 3)", FormatVersion::V3),
        ("t_string", "WITH (format_version = '3')", FormatVersion::V3),
        (
            "t_iceberg",
            "TBLPROPERTIES ('format-version' = '3')",
            FormatVersion::V3,
        ),
        ("t_v1", "WITH (format_version = 1)", FormatVersion::V1),
    ] {
        run_ctas(
            &h,
            &rig,
            &format!("CREATE TABLE {name} {with} AS SELECT 1 AS id"),
        )
        .await;
        let table = rig.load(&rig.ident(name)).await;
        assert_eq!(table.metadata().format_version(), want, "{name}");
        let props = table.metadata().properties();
        assert!(!props.contains_key("format_version"), "{name}: {props:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_set_properties_upgrades_and_refuses_a_downgrade() {
    let rig = SqliteRig::new().await;
    let h = v2_handler(&rig);
    run_ctas(&h, &rig, "CREATE TABLE t AS SELECT 1 AS id").await;
    let ident = rig.ident("t");
    assert_eq!(
        rig.load(&ident).await.metadata().format_version(),
        FormatVersion::V2
    );

    // Trino's SET PROPERTIES reaches here as SET TBLPROPERTIES.
    alter(
        &rig,
        "ALTER TABLE t SET TBLPROPERTIES ('format_version' = '3', 'a' = 'x')",
    )
    .await
    .expect("upgrade");
    let table = rig.load(&ident).await;
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
    let props = table.metadata().properties();
    assert_eq!(props.get("a").map(String::as_str), Some("x"));
    assert!(!props.contains_key("format_version"), "{props:?}");

    // The same version again is a no-op; a lower one is refused.
    alter(
        &rig,
        "ALTER TABLE t SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("same version");
    let err = alter(
        &rig,
        "ALTER TABLE t SET TBLPROPERTIES ('format_version' = '2')",
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("cannot lower the format version"), "{err}");
    assert_eq!(
        rig.load(&ident).await.metadata().format_version(),
        FormatVersion::V3
    );

    // The upgraded table takes writes.
    run_insert(&h, &rig, "INSERT INTO t VALUES (2)")
        .await
        .expect("insert");
}
