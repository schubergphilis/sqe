//! Iceberg V3 geospatial columns across engines (#863): Spark 4.1 with
//! Iceberg 1.12 (`spark.sql.geospatial.enabled`) writes GEOMETRY and
//! GEOGRAPHY columns; SQE reads them back as WKB bytes and refuses to write
//! into the table. Needs the integration stack and the Spark container
//! (`./scripts/start-spark-interop.sh`); without the container the test skips
//! with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::sqe_rows;

/// WKB for `POINT (1 2)`, little-endian. The plain BINARY column `b` holds
/// the same bytes: a Spark file has no Arrow schema, so both decode as
/// `Binary` and widen to the table's `LargeBinary`.
const POINT_1_2: &str = "0101000000000000000000f03f0000000000000040";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn sqe_reads_a_spark_geospatial_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping sqe_reads_a_spark_geospatial_table: {reason}");
        return;
    }
    let fq = "default.geo_from_spark";
    spark_sql(&format!(
        r#"SET spark.sql.geospatial.enabled = true;
           DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, b BINARY, g GEOMETRY(4326), h GEOGRAPHY(4326))
             USING iceberg TBLPROPERTIES ('format-version' = '3');
           INSERT INTO rest.{fq} VALUES
             (1, X'{POINT_1_2}', ST_SetSRID(ST_GeomFromWKB(X'{POINT_1_2}'), 4326),
                 ST_SetSRID(ST_GeogFromWKB(X'{POINT_1_2}'), 4326)),
             (2, NULL, NULL, NULL);"#
    ))
    .expect("spark writes the geospatial table");

    let (session, handler) = crate::common::setup_handler().await;

    // The scan hands back the WKB bytes, tagged as GeoArrow WKB.
    let batches = handler
        .execute(&session, &format!("SELECT g, h FROM {fq}"), None)
        .await
        .expect("select geospatial columns");
    let schema = batches.first().expect("one batch").schema();
    for name in ["g", "h"] {
        let field = schema.field_with_name(name).unwrap();
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("geoarrow.wkb"),
            "{name}: {field:?}"
        );
    }

    let read = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id, arrow_cast(encode(b, 'hex'), 'Utf8'), \
                    arrow_cast(encode(g, 'hex'), 'Utf8'), \
                    arrow_cast(encode(h, 'hex'), 'Utf8'), g IS NULL \
             FROM {fq} WHERE id > 0 ORDER BY id"
        ),
    )
    .await;

    // A predicate on a geospatial column must not prune on its bounds: they
    // are a bounding box, not byte-wise min/max.
    let filtered = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id FROM {fq} WHERE g = X'{POINT_1_2}' OR h IS NOT NULL ORDER BY id"),
    )
    .await;

    // A join on a geospatial column hands the probe side a runtime filter
    // on it; that must not reach iceberg's binder either.
    let joined = sqe_rows(
        &handler,
        &session,
        &format!("SELECT a.id FROM {fq} a JOIN {fq} b ON a.g = b.g ORDER BY a.id"),
    )
    .await;

    // Writes stay refused until the writer can compute geospatial bounds.
    let insert = handler
        .execute(&session, &format!("INSERT INTO {fq} (id) VALUES (3)"), None)
        .await;
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));
    // SQE has no GEOMETRY / GEOGRAPHY DDL yet: declaring one is an error,
    // never a silently different column type.
    let ddl = handler
        .execute(
            &session,
            "CREATE TABLE default.geo_from_sqe (id INT, g GEOMETRY)",
            None,
        )
        .await;
    let _ = handler
        .execute(&session, "DROP TABLE IF EXISTS default.geo_from_sqe", None)
        .await;

    assert_eq!(
        read,
        vec![
            format!("1|{POINT_1_2}|{POINT_1_2}|{POINT_1_2}|false"),
            "2|NULL|NULL|NULL|true".to_string()
        ],
        "SQE's rows of the Spark-written geospatial table"
    );
    assert_eq!(
        filtered,
        vec!["1".to_string()],
        "filter on geospatial columns"
    );
    assert_eq!(joined, vec!["1".to_string()], "join on a geospatial column");
    let ddl_err = ddl.expect_err("CREATE TABLE with a GEOMETRY column is refused");
    assert!(
        ddl_err
            .to_string()
            .contains("not supported for CREATE TABLE: GEOMETRY"),
        "unexpected error: {ddl_err}"
    );
    let err = insert.expect_err("an INSERT into a geospatial table is refused");
    assert!(
        err.to_string().contains("cannot write them yet"),
        "unexpected error: {err}"
    );
}
