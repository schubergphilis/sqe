//! Iceberg V3 geospatial columns across engines (#863): Spark 4.1 with
//! Iceberg 1.12 (`spark.sql.geospatial.enabled`) writes GEOMETRY and
//! GEOGRAPHY columns; SQE reads them back as WKB bytes, writes into the table
//! with geometry bounds, and compacts it. Needs the integration stack and the Spark container
//! (`./scripts/start-spark-interop.sh`); without the container the test skips
//! with the reason.

use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
use crate::variant_spark_interop::sqe_rows;

/// WKB for `POINT (1 2)`, little-endian. The plain BINARY column `b` holds
/// the same bytes: a Spark file has no Arrow schema, so both decode as
/// `Binary` and widen to the table's `LargeBinary`.
const POINT_1_2: &str = "0101000000000000000000f03f0000000000000040";
const POINT_1_2_UPPER: &str = "0101000000000000000000F03F0000000000000040";
/// WKB for `POINT (5 6)`.
const POINT_5_6: &str = "010100000000000000000014400000000000001840";
/// Geometry bounds as `table_files` prints them: the corner's x and y as
/// little-endian doubles, in hex.
const BOX_1_2: &str = "000000000000F03F0000000000000040";
const BOX_5_6: &str = "00000000000014400000000000001840";

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

    // SQE writes WKB into both columns. The geometry column gets a bounding
    // box as its bounds; the geography column gets none (no box is computed
    // for it, and a byte-wise min/max would be wrong).
    let insert = handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (3, NULL, X'{POINT_5_6}', X'{POINT_5_6}')"),
            None,
        )
        .await;
    let sqe_file_bounds = file_bounds(&handler, &session).await;
    // Bytes that are not WKB never reach a data file.
    let invalid = handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} (id, g) VALUES (4, X'00')"),
            None,
        )
        .await;
    // An UPDATE rewrites the geospatial values it carries along.
    let update = handler
        .execute(
            &session,
            &format!("UPDATE {fq} SET b = X'{POINT_5_6}' WHERE id = 3"),
            None,
        )
        .await;
    let spark_read = spark_geo_rows(fq);
    // Compaction rewrites Spark's file and SQE's file into one, whose box
    // covers both points.
    let compact = handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{fq}', min_input_files => 2)"),
            None,
        )
        .await;
    let compacted_bounds = file_bounds(&handler, &session).await;
    let compacted_read = spark_geo_rows(fq);
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
    insert.expect("SQE inserts WKB into the geospatial columns");
    let sqe_file = sqe_file_bounds
        .iter()
        .find(|(lo, _)| lo.contains("\"1\":3"))
        .unwrap_or_else(|| panic!("no file holds id 3: {sqe_file_bounds:?}"));
    assert!(
        sqe_file.0.contains(&format!("\"3\":{BOX_5_6}"))
            && sqe_file.1.contains(&format!("\"3\":{BOX_5_6}")),
        "geometry bounds of SQE's file: {sqe_file:?}"
    );
    assert!(
        !sqe_file.0.contains("\"4\":") && !sqe_file.1.contains("\"4\":"),
        "geography has no bounds: {sqe_file:?}"
    );
    let err = invalid.expect_err("bytes that are not WKB are refused");
    assert!(
        err.to_string().contains("is not a valid WKB"),
        "unexpected error: {err}"
    );
    update.expect("UPDATE of a geospatial table");
    let expected_spark = vec![
        format!("1\t{POINT_1_2_UPPER}\t{POINT_1_2_UPPER}"),
        "2\tNULL\tNULL".to_string(),
        format!("3\t{POINT_5_6}\t{POINT_5_6}"),
    ];
    assert_eq!(spark_read, expected_spark, "Spark reads SQE's row");
    compact.expect("compaction of a geospatial table");
    assert_eq!(
        compacted_bounds.len(),
        1,
        "one file after compaction: {compacted_bounds:?}"
    );
    let (lo, hi) = &compacted_bounds[0];
    assert!(
        lo.contains(&format!("\"3\":{BOX_1_2}")) && hi.contains(&format!("\"3\":{BOX_5_6}")),
        "compacted geometry bounds: {lo} / {hi}"
    );
    assert!(
        !lo.contains("\"4\":") && !hi.contains("\"4\":"),
        "compacted geography has no bounds: {lo} / {hi}"
    );
    assert_eq!(
        compacted_read, expected_spark,
        "Spark reads the compacted file"
    );
}

/// Per-file `(lower_bounds, upper_bounds)` of the test table. Field ids:
/// 1 id, 2 b, 3 g, 4 h.
async fn file_bounds(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
) -> Vec<(String, String)> {
    let sql = "SELECT lower_bounds, upper_bounds FROM table_files('default', 'geo_from_spark')";
    sqe_rows(handler, session, sql)
        .await
        .into_iter()
        .map(|row| {
            let (lo, hi) = row.split_once('|').expect("two columns");
            (lo.to_string(), hi.to_string())
        })
        .collect()
}

/// The table's rows as Spark reads them: `id`, then the hex WKB of `g`, `h`.
fn spark_geo_rows(fq: &str) -> Vec<String> {
    let out = spark_sql(&format!(
        "SET spark.sql.geospatial.enabled = true;
         SELECT id, hex(ST_AsBinary(g)), hex(ST_AsBinary(h)) FROM rest.{fq} ORDER BY id;"
    ))
    .expect("spark reads the geospatial table");
    out.lines()
        .filter(|l| l.starts_with(|c: char| c.is_ascii_digit()))
        .map(str::to_string)
        .collect()
}
