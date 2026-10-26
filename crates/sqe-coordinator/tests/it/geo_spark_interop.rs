//! Iceberg V3 geospatial columns across engines (#863): Spark 4.1 with
//! Iceberg 1.12 (`spark.sql.geospatial.enabled`) writes GEOMETRY and
//! GEOGRAPHY columns; SQE reads them back as WKB bytes, writes into the table
//! with geometry bounds, and compacts it. SQE creates a geospatial table that
//! Spark reads. Needs the integration stack and the Spark container
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn spark_reads_an_sqe_geospatial_table() {
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping spark_reads_an_sqe_geospatial_table: {reason}");
        return;
    }
    let (session, handler) = crate::common::setup_handler().await;
    let run = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move { handler.execute(session, &sql, None).await }
    };
    for t in ["geo_from_sqe", "geo_from_sqe_ctas", "geo_from_sqe_v2"] {
        let _ = run(format!("DROP TABLE IF EXISTS default.{t}")).await;
    }

    // SRID 4326 is Iceberg's default CRS; other SRIDs are EPSG codes. The
    // table becomes format v3 without asking.
    let create = run("CREATE TABLE default.geo_from_sqe \
         (id INT, g GEOMETRY(4326), m GEOMETRY(3857), h GEOGRAPHY(4326))"
        .to_string())
    .await;
    let insert = run(format!(
        "INSERT INTO default.geo_from_sqe VALUES (1, X'{POINT_1_2}', X'{POINT_5_6}', X'{POINT_1_2}')"
    ))
    .await;
    let add = run("ALTER TABLE default.geo_from_sqe ADD COLUMN k GEOGRAPHY".to_string()).await;
    let described = spark_sql(
        "SET spark.sql.geospatial.enabled = true; DESCRIBE TABLE rest.default.geo_from_sqe;",
    )
    .expect("spark describes the SQE table");
    let spark_read = spark_sql(
        "SET spark.sql.geospatial.enabled = true;
         SELECT id, hex(ST_AsBinary(g)), hex(ST_AsBinary(m)), hex(ST_AsBinary(h)), k
         FROM rest.default.geo_from_sqe ORDER BY id;",
    )
    .expect("spark reads the SQE table");

    // A geospatial column of a v2 table is refused, also through ALTER.
    let v2 = run("CREATE TABLE default.geo_from_sqe_v2 (id INT, g GEOMETRY) \
         WITH ('format-version' = '2')"
        .to_string())
    .await;
    let v2_create = "CREATE TABLE default.geo_from_sqe_v2 (id INT) WITH ('format-version' = '2')";
    let _ = run(v2_create.to_string()).await;
    let v2_add = run("ALTER TABLE default.geo_from_sqe_v2 ADD COLUMN g GEOMETRY".to_string()).await;
    // CTAS keeps the geospatial types: the scan tags the columns as WKB.
    let ctas = run(
        "CREATE TABLE default.geo_from_sqe_ctas AS SELECT id, g, h FROM default.geo_from_sqe"
            .to_string(),
    )
    .await;
    let ctas_types = spark_sql(
        "SET spark.sql.geospatial.enabled = true; DESCRIBE TABLE rest.default.geo_from_sqe_ctas;",
    );
    for t in ["geo_from_sqe", "geo_from_sqe_ctas", "geo_from_sqe_v2"] {
        let _ = run(format!("DROP TABLE IF EXISTS default.{t}")).await;
    }

    create.expect("CREATE TABLE with geospatial columns");
    insert.expect("INSERT into the SQE geospatial table");
    add.expect("ALTER TABLE ADD COLUMN GEOGRAPHY");
    assert_spark_types(
        &described,
        &[
            ("g", "geometry(4326)"),
            ("m", "geometry(3857)"),
            ("h", "geography(4326)"),
            ("k", "geography(4326)"),
        ],
    );
    let rows: Vec<&str> = spark_read
        .lines()
        .filter(|l| l.starts_with(|c: char| c.is_ascii_digit()))
        .collect();
    let point_5_6 = POINT_5_6.to_uppercase();
    let expected = format!("1\t{POINT_1_2_UPPER}\t{point_5_6}\t{POINT_1_2_UPPER}\tNULL");
    assert_eq!(rows, [expected], "Spark reads SQE's geospatial table");
    let v2_err = v2.expect_err("a v2 table with a GEOMETRY column is refused");
    assert!(v2_err.to_string().contains("needs format v3"), "{v2_err}");
    let v2_add_err = v2_add.expect_err("ADD COLUMN GEOMETRY on a v2 table is refused");
    let v2_add_err = v2_add_err.to_string();
    assert!(v2_add_err.contains("types need format v3"), "{v2_add_err}");
    ctas.expect("CTAS from the geospatial table");
    let ctas_types = ctas_types.expect("spark describes the CTAS table");
    assert_spark_types(
        &ctas_types,
        &[("g", "geometry(4326)"), ("h", "geography(4326)")],
    );
}

/// Assert each `(column, type)` is a row of Spark's `DESCRIBE TABLE` output.
fn assert_spark_types(described: &str, expected: &[(&str, &str)]) {
    for (name, ty) in expected {
        assert!(
            described
                .lines()
                .any(|l| l.split('\t').map(str::trim).eq([*name, *ty, ""])),
            "Spark's type of {name} is not {ty}: {described}"
        );
    }
}

/// Per-file `(lower_bounds, upper_bounds)` of the test table, with the
/// quotes around binary bound values removed. Field ids: 1 id, 2 b, 3 g, 4 h.
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
            let unquote = |s: &str| {
                s.replace(":\"", ":")
                    .replace("\",", ",")
                    .replace("\"}", "}")
            };
            (unquote(lo), unquote(hi))
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
