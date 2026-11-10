//! `ST_*` functions over an Iceberg V3 geospatial table SQE created (#863):
//! on the coordinator's own scan, written back into a geometry column, and
//! over a worker scan.

use crate::variant_spark_interop::sqe_rows;

/// Three rows across two INSERTs (two data files): a point, a NULL, and a
/// 4 x 3 rectangle. `h` is GEOGRAPHY: the measures work in meters on the
/// sphere, the other planar functions refuse it.
async fn create_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    fq: &str,
) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {fq} (id INT, g GEOMETRY, h GEOGRAPHY)"),
        format!(
            "INSERT INTO {fq} VALUES \
             (1, ST_GeomFromText('POINT (1 2)'), ST_Point(1, 2)), (2, NULL, NULL)"
        ),
        format!(
            "INSERT INTO {fq} VALUES \
             (3, ST_GeometryFromText('POLYGON ((0 0, 4 0, 4 3, 0 3, 0 0))'), NULL)"
        ),
    ] {
        handler.execute(session, &sql, None).await.expect(&sql);
    }
}

#[tokio::test]
#[ignore = "needs:polaris"]
async fn st_functions_over_an_sqe_geometry_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.geo_st_functions";
    create_table(&handler, &session, fq).await;

    let read = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id, ST_AsText(g), ST_GeometryType(g), ST_Area(g), ST_AsText(ST_Centroid(g)) \
             FROM {fq} ORDER BY id"
        ),
    )
    .await;
    let inside = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id FROM {fq} WHERE ST_Contains(ST_MakeEnvelope(0, 0, 10, 10), g) ORDER BY id"
        ),
    )
    .await;
    let hit = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id FROM {fq} WHERE ST_Intersects(g, ST_Point(3, 1))"),
    )
    .await;
    // A geometry result writes back into a geometry column.
    let insert =
        format!("INSERT INTO {fq} SELECT id + 10, ST_Centroid(g), NULL FROM {fq} WHERE id = 3");
    handler
        .execute(&session, &insert, None)
        .await
        .expect(&insert);
    let written = sqe_rows(
        &handler,
        &session,
        &format!("SELECT id, ST_AsText(g), ST_X(g) FROM {fq} WHERE id = 13"),
    )
    .await;
    let geography = sqe_rows(
        &handler,
        &session,
        &format!("SELECT ST_AsText(h), ST_GeometryType(h) FROM {fq} WHERE id = 1"),
    )
    .await;
    let meters = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id, CAST(ROUND(ST_Distance(h, ST_Point(0, 0))) AS BIGINT) \
             FROM {fq} WHERE h IS NOT NULL"
        ),
    )
    .await;
    let planar_on_geography = handler
        .execute(&session, &format!("SELECT ST_Centroid(h) FROM {fq}"), None)
        .await;
    let mixed = handler
        .execute(
            &session,
            &format!("SELECT ST_Distance(g, h) FROM {fq}"),
            None,
        )
        .await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        read,
        vec![
            "1|POINT(1 2)|ST_Point|0.00|POINT(1 2)".to_string(),
            "2|NULL|NULL|NULL|NULL".to_string(),
            "3|POLYGON((0 0,4 0,4 3,0 3,0 0))|ST_Polygon|12.00|POINT(2 1.5)".to_string(),
        ]
    );
    assert_eq!(
        inside,
        vec!["1".to_string(), "3".to_string()],
        "ST_Contains"
    );
    assert_eq!(hit, vec!["3".to_string()], "ST_Intersects");
    assert_eq!(
        written,
        vec!["13|POINT(2 1.5)|2.00".to_string()],
        "ST_Centroid written back"
    );
    assert_eq!(geography, vec!["POINT(1 2)|ST_Point".to_string()]);
    // Haversine on the GRS80 mean radius, POINT(1 2) to POINT(0 0).
    assert_eq!(meters, vec!["1|248630".to_string()], "meters on the sphere");
    let err = planar_on_geography
        .expect_err("planar math on GEOGRAPHY is refused")
        .to_string();
    assert!(err.contains("GEOGRAPHY"), "{err}");
    let err = mixed
        .expect_err("GEOMETRY next to GEOGRAPHY is refused")
        .to_string();
    assert!(err.contains("GEOMETRY value"), "{err}");
}

/// DESCRIBE and SHOW CREATE TABLE print a geospatial column's type with its
/// CRS (`geometry(3857)`), not its `varbinary` storage, and the DDL SHOW
/// CREATE prints re-creates the same table: same types, same format version.
/// Catalog-owned properties stay out of it. `ST_SRID` reads the CRS.
#[tokio::test]
#[ignore = "needs:polaris"]
async fn geospatial_types_round_trip_through_describe_and_show_create() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.geo_ddl_round_trip";
    let copy = "default.geo_ddl_round_trip_copy";
    for t in [fq, copy] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
            .await;
    }
    for sql in [
        format!(
            "CREATE TABLE {fq} (id INT, g GEOMETRY(4326), m GEOMETRY(3857), z GEOMETRY(0), \
             e GEOMETRY('ESRI:102100'), h GEOGRAPHY(4326), k GEOGRAPHY('OGC:CRS84', karney)) \
             TBLPROPERTIES ('sqe.probe' = 'yes', 'databricks.probe' = 'x', \
             'io.unitycatalog.tableId' = 'x')"
        ),
        format!(
            "INSERT INTO {fq} VALUES (1, ST_Point(1, 2), ST_Point(1, 2), ST_Point(1, 2), \
             ST_Point(1, 2), ST_Point(1, 2), ST_Point(1, 2)), (2, NULL, NULL, NULL, NULL, NULL, NULL)"
        ),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    let types = |rows: Vec<String>| {
        rows.into_iter()
            .map(|r| r.split('|').take(2).collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
    };
    let described = types(sqe_rows(&handler, &session, &format!("DESCRIBE {fq}")).await);
    let ddl = sqe_rows(&handler, &session, &format!("SHOW CREATE TABLE {fq}")).await;
    let ddl = ddl.first().cloned().unwrap_or_default();
    // Re-create the table from the printed DDL under another name.
    let body = &ddl[ddl.find(" (").expect("column list")..];
    let recreate = format!("CREATE TABLE {copy}{body}");
    handler
        .execute(&session, &recreate, None)
        .await
        .expect(&recreate);
    let copied = types(sqe_rows(&handler, &session, &format!("DESCRIBE {copy}")).await);
    let copied_ddl = sqe_rows(&handler, &session, &format!("SHOW CREATE TABLE {copy}")).await;
    let srids = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT id, ST_SRID(g), ST_SRID(m), ST_SRID(z), ST_SRID(e), ST_SRID(h), \
             ST_SRID(ST_Point(0, 0)) FROM {fq} ORDER BY id"
        ),
    )
    .await;
    for t in [fq, copy] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
            .await;
    }

    assert_eq!(
        described,
        vec![
            "id integer",
            "g geometry(4326)",
            "m geometry(3857)",
            "z geometry(0)",
            "e geometry('ESRI:102100')",
            "h geography(4326)",
            "k geography(4326, karney)",
        ],
        "DESCRIBE {fq}"
    );
    for want in [
        "g geometry(4326)",
        "k geography(4326, karney)",
        "'format-version' = '3'",
        "'sqe.probe' = 'yes'",
    ] {
        assert!(ddl.contains(want), "SHOW CREATE TABLE lacks {want}: {ddl}");
    }
    for hidden in ["databricks.", "io.unitycatalog."] {
        assert!(
            !ddl.contains(hidden),
            "SHOW CREATE TABLE shows {hidden}: {ddl}"
        );
    }
    assert_eq!(copied, described, "the table SHOW CREATE's DDL re-creates");
    assert_eq!(
        copied_ddl.first().map(|d| &d[d.find(" (").unwrap_or(0)..]),
        Some(body),
        "SHOW CREATE of the copy"
    );
    assert_eq!(
        srids,
        vec![
            "1|4326|3857|0|0|4326|0".to_string(),
            "2|NULL|NULL|NULL|NULL|NULL|0".to_string(),
        ],
        "ST_SRID"
    );
}

/// An `ST_*` predicate on a geometry column drops the data files whose
/// manifest bounding box misses it (#863), and returns what the unpruned
/// scan returns. Three INSERTs make three files of ten points each, around
/// (0, 0), (100, 100) and (200, 200).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn st_predicates_prune_files_by_bounding_box() {
    use crate::variant_e2e::scan_output_rows;
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.geo_prune_files";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    let create = format!("CREATE TABLE {fq} (id BIGINT, g GEOMETRY)");
    handler
        .execute(&session, &create, None)
        .await
        .expect(&create);
    for base in [0, 100, 200] {
        let insert = format!(
            "INSERT INTO {fq} SELECT {base} + value, \
             ST_Point(CAST({base} + value AS DOUBLE), CAST({base} + value AS DOUBLE)) \
             FROM range(0, 10)"
        );
        handler
            .execute(&session, &insert, None)
            .await
            .expect(&insert);
    }

    let cases = [
        // Every shape the scan prunes on, both argument orders.
        "ST_Intersects(g, ST_MakeEnvelope(102.5, 102.5, 105.5, 105.5))",
        "ST_Within(g, ST_GeomFromText('POLYGON ((102.5 102.5, 105.5 102.5, 105.5 105.5, 102.5 105.5, 102.5 102.5))'))",
        "ST_Contains(ST_MakeEnvelope(102.5, 102.5, 105.5, 105.5), g)",
        "ST_DWithin(g, ST_Point(104, 104), 1.5)",
    ];
    let mut got = Vec::new();
    let mut read = Vec::new();
    for predicate in &cases {
        let sql = format!("SELECT id FROM {fq} WHERE {predicate} ORDER BY id");
        got.push(sqe_rows(&handler, &session, &sql).await);
        read.push(scan_output_rows(&handler, &session, &sql).await);
    }
    // `NOT ST_Disjoint` means ST_Intersects but is not a shape the scan
    // prunes on: the full-scan baseline. (`ST_Intersects(..) = true` is no
    // baseline: the optimizer simplifies it to the bare call, which prunes.)
    let baseline = format!(
        "SELECT id FROM {fq} WHERE NOT ST_Disjoint(g, ST_MakeEnvelope(102.5, 102.5, 105.5, 105.5)) ORDER BY id"
    );
    let baseline_rows = sqe_rows(&handler, &session, &baseline).await;
    let baseline_read = scan_output_rows(&handler, &session, &baseline).await;
    // ST_Disjoint holds on the boxes a prune would drop: nothing may go.
    let disjoint = sqe_rows(
        &handler,
        &session,
        &format!(
            "SELECT count(*) FROM {fq} WHERE ST_Disjoint(g, ST_MakeEnvelope(102.5, 102.5, 105.5, 105.5))"
        ),
    )
    .await;
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    let want: Vec<String> = ["103", "104", "105"].map(String::from).to_vec();
    assert_eq!(baseline_rows, want, "baseline");
    assert_eq!(baseline_read, 30, "the baseline reads all three files");
    for ((predicate, rows), read) in cases.iter().zip(&got).zip(&read) {
        assert_eq!(rows, &want, "{predicate}");
        assert_eq!(*read, 10, "{predicate}: reads only the middle file");
    }
    assert_eq!(disjoint, vec!["27".to_string()], "ST_Disjoint");
}

/// Row groups whose geospatial statistics miss an `ST_*` predicate are
/// skipped (#863), on the small-file direct read and on the vendored reader.
/// One file of points along the diagonal, written in id order, so each row
/// group covers its own stretch of it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn st_predicates_prune_row_groups_by_bounding_box() {
    use crate::variant_e2e::{row_groups, scan_output_rows};
    let (session, handler) = crate::common::setup_handler().await;
    for (table, n) in [
        ("geo_prune_rg_small", 20_000_i64),
        ("geo_prune_rg_large", 300_000),
    ] {
        let fq = format!("default.{table}");
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;
        let create = format!(
            "CREATE TABLE {fq} (id BIGINT, g GEOMETRY, pad VARCHAR) \
             WITH ('write.parquet.row-group-size-bytes' = '262144')"
        );
        handler
            .execute(&session, &create, None)
            .await
            .expect(&create);
        // The padding is incompressible so the large file really is large.
        let insert = format!(
            "INSERT INTO {fq} SELECT value, \
             ST_Point(CAST(value AS DOUBLE), CAST(value AS DOUBLE)), md5(CAST(value AS VARCHAR)) \
             FROM range(0, {n})"
        );
        handler
            .execute(&session, &insert, None)
            .await
            .expect(&insert);

        let (groups, largest) = row_groups(&handler, &session, table).await;
        assert!(
            groups > 2,
            "{table}: several row groups to prune, got {groups}"
        );
        let direct = largest < 3 * 1024 * 1024;
        assert_eq!(direct, n == 20_000, "{table}: largest file {largest} bytes");

        let lo = n - 100;
        let pruned = format!(
            "SELECT count(*), min(id) FROM {fq} \
             WHERE ST_Intersects(g, ST_MakeEnvelope({lo}, {lo}, {n}, {n}))"
        );
        let got = sqe_rows(&handler, &session, &pruned).await;
        let read = scan_output_rows(&handler, &session, &pruned).await;
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;

        assert_eq!(got, vec![format!("100|{lo}")], "{table}");
        assert!(read < n / 2, "{table}: pruned scan read {read} of {n} rows");
    }
}

/// An `ST_*` query over a worker scan: the worker ships the geometry column
/// as WKB and the coordinator evaluates the functions. The coordinator drops
/// the file of the first INSERT, whose bounding box misses the predicate
/// (#863), so the worker reads only the rectangle's file.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn st_functions_over_a_worker_scan() {
    use crate::common::chaos_worker::{spawn_chaos_worker, ChaosMode};
    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let rig = crate::distributed_failure_e2e::rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let (session, handler) = (&rig.session, rig.handler.as_ref());
    let fq = "default.geo_st_on_worker";
    create_table(handler, session, fq).await;

    let before = worker.do_gets();
    let shipped = worker.rows_sent();
    let got = sqe_rows(
        handler,
        session,
        &format!(
            "SELECT id, ST_AsText(g), ST_XMax(g) FROM {fq} \
             WHERE ST_Intersects(g, ST_MakeEnvelope(3, 2, 6, 7))"
        ),
    )
    .await;
    let sent = worker.rows_sent() - shipped;
    let after = worker.do_gets();
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        got,
        vec!["3|POLYGON((0 0,4 0,4 3,0 3,0 0))|4.00".to_string()]
    );
    assert!(
        after > before,
        "the scan ran on the coordinator, not a worker: \
         the worker got no do_get ({before} -> {after})"
    );
    assert_eq!(
        sent, 1,
        "the worker read only the file whose bounding box meets the predicate"
    );
}
