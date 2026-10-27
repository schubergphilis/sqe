//! `ST_*` functions over an Iceberg V3 geospatial table SQE created (#863):
//! on the coordinator's own scan, written back into a geometry column, and
//! over a worker scan.

use crate::variant_spark_interop::sqe_rows;

/// Three rows across two INSERTs (two data files): a point, a NULL, and a
/// 4 x 3 rectangle. `h` is GEOGRAPHY, so the planar functions refuse it.
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
    let planar_on_geography = handler
        .execute(
            &session,
            &format!("SELECT ST_Distance(h, ST_Point(0, 0)) FROM {fq}"),
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
    let err = planar_on_geography
        .expect_err("planar math on GEOGRAPHY is refused")
        .to_string();
    assert!(err.contains("GEOGRAPHY"), "{err}");
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

/// An `ST_*` query over a worker scan: the worker ships the geometry column
/// as WKB and the coordinator evaluates the functions. The table provider
/// does not push an `ST_*` filter down yet, so the worker ships every row;
/// spatial file pruning will push it.
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
        sent, 3,
        "the worker read the geometry column of all three rows"
    );
}
