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
