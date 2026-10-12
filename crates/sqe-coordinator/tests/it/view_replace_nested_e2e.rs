//! Views that name the session's catalog, views over views, and a replace
//! that keeps the schema (#832, #833).
//!
//! The test config registers warehouse `test_warehouse` as catalog
//! `iceberg`. A view body planned in a context that only knew the warehouse
//! name failed `iceberg.test_ns.t` as "table not found", whether `t` was a
//! table or another view. `CREATE OR REPLACE VIEW` with an unchanged schema
//! failed with "Cannot set last added schema: no schema has been added",
//! because the server deduplicated the schema and refused the `-1` that
//! referenced it.

use arrow_array::cast::AsArray;

async fn ints(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<i64> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    batches
        .iter()
        .flat_map(|b| {
            let col = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Int64).unwrap();
            col.as_primitive::<arrow_array::types::Int64Type>().values().to_vec()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn views_over_views_and_same_schema_replace() {
    let (session, handler) = crate::common::setup_handler().await;
    let drop_all = || async {
        for v in ["vrn_over_view", "vrn_over_table", "vrn_base"] {
            let _ = handler
                .execute(&session, &format!("DROP VIEW IF EXISTS iceberg.test_ns.{v}"), None)
                .await;
        }
        let _ = handler
            .execute(&session, "DROP TABLE IF EXISTS iceberg.test_ns.vrn_t", None)
            .await;
    };
    drop_all().await;

    for sql in [
        "CREATE TABLE iceberg.test_ns.vrn_t AS SELECT CAST(1 AS BIGINT) AS a",
        "CREATE OR REPLACE VIEW iceberg.test_ns.vrn_base AS SELECT CAST(1 AS BIGINT) AS a",
        "CREATE OR REPLACE VIEW iceberg.test_ns.vrn_over_view AS SELECT * FROM iceberg.test_ns.vrn_base",
        "CREATE OR REPLACE VIEW iceberg.test_ns.vrn_over_table AS SELECT * FROM iceberg.test_ns.vrn_t",
    ] {
        handler.execute(&session, sql, None).await.expect(sql);
    }
    assert_eq!(ints(&handler, &session, "SELECT a FROM iceberg.test_ns.vrn_over_view").await, [1]);
    assert_eq!(ints(&handler, &session, "SELECT a FROM iceberg.test_ns.vrn_over_table").await, [1]);

    // Views created after the namespace was listed (planning vrn_over_view
    // listed it) must still show up: view DDL invalidates the listing.
    let listed = handler
        .execute(
            &session,
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE table_schema = 'test_ns' AND table_name LIKE 'vrn_over_%'",
            None,
        )
        .await
        .expect("information_schema.columns");
    let listed: Vec<i64> = listed
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<arrow_array::types::Int64Type>().values().to_vec())
        .collect();
    assert_eq!(listed, [2], "both new views listed in information_schema.columns");

    // Same schema, new definition; then the same definition again.
    for _ in 0..2 {
        handler
            .execute(
                &session,
                "CREATE OR REPLACE VIEW iceberg.test_ns.vrn_base AS SELECT CAST(2 AS BIGINT) AS a",
                None,
            )
            .await
            .expect("replace with an unchanged schema");
    }
    assert_eq!(ints(&handler, &session, "SELECT a FROM iceberg.test_ns.vrn_base").await, [2]);
    assert_eq!(ints(&handler, &session, "SELECT a FROM iceberg.test_ns.vrn_over_view").await, [2]);

    drop_all().await;
}
