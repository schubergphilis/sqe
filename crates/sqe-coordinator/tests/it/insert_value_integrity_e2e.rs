//! An INSERT stores what the user wrote or fails (#834).
//!
//! The writer cast every column to the table type with arrow's safe cast,
//! which turns a value it cannot convert into NULL, and the stamped schema
//! is all-nullable, so the parquet writer stored the zeroed slot of that
//! NULL in a required column. `INSERT INTO t VALUES ('abc')` into a BIGINT
//! column succeeded and read back as 0; so did `VALUES (NULL)` into a
//! NOT NULL column.

use arrow_array::cast::AsArray;

async fn values(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<Option<i64>> {
    let batches = handler
        .execute(session, &format!("SELECT n FROM {table} ORDER BY n"), None)
        .await
        .expect("read back");
    batches
        .iter()
        .flat_map(|b| {
            let col = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Int64).unwrap();
            col.as_primitive::<arrow_array::types::Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn insert_refuses_unconvertible_value_and_null_in_required_column() {
    let (session, handler) = crate::common::setup_handler().await;
    for t in ["ivi_req", "ivi_src"] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS test_ns.{t}"), None)
            .await;
    }
    // A CTAS of a non-null literal makes `n` a required column.
    handler
        .execute(&session, "CREATE TABLE test_ns.ivi_req AS SELECT CAST(1 AS BIGINT) AS n", None)
        .await
        .expect("fixture");
    handler
        .execute(&session, "CREATE TABLE test_ns.ivi_src AS SELECT 'abc' AS s", None)
        .await
        .expect("fixture");

    for sql in [
        "INSERT INTO test_ns.ivi_req VALUES ('abc')",
        "INSERT INTO test_ns.ivi_req SELECT 'abc'",
        "INSERT INTO test_ns.ivi_req SELECT s FROM test_ns.ivi_src",
        "INSERT INTO test_ns.ivi_req VALUES (NULL)",
        "INSERT INTO test_ns.ivi_req SELECT CAST(NULL AS BIGINT)",
    ] {
        handler.execute(&session, sql, None).await.expect_err(sql);
        assert_eq!(values(&handler, &session, "test_ns.ivi_req").await, vec![Some(1)], "{sql}");
    }

    // What converts still lands.
    handler
        .execute(&session, "INSERT INTO test_ns.ivi_req VALUES ('7')", None)
        .await
        .expect("'7' is a BIGINT");
    assert_eq!(values(&handler, &session, "test_ns.ivi_req").await, vec![Some(1), Some(7)]);

    for t in ["ivi_req", "ivi_src"] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS test_ns.{t}"), None)
            .await;
    }
}
