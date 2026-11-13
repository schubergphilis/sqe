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

/// #889: a parenthesised source query is a query. sqlparser only took
/// `(SELECT` after the table name and read `(WITH`, `(VALUES` and `((` as a
/// column list; dbt's append strategy wraps every model that way. Two equal
/// unnamed literals (`SELECT 4, 4`) failed to plan on duplicate names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn insert_takes_a_parenthesised_source_query_889() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = "test_ns.ivi_paren_query";
    let drop = format!("DROP TABLE IF EXISTS {t}");
    let _ = handler.execute(&session, &drop, None).await;
    let create = format!("CREATE TABLE {t} (a BIGINT, b BIGINT)");
    let fixture = handler.execute(&session, &create, None).await;
    fixture.expect("fixture");

    for sql in [
        format!("INSERT INTO {t} (WITH x AS (SELECT 1 AS a, 10 AS b) SELECT a, b FROM x)"),
        format!("INSERT INTO {t} (VALUES (2, 20))"),
        format!("INSERT INTO {t} ((SELECT 3, 30))"),
        format!("INSERT INTO {t} (a, b) SELECT 4, 4"),
        // The text dbt's append strategy sends.
        "INSERT INTO \"test_ns\".\"ivi_paren_query\"\n(\n  with x as (select 5 as a, 50 as b)\n  \
         select * from x\n)"
            .to_string(),
        format!("INSERT INTO {t} SELECT 6, 6"),
    ] {
        let res = handler.execute(&session, &sql, None).await;
        res.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let sql = format!("SELECT a * 100 + b AS n FROM {t}");
    let read = format!("({sql}) AS v");
    let got = values(&handler, &session, &read).await;
    let want = [110, 220, 330, 404, 550, 606].map(Some).to_vec();
    assert_eq!(got, want);
    let _ = handler.execute(&session, &drop, None).await;
}
