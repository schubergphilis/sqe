//! Statements that hide a write inside a read-looking shape (#811).
//!
//! The query path plans through `sqe_catalog::file_tvf_prepare::plan_sql`.
//! DataFusion's `SessionContext::sql` runs DDL while planning, so
//! `SELECT ... INTO datafusion.public.t FROM governed` used to copy the
//! governed table raw into a session MemTable before the policy rewriter saw
//! a plan; the next query read it back unfiltered and unrestricted. Planning
//! now refuses DDL. The other shapes here were already refused; they are
//! pinned so a planner change cannot quietly start running them.

use std::sync::Arc;

use arrow_array::cast::AsArray;

async fn count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Option<i64> {
    let batches = handler
        .execute(session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .ok()?;
    let col = batches.first()?.column(0);
    let col = arrow::compute::cast(col, &arrow_schema::DataType::Int64).ok()?;
    Some(col.as_primitive::<arrow_array::types::Int64Type>().value(0))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn write_shapes_on_the_query_path_are_refused() {
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    let (session, handler) = crate::common::setup_handler_with_policy_store(store).await;
    for t in ["shape_src", "shape_dst", "shape_into"] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS test_ns.{t}"), None)
            .await;
    }
    for t in ["shape_src", "shape_dst"] {
        handler
            .execute(
                &session,
                &format!("CREATE TABLE test_ns.{t} AS SELECT 1 AS id UNION ALL SELECT 2"),
                None,
            )
            .await
            .expect("fixture");
    }

    for sql in [
        "WITH x AS (SELECT id FROM test_ns.shape_src) INSERT INTO test_ns.shape_dst SELECT id FROM x",
        "SELECT id INTO test_ns.shape_into FROM test_ns.shape_src",
        "SELECT id INTO shape_into FROM test_ns.shape_src",
        "SELECT id INTO datafusion.public.shape_mem FROM test_ns.shape_src",
        "EXPLAIN ANALYZE INSERT INTO test_ns.shape_dst SELECT id FROM test_ns.shape_src",
        "EXPLAIN ANALYZE DELETE FROM test_ns.shape_dst",
        "EXPLAIN INSERT INTO test_ns.shape_dst SELECT id FROM test_ns.shape_src",
    ] {
        handler.execute(&session, sql, None).await.expect_err(sql);
        assert_eq!(count(&handler, &session, "test_ns.shape_dst").await, Some(2), "{sql}");
    }
    for t in [
        "test_ns.shape_into",
        "shape_into",
        "datafusion.public.shape_mem",
    ] {
        assert_eq!(count(&handler, &session, t).await, None, "{t} exists");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn select_into_memory_table_cannot_copy_governed_rows() {
    use datafusion::logical_expr::{col, lit};
    let (session, plain) = crate::common::setup_handler().await;
    let _ = plain
        .execute(&session, "DROP TABLE IF EXISTS test_ns.shape_gov", None)
        .await;
    plain
        .execute(
            &session,
            "CREATE TABLE test_ns.shape_gov AS SELECT * FROM (VALUES \
             (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, secret)",
            None,
        )
        .await
        .expect("CTAS");
    let store = Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new());
    store
        .add_table_policy(
            "test_ns",
            "shape_gov",
            sqe_policy::ResolvedPolicy {
                restricted_columns: vec!["secret".to_string()],
                row_filters: vec![col("id").gt(lit(1))],
                ..Default::default()
            },
        )
        .await;
    let (session, handler) = crate::common::setup_handler_with_policy_store(store).await;

    let governed = handler
        .execute(
            &session,
            "SELECT id, secret FROM test_ns.shape_gov ORDER BY id",
            None,
        )
        .await
        .expect("governed read");
    let rows: usize = governed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "row filter applies");
    assert!(
        governed
            .iter()
            .all(|b| b.column(1).null_count() == b.num_rows()),
        "secret is restricted"
    );

    // The second target is the engine's own scratch namespace, which
    // `enforce_dml_eval` treats as already governed.
    for (sql, table) in [
        (
            "SELECT * INTO datafusion.public.shape_leak FROM test_ns.shape_gov",
            "datafusion.public.shape_leak",
        ),
        (
            "SELECT * INTO datafusion.public.__sqe_merge_source_x FROM test_ns.shape_gov",
            "datafusion.public.__sqe_merge_source_x",
        ),
    ] {
        handler.execute(&session, sql, None).await.expect_err(sql);
        assert_eq!(
            count(&handler, &session, table).await,
            None,
            "{table} was created"
        );
    }
}
