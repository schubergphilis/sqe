//! A stored view plans as the statement that reads it would
//! (ai-portal-data#357, #359).
//!
//! A view body resolved its unqualified names in schema `default`, whatever
//! the version's `default-namespace` said, and SQE recorded the view's own
//! namespace there instead of the schema the body was planned in. It also
//! planned in a bare DataFusion context: `year()` and `if()` were unknown,
//! lambdas did not parse and `0.06` was a DOUBLE where a query has a
//! DECIMAL.

use arrow_array::cast::AsArray;

async fn strings(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    batches
        .iter()
        .flat_map(|b| {
            let col = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Utf8).unwrap();
            col.as_string::<i32>()
                .iter()
                .map(|v| v.unwrap_or("NULL").to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn view_resolves_unqualified_names_in_its_default_namespace() {
    let (session, handler) = crate::common::setup_handler().await;
    let cleanup = || async {
        let _ = handler
            .execute(
                &session,
                "DROP VIEW IF EXISTS iceberg.vpp_views.vpp_unqualified",
                None,
            )
            .await;
        let _ = handler
            .execute(&session, "DROP TABLE IF EXISTS iceberg.test_ns.vpp_t", None)
            .await;
    };
    cleanup().await;
    for sql in [
        "CREATE SCHEMA IF NOT EXISTS iceberg.vpp_views",
        "CREATE TABLE iceberg.test_ns.vpp_t AS SELECT CAST(7 AS BIGINT) AS a",
    ] {
        handler.execute(&session, sql, None).await.expect(sql);
    }

    // Created under `USE iceberg.test_ns`, in another namespace: `vpp_t` in
    // the body means `test_ns.vpp_t`, and the version records `test_ns`.
    let mut in_test_ns = session.clone();
    in_test_ns.default_catalog = Some("iceberg".to_string());
    in_test_ns.default_schema = Some("test_ns".to_string());
    handler
        .execute(
            &in_test_ns,
            "CREATE VIEW iceberg.vpp_views.vpp_unqualified AS SELECT a FROM vpp_t",
            None,
        )
        .await
        .expect("create the view under USE test_ns");

    // Read from a session in schema `default`, where no `vpp_t` exists.
    assert_eq!(
        strings(
            &handler,
            &session,
            "SELECT a FROM iceberg.vpp_views.vpp_unqualified"
        )
        .await,
        ["7"],
        "the body's `vpp_t` resolves in the view's default-namespace"
    );

    cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs:polaris"]
async fn view_body_plans_with_the_statement_function_set() {
    let (session, handler) = crate::common::setup_handler().await;
    let drop_view = || async {
        let _ = handler
            .execute(
                &session,
                "DROP VIEW IF EXISTS iceberg.test_ns.vpp_functions",
                None,
            )
            .await;
    };
    drop_view().await;

    let body = "SELECT year(DATE '2020-03-01') AS y, \
                if(1 > 0, 'yes', 'no') AS f, \
                cardinality(array_filter(make_array(1, 2, 3), x -> x > 1)) AS n, \
                arrow_typeof(0.06) AS rate_type";
    let expected = strings(&handler, &session, body).await;
    assert_eq!(expected, ["2020"], "first column of the statement itself");

    handler
        .execute(
            &session,
            &format!("CREATE VIEW iceberg.test_ns.vpp_functions AS {body}"),
            None,
        )
        .await
        .expect("create a view using year(), if() and a lambda");

    for column in ["y", "f", "n", "rate_type"] {
        assert_eq!(
            strings(
                &handler,
                &session,
                &format!("SELECT {column} FROM iceberg.test_ns.vpp_functions"),
            )
            .await,
            strings(
                &handler,
                &session,
                &format!("SELECT {column} FROM ({body}) AS q"),
            )
            .await,
            "view column {column} equals the same expression as a query"
        );
    }

    drop_view().await;
}
