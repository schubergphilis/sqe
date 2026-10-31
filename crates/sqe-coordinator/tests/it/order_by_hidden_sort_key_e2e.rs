//! `ORDER BY` a column the query does not select.
//!
//! `SELECT id + 0 AS r FROM t ORDER BY id` plans as a projection over a
//! per-partition sort. The projection drops `id`, so the root reports no
//! ordering. When the scan ran as several partitions, the single-partition
//! restore at the root used to coalesce them, interleaving the sorted
//! partitions: rows came back unordered.

use arrow_array::{Array, Int64Array};

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn first_column(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<i64> {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        let col = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Int64).expect("cast");
        let col = col.as_any().downcast_ref::<Int64Array>().expect("int64");
        out.extend((0..col.len()).map(|i| col.value(i)));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn order_by_a_column_that_is_not_selected() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = "default.order_by_hidden_key";
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {t}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {t} (id BIGINT, val STRING)"),
    )
    .await;
    // One insert per file, highest ids first, so a scan that reads the files
    // in order and concatenates them returns descending ids.
    for chunk in (0..8).rev() {
        let values: Vec<String> = (0..4)
            .map(|i| {
                let id = chunk * 4 + i;
                format!("({id}, 'v{id}')")
            })
            .collect();
        let insert = format!("INSERT INTO {t} VALUES {}", values.join(", "));
        exec(&handler, &session, &insert).await;
    }
    let expected: Vec<i64> = (0..32).collect();

    let sql = format!("SELECT id + 0 AS r FROM {t} ORDER BY id");
    assert_eq!(
        first_column(&handler, &session, &sql).await,
        expected,
        "{sql}"
    );

    let sql = format!("SELECT id + 0 AS r FROM {t} WHERE val <> 'x' ORDER BY id DESC");
    let mut descending = expected.clone();
    descending.reverse();
    assert_eq!(
        first_column(&handler, &session, &sql).await,
        descending,
        "{sql}"
    );

    let sql = format!("SELECT id + 0 AS r FROM {t} ORDER BY id LIMIT 5");
    assert_eq!(
        first_column(&handler, &session, &sql).await,
        vec![0, 1, 2, 3, 4],
        "{sql}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}
