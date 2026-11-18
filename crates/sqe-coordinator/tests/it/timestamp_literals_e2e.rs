//! Trino timestamp literals against a microsecond Iceberg column.
//!
//! `TIMESTAMP '...'` now plans at the precision of its fraction digits
//! (second, millisecond, microsecond or nanosecond) instead of always
//! nanoseconds. DELETE, UPDATE and MERGE predicates skip the coercion that
//! widens a literal to the column's unit, so a coarse literal must still
//! reach the table as an exact microsecond value. A literal with a zone keeps
//! its zone, and CTAS stores the same instant as UTC.
//!
//! Every test needs the running stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored timestamp_literals
//! ```

use arrow_array::{Array, Int64Array};

async fn exec(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<arrow_array::RecordBatch> {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"))
}

async fn count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = exec(handler, session, sql).await;
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

/// A table with a microsecond `timestamp` column and three rows, one per
/// day of January 2024.
async fn seed(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, fq: &str) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        handler,
        session,
        &format!("CREATE TABLE {fq} (id BIGINT, ts timestamp)"),
    )
    .await;
    exec(
        handler,
        session,
        &format!(
            "INSERT INTO {fq} VALUES \
             (1, TIMESTAMP '2024-01-01 00:00:00'), \
             (2, TIMESTAMP '2024-01-02 00:00:00'), \
             (3, TIMESTAMP '2024-01-03 00:00:00.500')"
        ),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_insert_values_into_microsecond_column() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_insert";
    seed(&handler, &session, fq).await;
    assert_eq!(
        count(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3
    );
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE ts = TIMESTAMP '2024-01-03 00:00:00.5'")
        )
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_delete_with_second_precision_literal() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_delete";
    seed(&handler, &session, fq).await;
    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE ts = TIMESTAMP '2024-01-01 00:00:00'"),
    )
    .await;
    assert_eq!(
        count(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2
    );
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE id = 1")
        )
        .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_update_with_second_precision_literal() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_update";
    seed(&handler, &session, fq).await;
    exec(
        &handler,
        &session,
        &format!("UPDATE {fq} SET id = 20 WHERE ts = TIMESTAMP '2024-01-02 00:00:00'"),
    )
    .await;
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE id = 20")
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE id = 2")
        )
        .await,
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_merge_with_second_precision_literal() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_merge";
    seed(&handler, &session, fq).await;
    exec(
        &handler,
        &session,
        &format!(
            "MERGE INTO {fq} t USING (SELECT 1 AS id, TIMESTAMP '2024-01-01 00:00:00' AS ts) s \
             ON t.ts = s.ts \
             WHEN MATCHED THEN UPDATE SET id = 100 \
             WHEN NOT MATCHED THEN INSERT (id, ts) VALUES (s.id, s.ts)"
        ),
    )
    .await;
    assert_eq!(
        count(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3
    );
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE id = 100")
        )
        .await,
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_filter_with_coarse_literal_in_both_operand_orders() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_filter";
    seed(&handler, &session, fq).await;
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE ts >= TIMESTAMP '2024-01-02 00:00:00'")
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &handler,
            &session,
            &format!("SELECT COUNT(*) FROM {fq} WHERE TIMESTAMP '2024-01-02 00:00:00' <= ts")
        )
        .await,
        2
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn timestamp_literals_ctas_of_a_non_utc_literal_keeps_the_instant() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ts_lit_ctas_zone";
    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!(
            "CREATE TABLE {fq} AS SELECT TIMESTAMP '2024-01-01 10:00:00 Europe/Amsterdam' AS ts"
        ),
    )
    .await;
    // 10:00 in Amsterdam (UTC+1 in winter) is 09:00 UTC.
    assert_eq!(
        count(
            &handler,
            &session,
            &format!(
                "SELECT COUNT(*) FROM {fq} \
                 WHERE ts = TIMESTAMP '2024-01-01 09:00:00 UTC'"
            )
        )
        .await,
        1
    );
}
