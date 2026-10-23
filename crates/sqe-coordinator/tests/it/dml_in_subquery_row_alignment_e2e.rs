//! DELETE / UPDATE with `IN (subquery)` change exactly the matched rows.
//!
//! The WHERE is evaluated as `SELECT ... FROM scratch LEFT JOIN keyset`,
//! and the join returns the matched rows first. The result used to be
//! zipped back onto the batch by position, so `DELETE WHERE id IN
//! (SELECT ...)` matching id 2 deleted id 1. Every case here hits a middle
//! row and asserts which ids remain, not just how many.

use arrow_array::{Array, Int64Array};

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// `t` holds ids 1..=4 with `v = id * 10`; `keys` holds id 2.
async fn reset(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    name: &str,
    props: &str,
) -> (String, String) {
    let t = format!("default.{name}");
    let keys = format!("default.{name}_keys");
    for fq in [&t, &keys] {
        exec(handler, session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    }
    exec(
        handler,
        session,
        &format!("CREATE TABLE {t} (id BIGINT NOT NULL, v BIGINT){props}"),
    )
    .await;
    exec(
        handler,
        session,
        &format!("INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30), (4, 40)"),
    )
    .await;
    exec(
        handler,
        session,
        &format!("CREATE TABLE {keys} (id BIGINT)"),
    )
    .await;
    exec(handler, session, &format!("INSERT INTO {keys} VALUES (2)")).await;
    (t, keys)
}

/// `(id, v)` rows ordered by id.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
) -> Vec<(i64, i64)> {
    let batches = handler
        .execute(session, &format!("SELECT id, v FROM {t} ORDER BY id"), None)
        .await
        .expect("select");
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let vs = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vs.value(i)));
        }
    }
    out
}

async fn cleanup(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
    keys: &str,
) {
    for fq in [t, keys] {
        let _ = handler
            .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;
    }
}

const COW: &str = "";
const MOR: &str =
    " TBLPROPERTIES ('write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read')";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn dml_in_subquery_delete_removes_the_matched_row() {
    let (session, handler) = crate::common::setup_handler().await;
    for (name, props) in [("dml_insq_del_cow", COW), ("dml_insq_del_mor", MOR)] {
        let (t, keys) = reset(&handler, &session, name, props).await;
        exec(
            &handler,
            &session,
            &format!("DELETE FROM {t} WHERE id IN (SELECT id FROM {keys})"),
        )
        .await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (3, 30), (4, 40)],
            "{name}: IN"
        );

        exec(
            &handler,
            &session,
            &format!("DELETE FROM {t} WHERE id NOT IN (SELECT id FROM {keys}) AND id > 3"),
        )
        .await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (3, 30)],
            "{name}: NOT IN"
        );
        cleanup(&handler, &session, &t, &keys).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn dml_in_subquery_update_changes_the_matched_row() {
    let (session, handler) = crate::common::setup_handler().await;
    for (name, props) in [("dml_insq_upd_cow", COW), ("dml_insq_upd_mor", MOR)] {
        let (t, keys) = reset(&handler, &session, name, props).await;
        if props == MOR {
            // Equality deletes: the MoR UPDATE path filters by position.
            exec(
                &handler,
                &session,
                &format!("ALTER TABLE {t} SET IDENTIFIER FIELDS id"),
            )
            .await;
        }
        exec(
            &handler,
            &session,
            &format!("UPDATE {t} SET v = v + 1 WHERE id IN (SELECT id FROM {keys})"),
        )
        .await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (2, 21), (3, 30), (4, 40)],
            "{name}"
        );
        cleanup(&handler, &session, &t, &keys).await;
    }
}
