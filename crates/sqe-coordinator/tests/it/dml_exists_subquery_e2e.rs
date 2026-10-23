//! DELETE / UPDATE with a correlated `EXISTS` / `NOT EXISTS` subquery.
//!
//! The WHERE used to be spliced into the SELECT list of the per-batch
//! evaluation query. DataFusion only decorrelates subqueries in a Filter, so
//! a correlated EXISTS failed with "Physical plan does not support logical
//! expression Exists". The WHERE now runs as a WHERE clause and its verdict
//! comes back as row positions.

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
    let create = format!("CREATE TABLE {t} (id BIGINT NOT NULL, v BIGINT){props}");
    exec(handler, session, &create).await;
    let insert = format!("INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    exec(handler, session, &insert).await;
    let create_keys = format!("CREATE TABLE {keys} (id BIGINT)");
    exec(handler, session, &create_keys).await;
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
async fn dml_exists_subquery_delete_correlated() {
    let (session, handler) = crate::common::setup_handler().await;
    for (name, props) in [("dml_exists_del_cow", COW), ("dml_exists_del_mor", MOR)] {
        let (t, keys) = reset(&handler, &session, name, props).await;
        let sql =
            format!("DELETE FROM {t} WHERE EXISTS (SELECT 1 FROM {keys} k WHERE k.id = {name}.id)");
        exec(&handler, &session, &sql).await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (3, 30), (4, 40)],
            "{name}: EXISTS"
        );

        let sql = format!(
            "DELETE FROM {t} WHERE NOT EXISTS (SELECT 1 FROM {keys} k WHERE k.id = {t}.id) \
             AND id > 3"
        );
        exec(&handler, &session, &sql).await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (3, 30)],
            "{name}: NOT EXISTS"
        );
        cleanup(&handler, &session, &t, &keys).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn dml_exists_subquery_update_correlated() {
    let (session, handler) = crate::common::setup_handler().await;
    for (name, props) in [("dml_exists_upd_cow", COW), ("dml_exists_upd_mor", MOR)] {
        let (t, keys) = reset(&handler, &session, name, props).await;
        if props == MOR {
            // Equality deletes: the MoR UPDATE path filters by position.
            let alter = format!("ALTER TABLE {t} SET IDENTIFIER FIELDS id");
            exec(&handler, &session, &alter).await;
        }
        let sql = format!(
            "UPDATE {t} SET v = v + 1 WHERE EXISTS (SELECT 1 FROM {keys} k WHERE k.id = {name}.id)"
        );
        exec(&handler, &session, &sql).await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (2, 21), (3, 30), (4, 40)],
            "{name}: EXISTS"
        );

        // A scalar subquery in the WHERE takes the same path.
        let sql = format!("UPDATE {t} SET v = 0 WHERE id > (SELECT max(id) FROM {keys}) + 1");
        exec(&handler, &session, &sql).await;
        assert_eq!(
            rows(&handler, &session, &t).await,
            vec![(1, 10), (2, 21), (3, 30), (4, 0)],
            "{name}: scalar subquery"
        );
        cleanup(&handler, &session, &t, &keys).await;
    }
}
