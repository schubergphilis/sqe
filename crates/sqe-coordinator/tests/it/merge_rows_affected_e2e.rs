//! MERGE reports the rows its clauses changed as `rows_affected`.
//!
//! Copy-on-write MERGE used to report the rows the rewrite wrote: the whole
//! surviving table. A 1-row `WHEN MATCHED THEN DELETE` on a 3-row table
//! reported 2. Merge-on-read always counted the claimed rows; both modes run
//! here so they stay in step.

use arrow_array::{Array, Int64Array};

async fn exec(handler: &sqe_coordinator::QueryHandler, session: &sqe_core::Session, sql: &str) {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// The `rows_affected` value a write statement returns.
async fn affected(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> i64 {
    let batches = handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let col = batches[0]
        .column_by_name("rows_affected")
        .unwrap_or_else(|| panic!("{sql}: no rows_affected column"));
    col.as_any()
        .downcast_ref::<Int64Array>()
        .expect("rows_affected is Int64")
        .value(0)
}

async fn ids(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    t: &str,
) -> Vec<i64> {
    let batches = handler
        .execute(session, &format!("SELECT id FROM {t} ORDER BY id"), None)
        .await
        .expect("select");
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        out.extend((0..col.len()).map(|i| col.value(i)));
    }
    out
}

const COW: &str = "";
const MOR: &str = " TBLPROPERTIES ('write.merge.mode' = 'merge-on-read')";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn merge_reports_rows_affected() {
    let (session, handler) = crate::common::setup_handler().await;
    for (name, props) in [("merge_rc_cow", COW), ("merge_rc_mor", MOR)] {
        let t = format!("default.{name}");
        let src = format!("default.{name}_src");
        for fq in [&t, &src] {
            exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
        }
        let create = format!("CREATE TABLE {t} (id BIGINT NOT NULL, val STRING){props}");
        exec(&handler, &session, &create).await;
        if props == MOR {
            // Merge-on-read MERGE writes equality deletes on the key.
            let alter = format!("ALTER TABLE {t} SET IDENTIFIER FIELDS id");
            exec(&handler, &session, &alter).await;
        }
        let insert = format!("INSERT INTO {t} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec(&handler, &session, &insert).await;
        let create_src = format!("CREATE TABLE {src} (id BIGINT, val STRING)");
        exec(&handler, &session, &create_src).await;
        exec(
            &handler,
            &session,
            &format!("INSERT INTO {src} VALUES (2, 'x')"),
        )
        .await;

        let sql = format!("MERGE INTO {t} t USING {src} s ON t.id = s.id WHEN MATCHED THEN DELETE");
        assert_eq!(
            affected(&handler, &session, &sql).await,
            1,
            "{name}: DELETE"
        );
        assert_eq!(ids(&handler, &session, &t).await, vec![1, 3], "{name}");

        // One update (id 1), one insert (id 4); id 3 is untouched.
        exec(&handler, &session, &format!("DELETE FROM {src}")).await;
        let refill = format!("INSERT INTO {src} VALUES (1, 'A'), (4, 'd')");
        exec(&handler, &session, &refill).await;
        let sql = format!(
            "MERGE INTO {t} t USING {src} s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET val = s.val \
             WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)"
        );
        assert_eq!(
            affected(&handler, &session, &sql).await,
            2,
            "{name}: UPDATE + INSERT"
        );
        assert_eq!(ids(&handler, &session, &t).await, vec![1, 3, 4], "{name}");

        for fq in [&t, &src] {
            let _ = handler
                .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
                .await;
        }
    }
}
