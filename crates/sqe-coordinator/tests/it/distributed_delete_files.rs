//! A snapshot with delete files is scanned on the coordinator, not on a
//! worker. A worker ticket carries only data file paths and the worker reads
//! them with a plain Parquet reader, so position deletes and deletion vectors
//! would not be applied there and the deleted rows would come back. Found by
//! the Databricks Unity Catalog interop run: a deletion vector Databricks
//! wrote into an SQE table was ignored by the distributed read.
//!
//! Runs against an in-process worker, so it tests the code in this tree.

async fn ids(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    let mut out = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            out.push(crate::common::fmt_val(batch.column(0).as_ref(), r));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn deleted_rows_stay_deleted_with_workers() {
    use crate::common::chaos_worker::{spawn_chaos_worker, ChaosMode};
    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    // `rig` zeroes the distribution thresholds, so without the gate these
    // small tables would be scanned on the worker.
    let rig = crate::distributed_failure_e2e::rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let (session, handler) = (&rig.session, rig.handler.as_ref());

    // V2 writes position delete files, V3 deletion vectors.
    for version in ["2", "3"] {
        let fq = format!("default.delete_files_on_worker_v{version}");
        let _ = handler
            .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;
        for sql in [
            format!(
                "CREATE TABLE {fq} (id INT, region VARCHAR) \
                 WITH ('format-version' = '{version}', 'write.delete.mode' = 'merge-on-read')"
            ),
            format!("INSERT INTO {fq} VALUES (1, 'eu'), (2, 'us')"),
            format!("INSERT INTO {fq} VALUES (3, 'eu'), (4, 'us')"),
        ] {
            handler.execute(session, &sql, None).await.expect(&sql);
        }
        // Control: without delete files the scan does go to the worker.
        let before = worker.do_gets();
        let all = ids(
            handler,
            session,
            &format!("SELECT id FROM {fq} ORDER BY id"),
        )
        .await;
        assert_eq!(all, vec!["1", "2", "3", "4"], "v{version}: before DELETE");
        assert!(
            worker.do_gets() > before,
            "v{version}: a delete-free scan stayed on the coordinator"
        );

        let sql = format!("DELETE FROM {fq} WHERE id IN (1, 3)");
        handler.execute(session, &sql, None).await.expect(&sql);

        let before = worker.do_gets();
        let all = ids(
            handler,
            session,
            &format!("SELECT id FROM {fq} ORDER BY id"),
        )
        .await;
        let us = ids(
            handler,
            session,
            &format!("SELECT id FROM {fq} WHERE region = 'us' ORDER BY id"),
        )
        .await;
        let after = worker.do_gets();
        let _ = handler
            .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
            .await;

        assert_eq!(all, vec!["2", "4"], "v{version}: deleted rows came back");
        assert_eq!(us, vec!["2", "4"], "v{version}: filtered read");
        // Until a ticket carries delete files, the scan must stay local.
        assert_eq!(
            after, before,
            "v{version}: a snapshot with delete files was sent to a worker"
        );
    }
}
