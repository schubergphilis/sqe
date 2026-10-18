//! A V3 `JSON` (variant) column scanned on a worker (#849): the worker reads
//! the variant from Parquet and streams it back over Flight, and the
//! coordinator still sees variant storage, renders it as JSON and runs the
//! JSON functions on it.
//!
//! Needs the `docker-compose.test.yml` stack plus a native `sqe-worker` on
//! `http://localhost:50052` (override with `SQE_IT_WORKER_URLS`,
//! comma-separated). `scripts/integration-test.sh` skips this file by default:
//!
//! ```text
//! SQE_WORKER__FLIGHT_PORT=50052 SQE_WORKER__ALLOW_UNAUTHENTICATED=true \
//!   cargo run -p sqe-server -- --mode worker --config tests/sqe-test.toml &
//! cargo test -p sqe-coordinator --test it -- --ignored variant_distributed
//! ```

use arrow_array::Array;

fn worker_urls() -> Vec<String> {
    std::env::var("SQE_IT_WORKER_URLS")
        .unwrap_or_else(|_| "http://localhost:50052".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Every row of `sql` as `col|col|...`, a variant cell as its JSON text.
async fn rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
    let mut out = Vec::new();
    for batch in &batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| {
                if sqe_core::variant::is_variant_storage(c.data_type()) {
                    sqe_core::variant::to_json(c.as_ref()).expect("variant to JSON")
                } else {
                    c.clone()
                }
            })
            .collect();
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = columns
                .iter()
                .map(|c| crate::common::fmt_val(c.as_ref(), r))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// Tasks in `system.runtime.tasks` that ran on one of `workers`: a
/// distributed fragment's `node_id` is the URL of the worker it ran on.
async fn worker_task_count(
    workers: &[String],
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
) -> usize {
    let batches = handler
        .execute(session, "SELECT node_id FROM system.runtime.tasks", None)
        .await
        .expect("system.runtime.tasks");
    let mut n = 0;
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("node_id is a string");
        n += (0..col.len())
            .filter(|&i| col.is_valid(i) && workers.iter().any(|w| w == col.value(i)))
            .count();
    }
    n
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris+workers"]
async fn json_variant_column_scans_on_a_worker() {
    // `rig` zeroes the distribution thresholds; a table this small would
    // otherwise be scanned on the coordinator.
    let workers = worker_urls();
    let rig = crate::distributed_failure_e2e::rig(&workers, |_| {}).await;
    let (session, handler) = (&rig.session, rig.handler.as_ref());
    let fq = "default.variant_on_worker";
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
    // Default configuration: V3, so `j` is a variant. Two INSERTs, two data
    // files.
    for sql in [
        format!("CREATE TABLE {fq} (id INT, j JSON)"),
        format!(r#"INSERT INTO {fq} VALUES (1, '{{"a": {{"b": 2}}, "s": "x"}}'), (2, NULL)"#),
        format!(
            r#"INSERT INTO {fq} VALUES (3, '[1, 2]'), (4, '{{"a": {{"b": 5}}}}'), (5, '"text"')"#
        ),
    ] {
        handler.execute(session, &sql, None).await.expect(&sql);
    }

    let before = worker_task_count(&workers, handler, session).await;
    let read = rows(
        handler,
        session,
        &format!("SELECT id, j, json_get_int(j, 'a', 'b') FROM {fq} WHERE id <> 3 ORDER BY id"),
    )
    .await;
    let after = worker_task_count(&workers, handler, session).await;
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;

    assert_eq!(
        read,
        vec![
            r#"1|{"a":{"b":2},"s":"x"}|2"#,
            "2|NULL|NULL",
            r#"4|{"a":{"b":5}}|5"#,
            r#"5|"text"|NULL"#,
        ]
    );
    assert!(
        after > before,
        "the scan ran on the coordinator, not a worker: \
         system.runtime.tasks gained no worker task ({before} -> {after})"
    );
}

/// A variant Spark 4.1 shredded (`write.parquet.shred-variants`) scanned on a
/// worker: the worker folds `typed_value` back into `value` before Flight,
/// so the shredded fields arrive intact. Also needs the Spark container.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris+workers"]
async fn shredded_variant_column_scans_on_a_worker() {
    use crate::common::spark_interop::{spark_sql, spark_stack_skip_reason};
    if let Some(reason) = spark_stack_skip_reason() {
        eprintln!("skipping shredded_variant_column_scans_on_a_worker: {reason}");
        return;
    }
    let fq = "default.variant_shredded_on_worker";
    spark_sql(&format!(
        r#"DROP TABLE IF EXISTS rest.{fq};
           CREATE TABLE rest.{fq} (id INT, v VARIANT) USING iceberg
             TBLPROPERTIES ('format-version' = '3',
                            'write.parquet.shred-variants' = 'true');
           INSERT INTO rest.{fq} VALUES
             (1, parse_json('{{"a": 1, "s": "x"}}')),
             (2, parse_json('{{"a": 2, "s": "y"}}')),
             (3, parse_json('[1, 2]')),
             (4, NULL);"#
    ))
    .expect("spark writes the shredded variant table");

    let workers = worker_urls();
    let rig = crate::distributed_failure_e2e::rig(&workers, |_| {}).await;
    let (session, handler) = (&rig.session, rig.handler.as_ref());
    let before = worker_task_count(&workers, handler, session).await;
    let read = rows(
        handler,
        session,
        &format!("SELECT id, v, json_get_int(v, 'a') FROM {fq} ORDER BY id"),
    )
    .await;
    let after = worker_task_count(&workers, handler, session).await;
    let _ = spark_sql(&format!("DROP TABLE IF EXISTS rest.{fq}"));

    assert_eq!(
        read,
        vec![
            r#"1|{"a":1,"s":"x"}|1"#,
            r#"2|{"a":2,"s":"y"}|2"#,
            "3|[1,2]|NULL",
            "4|NULL|NULL",
        ]
    );
    assert!(
        after > before,
        "the scan ran on the coordinator, not a worker: \
         system.runtime.tasks gained no worker task ({before} -> {after})"
    );
}
