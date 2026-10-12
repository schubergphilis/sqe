//! Distributed scans over files written before a schema change (#831).
//!
//! A worker projects by field id and used to drop a projected column its file
//! lacks, emitting columns in file order. A fragment that mixed files from
//! before and after an `ADD COLUMN` then failed in the Flight encoder
//! ("number of columns(1) must match number of fields(2)"), and a renamed
//! column could only be realigned by position on the coordinator.
//!
//! These tests dispatch to a real in-process worker (the production
//! `WorkerFlightService` behind `ChaosMode::PassThrough`). With one worker and
//! the default task size, every small file lands in ONE fragment, so the
//! worker always reads old and new files in the same stream.
//!
//! Stack: `make test-integration FILTER=distributed_schema_evolution_e2e`.

use crate::common::chaos_worker::{spawn_chaos_worker, ChaosMode};
use crate::distributed_failure_e2e::rig;

/// Every row of `sql` as `col|col|...` text, NULL rendered as `NULL`.
async fn rendered_rows(rig: &crate::distributed_failure_e2e::Rig, sql: &str) -> Vec<String> {
    let batches = rig
        .handler
        .execute(&rig.session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut rows = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| {
                    if c.is_null(r) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(c, r).expect("render")
                    }
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn files_from_before_add_column_and_rename_read_correctly_on_a_worker() {
    let table = "default.dist_evolve_831";
    let (session, setup) = crate::common::setup_handler().await;
    let run = |sql: String| {
        let setup = &setup;
        let session = &session;
        async move {
            setup
                .execute(session, &sql, None)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    };
    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    run(format!(
        "CREATE TABLE {table} (id BIGINT, name STRING) TBLPROPERTIES ('format-version'='3')"
    ))
    .await;
    // Two files with neither later column.
    run(format!("INSERT INTO {table} VALUES (1, 'a')")).await;
    run(format!("INSERT INTO {table} VALUES (2, 'b')")).await;
    run(format!(
        "ALTER TABLE {table} ADD COLUMN region STRING DEFAULT 'eu'"
    ))
    .await;
    run(format!("ALTER TABLE {table} ADD COLUMN note STRING")).await;
    run(format!("INSERT INTO {table} VALUES (3, 'c', 'us', 'x')")).await;
    // Files 1-3 store the column as `name`. Same type as `region`, so a
    // positional realignment would swap values without any type error.
    run(format!("ALTER TABLE {table} RENAME COLUMN name TO label")).await;

    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let rig = rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let q = |cols: &str, tail: &str| format!("SELECT {cols} FROM {table}{tail} ORDER BY id");

    assert_eq!(
        rendered_rows(&rig, &q("id, label, region, note", "")).await,
        vec!["1|a|eu|NULL", "2|b|eu|NULL", "3|c|us|x"],
        "pre-ALTER files read the DEFAULT, or NULL without one"
    );
    assert!(
        worker.do_gets() >= 1,
        "the scan must have run on the worker"
    );

    // Projection order differs from every file's storage order.
    assert_eq!(
        rendered_rows(&rig, &q("region, label, id", "")).await,
        vec!["eu|a|1", "eu|b|2", "us|c|3"],
        "a renamed column must be matched by field id, not by position"
    );

    // The worker's pushed predicate cannot bind a column its file lacks; the
    // coordinator's filter must still see the default, and NULL.
    assert_eq!(
        rendered_rows(&rig, &q("id", " WHERE region = 'eu'")).await,
        vec!["1", "2"]
    );
    assert_eq!(
        rendered_rows(&rig, &q("id", " WHERE note IS NULL")).await,
        vec!["1", "2"]
    );
    assert!(rendered_rows(&rig, &q("id", " WHERE region IS NULL"))
        .await
        .is_empty());

    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}
