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

/// Time travel to a snapshot from before a column was dropped and re-added
/// under the same name, and before another was renamed and its old name
/// reused. The worker's field ids came from the current schema: `d` resolved
/// to the new column (absent from the old file, so NULL) and `name` to the
/// new `name` column. The coordinator's own scan read both right.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn time_travel_reads_the_snapshot_columns_on_a_worker() {
    let table = "default.dist_time_travel_evolve";
    let (session, setup) = crate::common::setup_handler().await;
    let run = |sql: String| {
        let setup = &setup;
        let session = &session;
        async move {
            setup
                .execute(session, &sql, None)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
        }
    };
    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    run(format!(
        "CREATE TABLE {table} (id BIGINT, name STRING, d DATE)"
    ))
    .await;
    run(format!(
        "INSERT INTO {table} VALUES (1, 'a', DATE '2026-01-01'), (2, 'b', DATE '2026-02-01')"
    ))
    .await;
    let snapshots =
        run("SELECT snapshot_id FROM default.\"dist_time_travel_evolve$snapshots\"".into()).await;
    let old = arrow::util::display::array_value_to_string(snapshots[0].column(0), 0).unwrap();
    run(format!("ALTER TABLE {table} DROP COLUMN d")).await;
    run(format!("ALTER TABLE {table} ADD COLUMN d STRING")).await;
    run(format!("ALTER TABLE {table} RENAME COLUMN name TO label")).await;
    run(format!("ALTER TABLE {table} ADD COLUMN name STRING")).await;
    run(format!(
        "INSERT INTO {table} VALUES (3, 'c', 'new-d', 'new-name')"
    ))
    .await;

    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let rig = rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let at_old = format!("SELECT id, name, d FROM {table} FOR VERSION AS OF {old} ORDER BY id");
    assert_eq!(
        rendered_rows(&rig, &at_old).await,
        vec!["1|a|2026-01-01", "2|b|2026-02-01"],
        "the old snapshot's `name` and `d`, not the columns that reuse the names"
    );
    assert!(
        worker.do_gets() >= 1,
        "the scan must have run on the worker"
    );
    let now = format!("SELECT id, label, d, name FROM {table} ORDER BY id");
    assert_eq!(
        rendered_rows(&rig, &now).await,
        vec!["1|a|NULL|NULL", "2|b|NULL|NULL", "3|c|new-d|new-name"]
    );
    // The old files store `name` and `d` under the names the new columns
    // reuse. A worker predicate bound by name filtered the old columns.
    for (filter, ids) in [
        ("name IS NULL", vec!["1", "2"]),
        ("d IS NULL", vec!["1", "2"]),
        ("name = 'a'", vec![]),
        ("label = 'a'", vec!["1"]),
    ] {
        let sql = format!("SELECT id FROM {table} WHERE {filter} ORDER BY id");
        assert_eq!(rendered_rows(&rig, &sql).await, ids, "{filter}");
    }

    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}

/// No data file stores a V3 `unknown` field, top-level or nested (#859). The
/// worker reads the narrower struct and must put the member back as NULL, as
/// the coordinator's own scan does.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn unknown_fields_read_as_null_on_a_worker() {
    let table = "default.dist_unknown_859";
    let (session, setup) = crate::common::setup_handler().await;
    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
    for sql in [
        format!("CREATE TABLE {table} (id BIGINT, s STRUCT<a INT, u UNKNOWN>, w UNKNOWN)"),
        format!("INSERT INTO {table} VALUES (1, named_struct('a', 10, 'u', NULL), NULL)"),
        format!("INSERT INTO {table} VALUES (2, named_struct('a', 20, 'u', NULL), NULL)"),
    ] {
        setup
            .execute(&session, &sql, None)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let worker = spawn_chaos_worker(ChaosMode::PassThrough).await;
    let rig = rig(std::slice::from_ref(&worker.url), |_| {}).await;
    let fields = format!("SELECT id, s['a'], s['u'] IS NULL, w IS NULL FROM {table} ORDER BY id");
    assert_eq!(
        rendered_rows(&rig, &fields).await,
        vec!["1|10|true|true", "2|20|true|true"]
    );
    assert!(
        worker.do_gets() >= 1,
        "the scan must have run on the worker"
    );
    // `w` is an Arrow NullArray, which has no null buffer, so `is_null` is
    // false and it renders as empty text rather than `NULL`.
    let star = format!("SELECT * FROM {table} ORDER BY id");
    assert_eq!(
        rendered_rows(&rig, &star).await,
        vec!["1|{a: 10, u: }|", "2|{a: 20, u: }|"]
    );

    let _ = setup
        .execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None)
        .await;
}
