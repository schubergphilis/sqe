//! Distributed-rewrite correctness parity (Phase 4c Task 6).
//!
//! `CALL system.rewrite_data_files(..., distributed => 'require')` fans a
//! bin-packed rewrite out to the worker fleet via `compact_file_group`
//! (Task 3's worker action, Task 4's coordinator dispatch, Task 5's
//! `distribution.mode` routing). This file proves the distributed path is a
//! drop-in replacement for the coordinator-local path, not just "it runs":
//! two identically-seeded fixtures are rewritten, one with `distributed =>
//! 'require'` and one with `distributed => 'local'`, and the two outcomes
//! must be indistinguishable to a reader of the table:
//!
//! - the surviving row set is identical between the two paths, and matches
//!   the independently-computed expected set (deletes/updates applied, not
//!   resurrected or lost)
//! - both paths consolidate the small input files into fewer, larger files
//! - the distributed commit is exactly ONE new Iceberg snapshot, stamped
//!   with the standard `operation = "replace"` summary `RewriteFilesAction`
//!   gives every rewrite commit (manual `CALL` never adds a custom
//!   `sqe.maintenance.*` job-identity stamp -- that is the active-mode
//!   scheduler's path, not this one; see `maintenance.rs`'s comment on
//!   `handle()`'s `RewriteDataFiles` arm)
//!
//! Two independent fixtures are covered, matching the two MoR delete
//! taxonomies already proven for the *local* path in
//! `rewrite_data_files_deletes.rs` (kept as two separate DDL shapes rather
//! than one combined table, since a table with both `write.delete.mode` and
//! `write.update.mode` set to merge-on-read carrying both delete kinds at
//! once is an untested DDL combination -- not what either existing local
//! test exercises, and not something this file can execute here to
//! discover a latent bug in):
//!
//! - [`position_delete_parity`]: `DELETE FROM` on a plain table (position
//!   deletes only), mirroring `rewrite_data_files_deletes.rs`'s
//!   `seed_mor_table` / `rewrite_applies_position_deletes_and_consolidates`.
//! - [`equality_delete_parity`]: `UPDATE` on a table with
//!   `identifier_field_ids` (equality delete + a fresh data file), mirroring
//!   `rewrite_data_files_deletes.rs`'s `rewrite_applies_equality_deletes`.
//!
//! Each fixture forces multiple bin-pack groups (not just multiple files) by
//! measuring the actual per-file byte size after seeding and setting
//! `target_file_size_bytes` to ~2.5x that, so `pack_file_groups` cannot
//! collapse everything into a single group. With 2 healthy workers
//! registered and >= 2 groups to place, the load-balanced dispatch
//! (`WorkerLoadTracker`, least-loaded-first) is expected to use both -- this
//! is what makes the fixture an "N-group table on >= 2 workers" test, not
//! just a single group landing on whichever worker happens to be first.
//!
//! ## Running this test
//!
//! Both tests are `#[ignore]`: they need Polaris + RustFS (the same
//! `docker-compose.test.yml` stack every other `#[ignore]` test in this
//! binary uses) PLUS two live `sqe-worker` processes reachable at
//! `http://localhost:50052` and `http://localhost:50053` (override via
//! `SQE_IT_WORKER_URLS`, comma-separated).
//!
//! Deliberately NATIVE worker processes, not the `docker-compose.
//! distributed.yml` overlay's `worker-1`/`worker-2` containers: the
//! coordinator sends each worker an `S3Conn` built from *its own* storage
//! config (`tests/sqe-test.toml`'s `s3_endpoint = "http://localhost:19000"`),
//! and a container cannot reach the test host's `localhost`. A worker
//! process running natively on the same host as this test binary can.
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//!
//! # Two workers, same test config, distinct ports:
//! cargo run -p sqe-worker -- tests/sqe-test.toml &
//! SQE_WORKER__FLIGHT_PORT=50053 cargo run -p sqe-worker -- tests/sqe-test.toml &
//!
//! cargo test -p sqe-coordinator --test it -- --ignored \
//!   rewrite_data_files_distributed_parity --test-threads=1 --nocapture
//! ```
//!
//! Both worker processes use the test config's empty `worker_secret`, which
//! puts `compact_file_group` request signing in dev mode (unsigned, see
//! `sqe_compaction::wire`'s pinned `sign_compact_request_omits_signature_in_
//! dev_mode_with_empty_secret` test) -- no extra secret wiring needed for a
//! local run.

use std::collections::HashSet;

use arrow_array::{Array, Int64Array, StringArray};

fn worker_urls() -> Vec<String> {
    std::env::var("SQE_IT_WORKER_URLS")
        .unwrap_or_else(|_| "http://localhost:50052,http://localhost:50053".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

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

async fn count_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let b = exec(handler, session, &format!("SELECT COUNT(*) FROM {table}")).await;
    b[0].column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array").value(0)
}

/// The full surviving row set as `(id, v)` pairs, ordered by `id` so two
/// calls against equivalent tables compare equal regardless of physical file
/// layout. Used both to compare the distributed and local outcomes against
/// each other AND against the independently-computed expected set.
async fn collect_id_v_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<(i64, i64)> {
    let b = exec(handler, session, &format!("SELECT id, v FROM {table} ORDER BY id")).await;
    let mut out = Vec::new();
    for batch in &b {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id Int64Array");
        let vs = batch.column(1).as_any().downcast_ref::<Int64Array>().expect("v Int64Array");
        for i in 0..batch.num_rows() {
            out.push((ids.value(i), vs.value(i)));
        }
    }
    out
}

/// Same as [`collect_id_v_rows`] but for the single-column (`id` only)
/// position-delete fixture.
async fn collect_id_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<i64> {
    let b = exec(handler, session, &format!("SELECT id FROM {table} ORDER BY id")).await;
    let mut out = Vec::new();
    for batch in &b {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id Int64Array");
        for i in 0..batch.num_rows() {
            out.push(ids.value(i));
        }
    }
    out
}

async fn live_data_file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) -> i64 {
    let b = exec(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table_name}')"),
    )
    .await;
    b[0].column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array").value(0)
}

/// Largest live data-file size in bytes, used to derive a
/// `target_file_size_bytes` that forces multiple bin-pack groups instead of
/// collapsing every small file into a single group (the default 512 MiB
/// target would do exactly that at this fixture's tiny scale).
async fn max_file_size_bytes(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) -> i64 {
    let b = exec(
        handler,
        session,
        &format!("SELECT MAX(file_size_in_bytes) FROM table_files('{namespace}', '{table_name}')"),
    )
    .await;
    b[0].column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array").value(0)
}

async fn snapshot_ids(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) -> HashSet<i64> {
    let b = exec(
        handler,
        session,
        &format!("SELECT snapshot_id FROM table_snapshots('{namespace}', '{table_name}')"),
    )
    .await;
    let mut out = HashSet::new();
    for batch in &b {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array");
        for i in 0..batch.num_rows() {
            out.insert(ids.value(i));
        }
    }
    out
}

async fn snapshot_operation(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
    snapshot_id: i64,
) -> String {
    let b = exec(
        handler,
        session,
        &format!(
            "SELECT operation FROM table_snapshots('{namespace}', '{table_name}') \
             WHERE snapshot_id = {snapshot_id}"
        ),
    )
    .await;
    b[0]
        .column_by_name("operation")
        .expect("operation column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("operation StringArray")
        .value(0)
        .to_string()
}

fn status_of(summary: &[arrow_array::RecordBatch]) -> String {
    summary[0]
        .column_by_name("status")
        .expect("status column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("status StringArray")
        .value(0)
        .to_string()
}

/// Run `CALL system.rewrite_data_files` against `table` with the given
/// `distributed` override and `target_file_size_bytes`, asserting it
/// actually ran (did not skip), and return the summary batch for further
/// inspection.
async fn run_rewrite(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    target_file_size_bytes: i64,
    distributed: &str,
) -> Vec<arrow_array::RecordBatch> {
    let summary = exec(
        handler,
        session,
        &format!(
            "CALL system.rewrite_data_files(table => '{table}', min_input_files => 2, \
             target_file_size_bytes => {target_file_size_bytes}, distributed => '{distributed}')"
        ),
    )
    .await;
    assert!(
        !status_of(&summary).contains("skipped"),
        "distributed='{distributed}' rewrite of {table} must run, not skip; got '{}'",
        status_of(&summary)
    );
    summary
}

/// Assert the rewrite committed exactly one new snapshot, stamped with the
/// standard Iceberg `replace` operation `RewriteFilesAction` always sets.
async fn assert_one_new_replace_snapshot(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
    before: &HashSet<i64>,
) {
    let after = snapshot_ids(handler, session, namespace, table_name).await;
    assert_eq!(
        after.len(),
        before.len() + 1,
        "rewrite must commit exactly one new snapshot: before={before:?} after={after:?}"
    );
    let mut new_ids: Vec<i64> = after.difference(before).copied().collect();
    assert_eq!(new_ids.len(), 1, "exactly one new snapshot id must appear: {new_ids:?}");
    let new_id = new_ids.pop().unwrap();
    let op = snapshot_operation(handler, session, namespace, table_name, new_id).await;
    assert_eq!(
        op, "replace",
        "the new snapshot must be stamped with Iceberg's 'replace' operation \
         (RewriteFilesAction), got '{op}'"
    );
}

/// Seed a position-delete-only MoR fixture: `rows` single-row data files
/// (`id` only), then `DELETE FROM ... WHERE id < delete_below`. Verbatim
/// shape of `rewrite_data_files_deletes.rs::seed_mor_table` plus its DELETE.
async fn seed_position_delete_fixture(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    rows: i64,
    delete_below: i64,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        handler,
        session,
        &format!(
            "CREATE TABLE {table} TBLPROPERTIES ('write.delete.mode' = 'merge-on-read') \
             AS SELECT CAST(0 AS BIGINT) AS id"
        ),
    )
    .await;
    for i in 1..rows {
        exec(handler, session, &format!("INSERT INTO {table} VALUES ({i})")).await;
    }
    exec(handler, session, &format!("DELETE FROM {table} WHERE id < {delete_below}")).await;
}

/// Seed an equality-delete MoR fixture: `rows` single-row data files (`id`,
/// `v = id * 10`), then `UPDATE ... SET v = new_v WHERE id = updated_id`
/// (equality delete + a fresh data file). Verbatim shape of
/// `rewrite_data_files_deletes.rs::rewrite_applies_equality_deletes`.
async fn seed_equality_delete_fixture(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    rows: i64,
    updated_id: i64,
    new_v: i64,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        handler,
        session,
        &format!(
            "CREATE TABLE {table} (id BIGINT, v BIGINT) WITH (identifier_field_ids = 'id', \
             'write.update.mode' = 'merge-on-read')"
        ),
    )
    .await;
    for i in 0..rows {
        exec(handler, session, &format!("INSERT INTO {table} VALUES ({i}, {})", i * 10)).await;
    }
    exec(
        handler,
        session,
        &format!("UPDATE {table} SET v = {new_v} WHERE id = {updated_id}"),
    )
    .await;
}

/// Position-delete parity: distributed rewrite of an N-group table on >= 2
/// workers matches a coordinator-local rewrite of the same fixture.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris + 2 live sqe-worker processes"]
async fn position_delete_parity() {
    let (session, handler) = crate::common::setup_handler_with_workers(&worker_urls()).await;
    let namespace = "default";
    let dist_name = "rewrite_dist_parity_pos_distributed";
    let local_name = "rewrite_dist_parity_pos_local";
    let dist_table = format!("{namespace}.{dist_name}");
    let local_table = format!("{namespace}.{local_name}");

    const ROWS: i64 = 16;
    const DELETE_BELOW: i64 = 3;

    seed_position_delete_fixture(&handler, &session, &dist_table, ROWS, DELETE_BELOW).await;
    seed_position_delete_fixture(&handler, &session, &local_table, ROWS, DELETE_BELOW).await;

    // Fixtures must start byte-identical (same recipe, independently seeded).
    let pre_dist = collect_id_rows(&handler, &session, &dist_table).await;
    let pre_local = collect_id_rows(&handler, &session, &local_table).await;
    assert_eq!(pre_dist, pre_local, "both fixtures must start with identical content");

    let expected: Vec<i64> = (DELETE_BELOW..ROWS).collect();
    assert_eq!(pre_dist, expected, "setup invariant: deletes must already be visible pre-rewrite");

    let before_files_dist = live_data_file_count(&handler, &session, namespace, dist_name).await;
    let before_files_local = live_data_file_count(&handler, &session, namespace, local_name).await;
    assert!(
        before_files_dist >= 8,
        "setup invariant: need enough small files to form multiple bin-pack groups, got {before_files_dist}"
    );
    assert_eq!(before_files_dist, before_files_local);

    // Force multiple groups: target ~2.5x the largest single-row file, so
    // pack_file_groups fits about 2 files per group instead of everything
    // into one (the default 512 MiB target would do the latter here).
    let max_size = max_file_size_bytes(&handler, &session, namespace, dist_name).await;
    let target_bytes = (max_size * 5) / 2;

    let dist_snapshots_before = snapshot_ids(&handler, &session, namespace, dist_name).await;

    run_rewrite(&handler, &session, &dist_table, target_bytes, "require").await;
    run_rewrite(&handler, &session, &local_table, target_bytes, "local").await;

    // Correctness parity: the two paths must be indistinguishable.
    let post_dist = collect_id_rows(&handler, &session, &dist_table).await;
    let post_local = collect_id_rows(&handler, &session, &local_table).await;
    assert_eq!(
        post_dist, post_local,
        "distributed rewrite must produce the same surviving rows as a coordinator-local \
         rewrite of the same fixture"
    );
    assert_eq!(
        post_dist, expected,
        "deleted rows must stay deleted (not resurrected) and no other row must be lost"
    );
    assert_eq!(count_rows(&handler, &session, &dist_table).await, expected.len() as i64);

    // Consolidation on both paths.
    let after_files_dist = live_data_file_count(&handler, &session, namespace, dist_name).await;
    let after_files_local = live_data_file_count(&handler, &session, namespace, local_name).await;
    assert!(
        after_files_dist < before_files_dist,
        "distributed rewrite must consolidate: {before_files_dist} -> {after_files_dist}"
    );
    assert!(
        after_files_local < before_files_local,
        "local rewrite must consolidate: {before_files_local} -> {after_files_local}"
    );

    // Exactly one new snapshot, stamped.
    assert_one_new_replace_snapshot(&handler, &session, namespace, dist_name, &dist_snapshots_before)
        .await;

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {dist_table}")).await;
    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {local_table}")).await;
}

/// Equality-delete parity: same properties as [`position_delete_parity`],
/// exercised on the equality-delete taxonomy (MoR `UPDATE`) instead.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris + 2 live sqe-worker processes"]
async fn equality_delete_parity() {
    let (session, handler) = crate::common::setup_handler_with_workers(&worker_urls()).await;
    let namespace = "default";
    let dist_name = "rewrite_dist_parity_eq_distributed";
    let local_name = "rewrite_dist_parity_eq_local";
    let dist_table = format!("{namespace}.{dist_name}");
    let local_table = format!("{namespace}.{local_name}");

    const ROWS: i64 = 16;
    const UPDATED_ID: i64 = 10;
    const NEW_V: i64 = 999;

    seed_equality_delete_fixture(&handler, &session, &dist_table, ROWS, UPDATED_ID, NEW_V).await;
    seed_equality_delete_fixture(&handler, &session, &local_table, ROWS, UPDATED_ID, NEW_V).await;

    let pre_dist = collect_id_v_rows(&handler, &session, &dist_table).await;
    let pre_local = collect_id_v_rows(&handler, &session, &local_table).await;
    assert_eq!(pre_dist, pre_local, "both fixtures must start with identical content");

    let expected: Vec<(i64, i64)> = (0..ROWS)
        .map(|id| (id, if id == UPDATED_ID { NEW_V } else { id * 10 }))
        .collect();
    assert_eq!(pre_dist, expected, "setup invariant: the UPDATE must already be visible pre-rewrite");

    let before_files_dist = live_data_file_count(&handler, &session, namespace, dist_name).await;
    let before_files_local = live_data_file_count(&handler, &session, namespace, local_name).await;
    assert!(
        before_files_dist >= 8,
        "setup invariant: need enough small files to form multiple bin-pack groups, got {before_files_dist}"
    );
    assert_eq!(before_files_dist, before_files_local);

    let max_size = max_file_size_bytes(&handler, &session, namespace, dist_name).await;
    let target_bytes = (max_size * 5) / 2;

    let dist_snapshots_before = snapshot_ids(&handler, &session, namespace, dist_name).await;

    run_rewrite(&handler, &session, &dist_table, target_bytes, "require").await;
    run_rewrite(&handler, &session, &local_table, target_bytes, "local").await;

    let post_dist = collect_id_v_rows(&handler, &session, &dist_table).await;
    let post_local = collect_id_v_rows(&handler, &session, &local_table).await;
    assert_eq!(
        post_dist, post_local,
        "distributed rewrite must produce the same surviving rows as a coordinator-local \
         rewrite of the same fixture"
    );
    assert_eq!(
        post_dist, expected,
        "the equality delete must stay applied: the updated value must survive and the \
         stale pre-update value must not be resurrected"
    );

    let after_files_dist = live_data_file_count(&handler, &session, namespace, dist_name).await;
    let after_files_local = live_data_file_count(&handler, &session, namespace, local_name).await;
    assert!(
        after_files_dist < before_files_dist,
        "distributed rewrite must consolidate: {before_files_dist} -> {after_files_dist}"
    );
    assert!(
        after_files_local < before_files_local,
        "local rewrite must consolidate: {before_files_local} -> {after_files_local}"
    );

    assert_one_new_replace_snapshot(&handler, &session, namespace, dist_name, &dist_snapshots_before)
        .await;

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {dist_table}")).await;
    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {local_table}")).await;
}
