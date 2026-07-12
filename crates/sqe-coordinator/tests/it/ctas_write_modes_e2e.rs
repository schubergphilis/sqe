//! End-to-end verification that `write.{delete,update,merge}.mode` set via
//! CTAS `TBLPROPERTIES` is honored at write time, not just persisted (#371).
//!
//! MR !557 fixed both CTAS handlers silently dropping user `TBLPROPERTIES`,
//! so the mode strings now reach Iceberg table metadata. These tests close
//! the remaining gap: proving the DML dispatchers read the property at
//! MERGE/DELETE/UPDATE time and produce the matching file layout.
//!
//! Dispatch semantics under test (write_handler.rs):
//!
//! - DELETE + `merge-on-read`: equality deletes when the table declares
//!   identifier-field-ids, otherwise position deletes. Never rewrites
//!   data files.
//! - UPDATE/MERGE + `merge-on-read`: equality deletes when the table
//!   declares identifier-field-ids, otherwise a DOCUMENTED fallback to
//!   CoW (SQE DDL, including CTAS, has no way to declare a primary key,
//!   so this fallback is the expected behaviour for every CTAS table).
//! - Property absent: CoW everywhere (Iceberg spec default).
//!
//! Every test is `#[ignore]` because each needs the running stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored ctas_write_modes
//! ```
//!
//! File-level evidence comes from the `table_files` TVF (live data files
//! only; delete files are invisible to it) and the latest snapshot's
//! `summary` JSON from `table_snapshots`.

use arrow_array::{Array, Int64Array, StringArray};
use std::collections::HashMap;

/// Count live data files via the `table_files` TVF. Position/equality
/// delete files do not appear here, so this measures exactly the
/// "did the engine rewrite data files" question.
async fn live_data_file_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> i64 {
    let batches = handler
        .execute(
            session,
            &format!("SELECT COUNT(*) FROM table_files('{namespace}', '{table}')"),
            None,
        )
        .await
        .expect("table_files scan");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

/// Fetch `(operation, summary-json)` of the most recent snapshot.
async fn latest_snapshot(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> (String, HashMap<String, String>) {
    let batches = handler
        .execute(
            session,
            &format!(
                "SELECT operation, summary \
                 FROM table_snapshots('{namespace}', '{table}') \
                 ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
            ),
            None,
        )
        .await
        .expect("table_snapshots scan");
    let op = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("operation column")
        .value(0)
        .to_string();
    let summary_json = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("summary column")
        .value(0)
        .to_string();
    let summary: HashMap<String, String> = serde_json::from_str(&summary_json).unwrap_or_default();
    (op, summary)
}

/// Numeric summary key lookup; missing key counts as 0 (Iceberg omits
/// zero-valued counters from the snapshot summary).
fn summary_count(summary: &HashMap<String, String>, key: &str) -> i64 {
    summary.get(key).and_then(|v| v.parse().ok()).unwrap_or(0)
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

async fn scalar_i64(
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

/// CTAS seed + two single-row INSERTs -> three live data files, one row
/// each, ids 1..=3. Single-row files make CoW-vs-MoR discrimination a
/// plain file-count check.
async fn seed_three_files(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    fq: &str,
    tblproperties: &str,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    let props = if tblproperties.is_empty() {
        String::new()
    } else {
        format!(" TBLPROPERTIES ({tblproperties})")
    };
    exec(
        handler,
        session,
        &format!("CREATE TABLE {fq}{props} AS SELECT 1 AS id, 10 AS v"),
    )
    .await;
    exec(
        handler,
        session,
        &format!("INSERT INTO {fq} VALUES (2, 20)"),
    )
    .await;
    exec(
        handler,
        session,
        &format!("INSERT INTO {fq} VALUES (3, 30)"),
    )
    .await;
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE: merge-on-read via CTAS property -> position deletes, no rewrite
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn ctas_delete_mode_mor_writes_position_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "ctas_del_mor_371";
    let fq = format!("{ns}.{name}");

    seed_three_files(
        &handler,
        &session,
        &fq,
        "'write.delete.mode' = 'merge-on-read'",
    )
    .await;
    assert_eq!(live_data_file_count(&handler, &session, ns, name).await, 3);

    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE id = 1"),
    )
    .await;

    // MoR must not rewrite data files: all three stay live, the delete
    // lands as a delete file in the delete manifest.
    assert_eq!(
        live_data_file_count(&handler, &session, ns, name).await,
        3,
        "MoR DELETE on a CTAS table must NOT rewrite data files"
    );
    let (op, summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert!(
        summary_count(&summary, "added-delete-files") >= 1,
        "MoR DELETE must commit at least one delete file; \
         operation={op} summary={summary:?}"
    );
    assert_eq!(
        summary_count(&summary, "deleted-data-files"),
        0,
        "MoR DELETE must not remove data files; summary={summary:?}"
    );

    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2,
        "deleted row must be invisible through the read path"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE: property absent -> documented default is copy-on-write
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn ctas_delete_mode_default_is_cow() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "ctas_del_cow_371";
    let fq = format!("{ns}.{name}");

    seed_three_files(&handler, &session, &fq, "").await;
    assert_eq!(live_data_file_count(&handler, &session, ns, name).await, 3);

    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE id = 1"),
    )
    .await;

    // CoW rewrites: the single-row file holding id=1 disappears and no
    // replacement is written (nothing survives the predicate in it).
    assert_eq!(
        live_data_file_count(&handler, &session, ns, name).await,
        2,
        "default (CoW) DELETE must rewrite the affected data file away"
    );
    let (op, summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert_eq!(
        summary_count(&summary, "added-delete-files"),
        0,
        "CoW DELETE must not write delete files; operation={op} summary={summary:?}"
    );

    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE: merge-on-read via CTAS property. CTAS tables cannot declare
// identifier-field-ids (SQE DDL has no PRIMARY KEY / IDENTIFIER FIELDS
// syntax), so the dispatcher's documented fallback to CoW must kick in:
// correct results, no delete files.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn ctas_update_mode_mor_without_pk_falls_back_to_cow() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "ctas_upd_mor_371";
    let fq = format!("{ns}.{name}");

    seed_three_files(
        &handler,
        &session,
        &fq,
        "'write.update.mode' = 'merge-on-read'",
    )
    .await;

    exec(
        &handler,
        &session,
        &format!("UPDATE {fq} SET v = 99 WHERE id = 2"),
    )
    .await;

    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 2")
        )
        .await,
        99,
        "UPDATE result must be visible regardless of mode"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3
    );

    // No PK -> the MoR request must degrade to CoW: a rewrite commit
    // with zero delete files.
    let (op, summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert_eq!(
        summary_count(&summary, "added-delete-files"),
        0,
        "MoR UPDATE without identifier-field-ids must fall back to CoW \
         (no delete files); operation={op} summary={summary:?}"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// ─────────────────────────────────────────────────────────────────────────
// MERGE: merge-on-read via CTAS property, same no-PK fallback contract.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn ctas_merge_mode_mor_without_pk_falls_back_to_cow() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "ctas_merge_mor_371";
    let fq = format!("{ns}.{name}");

    seed_three_files(
        &handler,
        &session,
        &fq,
        "'write.merge.mode' = 'merge-on-read'",
    )
    .await;

    exec(
        &handler,
        &session,
        &format!(
            "MERGE INTO {fq} t \
             USING (SELECT 2 AS id, 200 AS v UNION ALL SELECT 4 AS id, 40 AS v) src \
             ON t.id = src.id \
             WHEN MATCHED THEN UPDATE SET v = src.v \
             WHEN NOT MATCHED THEN INSERT (id, v) VALUES (src.id, src.v)"
        ),
    )
    .await;

    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 2")
        )
        .await,
        200,
        "MATCHED UPDATE branch must surface the new value"
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 4")
        )
        .await,
        40,
        "NOT MATCHED INSERT branch must surface the new row"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        4
    );

    let (op, summary) = latest_snapshot(&handler, &session, ns, name).await;
    assert_eq!(
        summary_count(&summary, "added-delete-files"),
        0,
        "MoR MERGE without identifier-field-ids must fall back to CoW \
         (no delete files); operation={op} summary={summary:?}"
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// ─────────────────────────────────────────────────────────────────────────
// Persistence: all three mode properties set on a CTAS survive to table
// metadata and surface through SHOW CREATE TABLE (the !557 fix itself).
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn ctas_write_mode_properties_survive_to_metadata() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.ctas_props_371";

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!(
            "CREATE TABLE {fq} TBLPROPERTIES ( \
                 'write.delete.mode' = 'merge-on-read', \
                 'write.update.mode' = 'merge-on-read', \
                 'write.merge.mode'  = 'merge-on-read' \
             ) AS SELECT 1 AS id, 10 AS v"
        ),
    )
    .await;

    let batches = exec(&handler, &session, &format!("SHOW CREATE TABLE {fq}")).await;
    let ddl = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("ddl");
            (0..col.len())
                .map(|i| col.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    for key in ["write.delete.mode", "write.update.mode", "write.merge.mode"] {
        assert!(
            ddl.contains(key),
            "SHOW CREATE TABLE lost the {key} property set via CTAS: {ddl}"
        );
    }

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// ─────────────────────────────────────────────────────────────────────────
// Round trip over the upstream #179 path: a CoW rewrite on a table that
// already carries delete manifests (from a prior MoR DELETE) must fold
// the deletes into the rewrite instead of resurrecting or dropping rows.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn mor_delete_then_cow_update_round_trip() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "ctas_179_roundtrip_371";
    let fq = format!("{ns}.{name}");

    // delete.mode=MoR only; UPDATE stays on the default CoW path.
    seed_three_files(
        &handler,
        &session,
        &fq,
        "'write.delete.mode' = 'merge-on-read'",
    )
    .await;

    // 1. MoR DELETE id=1 -> delete manifest, 3 live data files.
    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE id = 1"),
    )
    .await;
    assert_eq!(live_data_file_count(&handler, &session, ns, name).await, 3);

    // 2. CoW UPDATE id=2 -> rewrite while a delete manifest is live.
    exec(
        &handler,
        &session,
        &format!("UPDATE {fq} SET v = 222 WHERE id = 2"),
    )
    .await;

    // 3. Full read-back: id=1 stays deleted, id=2 updated, id=3 intact.
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        2,
        "CoW rewrite over a live delete manifest must not resurrect the deleted row"
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 2")
        )
        .await,
        222
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 3")
        )
        .await,
        30
    );

    // 4. Second MoR DELETE keeps composing.
    exec(
        &handler,
        &session,
        &format!("DELETE FROM {fq} WHERE id = 3"),
    )
    .await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        1
    );
    assert_eq!(
        scalar_i64(
            &handler,
            &session,
            &format!("SELECT v FROM {fq} WHERE id = 2")
        )
        .await,
        222
    );

    let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}
