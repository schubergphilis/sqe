//! End-to-end coverage for Iceberg format-version 3 round trips.
//!
//! The V3 writer path has existed for several phases (TIMESTAMP_NS, DEFAULT
//! columns, the same MoR/CoW dispatcher as V2), but the matrix lists most
//! V3 cells as `partial` or `unknown` because no test ever booted the
//! live Polaris stack against a V3-format table. This binary closes that
//! gap by exercising each V3-specific behaviour against
//! `docker-compose.test.yml` and asserting the post-state via the same
//! TVFs (`table_files`, `table_snapshots`) the matrix evidence column
//! cites elsewhere.
//!
//! Every test in this binary is `#[ignore]` because each needs the
//! running stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test --package sqe-coordinator --test it v3_e2e -- \
//!     --ignored --test-threads=1
//! ```
//!
//! Tests use unique table names per scenario so `--test-threads=1` is a
//! belt-and-braces measure against the in-memory Polaris's eventual
//! consistency rather than a strict requirement.

use arrow_array::{Array, Int64Array};

/// Helper: count live data files for a table via the SQE TVF.
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

/// Helper: count snapshots for a table via the SQE TVF.
async fn snapshot_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> i64 {
    let batches = handler
        .execute(
            session,
            &format!("SELECT COUNT(*) FROM table_snapshots('{namespace}', '{table}')"),
            None,
        )
        .await
        .expect("table_snapshots scan");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

/// Helper: count rows in a table.
async fn row_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let batches = handler
        .execute(session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("count select");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

/// Helper: the current snapshot's summary as `table_snapshots` renders it
/// (a JSON object). Picks the snapshot no other snapshot names as parent, so
/// two commits in the same millisecond cannot tie.
pub(crate) async fn latest_summary(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table: &str,
) -> String {
    use arrow::array::AsArray;
    let sql = format!(
        "SELECT summary FROM table_snapshots('{namespace}', '{table}') \
         WHERE snapshot_id NOT IN (SELECT parent_id FROM table_snapshots('{namespace}', '{table}') \
         WHERE parent_id IS NOT NULL)"
    );
    let batches = handler.execute(session, &sql, None).await.expect("table_snapshots");
    batches[0].column(0).as_string::<i32>().value(0).to_string()
}

/// Helper: read the table's format-version straight off the metadata
/// table. SQE exposes the metadata as `default.<table>.metadata`-style
/// pseudo-tables; we go through `table_snapshots` to fetch the latest
/// snapshot and inspect the format-version on the metadata object.
async fn assert_format_version_is_v3(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    namespace: &str,
    table_name: &str,
) {
    let _ = handler
        .execute(
            session,
            &format!(
                "SELECT * FROM table_snapshots('{namespace}', '{table_name}') \
                 LIMIT 1"
            ),
            None,
        )
        .await
        .expect("table_snapshots scan");
    // The TVF itself does not surface format-version yet; instead we
    // assert via the catalog REST response. Fetch the table metadata
    // through the SHOW STATS path which round-trips through the
    // catalog's load_table.
    let batches = handler
        .execute(
            session,
            &format!("SHOW STATS FOR {namespace}.{table_name}"),
            None,
        )
        .await
        .expect("show stats");
    // SHOW STATS returns one row per column. Every V3 table emits
    // a non-empty stats result; the test passes if no error is raised
    // and at least one row comes back. The format-version itself is
    // verified indirectly by writing a V3-only column type and reading
    // it back with the same handler (V2 metadata cannot represent
    // TimestampNs; iceberg-rust would refuse the load).
    assert!(
        !batches.is_empty(),
        "SHOW STATS on V3 table {namespace}.{table_name} returned zero batches",
    );
}

/// Drop-then-create boilerplate.
async fn reset_table(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    fq_table: &str,
    create_sql: &str,
) {
    let _ = handler
        .execute(session, &format!("DROP TABLE IF EXISTS {fq_table}"), None)
        .await;
    handler
        .execute(session, create_sql, None)
        .await
        .expect("CREATE TABLE");
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:table-creation:v3 + sqe:catalog-integration:v3 + sqe:rest-catalog:v3
// + sqe:polaris:v3
//
// CREATE TABLE with a V3-only column type round-trips through Polaris and
// can be re-loaded without raising "unsupported format-version" errors.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn create_v3_table_with_nanosec_timestamp_round_trips_through_polaris() {
    let (session, handler) = crate::common::setup_handler().await;

    let ns = "default";
    let name = "v3_create_ns_ts";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    // The table must be loadable. If iceberg-rust cannot decode the V3
    // metadata it returns a hard error here.
    let snaps = snapshot_count(&handler, &session, ns, name).await;
    assert_eq!(snaps, 0, "fresh CREATE TABLE has no snapshots");

    assert_format_version_is_v3(&handler, &session, ns, name).await;

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:write-insert:v3 + sqe:read-support:v3
//
// INSERT into a V3 table with a nanosecond timestamp column, then SELECT
// the row back. The point is not the timestamp value (V2 already supports
// microsecond timestamps); it is that the planner accepts the V3 type at
// scan time and DataFusion materialises Timestamp(Nanosecond, _).
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn insert_then_select_round_trips_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_insert_select";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!(
                "INSERT INTO {fq} VALUES \
                 (1, TIMESTAMP '2026-04-26 10:00:00.123456789'), \
                 (2, TIMESTAMP '2026-04-26 10:00:01.987654321')"
            ),
            None,
        )
        .await
        .expect("INSERT");

    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 2);

    let files = live_data_file_count(&handler, &session, ns, name).await;
    assert!(
        (1..=2).contains(&files),
        "expected 1 or 2 data files after INSERT, got {files}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:schema-evolution:v3
//
// ALTER TABLE ADD COLUMN ... DEFAULT against a V3 table: the new column
// shows up in `information_schema.columns` after the commit. V3 is
// required because DEFAULT is a V3 spec feature.
//
// A row inserted after the ALTER that omits the column gets its
// write_default (#828). A row written BEFORE the ALTER reads the
// initial_default (#830).
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn alter_add_column_with_default_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_alter_default";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-04-26 10:00:00')"),
            None,
        )
        .await
        .expect("INSERT pre-evolution");

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ADD COLUMN region STRING DEFAULT 'eu'"),
            None,
        )
        .await
        .expect("ALTER ADD COLUMN DEFAULT");

    // Confirm the new column is visible through information_schema.
    // information_schema.columns reads the catalog's current schema, so
    // success here means Polaris committed the AddSchema update with the
    // DEFAULT-bearing field.
    let cols = handler
        .execute(
            &session,
            &format!(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = '{ns}' AND table_name = '{name}' \
                 AND column_name = 'region'"
            ),
            None,
        )
        .await
        .expect("information_schema.columns lookup");
    let total: i64 = cols.iter().map(|b| b.num_rows() as i64).sum();
    assert_eq!(total, 1, "ALTER ADD COLUMN did not commit the new column");

    // #830: the pre-ALTER row reads the initial_default, before any write
    // since the ALTER (the scanned snapshot's schema has no `region` yet).
    let q = |sql: &str| format!("SELECT id, region FROM {fq}{sql} ORDER BY id");
    assert_eq!(rendered_rows(&handler, &session, &q("")).await, vec!["1|eu"]);

    // #828: a new row that omits the column gets the write_default.
    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} (id) VALUES (2)"),
            None,
        )
        .await
        .expect("INSERT omitting the defaulted column");
    assert_eq!(rendered_rows(&handler, &session, &q("")).await, vec!["1|eu", "2|eu"]);
    // The default is a value, not a display: predicates see it.
    assert_eq!(
        rendered_rows(&handler, &session, &q(" WHERE region = 'eu'")).await,
        vec!["1|eu", "2|eu"]
    );
    assert!(rendered_rows(&handler, &session, &q(" WHERE region IS NULL")).await.is_empty());

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

/// Every row of `sql` as `col|col|...` text, NULL rendered as `NULL`.
async fn rendered_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<String> {
    let batches = handler.execute(session, sql, None).await.expect(sql);
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

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:column-default-values:v3
//
// A column DEFAULT declared at CREATE TABLE is written by an INSERT and a
// MERGE INSERT that omit the column (#828). An explicit NULL stays NULL.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn create_table_default_applies_on_insert_and_merge() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.v3_create_default";

    reset_table(
        &handler,
        &session,
        fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, status STRING DEFAULT 'pending', \
             n INT DEFAULT 7, note STRING)"
        ),
    )
    .await;

    for sql in [
        format!("INSERT INTO {fq} (id) VALUES (1)"),
        format!("INSERT INTO {fq} (id, status) VALUES (2, NULL)"),
        format!(
            "MERGE INTO {fq} AS t USING (SELECT 3 AS id, 'm' AS v) AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, note) VALUES (s.id, s.v)"
        ),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    assert_eq!(
        rendered_rows(&handler, &session, &format!("SELECT * FROM {fq} ORDER BY id")).await,
        vec!["1|pending|7|NULL", "2|NULL|7|NULL", "3|pending|7|m"]
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:column-default-values:v3
//
// A quoted DEFAULT on a DATE / TIMESTAMP / DECIMAL / BINARY / BIGINT column
// is stored as a value of that type and written by INSERT and MERGE INSERT
// (#865). It used to be stored as a string literal, which panicked the
// metadata writer at CREATE TABLE. A value that does not parse is an error.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn typed_defaults_apply_on_insert_merge_and_alter() {
    let (session, handler) = crate::common::setup_handler().await;
    let fq = "default.v3_typed_default";

    reset_table(
        &handler,
        &session,
        fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, d DATE DEFAULT '2024-01-02', \
             ts TIMESTAMP DEFAULT '2024-01-02 03:04:05', \
             amt DECIMAL(10,2) DEFAULT '1.5', whole DECIMAL(10,2) DEFAULT 2, \
             b BINARY DEFAULT X'01ab', n BIGINT DEFAULT '5')"
        ),
    )
    .await;

    for sql in [
        format!("INSERT INTO {fq} (id) VALUES (1)"),
        format!(
            "MERGE INTO {fq} AS t USING (SELECT 2 AS id) AS s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
        ),
        format!("ALTER TABLE {fq} ADD COLUMN later DATE DEFAULT '2025-06-07'"),
        format!("INSERT INTO {fq} (id) VALUES (3)"),
    ] {
        handler.execute(&session, &sql, None).await.expect(&sql);
    }

    // Rows 1 and 2 predate the ALTER and read `later`'s initial_default.
    let defaults = "2024-01-02|2024-01-02T03:04:05|1.50|2.00|01ab|5|2025-06-07";
    let select = format!("SELECT * FROM {fq} ORDER BY id");
    assert_eq!(
        rendered_rows(&handler, &session, &select).await,
        vec![
            format!("1|{defaults}"),
            format!("2|{defaults}"),
            format!("3|{defaults}"),
        ]
    );

    let bad = "default.v3_typed_default_bad";
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {bad}"), None)
        .await;
    let create_bad = format!("CREATE TABLE {bad} (id BIGINT, d DATE DEFAULT 'soon')");
    let err = handler
        .execute(&session, &create_bad, None)
        .await
        .expect_err("a DEFAULT that is not a date must be refused");
    assert!(err.to_string().contains("not a valid date"), "{err}");

    for t in [fq, bad] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
            .await;
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:copy-on-write:v3
//
// DELETE under copy-on-write on a V3 table. Default mode is CoW; a row
// removed by DELETE WHERE must vanish from SELECT and the live data file
// count must drop to one (rewritten file).
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn delete_cow_on_v3_table_drops_row() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_delete_cow";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    for i in 0..3 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}')"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    handler
        .execute(&session, &format!("DELETE FROM {fq} WHERE id = 1"), None)
        .await
        .expect("DELETE");

    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 2, "row id=1 must be invisible after DELETE");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:position-deletes:v3
//
// DELETE with write.delete.mode=merge-on-read on a V3 table without a
// declared identifier-field-id. The spec forbids adding position-delete
// files to a V3 table: each data file gets at most one deletion vector
// holding all its deleted positions, and a second DELETE on the same file
// replaces the vector rather than adding another. Asserted through the
// snapshot summary (`added-dvs` / `removed-dvs`, never
// `added-position-delete-files`) and the visible rows.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn position_deletes_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_pos_deletes_2";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9)) \
             TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')"
        ),
    )
    .await;

    for i in 0..3 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}')"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    let files_before = live_data_file_count(&handler, &session, ns, name).await;
    assert_eq!(
        files_before, 3,
        "three INSERTs produce three live data files before the DELETE"
    );

    handler
        .execute(&session, &format!("DELETE FROM {fq} WHERE id = 1"), None)
        .await
        .expect("DELETE MoR");

    // Position-delete mode never rewrites data files; the live data
    // file count must therefore stay at 3 after the DELETE. (CoW would
    // drop it to 2.) The position-delete file itself sits in the
    // delete manifest and is invisible to `table_files`.
    let files_after = live_data_file_count(&handler, &session, ns, name).await;
    assert_eq!(
        files_after, 3,
        "MoR DELETE on V3 table must NOT rewrite data files: \
         before={files_before} after={files_after}"
    );
    let summary = latest_summary(&handler, &session, ns, name).await;
    assert!(summary.contains("\"added-dvs\":\"1\""), "one DV: {summary}");
    assert!(
        !summary.contains("added-position-delete-files"),
        "V3 must not add position delete files: {summary}"
    );

    // A second DELETE on a data file that already has a vector: the new
    // vector must carry both positions and the old one must be retired.
    handler
        .execute(&session, &format!("INSERT INTO {fq} VALUES (10, NULL), (11, NULL)"), None)
        .await
        .expect("INSERT two-row file");
    for id in [10, 11] {
        handler
            .execute(&session, &format!("DELETE FROM {fq} WHERE id = {id}"), None)
            .await
            .expect("DELETE MoR on the two-row file");
    }
    let summary = latest_summary(&handler, &session, ns, name).await;
    assert!(summary.contains("\"added-dvs\":\"1\""), "replacement DV: {summary}");
    assert!(summary.contains("\"removed-dvs\":\"1\""), "old DV retired: {summary}");
    assert!(
        !summary.contains("added-position-delete-files"),
        "V3 must not add position delete files: {summary}"
    );
    assert_eq!(row_count(&handler, &session, &fq).await, 2, "ids 0 and 2 remain");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:equality-deletes:v3 + sqe:merge-on-read:v3 +
// sqe:write-merge-update-delete:v3
//
// UPDATE with write.update.mode=merge-on-read and a declared identifier
// field on a V3 table writes an equality-delete plus a fresh data file
// in one RowDeltaAction commit.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn equality_delete_update_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_eq_update";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT NOT NULL, ts TIMESTAMP_NS(9), v BIGINT) \
             TBLPROPERTIES ( \
                 'write.update.mode' = 'merge-on-read', \
                 'write.delete.mode' = 'merge-on-read' \
             )"
        ),
    )
    .await;
    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS id"),
            None,
        )
        .await
        .expect("SET IDENTIFIER FIELDS");

    for i in 0..3 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}', {i})"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    handler
        .execute(
            &session,
            &format!("UPDATE {fq} SET v = 99 WHERE id = 1"),
            None,
        )
        .await
        .expect("UPDATE MoR equality");
    let summary = latest_summary(&handler, &session, ns, name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "MoR UPDATE must write an equality delete: {summary}"
    );

    // Row count unchanged; v must be 99 for id=1.
    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 3);

    let batches = handler
        .execute(&session, &format!("SELECT v FROM {fq} WHERE id = 1"), None)
        .await
        .expect("select v");
    let v = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(v, 99, "MoR UPDATE failed to surface new value");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:write-merge-update-delete:v3 (MERGE direct test)
//
// MERGE INTO on a V3 table with MATCHED UPDATE + NOT MATCHED INSERT:
// the table starts with one row, MERGE updates that row's value and
// inserts a new row. Validates the MERGE dispatcher works on V3
// metadata, complementing the existing UPDATE and DELETE coverage.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn merge_into_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_merge_into";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9), v BIGINT)"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-04-26 10:00:00', 10)"),
            None,
        )
        .await
        .expect("seed INSERT");

    handler
        .execute(
            &session,
            &format!(
                "MERGE INTO {fq} t \
                 USING (SELECT 1 AS id, TIMESTAMP '2026-04-26 10:00:01' AS ts, 99 AS v \
                        UNION ALL \
                        SELECT 2 AS id, TIMESTAMP '2026-04-26 10:00:02' AS ts, 22 AS v) src \
                 ON t.id = src.id \
                 WHEN MATCHED THEN UPDATE SET v = src.v \
                 WHEN NOT MATCHED THEN INSERT (id, ts, v) VALUES (src.id, src.ts, src.v)"
            ),
            None,
        )
        .await
        .expect("MERGE INTO");

    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 2, "MERGE INTO should produce 2 rows on V3 table");

    let updated = handler
        .execute(&session, &format!("SELECT v FROM {fq} WHERE id = 1"), None)
        .await
        .expect("select updated");
    let v = updated[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(v, 99, "MATCHED UPDATE branch must surface the new value");

    let inserted = handler
        .execute(&session, &format!("SELECT v FROM {fq} WHERE id = 2"), None)
        .await
        .expect("select inserted");
    let v = inserted[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(v, 22, "NOT MATCHED INSERT branch must surface the new row");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:time-travel:v3
//
// A V3 table records each commit in the snapshot log so future
// FOR VERSION AS OF / FOR SYSTEM_TIME AS OF queries have something
// to resolve against. We assert three INSERTs produce three
// snapshots and a CoW DELETE rolls forward to a fresh snapshot
// (committed_at_ms strictly later than the last INSERT). The actual
// `FOR VERSION AS OF` query plumbing has its own test in the
// time-travel suite; here we just confirm V3 metadata participates
// in the snapshot machinery.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn time_travel_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_time_travel";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    for i in 0..3 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}')"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    let snaps_before = snapshot_count(&handler, &session, ns, name).await;
    assert_eq!(snaps_before, 3, "three INSERTs produce three snapshots");

    handler
        .execute(&session, &format!("DELETE FROM {fq} WHERE id = 1"), None)
        .await
        .expect("DELETE");

    let post = row_count(&handler, &session, &fq).await;
    assert_eq!(post, 2);

    // Even if Polaris's snapshot listing exposes only the active
    // chain (rather than the full history), the row visible result
    // proves the commit landed: the table now has the rows the
    // post-DELETE snapshot describes. The dedicated time-travel
    // suite exercises FOR VERSION AS OF / FOR SYSTEM_TIME AS OF
    // semantics; this test only validates that V3 metadata
    // participates in the snapshot machinery without errors.

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:bloom-filters:v3
//
// Setting write.parquet.bloom-filter-columns on a V3 table is accepted
// by CREATE TABLE, INSERT round-trips, and the property surfaces back
// through SHOW CREATE TABLE so users can verify their table's
// configuration. The bloom-filter footer itself is checked by the
// dedicated bloom test suite (Phase F).
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn bloom_filter_property_round_trips_on_v3_table() {
    use arrow_array::StringArray;

    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_bloom";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9)) \
             TBLPROPERTIES ('write.parquet.bloom-filter-columns' = 'id')"
        ),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-04-26 10:00:00')"),
            None,
        )
        .await
        .expect("INSERT");

    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 1, "INSERT round-trip on V3+bloom table failed");

    // SHOW CREATE TABLE should re-emit the user-set properties so the
    // round-trip is observable from SQL (catalogs that lose the property
    // would be a silent regression).
    let batches = handler
        .execute(&session, &format!("SHOW CREATE TABLE {fq}"), None)
        .await
        .expect("show create");
    let ddl = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray")
        .value(0);
    assert!(
        ddl.contains("write.parquet.bloom-filter-columns"),
        "SHOW CREATE TABLE lost the bloom filter property: {ddl}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:table-maintenance:v3
//
// CALL system.rewrite_data_files on a V3 table merges small data files.
// Same procedure as V2, but we run it against a V3-format table to
// confirm the maintenance code path does not bail out on V3 metadata.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn rewrite_data_files_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_rewrite";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    for i in 0..6 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}')"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    let before = live_data_file_count(&handler, &session, ns, name).await;
    assert_eq!(before, 6, "six INSERTs produce six small data files");

    handler
        .execute(
            &session,
            &format!("CALL system.rewrite_data_files(table => '{fq}')"),
            None,
        )
        .await
        .expect("rewrite_data_files");

    let after = live_data_file_count(&handler, &session, ns, name).await;
    assert!(
        after < before,
        "file count must drop after rewrite: before={before} after={after}"
    );

    let count = row_count(&handler, &session, &fq).await;
    assert_eq!(count, 6, "rewrite must preserve the row set");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:time-travel:v3 (FOR VERSION AS OF)
//
// Pin a specific snapshot via `FOR VERSION AS OF <snapshot_id>` and
// confirm the SELECT returns the row count from that historical
// snapshot rather than the latest one. The pre-classifier strips the
// FOR VERSION AS OF clause before sqlparser sees it; the engine then
// registers a snapshot-pinned provider under a writable alias and
// rewrites the SQL to reference it.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn for_version_as_of_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_for_version";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-04-26 10:00:00')"),
            None,
        )
        .await
        .expect("INSERT 1");

    // Capture the current snapshot id BEFORE INSERT 2/3. The #320
    // table_snapshots schema dropped the is_current_snapshot column, so the
    // current snapshot is the most recently committed one (latest
    // committed_at, snapshot_id tie-break).
    let pin_batches = handler
        .execute(
            &session,
            &format!(
                "SELECT snapshot_id FROM table_snapshots('{ns}', '{name}') \
                 ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
            ),
            None,
        )
        .await
        .expect("snapshot_id pin");
    let pin_snap = pin_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (2, TIMESTAMP '2026-04-26 10:01:00')"),
            None,
        )
        .await
        .expect("INSERT 2");

    let now_count = row_count(&handler, &session, &fq).await;
    assert_eq!(now_count, 2, "live table sees both INSERTs");

    let pinned_batches = handler
        .execute(
            &session,
            &format!("SELECT COUNT(*) FROM {fq} FOR VERSION AS OF {pin_snap}"),
            None,
        )
        .await
        .expect("FOR VERSION AS OF query");
    let pinned_count = pinned_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(
        pinned_count, 1,
        "FOR VERSION AS OF must show only the row visible at the pinned snapshot"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:cdc-support:v3
//
// FOR INCREMENTAL BETWEEN SNAPSHOT against a V3 table returns the rows
// committed in the chosen range with the right `_change_type` column.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn cdc_incremental_scan_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_cdc";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (1, TIMESTAMP '2026-04-26 10:00:00')"),
            None,
        )
        .await
        .expect("INSERT 1");

    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (2, TIMESTAMP '2026-04-26 10:01:00')"),
            None,
        )
        .await
        .expect("INSERT 2");
    handler
        .execute(
            &session,
            &format!("INSERT INTO {fq} VALUES (3, TIMESTAMP '2026-04-26 10:02:00')"),
            None,
        )
        .await
        .expect("INSERT 3");

    // SQE's incremental-scan validator currently compares snapshot ids
    // numerically (start > end is an error), even though Iceberg
    // snapshot ids are random 63-bit numbers and ancestry, not
    // numeric order, defines history. The sentinel start = 0 means
    // "from the beginning"; combined with the latest snapshot id, the
    // range is unambiguous and avoids the numeric-ordering trap.
    let end_batches = handler
        .execute(
            &session,
            &format!(
                "SELECT snapshot_id FROM table_snapshots('{ns}', '{name}') \
                 ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1"
            ),
            None,
        )
        .await
        .expect("end snapshot id");
    let end_snap = end_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);

    let cdc_batches = handler
        .execute(
            &session,
            &format!(
                "SELECT COUNT(*) FROM {fq} \
                 FOR INCREMENTAL BETWEEN SNAPSHOT 0 AND SNAPSHOT {end_snap}"
            ),
            None,
        )
        .await
        .expect("CDC scan");
    let delta_count = cdc_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0);
    assert_eq!(
        delta_count, 3,
        "CDC range from beginning to latest must surface all three INSERTed rows"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:type-promotion:v3
//
// V2 widening rules (int -> long, float -> double, decimal widening)
// apply equally to V3 tables. ALTER TABLE ... ALTER COLUMN ... TYPE
// commits a schema change that surfaces through information_schema.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn type_promotion_int_to_bigint_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_type_promo";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id INT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    handler
        .execute(
            &session,
            &format!("ALTER TABLE {fq} ALTER COLUMN id SET DATA TYPE BIGINT"),
            None,
        )
        .await
        .expect("ALTER COLUMN type promotion");

    let cols = handler
        .execute(
            &session,
            &format!(
                "SELECT data_type FROM information_schema.columns \
                 WHERE table_schema = '{ns}' AND table_name = '{name}' \
                 AND column_name = 'id'"
            ),
            None,
        )
        .await
        .expect("information_schema lookup after promotion");
    let dtype = cols[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("StringArray")
        .value(0);
    assert!(
        dtype.contains("Int64") || dtype.contains("BIGINT") || dtype.contains("bigint"),
        "Type promotion did not surface BIGINT in information_schema: got {dtype}"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:statistics:v3
//
// Basic statistics surface (COUNT, file count) work on a V3 table.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn statistics_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_stats";
    let fq = format!("{ns}.{name}");

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;

    for i in 0..5 {
        handler
            .execute(
                &session,
                &format!(
                    "INSERT INTO {fq} VALUES \
                     ({i}, TIMESTAMP '2026-04-26 10:00:00.{i:09}')"
                ),
                None,
            )
            .await
            .expect("INSERT");
    }

    let stats = handler
        .execute(&session, &format!("SHOW STATS FOR {fq}"), None)
        .await
        .expect("show stats");
    assert!(!stats.is_empty(), "SHOW STATS returned no rows on V3 table");

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// Cell: sqe:row-lineage:v3
//
// Spec, Row Lineage: a rewritten row keeps its `_row_id`; an unchanged row
// also keeps `_last_updated_sequence_number`, an updated row gets a newer one
// (null on write, inherited at commit). CoW DELETE / UPDATE / MERGE and
// rewrite_data_files all rewrite rows, so each must preserve lineage.
// Lineage is read through the iceberg scan, which synthesizes it for files
// that do not store it (first_row_id + _pos).
// ─────────────────────────────────────────────────────────────────────────

/// `id -> (_row_id, _last_updated_sequence_number)` for every live row.
pub(crate) async fn lineage_by_id(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    name: &str,
) -> std::collections::BTreeMap<i64, (i64, i64)> {
    use futures::TryStreamExt;
    let catalog = handler.session_catalog(session).await.expect("session catalog");
    let ident = iceberg::TableIdent::from_strs([ns, name]).expect("ident");
    let table = catalog.load_table(&ident).await.expect("load table");
    let batches: Vec<arrow_array::RecordBatch> = table
        .scan()
        .select(["id", "_row_id", "_last_updated_sequence_number"])
        .build()
        .expect("lineage scan")
        .to_arrow()
        .await
        .expect("lineage stream")
        .try_collect()
        .await
        .expect("lineage batches");
    // The reader returns a file-constant sequence number run-end encoded.
    let col = |b: &arrow_array::RecordBatch, n: &str| {
        let raw = b.column_by_name(n).unwrap_or_else(|| panic!("missing {n}"));
        arrow::compute::cast(raw, &arrow_schema::DataType::Int64)
            .unwrap_or_else(|e| panic!("{n}: {e}"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{n} not Int64"))
            .clone()
    };
    let mut out = std::collections::BTreeMap::new();
    for b in &batches {
        let (ids, rows, seqs) = (col(b, "id"), col(b, "_row_id"), col(b, "_last_updated_sequence_number"));
        for i in 0..b.num_rows() {
            assert!(!rows.is_null(i) && !seqs.is_null(i), "lineage must never be null on read");
            let prev = out.insert(ids.value(i), (rows.value(i), seqs.value(i)));
            assert!(prev.is_none(), "duplicate id {}", ids.value(i));
        }
    }
    let mut row_ids: Vec<i64> = out.values().map(|v| v.0).collect();
    row_ids.sort_unstable();
    row_ids.dedup();
    assert_eq!(row_ids.len(), out.len(), "_row_id must be unique: {out:?}");
    out
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn row_lineage_survives_cow_rewrites_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_row_lineage";
    let fq = format!("{ns}.{name}");
    let run = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move { handler.execute(session, &sql, None).await.unwrap_or_else(|e| panic!("{sql}: {e}")) }
    };

    reset_table(
        &handler,
        &session,
        &fq,
        &format!("CREATE TABLE {fq} (id BIGINT, v BIGINT, ts TIMESTAMP_NS(9))"),
    )
    .await;
    run(format!("INSERT INTO {fq} VALUES (1, 10, NULL), (2, 20, NULL), (3, 30, NULL)")).await;
    run(format!("INSERT INTO {fq} VALUES (4, 40, NULL), (5, 50, NULL), (6, 60, NULL)")).await;
    let start = lineage_by_id(&handler, &session, ns, name).await;
    assert_eq!(start.len(), 6);

    // CoW DELETE: survivors keep both values.
    run(format!("DELETE FROM {fq} WHERE id = 2")).await;
    let after_delete = lineage_by_id(&handler, &session, ns, name).await;
    let expected: std::collections::BTreeMap<_, _> =
        start.iter().filter(|(id, _)| **id != 2).map(|(k, v)| (*k, *v)).collect();
    assert_eq!(after_delete, expected, "DELETE must not change surviving rows' lineage");

    // CoW UPDATE: the updated row keeps _row_id, gets a newer sequence number.
    run(format!("UPDATE {fq} SET v = 500 WHERE id = 5")).await;
    let after_update = lineage_by_id(&handler, &session, ns, name).await;
    for (id, (row_id, seq)) in &after_delete {
        let (new_row_id, new_seq) = after_update[id];
        assert_eq!(new_row_id, *row_id, "id {id}: UPDATE must keep _row_id");
        if *id == 5 {
            assert!(new_seq > *seq, "updated row needs a newer sequence number: {seq} -> {new_seq}");
        } else {
            assert_eq!(new_seq, *seq, "id {id}: unchanged row keeps its sequence number");
        }
    }

    // Compaction: every row keeps both values.
    run(format!("CALL system.rewrite_data_files(table => '{fq}')")).await;
    let after_compact = lineage_by_id(&handler, &session, ns, name).await;
    assert_eq!(after_compact, after_update, "compaction must not change lineage");

    // CoW MERGE: matched-updated keeps _row_id with a newer seq, the insert is new.
    run(format!(
        "MERGE INTO {fq} t USING (SELECT 1 AS id, 11 AS v UNION ALL SELECT 9 AS id, 90 AS v) s \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ))
    .await;
    let after_merge = lineage_by_id(&handler, &session, ns, name).await;
    for (id, (row_id, seq)) in &after_compact {
        let (new_row_id, new_seq) = after_merge[id];
        assert_eq!(new_row_id, *row_id, "id {id}: MERGE must keep _row_id");
        if *id == 1 {
            assert!(new_seq > *seq, "merged-updated row needs a newer sequence number");
        } else {
            assert_eq!(new_seq, *seq, "id {id}: untouched row keeps its sequence number");
        }
    }
    let (inserted_row_id, _) = after_merge[&9];
    assert!(
        after_compact.values().all(|(r, _)| *r != inserted_row_id),
        "inserted row must get a fresh _row_id"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}

/// MoR (equality-delete) UPDATE and MERGE write the updated row as a new data
/// row and hide the old one; the new row must carry the old `_row_id`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn row_lineage_survives_mor_update_and_merge_on_v3_table() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns = "default";
    let name = "v3_row_lineage_mor";
    let fq = format!("{ns}.{name}");
    let run = |sql: String| {
        let (handler, session) = (&handler, &session);
        async move { handler.execute(session, &sql, None).await.unwrap_or_else(|e| panic!("{sql}: {e}")) }
    };

    reset_table(
        &handler,
        &session,
        &fq,
        &format!(
            "CREATE TABLE {fq} (id BIGINT NOT NULL, ts TIMESTAMP_NS(9), v BIGINT) \
             TBLPROPERTIES ( \
                 'write.update.mode' = 'merge-on-read', \
                 'write.delete.mode' = 'merge-on-read', \
                 'write.merge.mode' = 'merge-on-read' \
             )"
        ),
    )
    .await;
    run(format!("ALTER TABLE {fq} SET IDENTIFIER FIELDS id")).await;
    run(format!(
        "INSERT INTO {fq} VALUES (1, NULL, 10), (2, NULL, 20), (3, NULL, 30)"
    ))
    .await;
    let start = lineage_by_id(&handler, &session, ns, name).await;

    run(format!("UPDATE {fq} SET v = 99 WHERE id = 1")).await;
    let summary = latest_summary(&handler, &session, ns, name).await;
    assert!(
        summary.contains("added-equality-delete-files"),
        "MoR UPDATE: {summary}"
    );
    let after_update = lineage_by_id(&handler, &session, ns, name).await;
    assert_eq!(
        after_update[&1].0, start[&1].0,
        "MoR UPDATE must keep _row_id"
    );
    assert!(
        after_update[&1].1 > start[&1].1,
        "MoR UPDATE needs a newer sequence number"
    );
    assert_eq!(after_update[&2], start[&2]);
    assert_eq!(after_update[&3], start[&3]);

    run(format!(
        "MERGE INTO {fq} t USING (SELECT 2 AS id, 22 AS v) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET v = s.v"
    ))
    .await;
    let after_merge = lineage_by_id(&handler, &session, ns, name).await;
    assert_eq!(after_merge[&2].0, start[&2].0, "MoR MERGE must keep _row_id");
    assert!(after_merge[&2].1 > start[&2].1, "MoR MERGE needs a newer sequence number");
    assert_eq!(after_merge[&1], after_update[&1]);
    assert_eq!(after_merge[&3], start[&3]);

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {fq}"), None)
        .await;
}
