//! A `CREATE OR REPLACE TABLE ... AS SELECT` that changes the columns or the
//! partitioning is one commit on the existing table (#892).
//!
//! It used to stage the replacement under a temporary `__sqe_replace_` name
//! and swap it in with DROP + RENAME. Between the two calls the table was
//! absent, so `information_schema.columns` could answer zero rows for it, and
//! the swap dropped the table's history, refs and grants. Now the new schema
//! and spec become current in the same commit as the data, so readers see the
//! old table or the new one, and everything attached to the table stays.
//!
//! The concurrency scenarios (an append or a schema change landing while the
//! replace writes) and the format-version rules are driven in process by the
//! SQLite rig in `sqe-write` (`write_handler::tests::committing`).

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};

const NS: &str = "default";

async fn exec(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<RecordBatch> {
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
        .unwrap_or_else(|| panic!("{sql}: not an Int64 column"))
        .value(0)
}

/// The column names `information_schema.columns` lists for `table`.
async fn columns(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<String> {
    let sql = format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = '{NS}' AND table_name = '{table}' ORDER BY ordinal_position"
    );
    let mut names = Vec::new();
    for b in exec(handler, session, &sql).await {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        names.extend((0..b.num_rows()).map(|r| col.value(r).to_string()));
    }
    names
}

/// `(snapshot_id, parent_id)` of every snapshot of `table`, oldest first.
async fn snapshots(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<(i64, Option<i64>)> {
    let sql = format!(
        "SELECT snapshot_id, parent_id FROM table_snapshots('{NS}', '{table}') \
         ORDER BY committed_at, snapshot_id"
    );
    let mut rows = Vec::new();
    for b in exec(handler, session, &sql).await {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let parents = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("parent");
        for r in 0..b.num_rows() {
            let parent = (!parents.is_null(r)).then(|| parents.value(r));
            rows.push((ids.value(r), parent));
        }
    }
    rows
}

/// The `b` values of `from` (a table, optionally with `FOR VERSION AS OF`),
/// read as the INT they were written as.
async fn int_b(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    from: &str,
) -> Vec<i32> {
    let sql = format!("SELECT b FROM {from} ORDER BY b");
    let mut out = Vec::new();
    for b in exec(handler, session, &sql).await {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap_or_else(|| panic!("{sql}: b is {}", b.column(0).data_type()));
        out.extend((0..b.num_rows()).map(|r| col.value(r)));
    }
    out
}

/// `session`'s token with an admin role, for the admin-only statements.
fn admin_of(session: &sqe_core::Session) -> sqe_core::Session {
    sqe_core::Session::new(
        "root".to_string(),
        session.access_token().clone(),
        None,
        chrono::Utc::now() + chrono::Duration::hours(1),
        vec!["service_admin".to_string()],
    )
}

async fn staging_tables(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_schema = '{NS}' AND strpos(table_name, '__sqe_replace_') > 0"
    );
    scalar_i64(handler, session, &sql).await
}

// ─────────────────────────────────────────────────────────────────────────
// Kept, promoted and new columns in one commit on the same table. The
// earlier snapshot stays: time travel reads it with its own schema (`b` as
// the INT it was written as, not the new string column), the branch and the
// tag still point at it, and rollback goes back to it.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ctas_replace_shape_is_one_commit_on_the_same_table_892() {
    let (session, handler) = crate::common::setup_handler().await;
    let name = "ctas_shape_892";
    let t = format!("{NS}.{name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
    let create =
        format!("CREATE TABLE {t} AS SELECT CAST(1 AS INT) AS a, CAST(10 AS INT) AS b, 'c' AS c");
    exec(&handler, &session, &create).await;
    for sql in [
        format!("ALTER TABLE {t} CREATE BRANCH dev"),
        format!("ALTER TABLE {t} CREATE TAG v1"),
    ] {
        exec(&handler, &session, &sql).await;
    }
    let before = snapshots(&handler, &session, name).await;
    let s1 = before.last().expect("a snapshot").0;

    let replace =
        format!("CREATE OR REPLACE TABLE {t} AS SELECT CAST(1 AS BIGINT) AS a, 'x' AS b, 2 AS d");
    exec(&handler, &session, &replace).await;

    assert_eq!(columns(&handler, &session, name).await, ["a", "b", "d"]);
    assert_eq!(staging_tables(&handler, &session).await, 0);
    let after = snapshots(&handler, &session, name).await;
    assert_eq!(after.len(), before.len() + 1, "one commit, history kept");
    assert_eq!(
        after.last().expect("replace").1,
        Some(s1),
        "S1 is the parent"
    );
    let rows = format!("SELECT COUNT(*) FROM {t} WHERE b = 'x' AND d = 2");
    assert_eq!(scalar_i64(&handler, &session, &rows).await, 1);
    // S1's file lacks the retyped `b` and the new `d`, so they cannot be
    // NOT NULL even though their SELECT values are literals.
    let nullable = format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{NS}' \
         AND table_name = '{name}' AND is_nullable = 'YES' AND column_name IN ('b', 'd')"
    );
    assert_eq!(scalar_i64(&handler, &session, &nullable).await, 2);

    // The old files read with the old schema.
    let at_s1 = format!("{t} FOR VERSION AS OF {s1}");
    assert_eq!(int_b(&handler, &session, &at_s1).await, [10]);
    for r in ["dev", "v1"] {
        let at_ref = format!("{t} FOR VERSION AS OF '{r}'");
        assert_eq!(int_b(&handler, &session, &at_ref).await, [10], "ref {r}");
    }

    // Procedures are admin-only; same token, with an admin role.
    let admin = admin_of(&session);
    let rollback = format!("CALL system.rollback_to_snapshot(table => '{t}', snapshot_id => {s1})");
    exec(&handler, &admin, &rollback).await;
    // As in Iceberg Java, a rollback moves `main`, not the schema: S1's file
    // reads through the current schema. `a` keeps its field id (a promotion);
    // the string `b` and `d` are new fields, absent from that file, so NULL.
    assert_eq!(columns(&handler, &session, name).await, ["a", "b", "d"]);
    let row = format!(
        "SELECT CAST(a AS BIGINT) * 100 + CAST(COUNT(b) AS BIGINT) * 10 + COUNT(d) FROM {t} GROUP BY a"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &row).await,
        100,
        "a=1, b and d NULL"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}

// ─────────────────────────────────────────────────────────────────────────
// PARTITIONED BY set by one replace and dropped by the next: no PARTITIONED
// BY means unpartitioned, as in an Iceberg replace transaction.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ctas_replace_shape_changes_partitioning_892() {
    let (session, handler) = crate::common::setup_handler().await;
    let name = "ctas_shape_part_892";
    let t = format!("{NS}.{name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {t} AS SELECT 1 AS a"),
    )
    .await;

    let partitioned = format!(
        "CREATE OR REPLACE TABLE {t} PARTITIONED BY (bucket(4, a)) AS SELECT 2 AS a, 'y' AS b"
    );
    exec(&handler, &session, &partitioned).await;
    let ddl = show_create(&handler, &session, &t).await;
    assert!(ddl.to_lowercase().contains("bucket(4, a)"), "{ddl}");

    let plain = format!("CREATE OR REPLACE TABLE {t} AS SELECT 3 AS a");
    exec(&handler, &session, &plain).await;
    let ddl = show_create(&handler, &session, &t).await;
    assert!(!ddl.to_lowercase().contains("bucket"), "{ddl}");
    assert_eq!(columns(&handler, &session, name).await, ["a"]);
    assert_eq!(snapshots(&handler, &session, name).await.len(), 3);
    let sum = format!("SELECT CAST(SUM(a) AS BIGINT) FROM {t}");
    assert_eq!(scalar_i64(&handler, &session, &sum).await, 3);

    // A replace that drops the partition source column: the files it removes
    // are still listed under the old spec, whose source only an older schema
    // holds.
    let by_a = format!("CREATE OR REPLACE TABLE {t} PARTITIONED BY (a) AS SELECT 4 AS a");
    exec(&handler, &session, &by_a).await;
    let drop_a = format!("CREATE OR REPLACE TABLE {t} AS SELECT 'z' AS c");
    exec(&handler, &session, &drop_a).await;
    assert_eq!(columns(&handler, &session, name).await, ["c"]);
    assert_eq!(snapshots(&handler, &session, name).await.len(), 5);
    let count = format!("SELECT COUNT(*) FROM {t}");
    assert_eq!(scalar_i64(&handler, &session, &count).await, 1);

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}

async fn show_create(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> String {
    let batches = exec(handler, session, &format!("SHOW CREATE TABLE {table}")).await;
    let col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8");
    col.value(0).to_string()
}

// ─────────────────────────────────────────────────────────────────────────
// Column tags follow the columns that survive by name. A tag on a column the
// replace drops goes with it, and does not come back when a later replace
// adds a column of that name again.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ctas_replace_shape_keeps_tags_of_surviving_columns_892() {
    let (session, handler) = crate::common::setup_handler().await;
    let t = format!("{NS}.ctas_shape_tags_892");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {t} AS SELECT 1 AS a, 2 AS b"),
    )
    .await;
    // SET TAG is admin-only.
    let admin = admin_of(&session);
    for column in ["a", "b"] {
        let tag = format!("ALTER TABLE {t} MODIFY COLUMN {column} SET TAG pii_892 = 'true'");
        exec(&handler, &admin, &tag).await;
    }

    let drop_b = format!("CREATE OR REPLACE TABLE {t} AS SELECT 1 AS a, 'x' AS c");
    exec(&handler, &session, &drop_b).await;
    assert_eq!(tagged_columns(&handler, &session, &t).await, ["a"]);

    let readd_b = format!("CREATE OR REPLACE TABLE {t} AS SELECT 1 AS a, 2 AS b");
    exec(&handler, &session, &readd_b).await;
    assert_eq!(
        tagged_columns(&handler, &session, &t).await,
        ["a"],
        "the tag on the dropped b does not come back"
    );

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}

async fn tagged_columns(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    for b in exec(handler, session, &format!("SHOW TAGS ON {table}")).await {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        out.extend((0..b.num_rows()).map(|r| col.value(r).to_string()));
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────
// With WRITE_BRANCH set, a schema-changing replace is refused: the schema is
// table-wide and cannot stay on a branch. A failing SELECT leaves the table
// as it was. Both fail before any commit.
// ─────────────────────────────────────────────────────────────────────────
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn ctas_replace_shape_refusals_leave_the_table_892() {
    let (session, handler) = crate::common::setup_handler().await;
    let name = "ctas_shape_refused_892";
    let t = format!("{NS}.{name}");
    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {t} AS SELECT 1 AS a, 2 AS b"),
    )
    .await;
    exec(
        &handler,
        &session,
        &format!("ALTER TABLE {t} CREATE BRANCH dev"),
    )
    .await;
    let before = snapshots(&handler, &session, name).await;

    let mut on_dev = session.clone();
    on_dev.write_branch = Some("dev".to_string());
    let replace = format!("CREATE OR REPLACE TABLE {t} AS SELECT 1 AS a, 'x' AS c");
    let err = handler
        .execute(&on_dev, &replace, None)
        .await
        .expect_err("a schema change on a branch");
    assert!(err.to_string().contains("RESET WRITE_BRANCH"), "{err}");

    let failing =
        format!("CREATE OR REPLACE TABLE {t} AS SELECT CAST('not a number' AS INT) AS a, 'x' AS c");
    handler
        .execute(&session, &failing, None)
        .await
        .expect_err("the SELECT fails");

    assert_eq!(columns(&handler, &session, name).await, ["a", "b"]);
    assert_eq!(snapshots(&handler, &session, name).await, before);
    let sum = format!("SELECT CAST(SUM(b) AS BIGINT) FROM {t}");
    assert_eq!(scalar_i64(&handler, &session, &sum).await, 2);

    let _ = handler
        .execute(&session, &format!("DROP TABLE IF EXISTS {t}"), None)
        .await;
}
