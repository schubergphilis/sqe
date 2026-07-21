# INSERT OVERWRITE … SELECT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `INSERT OVERWRITE <table> SELECT/VALUES …` replace table data (full for unpartitioned, dynamic per-partition for partitioned) instead of silently appending.

**Architecture:** Route on the sqlparser `overwrite` flag. Reuse the existing streaming writer to produce new data files, then commit an atomic Iceberg `rewrite_files().add_data_files(new).delete_files(removed)` swap (the same primitive DELETE CoW uses) inside a hand-rolled optimistic-concurrency retry loop. The removal set is either all current data files (unpartitioned) or only the files in partitions the SELECT output touched (dynamic), plus the covered position/equality-delete files for those removed data files.

**Tech Stack:** Rust, DataFusion, iceberg-rust (vendored), sqlparser 0.62, Arrow, tokio.

## Global Constraints

- Worktree: `../sqe-insert-overwrite`, branch `feat/insert-overwrite`, based off `main`. All work + commits happen here.
- `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- Never use emdash/endash/Unicode-arrows in prose or comments (CLAUDE.md).
- No silent-append fallback: any unsupported overwrite form errors loudly.
- Do NOT modify the existing DELETE/UPDATE/MERGE CoW paths (that is #376's scope). MoR-delete cleanup belongs only to the new overwrite path.
- Iceberg dialect / parser: statements reaching `write_handler` are parsed with `GenericDialect` (`query_handler.rs:3788`).

---

## File Structure

- `crates/sqe-coordinator/src/write_handler.rs` — thread `overwrite`, add the static-PARTITION guard, add the shared `commit_written_files` helper (append vs overwrite swap), wire both INSERT entrypoints.
- `crates/sqe-coordinator/src/maintenance.rs` — expose `collect_live_delete_files` and `covered_position_deletes` as `pub(crate)`.
- `crates/sqe-sql/src/` — add a unit test proving `INSERT OVERWRITE` parses to `overwrite: true` (no grammar change).
- `crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs` — new `#[ignore]` e2e suite; register in `tests/it/main.rs`.
- `README.md`, `nextsteps.md` — status/roadmap updates.

---

### Task 1: Parse-flag unit test (sqe-sql)

Proves the parse gate holds and guards against a future dialect regression. Fast, no stack.

**Files:**
- Test: `crates/sqe-sql/src/lib.rs` (add a `#[cfg(test)]` test, or append to an existing tests module in that file).

**Interfaces:**
- Consumes: `sqlparser::parser::Parser`, `sqlparser::dialect::GenericDialect`, `sqlparser::ast::Statement`.
- Produces: nothing consumed downstream; a regression guard only.

- [ ] **Step 1: Write the failing test**

Add to `crates/sqe-sql/src/lib.rs`:

```rust
#[cfg(test)]
mod insert_overwrite_parse_tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .pop()
            .expect("one statement")
    }

    #[test]
    fn insert_overwrite_sets_overwrite_flag() {
        for sql in [
            "INSERT OVERWRITE t SELECT 1 AS id",
            "INSERT OVERWRITE INTO t SELECT 1 AS id",
            "INSERT OVERWRITE TABLE t SELECT 1 AS id",
        ] {
            match parse_one(sql) {
                Statement::Insert(ins) => {
                    assert!(ins.overwrite, "overwrite flag not set for: {sql}");
                    assert!(ins.partitioned.is_none(), "unexpected PARTITION for: {sql}");
                }
                other => panic!("expected Insert, got {other:?} for {sql}"),
            }
        }
    }

    #[test]
    fn plain_insert_does_not_set_overwrite() {
        match parse_one("INSERT INTO t SELECT 1 AS id") {
            Statement::Insert(ins) => assert!(!ins.overwrite),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_overwrite_static_partition_is_captured() {
        // Static Hive PARTITION clause must be visible so the handler can
        // reject it loudly rather than mishandle it.
        match parse_one("INSERT OVERWRITE t PARTITION (region='eu') SELECT 1 AS id") {
            Statement::Insert(ins) => {
                assert!(ins.overwrite);
                assert!(ins.partitioned.is_some(), "static PARTITION not captured");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run to verify (should PASS immediately — this documents current parser behavior)**

Run: `cargo test -p sqe-sql insert_overwrite_parse -- --nocapture`
Expected: all three tests PASS. If `insert_overwrite_sets_overwrite_flag` FAILS, STOP — the parse-gate premise is wrong and the design needs a post-parse transform; report before continuing.

- [ ] **Step 3: Commit**

```bash
cd ../sqe-insert-overwrite
git add crates/sqe-sql/src/lib.rs
git commit -m "test(sql): assert INSERT OVERWRITE parses to overwrite flag (#378)"
```

---

### Task 2: Expose MoR-delete cleanup helpers as `pub(crate)`

The overwrite swap must drop position/equality-delete files whose data files it removes. The helpers already exist in `maintenance.rs` but are module-private.

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs:1463` and `:1598`.

**Interfaces:**
- Produces:
  - `pub(crate) async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>>`
  - `pub(crate) fn covered_position_deletes(removed_data_paths: &std::collections::HashSet<String>, live_deletes: &[DataFile]) -> Vec<DataFile>`

- [ ] **Step 1: Widen visibility**

In `crates/sqe-coordinator/src/maintenance.rs`, change:

```rust
async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
```
to
```rust
pub(crate) async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
```

and

```rust
fn covered_position_deletes(
    removed_data_paths: &std::collections::HashSet<String>,
    live_deletes: &[DataFile],
) -> Vec<DataFile> {
```
to
```rust
pub(crate) fn covered_position_deletes(
    removed_data_paths: &std::collections::HashSet<String>,
    live_deletes: &[DataFile],
) -> Vec<DataFile> {
```

- [ ] **Step 2: Verify it still builds**

Run: `cargo build -p sqe-coordinator`
Expected: builds. (The existing `maintenance.rs` unit tests `covered_position_deletes_*` still compile and reference the now-`pub(crate)` fn.)

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/src/maintenance.rs
git commit -m "refactor(coordinator): expose delete-cleanup helpers to write_handler (#378)"
```

---

### Task 3: Shared overwrite commit helper + entrypoint wiring

The core. Add one private helper that both INSERT entrypoints call to commit their already-written data files, choosing append vs overwrite. Overwrite computes the removal set (full or dynamic), chains covered delete files, and commits the atomic swap in a hand-rolled retry loop (mirroring the DELETE CoW loop at `write_handler.rs:2113-2233`, which reloads the table each attempt — do NOT use `commit_with_retry`, whose move-closure cannot borrow `self` for `collect_data_files`).

**Files:**
- Modify: `crates/sqe-coordinator/src/write_handler.rs`
  - `handle_insert_streaming` (fn starting ~1339; `Statement::Insert` match at 1348; commit block 1445-1476).
  - `handle_insert` (fn starting ~1704; `Statement::Insert` match at 1711).
  - Add private helper `commit_written_files`.

**Interfaces:**
- Consumes: `collect_data_files` (method, `write_handler.rs:4196`); `maintenance::{collect_live_delete_files, covered_position_deletes}` (Task 2); constants `COW_MAX_ATTEMPTS`, `cow_conflict_backoff_ms`, `is_conflict_message`; `Transaction`, `WriteCleanupGuard`.
- Produces:
  - `async fn commit_written_files(&self, catalog: &SessionCatalog, table: IcebergTable, table_ident: &TableIdent, new_data_files: Vec<DataFile>, overwrite: bool) -> sqe_core::Result<()>`

- [ ] **Step 1: Guard the static PARTITION clause in both entrypoints**

In `handle_insert_streaming`, replace the match arm at `write_handler.rs:1348`:

```rust
            Statement::Insert(ins) => match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => (name, &ins.columns),
```
with an added guard and capture of the overwrite flag:

```rust
            Statement::Insert(ins) => {
                if ins.partitioned.is_some() {
                    return Err(SqeError::NotImplemented(
                        "INSERT OVERWRITE with a static PARTITION (col=val) clause is not supported; \
                         omit the PARTITION clause for dynamic partition overwrite".into(),
                    ));
                }
                match &ins.table {
                    sqlparser::ast::TableObject::TableName(name) => (name, &ins.columns, ins.overwrite),
                    other => {
                        return Err(SqeError::Execution(format!(
                            "INSERT INTO table functions not supported: {other}"
                        )));
                    }
                }
            }
```

Update the binding at the top of the match to `let (table_name, explicit_columns, overwrite) = match stmt { ... }`. (Verify `SqeError::NotImplemented` exists in `sqe-core`; it is used at `sqe-core/src/error.rs:1207`. If its variant takes a `String`, pass `.into()` as shown.)

Apply the identical guard + `ins.overwrite` capture in `handle_insert` at `write_handler.rs:1711` (its binding is `let table_name = match stmt { ... }`; change to `let (table_name, overwrite) = ...`).

- [ ] **Step 2: Add the shared commit helper**

Add this method to the `impl WriteHandler` block in `write_handler.rs` (place it near `collect_data_files`, ~line 4196). Mirror the DELETE CoW retry loop shape (2113-2233):

```rust
    /// Commit already-written data files, either as an append (overwrite=false)
    /// or as an atomic overwrite swap (overwrite=true).
    ///
    /// Overwrite semantics:
    /// - Unpartitioned table (or a spec with no fields): full replace. Every
    ///   current data file is removed.
    /// - Partitioned table: dynamic overwrite. Only current data files whose
    ///   partition value appears in `new_data_files` are removed; untouched
    ///   partitions are preserved.
    /// In both cases the position/equality-delete files covering the removed
    /// data files are dropped in the same commit (no superseded-delete debris).
    ///
    /// A zero-length `new_data_files` with overwrite=true is a truncate on an
    /// unpartitioned table and a no-op on a partitioned one (no partitions
    /// touched). Callers MUST route zero-row overwrites here rather than taking
    /// an "empty SELECT" early return.
    async fn commit_written_files(
        &self,
        catalog: &SessionCatalog,
        mut table: IcebergTable,
        table_ident: &TableIdent,
        new_data_files: Vec<DataFile>,
        overwrite: bool,
    ) -> sqe_core::Result<()> {
        use std::collections::HashSet;

        if !overwrite {
            if new_data_files.is_empty() {
                return Ok(());
            }
            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(new_data_files);
            let tx = action
                .apply(tx)
                .map_err(|e| SqeError::Execution(format!("append apply failed: {e}")))?;
            tx.commit(catalog.as_catalog().as_ref())
                .await
                .map_err(|e| SqeError::Execution(format!("Failed to commit INSERT: {e}")))?;
            return Ok(());
        }

        // Partitioned iff the default spec has fields.
        let partitioned = !table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty();

        // Distinct partitions touched by the new files (dynamic overwrite).
        // Struct implements PartialEq; partition cardinality per write is small,
        // so a Vec + linear membership check is fine.
        let touched_partitions: Vec<iceberg::spec::Struct> = if partitioned {
            let mut v: Vec<iceberg::spec::Struct> = Vec::new();
            for f in &new_data_files {
                let p = f.partition().clone();
                if !v.contains(&p) {
                    v.push(p);
                }
            }
            v
        } else {
            Vec::new()
        };

        let mut attempt = 1u32;
        loop {
            let all_old = self.collect_data_files(&table).await?;

            let removed_data: Vec<DataFile> = if !partitioned {
                all_old
            } else {
                all_old
                    .into_iter()
                    .filter(|df| touched_partitions.contains(df.partition()))
                    .collect()
            };

            // Nothing to replace and nothing to add: genuine no-op.
            if removed_data.is_empty() && new_data_files.is_empty() {
                info!(table = %table_ident, "INSERT OVERWRITE: no partitions touched and no new rows; no-op");
                return Ok(());
            }

            // Drop delete files whose data files are all being removed.
            let removed_paths: HashSet<String> =
                removed_data.iter().map(|d| d.file_path().to_string()).collect();
            let live_deletes = crate::maintenance::collect_live_delete_files(&table).await?;
            let covered_deletes =
                crate::maintenance::covered_position_deletes(&removed_paths, &live_deletes);

            let files_to_remove: Vec<DataFile> =
                removed_data.iter().cloned().chain(covered_deletes.into_iter()).collect();

            info!(
                table = %table_ident,
                partitioned,
                new_files = new_data_files.len(),
                removed_data = removed_data.len(),
                attempt,
                "INSERT OVERWRITE: committing atomic swap"
            );

            let tx = Transaction::new(&table);
            let mut action = tx.rewrite_files().add_data_files(new_data_files.clone());
            if !files_to_remove.is_empty() {
                action = action.delete_files(files_to_remove);
            }
            let commit_result = match action.apply(tx) {
                Ok(tx) => tx.commit(catalog.as_catalog().as_ref()).await,
                Err(e) => Err(e),
            };
            match commit_result {
                Ok(_) => {
                    info!(table = %table_ident, "INSERT OVERWRITE committed successfully");
                    return Ok(());
                }
                Err(e)
                    if (e.retryable() || is_conflict_message(&e.to_string()))
                        && attempt < COW_MAX_ATTEMPTS =>
                {
                    let sleep_ms = cow_conflict_backoff_ms(attempt);
                    warn!(table = %table_ident, op = "insert-overwrite", attempt, backoff_ms = sleep_ms, error = %e, "commit conflict; reloading table and retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    table = catalog.load_table(table_ident).await?;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(SqeError::Execution(format!("Failed to commit INSERT OVERWRITE: {e}")));
                }
            }
        }
    }
```

Notes for the implementer:
- `new_data_files.clone()` inside the loop: `add_data_files` consumes the Vec; the retry needs a fresh copy each attempt. `DataFile` is `Clone`.
- Confirm `catalog: &SessionCatalog` exposes `as_catalog()` and `load_table` (both used in the DELETE path at 2074/2089). The streaming path holds `catalog: Arc<SessionCatalog>` — pass `catalog.as_ref()`.
- `iceberg::spec::Struct` `PartialEq`: verify it derives/implements `PartialEq` in the vendored iceberg-rust (`crates/... spec/values` — search `struct Struct`). If it does NOT, key the membership check on a stable serialization instead (e.g. `format!("{:?}", p)`), and note the change.

- [ ] **Step 3: Wire `handle_insert_streaming` to use the helper (incl. zero-row overwrite = truncate)**

In `handle_insert_streaming`, the current zero-row early-out (`write_handler.rs:1439-1443`) returns before committing. Change it so a zero-row **overwrite** still routes to the swap (truncate), while a zero-row append stays a no-op:

```rust
        if total_rows == 0 && !overwrite {
            info!(table = %table_ident, "INSERT SELECT returned no rows — nothing to write");
            cleanup_guard.mark_committed();
            return Ok(affected_rows_batch(0));
        }
```

Then replace the whole `if !data_files.is_empty() { commit_with_retry(...) } else { ... }` block (1445-1476) with:

```rust
        self.commit_written_files(
            catalog.as_ref(),
            table,
            &table_ident,
            data_files,
            overwrite,
        )
        .await?;
        cleanup_guard.mark_committed();

        info!(
            table = %table_ident,
            total_rows,
            overwrite,
            "INSERT committed successfully (streaming)"
        );
```

(`table` is the `IcebergTable` loaded at 1375; it is moved into the helper. If it is borrowed later in the fn, load a fresh one or reorder so the helper is the last use. Verify no later use of `table` in this fn.)

- [ ] **Step 4: Wire `handle_insert` (batches path) the same way**

In `handle_insert` (~1704), after the data files are written (mirror the streaming write; the fn already writes `data_files`), replace its fast-append commit with:

```rust
        self.commit_written_files(catalog.as_ref(), table, &table_ident, data_files, overwrite)
            .await?;
```

Also relax its zero-row early return (`write_handler.rs:1737-1740`) the same way: only short-circuit when `!overwrite`. For a zero-row overwrite it must still call the helper with an empty `data_files` vec so an unpartitioned table truncates.

- [ ] **Step 5: Build + clippy**

Run: `cargo build -p sqe-coordinator && cargo clippy -p sqe-coordinator --all-targets -- -D warnings`
Expected: clean. Fix any borrow/move errors on `table`/`catalog` per the notes above.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/write_handler.rs
git commit -m "feat(write): INSERT OVERWRITE atomic swap — full + dynamic partition (#378)"
```

---

### Task 4: End-to-end test suite

**Files:**
- Create: `crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs`
- Modify: `crates/sqe-coordinator/tests/it/main.rs` (add `mod insert_overwrite_e2e;` in the module list).

**Interfaces:**
- Consumes: `crate::common::setup_handler()`, `sqe_coordinator::QueryHandler`, `sqe_core::Session`. Mirror the helpers in `ctas_write_modes_e2e.rs` (`exec`, `scalar_i64`, `live_data_file_count`, `latest_snapshot`, `summary_count`).

- [ ] **Step 1: Register the module**

In `crates/sqe-coordinator/tests/it/main.rs`, add alphabetically near the other `mod` lines:

```rust
mod insert_overwrite_e2e;
```

- [ ] **Step 2: Write the e2e suite**

Create `crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs`:

```rust
//! End-to-end verification of `INSERT OVERWRITE … SELECT/VALUES` (#378).
//!
//! Every test is `#[ignore]` because each needs the running stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it -- --ignored insert_overwrite
//! ```

use arrow_array::Int64Array;

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

async fn snapshot_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    ns: &str,
    table: &str,
) -> i64 {
    scalar_i64(
        handler,
        session,
        &format!("SELECT COUNT(*) FROM table_snapshots('{ns}', '{table}')"),
    )
    .await
}

// 1 + 2: no silent append, full replace, new snapshot, prior snapshot retained.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_replaces_unpartitioned_and_retains_history() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_full_378");
    let fq = format!("{ns}.{name}");

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(&handler, &session, &format!("CREATE TABLE {fq} AS SELECT 1 AS id, 10 AS v")).await;
    exec(&handler, &session, &format!("INSERT INTO {fq} VALUES (2, 20), (3, 30)")).await;
    assert_eq!(scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await, 3);
    let snaps_before = snapshot_count(&handler, &session, ns, name).await;

    exec(&handler, &session, &format!("INSERT OVERWRITE {fq} SELECT 99 AS id, 990 AS v")).await;

    // Not appended: exactly the overwrite result remains.
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        1,
        "INSERT OVERWRITE must replace, not append"
    );
    assert_eq!(scalar_i64(&handler, &session, &format!("SELECT id FROM {fq}")).await, 99);
    // A new snapshot was committed and the prior ones are retained (time travel).
    assert!(
        snapshot_count(&handler, &session, ns, name).await > snaps_before,
        "overwrite must add a snapshot, not rewrite history"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 3: idempotency.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_is_idempotent() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_idem_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(&handler, &session, &format!("CREATE TABLE {fq} AS SELECT 1 AS id")).await;

    exec(&handler, &session, &format!("INSERT OVERWRITE {fq} SELECT 5 AS id UNION ALL SELECT 6")).await;
    let first = scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await;
    exec(&handler, &session, &format!("INSERT OVERWRITE {fq} SELECT 5 AS id UNION ALL SELECT 6")).await;
    let second = scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await;
    assert_eq!(first, 2);
    assert_eq!(second, 2, "re-running the same overwrite must not double rows");

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 4: dynamic partition overwrite — touched replaced, untouched preserved.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_dynamic_preserves_untouched_partitions() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_dyn_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING, v BIGINT) PARTITIONED BY (region)"),
    )
    .await;
    exec(&handler, &session, &format!("INSERT INTO {fq} VALUES (1,'eu',10),(2,'us',20)")).await;
    assert_eq!(scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await, 2);

    // Overwrite only the 'eu' partition with two new rows.
    exec(
        &handler,
        &session,
        &format!("INSERT OVERWRITE {fq} SELECT 3 AS id, 'eu' AS region, 30 AS v UNION ALL SELECT 4, 'eu', 40"),
    )
    .await;

    // 'us' partition preserved (1 row), 'eu' replaced (2 new rows) => 3 total.
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        3,
        "untouched 'us' partition must survive; 'eu' replaced"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq} WHERE region='us'")).await,
        1,
        "untouched partition over-deleted — CATASTROPHIC"
    );
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq} WHERE region='eu'")).await,
        2,
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 5: zero-row unpartitioned overwrite = truncate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_empty_select_truncates_unpartitioned() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_trunc_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(&handler, &session, &format!("CREATE TABLE {fq} AS SELECT 1 AS id")).await;
    exec(&handler, &session, &format!("INSERT INTO {fq} VALUES (2),(3)")).await;

    exec(&handler, &session, &format!("INSERT OVERWRITE {fq} SELECT 1 AS id WHERE 1=0")).await;
    assert_eq!(
        scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await,
        0,
        "empty-SELECT overwrite must truncate an unpartitioned table"
    );

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 7: self-overwrite.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_from_self() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_self_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(&handler, &session, &format!("CREATE TABLE {fq} AS SELECT 1 AS id")).await;
    exec(&handler, &session, &format!("INSERT INTO {fq} VALUES (2),(3)")).await;

    exec(&handler, &session, &format!("INSERT OVERWRITE {fq} SELECT id + 100 FROM {fq}")).await;
    assert_eq!(scalar_i64(&handler, &session, &format!("SELECT COUNT(*) FROM {fq}")).await, 3);
    assert_eq!(scalar_i64(&handler, &session, &format!("SELECT MIN(id) FROM {fq}")).await, 101);

    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}

// 8: static PARTITION clause errors loudly (no stack needed, but kept here).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs docker-compose.test.yml + Polaris"]
async fn overwrite_static_partition_errors() {
    let (session, handler) = crate::common::setup_handler().await;
    let (ns, name) = ("default", "iow_static_378");
    let fq = format!("{ns}.{name}");
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
    exec(
        &handler,
        &session,
        &format!("CREATE TABLE {fq} (id BIGINT, region STRING) PARTITIONED BY (region)"),
    )
    .await;
    let err = handler
        .execute(
            &session,
            &format!("INSERT OVERWRITE {fq} PARTITION (region='eu') SELECT 1, 'eu'"),
            None,
        )
        .await
        .expect_err("static PARTITION overwrite must error");
    assert!(
        format!("{err}").to_lowercase().contains("partition"),
        "error should name the unsupported PARTITION clause: {err}"
    );
    exec(&handler, &session, &format!("DROP TABLE IF EXISTS {fq}")).await;
}
```

Note: case 6 (MoR delete cleanup) requires a table with position deletes; add it only if `setup_handler` supports creating a MoR table with a delete (see `ctas_write_modes_e2e` MoR helpers). If the identifier-field-id requirement makes position deletes hard to seed via CTAS, document that case 6 is covered by the covered-delete unit tests in `maintenance.rs` plus manual inspection, and note the gap in the MR rather than faking it.

- [ ] **Step 3: Compile the tests (no run yet)**

Run: `cargo test -p sqe-coordinator --test it --no-run`
Expected: compiles. Fix any signature mismatches against `common::setup_handler`.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs crates/sqe-coordinator/tests/it/main.rs
git commit -m "test(coordinator): e2e suite for INSERT OVERWRITE (#378)"
```

---

### Task 5: Bring up the stack and run the e2e suite

**Files:** none (validation).

- [ ] **Step 1: Start the test stack**

Run:
```bash
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
```
Expected: Polaris + storage healthy. If the compose file or bootstrap script differs, consult `ctas_write_modes_e2e.rs` header (lines 20-26) for the exact incantation. If the stack cannot come up locally, STOP and surface the blocker to the user — do not declare the tests passing.

- [ ] **Step 2: Run the suite**

Run: `cargo test -p sqe-coordinator --test it -- --ignored insert_overwrite --nocapture`
Expected: all tests PASS. The `overwrite_dynamic_preserves_untouched_partitions` "us partition survives" assertion is the single most important signal (over-deletion is catastrophic).

- [ ] **Step 3: If any test fails, debug against the DELETE CoW reference**

Use systematic-debugging. Common suspects: `Struct` `PartialEq` not matching (dynamic filter removes nothing or everything — inspect `table_files` before/after); `table` moved/borrowed error already fixed in Task 3; `total_rows==0` path still short-circuiting (truncate test). Fix in `write_handler.rs`, rebuild, re-run.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A && git commit -m "fix(write): INSERT OVERWRITE e2e corrections (#378)"
```

- [ ] **Step 5: Tear down**

Run: `docker compose -f docker-compose.test.yml down`

---

### Task 6: Trino syntax probe + parity decision

**Files:** update the spec's Trino-parity section with the finding.

- [ ] **Step 1: Determine whether standard Trino accepts the literal `INSERT OVERWRITE` syntax**

Check Trino's Iceberg-connector docs / grammar (Trino uses the session property `insert_existing_partitions_behavior`, and historically rejects `INSERT OVERWRITE` as a syntax error). Use context7 or the Trino SQL grammar reference.

- [ ] **Step 2: Record the decision**

If Trino rejects the syntax: edit `docs/superpowers/specs/2026-07-21-insert-overwrite-design.md` Trino-parity section to state parity is N/A (syntax not shared) and the acceptance criterion is dropped. If Trino accepts it: note that a bench-compare parity run is a follow-up. Either way, this is not a blocker.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-07-21-insert-overwrite-design.md
git commit -m "docs(spec): record Trino INSERT OVERWRITE parity finding (#378)"
```

---

### Task 7: Docs, roadmap, and MR

**Files:** `README.md`, `nextsteps.md`.

- [ ] **Step 1: Update `README.md` roadmap** — mark `INSERT OVERWRITE` as implemented (find the write-path / Phase 2c checklist section; add a line if none exists).

- [ ] **Step 2: Update `nextsteps.md`** — move the status pointer, note #378 done, dynamic-partition overwrite shipped, static-PARTITION + Trino-parity-run as follow-ups.

- [ ] **Step 3: Final full-gate build**

Run: `cargo clippy --all-targets --all-features -- -D warnings && cargo test -p sqe-sql insert_overwrite_parse`
Expected: clean + parse tests pass.

- [ ] **Step 4: Commit docs**

```bash
git add README.md nextsteps.md
git commit -m "docs: INSERT OVERWRITE shipped; roadmap + nextsteps (#378)"
```

- [ ] **Step 5: Push and open the MR (GitLab, via glab)**

```bash
git push -u origin feat/insert-overwrite
glab mr create --fill --source-branch feat/insert-overwrite --target-branch main \
  --title "feat(write): INSERT OVERWRITE … SELECT (dynamic partition overwrite) (#378)" \
  --description "Implements #378. See docs/superpowers/specs/2026-07-21-insert-overwrite-design.md. Closes #378."
```

Expected: MR URL printed. Report it to the user with a summary of e2e results.

---

## Self-Review

**Spec coverage:**
- Parse gate → Task 1. Routing/flag → Task 3 Step 1. Full replace → Task 3 helper (unpartitioned branch) + Task 4 case 1/2. Dynamic → Task 3 helper (partitioned branch) + Task 4 case 4. Zero-row truncate/no-op → Task 3 Steps 3-4 + Task 4 case 5. Static-PARTITION guard → Task 3 Step 1 + Task 4 case 8. MoR cleanup → Task 2 + Task 3 helper (covered_deletes) + Task 4 case 6 note. Self-overwrite → Task 4 case 7. Snapshot retention/time-travel → Task 4 case 1/2. Atomicity/retry → Task 3 helper loop. Trino parity → Task 6. Validation on real stack → Task 5. Docs → Task 7. All spec sections mapped.

**Placeholder scan:** No TBD/TODO. Case 6 is explicitly conditional with a documented fallback, not a placeholder. Every code step shows real code.

**Type consistency:** `commit_written_files(&self, catalog: &SessionCatalog, table: IcebergTable, table_ident: &TableIdent, new_data_files: Vec<DataFile>, overwrite: bool)` used consistently in Tasks 3 Steps 2/3/4. Helper names `collect_live_delete_files`/`covered_position_deletes` match Task 2. `overwrite` binding threaded in both entrypoints.

**Known verification points flagged inline (not placeholders):** `SqeError::NotImplemented` variant shape; `iceberg::spec::Struct: PartialEq`; `SessionCatalog::as_catalog()/load_table` availability; no later borrow of `table` in `handle_insert_streaming`. Each has a stated fallback.
