# Enterprise Compaction Phase 1 (Correctness Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `CALL system.rewrite_data_files` safe on Merge-on-Read tables (stop silent row resurrection) and effective on partitioned tables (group within partitions), as the first shippable MR of the enterprise-compaction program.

**Architecture:** Two changes in `crates/sqe-coordinator/src/maintenance.rs`: (1) a delete-file guard that makes `rewrite_data_files` refuse tables carrying live delete files instead of corrupting them, and (2) partition-aware pre-grouping so bin-packing never mixes partitions. A live-stack integration test documents the resurrection bug and stays red until Phase 2 implements the delete-applying rewrite. Docs corrected.

**Tech Stack:** Rust, tokio, DataFusion, vendored iceberg-rust 0.8.0, arrow, cargo test.

## Global Constraints

- No changes to the vendored iceberg-rust fork (`vendor/iceberg-rust/`) in this phase.
- Follow existing `maintenance.rs` patterns: pure helpers at module scope, `#[cfg(test)]` unit tests at the bottom, `#[ignore]` live-stack integration tests in `crates/sqe-coordinator/tests/it/`.
- Locally-runnable red/green cycle uses pure unit tests. Integration tests are `#[ignore]` (need `docker-compose.test.yml` + `scripts/bootstrap-test.sh`); they are authored here but only run against the stack.
- Clippy strict: `cargo clippy --all-targets --all-features -- -D warnings` must pass.
- Docs voice rules (CLAUDE.md): no emdash/endash/Unicode arrows in prose.
- Commit after each task.

---

## File Structure

- Modify: `crates/sqe-coordinator/src/maintenance.rs`
  - `collect_live_data_files` (currently maintenance.rs:1214-1259) - add a sibling that also reports live delete-file entries.
  - `rewrite_data_files` (maintenance.rs:494-735) - add the delete guard early; swap `pack_file_groups` for the partition-aware grouping.
  - `pack_file_groups` (maintenance.rs:1268-1296) - keep as-is (still used per-partition); add `partition_key` + `pack_file_groups_partition_aware` beside it.
  - `#[cfg(test)] mod tests` (maintenance.rs:1452+) - add unit tests.
- Create: `crates/sqe-coordinator/tests/it/rewrite_data_files_deletes.rs` - live-stack resurrection + guard integration tests.
- Modify: `crates/sqe-coordinator/tests/it/main.rs` (or the `it` module root) - register the new test module.
- Modify: `docs/site/book/src/design-notes/mor-vs-cow.md` (line ~97) - correct the unsafe advice.
- Modify: `docs/site/book/src/sql-reference/procedures.md` - note the current MoR limitation.

---

### Task 1: Partition-aware file grouping (pure, unit-tested)

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (add helpers near `pack_file_groups` at :1268; add tests in `mod tests`)

**Interfaces:**
- Consumes: `pack_file_groups(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>>` (existing), `DataFile::partition() -> &Struct`, `DataFile::partition_spec_id() -> i32`.
- Produces:
  - `fn partition_key(f: &DataFile) -> String` - stable grouping key `"{spec_id}:{partition:?}"`.
  - `fn pack_file_groups_partition_aware(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>>` - groups files by `partition_key` first, then bin-packs within each partition via `pack_file_groups`. All returned groups contain files from exactly one partition.

- [ ] **Step 1: Write the failing unit tests**

Add to `#[cfg(test)] mod tests` in `maintenance.rs` (the helper `data_file_of_size` at :1510 hardcodes `partition_spec_id(0)` and partition `long(0)`; add a second helper that varies them):

```rust
fn data_file_part(path: &str, size: u64, spec_id: i32, part: i64) -> DataFile {
    use iceberg::spec::{DataContentType, DataFileFormat, Literal, Struct};
    DataFile::builder()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(size)
        .record_count(1)
        .partition(Struct::from_iter([Some(Literal::long(part))]))
        .partition_spec_id(spec_id)
        .build()
        .expect("data file")
}

#[test]
fn partition_key_distinguishes_partitions_and_specs() {
    let a = data_file_part("a", 10, 0, 1);
    let b = data_file_part("b", 10, 0, 2);
    let c = data_file_part("c", 10, 1, 1);
    assert_eq!(partition_key(&a), partition_key(&a));
    assert_ne!(partition_key(&a), partition_key(&b), "different partition value");
    assert_ne!(partition_key(&a), partition_key(&c), "different spec id");
}

#[test]
fn partition_aware_never_mixes_partitions() {
    // Two partitions, small files in each, all well under target.
    let files = vec![
        data_file_part("p1-a", 10, 0, 1),
        data_file_part("p1-b", 10, 0, 1),
        data_file_part("p2-a", 10, 0, 2),
        data_file_part("p2-b", 10, 0, 2),
    ];
    let groups = pack_file_groups_partition_aware(&files, 1024);
    for g in &groups {
        let keys: std::collections::HashSet<String> =
            g.iter().map(partition_key).collect();
        assert_eq!(keys.len(), 1, "each group must be single-partition, got {keys:?}");
    }
    // Every input file appears exactly once across the groups.
    let total: usize = groups.iter().map(|g| g.len()).sum();
    assert_eq!(total, 4);
}

#[test]
fn partition_aware_matches_global_when_single_partition() {
    let files = vec![
        data_file_part("a", 300, 0, 0),
        data_file_part("b", 300, 0, 0),
        data_file_part("c", 300, 0, 0),
    ];
    let pa = pack_file_groups_partition_aware(&files, 1024);
    let global = pack_file_groups(&files, 1024);
    let pa_sizes: usize = pa.iter().map(|g| g.len()).sum();
    let gl_sizes: usize = global.iter().map(|g| g.len()).sum();
    assert_eq!(pa_sizes, gl_sizes);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sqe-coordinator partition_ 2>&1 | tail -20`
Expected: FAIL - `cannot find function partition_key` / `pack_file_groups_partition_aware`.

- [ ] **Step 3: Implement the helpers**

Add directly below `pack_file_groups` (after maintenance.rs:1296):

```rust
/// Stable grouping key for a data file's partition. Files that share a key
/// belong to the same partition of the same partition spec and can be safely
/// compacted together; files with different keys must never share an output
/// file. `Struct` is not `Hash`, so we key on its `Debug` form, which is
/// deterministic and sufficient as an in-memory grouping key (not persisted).
fn partition_key(f: &DataFile) -> String {
    format!("{}:{:?}", f.partition_spec_id(), f.partition())
}

/// Bin-pack files without ever mixing partitions. Groups by `partition_key`
/// first, then applies the greedy `pack_file_groups` within each partition.
/// Every returned group contains files from exactly one partition.
fn pack_file_groups_partition_aware(files: &[DataFile], target_bytes: u64) -> Vec<Vec<DataFile>> {
    use std::collections::BTreeMap;
    let mut by_partition: BTreeMap<String, Vec<DataFile>> = BTreeMap::new();
    for f in files {
        by_partition.entry(partition_key(f)).or_default().push(f.clone());
    }
    let mut out: Vec<Vec<DataFile>> = Vec::new();
    for (_key, part_files) in by_partition {
        out.extend(pack_file_groups(&part_files, target_bytes));
    }
    out
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sqe-coordinator partition_ 2>&1 | tail -20`
Expected: PASS (3 new tests) plus the existing `pack_*` tests still green.

- [ ] **Step 5: Wire the partition-aware grouping into `rewrite_data_files`**

In `maintenance.rs:547` replace:

```rust
        let groups = pack_file_groups(&old_data_files, target_bytes);
```

with:

```rust
        // Partition-aware: never bin-pack across partition boundaries. A
        // cross-partition group would fan back out to ~1 output file per
        // partition on write (write_data_files re-splits per row), paying full
        // I/O for near-zero consolidation.
        let groups = pack_file_groups_partition_aware(&old_data_files, target_bytes);
```

- [ ] **Step 6: Rebuild and run the maintenance unit tests**

Run: `cargo test -p sqe-coordinator --lib maintenance 2>&1 | tail -20`
Expected: PASS. Then `cargo clippy -p sqe-coordinator --all-targets -- -D warnings 2>&1 | tail -20` clean.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/maintenance.rs
git commit -m "feat(compaction): partition-aware file grouping in rewrite_data_files

Never bin-pack across partition boundaries; group by (spec_id, partition)
before packing so partitioned tables actually consolidate."
```

---

### Task 2: Delete-file guard (stop MoR row resurrection)

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs`

**Interfaces:**
- Consumes: `collect_live_data_files` (existing), `ManifestStatus`, `DataContentType`.
- Produces:
  - `async fn count_live_delete_files(table: &IcebergTable) -> sqe_core::Result<usize>` - number of live manifest entries whose `content_type() != DataContentType::Data` (position + equality deletes) in the current snapshot.

- [ ] **Step 1: Write the failing unit test for the counter's classification**

The counter needs a live catalog, so unit-test the *classification predicate* it uses instead. Add a tiny pure helper and test it:

```rust
// in maintenance.rs, near collect_live_data_files
/// True when a manifest entry is a live delete file (position or equality).
fn is_live_delete_entry(entry: &iceberg::spec::ManifestEntry) -> bool {
    entry.status() != ManifestStatus::Deleted
        && entry.data_file().content_type() != DataContentType::Data
}
```

```rust
// in mod tests
#[test]
fn is_live_delete_entry_flags_position_deletes() {
    use iceberg::spec::{DataContentType, DataFileFormat, Literal, Struct, ManifestEntry, ManifestStatus};
    let df = DataFile::builder()
        .content(DataContentType::PositionDeletes)
        .file_path("pd".to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .expect("df");
    let entry = ManifestEntry::builder()
        .status(ManifestStatus::Added)
        .data_file(df)
        .build();
    assert!(is_live_delete_entry(&entry));
}

#[test]
fn is_live_delete_entry_rejects_data_and_deleted() {
    use iceberg::spec::{DataContentType, DataFileFormat, Literal, Struct, ManifestEntry, ManifestStatus};
    let data = DataFile::builder()
        .content(DataContentType::Data)
        .file_path("d".to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(1)
        .record_count(1)
        .partition(Struct::from_iter([Some(Literal::long(0))]))
        .partition_spec_id(0)
        .build()
        .expect("df");
    let entry = ManifestEntry::builder()
        .status(ManifestStatus::Added)
        .data_file(data)
        .build();
    assert!(!is_live_delete_entry(&entry), "data file is not a delete");
}
```

NOTE: verify the exact `ManifestEntry::builder()` API against `vendor/iceberg-rust/crates/iceberg/src/spec/manifest.rs` before finalizing; adjust the builder calls to match. If `ManifestEntry` has no public builder, drop these two tests and rely on the integration test in Task 3 plus the `is_live_delete_entry` predicate being a two-line composition of already-tested accessors.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sqe-coordinator is_live_delete_entry 2>&1 | tail -20`
Expected: FAIL - `cannot find function is_live_delete_entry`.

- [ ] **Step 3: Implement `is_live_delete_entry` and `count_live_delete_files`**

Add `is_live_delete_entry` (above). Add the counter modeled on `collect_live_data_files` (maintenance.rs:1214):

```rust
/// Count live delete files (position + equality) in the current snapshot.
/// Non-zero means `rewrite_data_files` cannot safely bin-pack: the raw-Parquet
/// read path does not apply deletes, so rewriting would resurrect deleted rows.
async fn count_live_delete_files(table: &IcebergTable) -> sqe_core::Result<usize> {
    use futures::{StreamExt, TryStreamExt};

    let metadata_ref = table.metadata_ref();
    let snapshot = match metadata_ref.current_snapshot() {
        Some(s) => s,
        None => return Ok(0),
    };
    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;

    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| {
                let cache = cache.clone();
                async move { cache.get_manifest(&mf).await }
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;

    let count = manifests
        .into_iter()
        .flat_map(|m| m.entries().iter().cloned().collect::<Vec<_>>())
        .filter(|e| is_live_delete_entry(e))
        .count();
    Ok(count)
}
```

- [ ] **Step 4: Add the guard in `rewrite_data_files`**

In `rewrite_data_files`, immediately after `let table = load_table(&catalog, &ident).await?;` (maintenance.rs:515) and before `collect_live_data_files`:

```rust
        // Delete-safety guard (Phase 1). The current rewrite path reads raw
        // Parquet without applying position/equality deletes, and the commit
        // drops orphaned delete files. On a Merge-on-Read table that silently
        // resurrects deleted rows. Until the delete-applying rewrite lands
        // (Phase 2), refuse rather than corrupt.
        let live_deletes = count_live_delete_files(&table).await?;
        if live_deletes > 0 {
            info!(
                table = %ident,
                live_deletes,
                "rewrite_data_files: skipping, table has live delete files"
            );
            return Ok(vec![summary_batch(
                call_name_rewrite(),
                &ident,
                0,
                0,
                0,
                0,
                format!(
                    "skipped: {live_deletes} live delete file(s); delete-aware rewrite not yet supported"
                ),
            )?]);
        }
```

- [ ] **Step 5: Run unit tests + clippy**

Run: `cargo test -p sqe-coordinator --lib maintenance 2>&1 | tail -20`
Expected: PASS.
Run: `cargo clippy -p sqe-coordinator --all-targets -- -D warnings 2>&1 | tail -20`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/maintenance.rs
git commit -m "fix(compaction): guard rewrite_data_files against MoR row resurrection

Raw-Parquet rewrite does not apply deletes and drops orphaned delete files,
silently resurrecting deleted rows. Refuse tables with live delete files
until the delete-applying rewrite (Phase 2) lands."
```

---

### Task 3: Live-stack integration tests (resurrection proof + guard)

**Files:**
- Create: `crates/sqe-coordinator/tests/it/rewrite_data_files_deletes.rs`
- Modify: the `it` test harness module root to declare `mod rewrite_data_files_deletes;`

**Interfaces:**
- Consumes: `crate::common::setup_handler()`, `handler.execute(&session, sql, None)` (see `rewrite_data_files_real.rs`).

- [ ] **Step 1: Find the test module root**

Run: `grep -rn "mod rewrite_data_files_real" crates/sqe-coordinator/tests/`
Expected: the file that declares the `it` submodules (e.g. `tests/it/main.rs` or `tests/it.rs`). Add `mod rewrite_data_files_deletes;` beside it.

- [ ] **Step 2: Write the integration tests (`#[ignore]`)**

Create `crates/sqe-coordinator/tests/it/rewrite_data_files_deletes.rs`:

```rust
//! Delete-safety for `CALL system.rewrite_data_files` on Merge-on-Read tables.
//!
//! `guard_skips_table_with_deletes` proves the Phase 1 guard: a table with
//! live position deletes is refused, not corrupted. `rewrite_preserves_deletes`
//! is the end-to-end resurrection proof; it is `#[ignore]` and expected to FAIL
//! until Phase 2 implements the delete-applying rewrite, at which point the
//! guard is removed and this test must pass. Both need the docker-compose.test
//! stack:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//! cargo test -p sqe-coordinator --test it rewrite_data_files_deletes -- --ignored
//! ```

use arrow_array::{Array, Int64Array};

async fn count_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let b = handler
        .execute(session, &format!("SELECT COUNT(*) FROM {table}"), None)
        .await
        .expect("count");
    b[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
}

async fn status_of(summary: &[arrow_array::RecordBatch]) -> String {
    summary[0]
        .column_by_name("status")
        .expect("status col")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("StringArray")
        .value(0)
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn guard_skips_table_with_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.rewrite_delete_guard";
    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;

    // Merge-on-Read so DELETE produces position deletes, not a rewrite.
    handler
        .execute(
            &session,
            &format!(
                "CREATE TABLE {table} (id BIGINT) \
                 TBLPROPERTIES ('write.delete.mode'='merge-on-read')"
            ),
            None,
        )
        .await
        .expect("CREATE MoR");
    for i in 0..10i64 {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({i})"), None)
            .await
            .expect("INSERT");
    }
    handler
        .execute(&session, &format!("DELETE FROM {table} WHERE id < 3"), None)
        .await
        .expect("DELETE");
    assert_eq!(count_rows(&handler, &session, table).await, 7);

    let summary = handler
        .execute(&session, &format!("CALL system.rewrite_data_files(table => '{table}')"), None)
        .await
        .expect("rewrite");
    let status = status_of(&summary).await;
    assert!(
        status.contains("delete file") || status.contains("skipped"),
        "guard must skip MoR table, got '{status}'"
    );
    // Row count unchanged, deletes still applied.
    assert_eq!(count_rows(&handler, &session, table).await, 7, "no resurrection");

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Expected FAIL until Phase 2 (delete-applying rewrite). Do not remove the guard until this passes.
async fn rewrite_preserves_deletes() {
    let (session, handler) = crate::common::setup_handler().await;
    let table = "default.rewrite_delete_preserve";
    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
    handler
        .execute(
            &session,
            &format!(
                "CREATE TABLE {table} (id BIGINT) \
                 TBLPROPERTIES ('write.delete.mode'='merge-on-read')"
            ),
            None,
        )
        .await
        .expect("CREATE MoR");
    for i in 0..20i64 {
        handler
            .execute(&session, &format!("INSERT INTO {table} VALUES ({i})"), None)
            .await
            .expect("INSERT");
    }
    handler
        .execute(&session, &format!("DELETE FROM {table} WHERE id < 5"), None)
        .await
        .expect("DELETE");
    assert_eq!(count_rows(&handler, &session, table).await, 15);

    let _ = handler
        .execute(&session, &format!("CALL system.rewrite_data_files(table => '{table}')"), None)
        .await
        .expect("rewrite");

    // The core invariant: deleted rows stay deleted.
    assert_eq!(
        count_rows(&handler, &session, table).await,
        15,
        "rewrite_data_files must not resurrect deleted rows"
    );

    let _ = handler.execute(&session, &format!("DROP TABLE IF EXISTS {table}"), None).await;
}
```

- [ ] **Step 3: Verify the tests compile (cannot run without the stack)**

Run: `cargo test -p sqe-coordinator --test it --no-run 2>&1 | tail -20`
Expected: compiles. If `TBLPROPERTIES ('write.delete.mode'='merge-on-read')` is not the exact SQE syntax, grep for how MoR is set: `grep -rn "write.delete.mode\|merge-on-read" crates/ docs/` and adjust the CREATE statement.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/
git commit -m "test(compaction): MoR delete-safety integration tests

guard_skips_table_with_deletes proves the Phase 1 guard; rewrite_preserves_deletes
is the resurrection proof, ignored + expected-fail until Phase 2."
```

---

### Task 4: Correct the docs

**Files:**
- Modify: `docs/site/book/src/design-notes/mor-vs-cow.md` (~line 97)
- Modify: `docs/site/book/src/sql-reference/procedures.md`

- [ ] **Step 1: Read the current advice**

Run: `grep -n "rewrite_data_files" docs/site/book/src/design-notes/mor-vs-cow.md docs/site/book/src/sql-reference/procedures.md`
Expected: find the line recommending `system.rewrite_data_files` to "collapse delete files."

- [ ] **Step 2: Replace the unsafe advice**

In `mor-vs-cow.md`, replace the recommendation to run `rewrite_data_files` for collapsing deletes with an honest limitation note (no emdash/endash/arrows):

```markdown
Delete-file consolidation on Merge-on-Read tables is not yet automatic.
`system.rewrite_data_files` currently skips tables that carry live delete
files: applying deletes during rewrite lands in a later release. Until then,
compaction runs on copy-on-write tables and on Merge-on-Read tables only after
their deletes have been materialized.
```

In `procedures.md`, add a one-line limitation under `rewrite_data_files`:

```markdown
Note: on Merge-on-Read tables with live delete files, `rewrite_data_files`
currently returns a skipped status rather than rewriting, to avoid resurrecting
deleted rows. Delete-aware rewrite is planned.
```

- [ ] **Step 3: Verify voice rules**

Run: `grep -n '—\|–\|→' docs/site/book/src/design-notes/mor-vs-cow.md docs/site/book/src/sql-reference/procedures.md`
Expected: no hits in the edited prose.

- [ ] **Step 4: Commit**

```bash
git add docs/site/book/src/design-notes/mor-vs-cow.md docs/site/book/src/sql-reference/procedures.md
git commit -m "docs(compaction): correct MoR rewrite_data_files guidance

The procedure now skips tables with live delete files; stop recommending it as
a delete-collapse step until the delete-applying rewrite lands."
```

---

## Later phases (separate plans, written when reached)

- **Phase 2 (MR #2)** - delete-applying, multi-stream, resilient engine: scan-based read via `plan_files()`, relaxed invariant (`added==rows_read`), delete-file removal, `set_new_data_file_sequence_number` pin, bounded parallel streams via `write_data_files_streaming`, `partial_progress` per-group commits, conflict retry, `MemoryPressure` gating. Removes the Task 2 guard; `rewrite_preserves_deletes` goes green.
- **Phase 3 (MR #3)** - `strategy`/`sort_order`/`zorder()` parser + DataFusion spillable `SortExec` pipeline + `zorder.rs` Morton UDF.
- **Phase 4 (MR #4)** - advisory-by-default `maintenance_scheduler.rs` + opt-in `client_credentials` principal + `system.table_health` + Prometheus gauges + audit.

Each is specced in `docs/superpowers/specs/2026-07-20-enterprise-compaction-design.md` and gets its own bite-sized plan.

## Self-Review

- **Spec coverage (Phase 1 scope):** P0 guard = Task 2 + Task 4; P1 partition grouping = Task 1; resurrection proof = Task 3. Covered.
- **Placeholders:** none; every code step shows code. Two explicit VERIFY notes (ManifestEntry builder API in Task 2 Step 1; MoR TBLPROPERTIES syntax in Task 3 Step 3) are guarded with a fallback action, not left open.
- **Type consistency:** `partition_key`, `pack_file_groups_partition_aware`, `is_live_delete_entry`, `count_live_delete_files` used with consistent signatures across tasks. `summary_batch` call matches the existing 7-arg signature (maintenance.rs:1384).
