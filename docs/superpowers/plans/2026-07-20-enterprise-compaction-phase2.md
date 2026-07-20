# Enterprise Compaction Phase 2: Delete-Applying, Multi-Stream, Resilient Rewrite

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `system.rewrite_data_files` correct on Merge-on-Read tables (apply position + equality deletes during the rewrite instead of skipping), streaming and memory-bounded, and resilient to concurrent writers.

**Architecture:** Replace the Phase 1 guard + raw-Parquet group read with a delete-aware Iceberg table scan streamed straight into the streaming writer, pinned to the compaction-start sequence number so concurrently-committed equality deletes still apply. Land the correctness core as one verifiable commit, then layer memory gating, partial-progress commits, and conflict retry.

**Tech Stack:** Rust, iceberg-rust (vendored fork), DataFusion stream types, existing SQE `writer.rs` / `memory.rs` primitives.

## Global Constraints

- No changes to the vendored iceberg-rust fork. Every needed API already exists.
- Branch: `feat/compaction-phase2-delete-aware`, stacked on `worktree-feat-enterprise-compaction` (Phase 1 / MR !660). Phase 2 is its own MR.
- All work in `crates/sqe-coordinator/src/maintenance.rs` and `crates/sqe-sql/src/procedures.rs`; unit tests inline, integration tests in `crates/sqe-coordinator/tests/it/`.
- TDD where locally testable. The correctness deliverable (`rewrite_preserves_deletes`) is an `#[ignore]` integration test verified against `docker-compose.test.yml` — the local `cargo test` lib loop does NOT prove delete application. A validation checkpoint against the live stack is REQUIRED before Phase 3.
- Do NOT relax the row-count guard to the tautological `added_rows == rows_read`. Use the delete-accounting cross-check (Task 3).

---

## Key API facts (verified against the tree)

- `write_data_files_streaming(table, stream: SendableRecordBatchStream, file_prefix, compression, tracker, fanout: FanoutLimits) -> Result<(Vec<DataFile>, usize)>` (`writer.rs:496`). Second tuple field is `total_rows` written. `FanoutLimits::unbounded()` is correct here: partition-aware grouping means each group is single-partition, so one open writer.
- Delete-applying scan pattern already in `write_handler.rs`: `plan_delete_aware_read` (`:4339`) builds `table.scan().select_all().build()`, `plan_files().try_collect()`, groups `FileScanTask`s by `data_file_path`. `read_data_file_applying_deletes` (`:4368`) turns a file's tasks into a `FileScanTaskStream` via `Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)))` and calls `scan.read_tasks_to_arrow_with_metrics(stream)`, consuming `result.stream()` (a `BoxStream<'static, Result<RecordBatch, iceberg::Error>>`).
- `RewriteFilesAction::set_new_data_file_sequence_number(i64)` (`rewrite_files.rs:165`) — the concurrent-equality-delete keystone.
- `.delete_files(iter)` routes `PositionDeletes|EqualityDeletes` DataFiles into `removed_delete_files` (`rewrite_files.rs:97-103`).
- `DataFile::referenced_data_file() -> Option<String>` (`data_file.rs:276`), `record_count()`, `content_type()`, `equality_ids()` — all public.
- `Snapshot::sequence_number()` on the current snapshot gives `seq_at_start`.
- `crate::memory::check_pressure(&Arc<dyn MemoryPool>) -> MemoryPressure` (`memory.rs:80`); `MemoryPressure::{Green,Yellow,Orange,Red}`.
- `RecordBatchStreamAdapter::new(schema, stream)` (`datafusion::physical_plan::stream`) adapts a mapped stream into a `SendableRecordBatchStream` (see `merge_target_provider.rs:98`).

---

## Task 1: Return delete files, not just a count

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (around `count_live_delete_files` :1258)

**Interfaces:**
- Produces: `async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>>` returning every live position/equality delete `DataFile`.

- [ ] **Step 1:** Add `collect_live_delete_files` modeled on `collect_live_data_files` (:1296) but filtering with `is_live_delete_entry(entry)` and mapping `entry.data_file().clone()`.

```rust
/// Collect the live delete files (position + equality) of the current snapshot.
async fn collect_live_delete_files(table: &IcebergTable) -> sqe_core::Result<Vec<DataFile>> {
    use futures::{StreamExt, TryStreamExt};
    let metadata_ref = table.metadata_ref();
    let Some(snapshot) = metadata_ref.current_snapshot() else { return Ok(vec![]) };
    let cache = table.object_cache();
    let manifest_list = cache
        .get_manifest_list(snapshot, &metadata_ref)
        .await
        .map_err(|e| SqeError::Execution(format!("Failed to load manifest list: {e}")))?;
    const CONCURRENCY: usize = 8;
    let manifests: Vec<Arc<iceberg::spec::Manifest>> =
        futures::stream::iter(manifest_list.entries().iter().cloned())
            .map(|mf| { let cache = cache.clone(); async move { cache.get_manifest(&mf).await } })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await
            .map_err(|e| SqeError::Execution(format!("Failed to load manifest: {e}")))?;
    Ok(manifests
        .into_iter()
        .flat_map(|m| m.entries().iter().filter(|e| is_live_delete_entry(e))
            .map(|e| e.data_file().clone()).collect::<Vec<_>>())
        .collect())
}
```

- [ ] **Step 2:** Keep `count_live_delete_files` (still used by tests / advisory later), or make it `collect_live_delete_files(table).await?.len()`. Simplest: leave `count_live_delete_files` as-is; both coexist.
- [ ] **Step 3:** `cargo build -p sqe-coordinator` — expect clean (unused-function warning is fine; used in Task 2).
- [ ] **Step 4:** Commit: `git commit -m "feat(compaction): collect_live_delete_files helper"`

---

## Task 2: Delete-aware streaming group rewrite (correctness core)

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (`rewrite_group` :1418, `rewrite_data_files` :494)

**Interfaces:**
- Consumes: `collect_live_delete_files` (Task 1), `write_data_files_streaming`, `RecordBatchStreamAdapter`.
- Produces: `rewrite_group` now applies deletes; returns `(new_files, old_files, rows_written)` where `rows_written` is post-delete.

- [ ] **Step 1:** Add a `DeleteAwareReadPlan` + planner as free functions in `maintenance.rs` (mirror `write_handler.rs:4339`). `plan_delete_aware_read(table) -> Result<DeleteAwareReadPlan>` with `scan: TableScan` and `tasks_by_path: HashMap<String, Vec<FileScanTask>>`.

- [ ] **Step 2:** Rewrite `rewrite_group` to build one `FileScanTaskStream` over ALL the group's data-file tasks (deletes attached), read once, and stream into the writer:

```rust
async fn rewrite_group(
    table: &IcebergTable,
    plan: &DeleteAwareReadPlan,
    group: Vec<DataFile>,
    compression: parquet::basic::Compression,
    tracker: crate::writer::UploadedPaths,
) -> sqe_core::Result<(Vec<DataFile>, Vec<DataFile>, u64)> {
    use futures::StreamExt;
    // Gather every scan task for the group's files (each carries its deletes).
    let mut tasks: Vec<iceberg::scan::FileScanTask> = Vec::new();
    for df in &group {
        match plan.tasks_by_path.get(df.file_path()) {
            Some(t) => tasks.extend(t.iter().cloned()),
            None => return Err(SqeError::Execution(format!(
                "delete-aware compaction: data file '{}' missing from scan plan; refusing to \
                 read it without its delete files", df.file_path()))),
        }
    }
    if tasks.is_empty() {
        return Ok((vec![], vec![], 0));
    }
    let task_stream: iceberg::scan::FileScanTaskStream =
        Box::pin(futures::stream::iter(tasks.into_iter().map(Ok)));
    let result = plan.scan.read_tasks_to_arrow_with_metrics(task_stream)
        .map_err(|e| SqeError::Execution(format!("delete-aware compaction read failed: {e}")))?;
    let arrow_schema = result.schema(); // SchemaRef for the adapter (see Step 3 note)
    let iceberg_stream = result.stream();
    // Adapt iceberg BoxStream<Result<_, iceberg::Error>> -> DataFusion SendableRecordBatchStream.
    let df_stream = iceberg_stream.map(|item| item
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e))));
    let sendable: datafusion::execution::SendableRecordBatchStream =
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            arrow_schema, df_stream));
    let (new_files, rows_written) = crate::writer::write_data_files_streaming(
        table, sendable, "rewrite", compression, tracker,
        crate::writer::FanoutLimits::unbounded(),
    ).await?;
    Ok((new_files, group, rows_written as u64))
}
```

Note on `result.schema()`: if `ScanResult` has no `schema()` accessor, derive the Arrow schema from the table: `iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema().as_ref())` (returns `Result<ArrowSchema>`). Verify the exact accessor at implementation time; prefer whichever the vendored `ScanResult` exposes.

- [ ] **Step 3:** In `rewrite_data_files` (:494): DELETE the Phase 1 guard block (:517-542). Before packing, capture `seq_at_start` and plan the delete-aware read from the SAME loaded table:

```rust
let seq_at_start = table.metadata_ref().current_snapshot()
    .map(|s| s.sequence_number()).unwrap_or(0);
let read_plan = plan_delete_aware_read(&table).await?;
let live_deletes = collect_live_delete_files(&table).await?;
```

- [ ] **Step 4:** Update the group-rewrite fan-out (:633) to pass `&read_plan` into `rewrite_group`. Keep `buffer_unordered(max_concurrent)`.

- [ ] **Step 5:** Add `set_new_data_file_sequence_number(seq_at_start)` to the commit action chain (:696-701), before `.add_data_files(...)`.

- [ ] **Step 6:** `cargo build -p sqe-coordinator` — clean.
- [ ] **Step 7:** `cargo test -p sqe-coordinator --lib` — 638 still green (no delete-path lib test yet; that's Task 5's stack run).
- [ ] **Step 8:** Commit: `git commit -m "feat(compaction): delete-applying streaming rewrite + seq-number pin"`

---

## Task 3: Delete-accounting cross-check (the real guard)

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs`

**Interfaces:**
- Produces: pure `fn expected_rows_after_deletes(group: &[DataFile], live_deletes: &[DataFile]) -> Option<u64>` — `Some(n)` when the group's row count after applying attributable position deletes is known exactly; `None` when equality deletes (or position deletes without `referenced_data_file`) make it ambiguous.

- [ ] **Step 1: Write the failing unit test:**

```rust
#[test]
fn expected_rows_subtracts_referenced_position_deletes() {
    let d = data_file("s3://b/d1.parquet", 100);          // 100 rows
    let pd = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet"); // 10 deletes -> d1
    assert_eq!(expected_rows_after_deletes(&[d.clone()], &[pd]), Some(90));
}
#[test]
fn expected_rows_ambiguous_on_equality_delete() {
    let d = data_file("s3://b/d1.parquet", 100);
    let ed = eq_delete_file("s3://b/ed1.parquet", 5);     // equality delete, no referenced file
    assert_eq!(expected_rows_after_deletes(&[d], &[ed]), None);
}
```

Add `data_file`, `pos_delete_file`, `eq_delete_file` test builders using `DataFileBuilder::default()` with `.content(DataContentType::PositionDeletes)`, `.referenced_data_file(Some(path))`, `.content(DataContentType::EqualityDeletes)`, `.equality_ids(vec![1])` respectively.

- [ ] **Step 2: Run it, expect FAIL** (function not defined): `cargo test -p sqe-coordinator --lib expected_rows`.

- [ ] **Step 3: Implement:**

```rust
/// Rows expected after applying deletes to `group`, or None when it cannot be
/// computed exactly. Only position-delete files whose `referenced_data_file`
/// points into the group are attributable; equality deletes and unattributable
/// position deletes make the exact count unknowable (fall back to <= check).
fn expected_rows_after_deletes(group: &[DataFile], live_deletes: &[DataFile]) -> Option<u64> {
    use std::collections::HashSet;
    let group_paths: HashSet<&str> = group.iter().map(|f| f.file_path()).collect();
    let base: u64 = group.iter().map(|f| f.record_count()).sum();
    let mut deleted: u64 = 0;
    // Dedupe delete files by path.
    let mut seen: HashSet<&str> = HashSet::new();
    for d in live_deletes {
        if !seen.insert(d.file_path()) { continue; }
        match d.content_type() {
            DataContentType::PositionDeletes => match d.referenced_data_file() {
                Some(ref_path) if group_paths.contains(ref_path.as_str()) => {
                    deleted += d.record_count();
                }
                Some(_) => {} // references a file outside the group; not our concern
                None => return None, // unattributable position delete -> ambiguous
            },
            DataContentType::EqualityDeletes => return None, // value-based, ambiguous
            DataContentType::Data => {}
        }
    }
    Some(base.saturating_sub(deleted))
}
```

- [ ] **Step 4: Run tests, expect PASS.**

- [ ] **Step 5:** Wire the cross-check into `rewrite_group` (or the caller, which has `live_deletes`). Replace the old `added_rows == removed_rows` block (:656-663) with:

```rust
// Per-group delete accounting. When the exact post-delete count is known
// (position deletes with referenced_data_file), assert the writer produced
// exactly that many rows. Otherwise (equality / unattributable) fall back to
// the looser bound: we can never write MORE rows than we read.
match expected_rows_after_deletes(&group, live_deletes) {
    Some(expected) if rows_written != expected => {
        return Err(SqeError::Execution(format!(
            "compaction delete-accounting mismatch: wrote {rows_written}, expected {expected} \
             after applying position deletes; aborting before commit")));
    }
    _ => {
        let base: u64 = group.iter().map(|f| f.record_count()).sum();
        if rows_written > base {
            return Err(SqeError::Execution(format!(
                "compaction wrote {rows_written} rows from {base} input rows; deletes cannot \
                 increase rows; aborting before commit")));
        }
    }
}
```

Note: the assertion is per group, so pass `live_deletes` down to `rewrite_group` (or compute the check in the results loop where both `group` and `live_deletes` are in scope). Prefer computing per group inside `rewrite_group` so the abort happens before any commit.

- [ ] **Step 6:** `cargo test -p sqe-coordinator --lib` — green.
- [ ] **Step 7:** Commit: `git commit -m "feat(compaction): delete-accounting cross-check guard"`

---

## Task 4: Remove fully-covered position delete files

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs`

**Interfaces:**
- Produces: pure `fn covered_position_deletes(removed_data_paths: &HashSet<String>, live_deletes: &[DataFile]) -> Vec<DataFile>` — position delete files whose `referenced_data_file` is entirely within the removed data set. Equality deletes are never returned (aged out via `drop_delete_files_older_than`).

- [ ] **Step 1: Write the failing unit test:**

```rust
#[test]
fn covered_position_deletes_selects_only_referenced() {
    let mut removed = std::collections::HashSet::new();
    removed.insert("s3://b/d1.parquet".to_string());
    let pd_in  = pos_delete_file("s3://b/pd1.parquet", 10, "s3://b/d1.parquet");
    let pd_out = pos_delete_file("s3://b/pd2.parquet", 5,  "s3://b/d2.parquet");
    let ed     = eq_delete_file("s3://b/ed1.parquet", 3);
    let got = covered_position_deletes(&removed, &[pd_in.clone(), pd_out, ed]);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].file_path(), "s3://b/pd1.parquet");
}
```

- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement** (dedupe by path; select `PositionDeletes` whose `referenced_data_file` ∈ removed set).
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5:** In `rewrite_data_files`, after collecting `old_files` (the removed data files), compute `covered = covered_position_deletes(&removed_paths, &live_deletes)` and add them to the commit: `.delete_files(old_files.into_iter().chain(covered).collect::<Vec<_>>())`. (`.delete_files` routes deletes into `removed_delete_files`.)
- [ ] **Step 6:** `cargo test -p sqe-coordinator --lib` — green. Commit: `git commit -m "feat(compaction): drop fully-covered position delete files on rewrite"`

---

## Task 5: VALIDATION CHECKPOINT — resurrection test green on the live stack

**This is Phase 2's real deliverable. Do NOT start Phase 3 until this passes.**

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/rewrite_data_files_deletes.rs`

- [ ] **Step 1:** `rewrite_preserves_deletes` currently passes because the Phase 1 guard skips. Update its assertion: after `rewrite_data_files`, the status must NOT be `skipped`, the deleted rows must stay gone (`count_rows` excludes them), and live data-file count must drop. Keep `guard_skips_table_with_deletes` renamed/repurposed or delete it (the guard is gone) — replace with `rewrite_applies_position_deletes` asserting the post-rewrite scan omits deleted rows.
- [ ] **Step 2:** Ensure the stack is up: `docker compose -f docker-compose.test.yml up -d && ./scripts/bootstrap-test.sh`.
- [ ] **Step 3:** Run the delete-safety integration tests:

```bash
RUST_MIN_STACK=8388608 cargo test -p sqe-coordinator --test it \
  rewrite_data_files_deletes -- --ignored --test-threads=1
```

Expected: PASS. Deleted rows stay deleted; file count drops.

- [ ] **Step 4:** Also re-run `rewrite_data_files_real -- --ignored` to confirm the CoW bin-pack path is unbroken.
- [ ] **Step 5:** If either fails, STOP and debug — do not proceed. Commit test updates: `git commit -m "test(compaction): delete-applying rewrite integration coverage"`

---

## Task 6: Resilience — memory gating, partial progress, conflict retry

**Files:**
- Modify: `crates/sqe-coordinator/src/maintenance.rs`

- [ ] **Step 1: Memory gating.** Before the group fan-out, and the handler has access to the shared runtime pool (via `self.runtime` / `self.config`), check `crate::memory::check_pressure(&pool)`. At `Red`, return a `summary_batch` with `status = "skipped: memory pressure (red); retry when load subsides"` instead of proceeding. (If the handler does not already hold the runtime pool, thread it in from `QueryHandler`; verify the field before wiring.)
- [ ] **Step 2: Partial progress.** Change the single-transaction-for-all-groups commit (:690-709) to commit per group (or per small batch of groups) so a mid-run conflict only loses the uncommitted groups. Each per-group commit uses its own `Transaction::new(&table)` with the same `set_new_data_file_sequence_number(seq_at_start)`.
- [ ] **Step 3: Conflict-aware retry.** Wrap the plan→commit sequence in a bounded retry loop (e.g. 3 attempts, exponential backoff). On a retryable error (`classify_commit_error` message contains "retryable"), RE-LOAD the table, RE-CAPTURE `seq_at_start` from the fresh snapshot, RE-PLAN `plan_delete_aware_read`, RE-COLLECT data/delete files, and retry. The seq pin MUST come from the fresh load each attempt (stale pin reopens the concurrent-delete hole).
- [ ] **Step 4:** `cargo test -p sqe-coordinator --lib` — green.
- [ ] **Step 5:** Re-run the integration checkpoint (Task 5 Step 3) — still green.
- [ ] **Step 6:** Commit: `git commit -m "feat(compaction): memory gating, partial-progress commits, conflict retry"`

---

## Task 7: Parser args — `delete_file_threshold` and `rewrite_all`

**Files:**
- Modify: `crates/sqe-sql/src/procedures.rs` (`RewriteDataFiles` variant + arg parsing)
- Modify: `crates/sqe-coordinator/src/maintenance.rs` (eligibility + handler signature)
- Modify: classifier/handler dispatch if the arg list changes the call shape.

- [ ] **Step 1:** Find the existing `RewriteDataFiles` parse + the accepted arg set (`target_file_size_bytes`, `min_input_files`, `max_concurrent_file_group_rewrites`). Add optional `delete_file_threshold => N` (usize) and `rewrite_all => true` (bool). Unknown args must still fail fast (existing behavior).
- [ ] **Step 2:** Add parser unit tests mirroring the existing ones for the new args (accepted, typo rejected).
- [ ] **Step 3:** Thread the two args through the handler into `rewrite_data_files`. Eligibility change: a data file at/above target size becomes eligible when the count of live delete files referencing it (via `referenced_data_file`) `>= delete_file_threshold`; `rewrite_all => true` makes every live data file eligible regardless of size.
- [ ] **Step 4:** `cargo test -p sqe-sql --lib` and `cargo test -p sqe-coordinator --lib` — green.
- [ ] **Step 5:** Commit: `git commit -m "feat(compaction): delete_file_threshold and rewrite_all rewrite args"`

---

## Task 8: Docs

**Files:**
- Modify: `docs/site/book/src/sql-reference/procedures.md`
- Modify: `docs/site/book/src/design-notes/mor-vs-cow.md`

- [ ] **Step 1:** `procedures.md`: replace the Phase 1 "skips MoR tables with live delete files" safety note with the delete-applying behavior; document the new `delete_file_threshold` / `rewrite_all` args and the sequence-number-pin conflict semantics.
- [ ] **Step 2:** `mor-vs-cow.md:96-101`: replace the "not yet automatic / do not rely on rewrite_data_files" limitation with the corrected statement that `rewrite_data_files` now applies deletes and consolidates MoR tables safely.
- [ ] **Step 3:** Emdash/voice check: `grep -n '—' docs/site/book/src/sql-reference/procedures.md docs/site/book/src/design-notes/mor-vs-cow.md` — zero hits in prose.
- [ ] **Step 4:** Commit: `git commit -m "docs(compaction): document delete-applying rewrite_data_files"`

---

## Self-Review

- Spec coverage: P2 (delete-applying scan) = Task 2; invariant change = Task 3; delete-file removal = Task 4; seq pin = Task 2 Step 5; multi-stream = Task 2 (streaming + existing `buffer_unordered`); resilience (partial progress, retry, memory gate) = Task 6; parser args = Task 7. All mapped.
- Advisor refinements folded in: real cross-check guard (Task 3, not the tautological check); seq re-capture per retry (Task 6 Step 3); validation checkpoint before Phase 3 (Task 5); correctness core isolated as its own commit (Tasks 1-4) before resilience (Task 6).
- Type consistency: `expected_rows_after_deletes`, `covered_position_deletes`, `collect_live_delete_files`, `plan_delete_aware_read`, `rewrite_group(plan, group, ...)` names used consistently.
- Open verification at implementation time: exact `ScanResult` schema accessor (Task 2 Step 2 note); whether `MaintenanceHandler` holds the runtime memory pool (Task 6 Step 1 note); exact `DataFileBuilder` setters for `referenced_data_file`/`equality_ids`/`content` (Task 3 Step 1).
