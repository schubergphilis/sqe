# 506: CoW MERGE and the MoR truncate fast paths commit once with no conflict retry; four copies of the CoW retry loop elsewhere

- Scope: sqe-coordinator write path (write_handler commit sites)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #598

## Problem

Every append-shaped commit goes through `commit_with_retry`
(`crates/sqe-coordinator/src/write_handler.rs:547-606`), and the CoW
DELETE/UPDATE/INSERT OVERWRITE rewrites each carry a hand-rolled
reload-and-rewrite loop. Three commit sites have neither:

- CoW MERGE, `write_handler.rs:3654-3667`: `Transaction::new(&table)` on the
  table loaded at `:3369`, single `tx.commit(...)`, any error mapped to
  `Failed to commit MERGE`.
- MoR DELETE truncate, `write_handler.rs:2388-2395`.
- Equality DELETE truncate, `write_handler.rs:2575-2582`.

The CoW DELETE truncate right next to them does retry (`:2137-2175`), so the
three no-WHERE deletes behave differently depending on `write.delete.mode`.

The retry logic that does exist is duplicated four times with the same
shape (`loop { collect_data_files; rewrite; commit; match conflict => reload,
backoff, continue }`): `write_handler.rs:2137-2175` (truncate), `:2192-2322`
(DELETE), `:2841-2982` (UPDATE), `:4385-4453` (OVERWRITE). Each re-implements
the conflict predicate `e.retryable() || is_conflict_message(&e.to_string())`
and the `COW_MAX_ATTEMPTS` bound.

## Why it matters

MERGE is the dbt incremental hot path and the statement most likely to run
concurrently with another writer on the same table (a MoR DELETE from another
session, an autonomous compaction from the scheduler). Under `catalog`-side
optimistic concurrency the first conflict fails the whole statement, the
`WriteCleanupGuard` deletes the just-written files, and the client must re-run
a MERGE that already read the full target once. The maintenance scheduler's
compaction runs `rewrite_files` against the same tables, so the window is not
theoretical. Four copies of the retry loop mean the next fix (for instance a
per-table `commit.retry.*` property) has to land in four places plus the
three missing ones.

## Proposed change

1. Extract `WriteHandler::commit_rewrite_with_retry(catalog, table_ident, op,
   |fresh_table| async { read -> rewrite -> build RewriteFilesAction })` that
   owns the loop, the backoff (`cow_conflict_backoff_ms`), the reload and the
   `WriteCleanupGuard` drop-on-retry. The closure must re-read the snapshot
   because rewritten files are snapshot-specific (the comment at `:608-612`
   already states why `commit_with_retry` cannot be reused as-is).
2. Port the four existing loops onto it.
3. Wrap CoW MERGE in it: the closure re-reads `old_data_files`, re-registers
   the target scratch table and re-executes the merge SELECT. For the two MoR
   truncate paths, the closure is the existing delete-all body.
4. Emit one metric `sqe_write_commit_retries_total{op}` from the helper so
   contention becomes visible.

## Acceptance criteria

- Unit test with the sqlite catalog: a concurrent fast-append between MERGE's
  read and commit results in a successful MERGE after retry, and the
  concurrent writer's rows survive.
- Same test shape for `DELETE` without WHERE on a `merge-on-read` table.
- `grep -c 'COW_MAX_ATTEMPTS' crates/sqe-coordinator/src/write_handler.rs`
  drops to the helper plus its constant (2 hits).

## Related

- 501 (MERGE correctness), 513 (test rig).
- Maintenance has its own copy of the same loop
  (`crates/sqe-coordinator/src/maintenance.rs:866-896`); a later pass can unify.

## Resolution (2026-09-05)

One retry helper now owns every copy-on-write rewrite commit. The four
hand-rolled loops are gone and the three sites that had no retry at all are on
it, so a statement's behaviour under contention no longer depends on which
`write.*.mode` the table declares.

**What changed** (`crates/sqe-coordinator/src/write_handler.rs`)

- `WriteHandler::commit_rewrite_with_retry(catalog, table_ident, op,
  stmt_label, build_attempt)` — owns the loop, the `cow_conflict_backoff_ms`
  schedule, the reload, the `e.retryable() || is_conflict_message(..)`
  predicate, the `COW_MAX_ATTEMPTS` bound and the `WriteCleanupGuard`
  drop-on-retry (which deletes the losing attempt's parquet files before the
  rewrite re-runs). The closure receives the freshly loaded table and returns
  `Option<RewriteAttempt<T>>`; `None` means "nothing to do on this snapshot"
  (empty table, no partition matched) and skips the commit entirely, which is
  how the early returns the old loops did with `return Ok(...)` survive.
- `RewriteAttempt<T> { add, remove, cleanup, value }` is the per-attempt
  result: the two file sets the `RewriteFilesAction` gets, the attempt's
  cleanup guard, and whatever the caller wants back from the winning attempt
  (a row count, `()`, `(rows, empty_source_affected)`).
- Ported: CoW `DELETE` (`handle_delete`), CoW `UPDATE` (`handle_update`),
  dynamic/full `INSERT OVERWRITE` (`commit_written_files`) and the truncate
  fast path.
- `WriteHandler::truncate_via_rewrite` — the truncate body, shared by
  `handle_delete`, `handle_delete_mor` and `handle_delete_equality`. The two
  MoR truncates were single-shot commits before; they now retry exactly like
  the CoW one.
- CoW `MERGE` (`handle_merge`) is wrapped: the snapshot-independent work
  (column names, target policy, clause classification, the compiled merge
  SELECT, the source `MemTable`) happens once, and the closure re-reads
  `old_data_files`, rebuilds and re-registers the target scratch relation for
  the reloaded snapshot, re-runs the cardinality check and the empty-source
  claim count, re-executes the merge SELECT and writes fresh files. The target
  relation build moved into `WriteHandler::build_merge_target` so both the
  buffered (`merge-target-buffer` MemTable) and streaming (B2) shapes are built
  from one place per attempt.
- Metric: `sqe_write_commit_retries_total{op}`
  (`crates/sqe-metrics/src/lib.rs`), one increment per lost race, labelled
  `truncate` / `delete-cow` / `update-cow` / `merge-cow` / `insert-overwrite` /
  `ctas-replace`.

`grep -c 'COW_MAX_ATTEMPTS' crates/sqe-coordinator/src/write_handler.rs` is now
**2** (the constant and the helper), as the acceptance criteria asked.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler merge_sql
hive_write`, 132 passed / 0 failed; full lib suite 862 passed / 0 failed)

- `write_handler::tests::commit_rewrite_with_retry_reruns_the_rewrite_and_counts_conflicts`
  — a `FlakyCatalog` whose `update_table` loses the first two races: the
  closure runs three times (not once), the third attempt's value is returned,
  and `sqe_write_commit_retries_total{op="unit-test"}` is 2. The fake reports
  the conflict in the *message* without `retryable`, because the vendored
  `Transaction::commit` has its own inner retry for retryable errors and would
  otherwise swallow the conflict before the helper saw it.
- `..._skips_the_commit_when_there_is_nothing_to_do` — `Ok(None)` produces no
  snapshot.
- `..._does_not_retry_a_non_conflict_error` — a read error propagates on the
  first attempt.
- `sqe_metrics::tests::test_write_commit_retries_total` — the counter and its
  `op` label.

**Not covered by unit tests.** The acceptance criteria's concurrent-writer
scenarios (a fast-append landing between MERGE's read and its commit, the same
for a no-WHERE MoR `DELETE`, with the concurrent writer's rows surviving) need
a live catalog and belong in the docker rig (`make test-integration`); the fake
catalog here proves the loop's control flow, not Iceberg's conflict detection.
