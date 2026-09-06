# 506: CoW MERGE and the MoR truncate fast paths commit once with no conflict retry; four copies of the CoW retry loop elsewhere

- Scope: sqe-coordinator write path (write_handler commit sites)
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: open
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
