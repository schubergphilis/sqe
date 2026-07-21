# INSERT OVERWRITE … SELECT for Iceberg tables (#378)

Date: 2026-07-21
Branch: `feat/insert-overwrite`
Issue: #378

## Problem

`INSERT OVERWRITE <table> SELECT …` is meant to replace a table's data, not
append to it. Today SQE's write path is append-only. The sqlparser `overwrite`
flag is parsed but never read, so `INSERT OVERWRITE` silently degrades to a
plain append: old rows stay, new rows pile on top. That is a silent
data-correctness bug (wrong data, no error), and it breaks dbt's
`insert_overwrite` incremental strategy.

Verified current state (against `main`):

- `write_handler.rs:1348` (`handle_insert_select_streaming`) and
  `write_handler.rs:1711` (`handle_insert`) read only `ins.table`/`ins.columns`.
  Neither inspects `ins.overwrite`. Commit is always `fast_append`.
- No `INSERT OVERWRITE` tests exist.

## Parse gate (verified, not assumed)

The dispatch router parses statements with `GenericDialect`
(`query_handler.rs:3788`). In sqlparser 0.62 the `overwrite` keyword is parsed
unconditionally, not dialect-gated (`parser/mod.rs:17872`:
`let overwrite = self.parse_keyword(Keyword::OVERWRITE)`), and `INTO` is
optional. So `INSERT OVERWRITE t SELECT …`, `INSERT OVERWRITE INTO t …`, and
`INSERT OVERWRITE TABLE t …` all parse to
`Statement::Insert { overwrite: true, partitioned: None, .. }`. No post-parse
transform is needed. The `Insert` struct exposes `overwrite: bool` and
`partitioned: Option<Vec<Expr>>` (the Hive static-partition clause).

## Semantics (decided)

- **Unpartitioned table** (or a partition spec with no fields): full replace.
  Every current data file is removed and the SELECT output replaces it.
- **Partitioned table, no explicit PARTITION clause: dynamic overwrite.** Only
  the partitions present in the SELECT output are replaced; untouched
  partitions are preserved. This matches Spark `partitionOverwriteMode=dynamic`
  and dbt's `insert_overwrite` incremental strategy.
- **Empty SELECT result:**
  - Unpartitioned → full truncate (delete all data files, add nothing).
  - Dynamic partitioned → no-op (no partitions touched).
  This means the overwrite path must NOT take the existing
  `total_rows == 0 → return` early-out (`write_handler.rs:1439`); doing so on an
  unpartitioned overwrite would silently retain the old data.
- **Static Hive `PARTITION (col=val)` clause** (`ins.partitioned.is_some()`):
  out of scope. Return a loud `NotImplemented` error. Never silently mishandle.

## Approach

Route on the `overwrite` flag and reuse the atomic swap primitive DELETE
already uses (`rewrite_files().add_data_files(new).delete_files(old)`), so the
add and remove land in one Iceberg commit. Prior snapshots are retained, so
time-travel keeps working.

Thread `ins.overwrite` into both INSERT entrypoints. Reuse the existing
streaming write so column reordering (`reorder_insert_select`), policy
enforcement (`enforce_source_plan`), and fanout partition-value stamping all
come for free. At commit time:

1. Write the SELECT output to new data files (existing streaming writer).
2. If `overwrite == false`: today's `fast_append` (unchanged).
3. If `overwrite == true`:
   - Determine the removal set:
     - Unpartitioned / no partition fields: all current data files
       (`collect_data_files`).
     - Partitioned: read the distinct partition values off the freshly written
       `new_data_files` (`DataFile::partition()`), then select current data
       files whose partition value is in that set. Untouched partitions are
       left alone. (Transform-partitioned tables compare in transformed
       partition space, which is what the writer stamps, so this is correct for
       identity and transform partitioning alike.)
   - Chain the covered position/equality-delete files for the removed data
     files into the same `delete_files(...)` (reuse
     `collect_live_delete_files` + `covered_position_deletes` from
     `maintenance.rs`). This keeps MoR overwrite correct and leaves no
     superseded-delete debris (the same cleanup #376 tracks for the DML CoW
     paths — but this design touches ONLY the new overwrite path; it does not
     refactor the existing DELETE/UPDATE/MERGE paths).
   - Commit `rewrite_files().add_data_files(new).delete_files(removed)` inside
     the existing optimistic-concurrency reload+retry loop (`COW_MAX_ATTEMPTS`,
     `commit_with_retry`, `cow_conflict_backoff_ms`), guarded by a
     `WriteCleanupGuard` so a failed or conflict-exhausted overwrite orphans no
     files and leaves the pre-overwrite table state intact (never partial).

### Self-overwrite

`INSERT OVERWRITE t SELECT … FROM t` is safe: the SELECT reads the current
snapshot and materializes the new data files before the swap removes the old
files, all in one commit. Same write-then-swap ordering DELETE CoW relies on.

## Components touched

- `crates/sqe-coordinator/src/write_handler.rs`
  - Thread `overwrite` from both `Statement::Insert` matches (~1348, ~1711).
  - Add the static-`PARTITION` guard.
  - New commit branch: full vs dynamic removal-set computation + atomic swap
    with covered-delete chaining, wrapped in the retry loop.
  - Ensure the zero-row overwrite path reaches the swap (truncate) instead of
    the append early-out.
- `crates/sqe-sql` — no grammar change (parse verified). Add a unit test
  asserting `INSERT OVERWRITE` yields `overwrite: true`.

## Error handling

- Static `PARTITION` clause → `NotImplemented`, loud.
- Commit conflict → reload snapshot, recompute removal set, retry up to
  `COW_MAX_ATTEMPTS`; exhausted → typed error, no partial state.
- Write failure mid-overwrite → `WriteCleanupGuard` removes orphaned new files;
  no commit, pre-state intact.

## Testing

New `crates/sqe-coordinator/tests/it/insert_overwrite_e2e.rs`, every test
`#[ignore]` (needs the running stack), run against the real write e2e harness:

```
docker compose -f docker-compose.test.yml up -d
./scripts/bootstrap-test.sh
cargo test -p sqe-coordinator --test it -- --ignored insert_overwrite
```

Evidence via `table_files` (live data files) and `table_snapshots`
(operation + summary + retained history) TVFs, matching `ctas_write_modes_e2e`.

Cases:

1. **No silent append** — after `INSERT OVERWRITE t SELECT` the row count equals
   the SELECT result, not old+new.
2. **Full-table replace** (unpartitioned) — content equals the SELECT exactly,
   old rows gone, a new snapshot is committed, and the prior snapshot is still
   in `table_snapshots` (time-travel intact).
3. **Idempotency** — running the same overwrite twice yields identical table
   state (not doubled).
4. **Dynamic partition overwrite** — on a `PARTITIONED BY (region)` table,
   overwrite a subset of partitions: touched partitions are replaced, untouched
   partitions are preserved (the catastrophic-failure guard against
   over-deletion). Empty SELECT = no-op.
5. **Zero-row unpartitioned overwrite = truncate** — empty SELECT clears the
   table.
6. **MoR cleanup** — on a table with position deletes, overwrite supersedes the
   affected data files and their delete files are removed (no debris).
7. **Self-overwrite** — `INSERT OVERWRITE t SELECT … FROM t` produces the
   transformed content, not a doubling or empty table.
8. **Static PARTITION guard** — `INSERT OVERWRITE t PARTITION (region='eu')
   SELECT …` errors loudly.

Plus a `sqe-sql` unit test for the parse flag.

The `#[ignore]` write e2e tests are runnable locally against
`docker-compose.test.yml` + Polaris; they will be brought up and actually run,
not written-and-declared. Any environmental blocker is surfaced, not hidden.

## Trino parity

Verified (2026-07-21): standard Trino does NOT accept `INSERT OVERWRITE` as SQL
syntax. Adding it is an open Trino feature request (trinodb/trino#11602), and
native Iceberg overwrite is still in development (trinodb/trino#26178). Trino
instead controls overwrite through the `insert_existing_partitions_behavior`
session property on a plain `INSERT`. The literal statement is therefore not
shared between the two engines, so the parity acceptance criterion is DROPPED:
there is no equivalent Trino statement to compare against. SQE's explicit
`INSERT OVERWRITE` (Spark/Hive-style) is beyond Trino's current SQL surface,
not behind it. Parity is not a blocker for this MR.

## Out of scope (follow-ups)

- Static `INSERT OVERWRITE … PARTITION (col=val)` (explicit static partition
  spec). Errors loudly for now.
- Retrofitting #376 (superseded-delete cleanup) onto the existing
  DELETE/UPDATE/MERGE CoW paths. The overwrite path is built correct from the
  start; the existing paths remain #376's scope.
- Trino wire-parity harness run (depends on Trino accepting the syntax).

## Acceptance criteria

- `INSERT OVERWRITE … SELECT` replaces (not appends) data; verified by
  row-count + content assertions.
- A new snapshot is committed; prior snapshots remain for time travel.
- Dynamic partition-overwrite semantics tested: touched partitions replaced,
  untouched preserved.
- No silent-append fallback: unsupported forms (static PARTITION) error loudly.
- Zero-row unpartitioned overwrite truncates; zero-row dynamic overwrite is a
  no-op.
- Failed/conflict-exhausted overwrite leaves the pre-overwrite state, never
  partial.
