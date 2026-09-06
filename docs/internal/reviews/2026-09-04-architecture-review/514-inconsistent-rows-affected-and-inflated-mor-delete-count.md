# 514: DML no-op paths return an empty result instead of `rows_affected = 0`, and MoR position deletes over-count already-deleted rows

- Scope: sqe-coordinator write path (write_handler DELETE/UPDATE/MERGE result shapes)
- Severity: low
- Category: reliability
- Effort: S (hours)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #606

## Problem

The DML handlers document that they return a one-row `rows_affected` batch
"consistent with ... the DELETE/UPDATE/MERGE handlers"
(`crates/sqe-coordinator/src/write_handler.rs:1414-1417`), and `affected_rows_batch`
exists for that (`:180-197`). Fifteen early returns hand back `Ok(vec![])`
instead: `write_handler.rs:2141, 2156, 2196, 2365, 2397, 2461, 2553, 2583,
2643, 2846, 3125, 3229, 3512, 3648, 3957, 4212`. Two nearby paths do it right
(`:2202` and `:2852` return `affected_rows_batch(0)` for the partition-pruned
no-match case), so the same statement returns different shapes depending on
whether pruning or the file walk found nothing.

The truncate paths (`:2141-2157`, `:2385-2397`, `:2572-2583`) drop every data
file and return `Ok(vec![])`, so `DELETE FROM t` never reports how many rows
it removed even though the manifest record counts are in hand.

The MoR position-delete path counts positions in a raw read of the file
(`:2417-2423` explains why the read must be raw) and reports
`deleted_count = position_deletes.len()` (`:2464`, `:2499`). Rows already
hidden by an earlier position delete are re-marked and counted again, so a
second `DELETE ... WHERE x = 1` on the same rows reports the same non-zero
count as the first.

## Why it matters

dbt, Trino JDBC and ADBC clients read the update count from the result; an
empty result set is rendered as "unknown" or 0 by different drivers, and
dbt's incremental logging shows different numbers for identical outcomes.
The inflated MoR count makes idempotent re-runs look like they changed data,
which defeats the simplest reconciliation check ("second run affects 0 rows").

## Proposed change

1. Replace every `return Ok(vec![])` in the DML handlers with
   `Ok(affected_rows_batch(n))`, where `n` is 0 for no-ops and the sum of
   `record_count()` over removed files for truncates.
2. In `handle_delete_mor`, subtract positions already present in live
   position-delete files for the same data file (the `read_plan` from
   `plan_delete_aware_read` already lists them per file), or compute the
   count from a delete-aware read and keep the raw read only for positions.
3. Add a `#[must_use]` helper `DmlOutcome::rows(n)` so a bare `vec![]` is a
   lint hit in review.

## Acceptance criteria

- `grep -n 'return Ok(vec!\[\]);' crates/sqe-coordinator/src/write_handler.rs`
  returns only CREATE TABLE / CTAS DDL sites.
- Test: `DELETE FROM t` on a 3-file table returns `rows_affected` equal to the
  table row count.
- Test: running the same MoR position DELETE twice returns N then 0.

## Related

- 501 (MERGE empty source also returns `vec![]`).

## Resolution (2026-09-05)

Every DML early return now goes through `affected_rows_batch`, the truncate
paths report the rows they removed, and the MoR position-delete count no longer
counts rows a live delete file already hides.

**What changed** — all in `crates/sqe-coordinator/src/write_handler.rs` unless
noted.

- Every `return Ok(vec![])` in a DML handler became `Ok(affected_rows_batch(n))`:
  `handle_insert` (empty SELECT), `handle_delete` (no data files, twice),
  `handle_delete_mor` (no data files, no live matches), `handle_delete_equality`
  (no data files, no matches), `handle_update` (no data files),
  `handle_update_equality` (no data files, no matches), `handle_merge` (no
  changes to commit, empty source), `handle_merge_equality` (nothing touched,
  empty source).
  `grep -n 'return Ok(vec!\[\]);' crates/sqe-coordinator/src/write_handler.rs`
  now hits exactly two lines, both `CREATE TABLE ... IF NOT EXISTS` DDL.
- New `data_file_record_count(&[DataFile])` sums the manifest `record_count`.
  The three truncate paths (`handle_delete`, `handle_delete_mor`,
  `handle_delete_equality`) report it, computed before
  `extend_with_superseded_deletes` consumes the file list so only *data* files
  are counted, never the delete files it appends. Documented caveat: on a table
  that still carries live deletes this over-reports by the number of delete
  records, and counting exactly would mean a delete-aware scan of a table that
  is about to be emptied.
- `affected_rows_batch` is `#[must_use]` and its doc comment now states the
  contract: every DML handler returns through it, a bare `vec![]` is an empty
  *result set* that ADBC / Trino JDBC / dbt each render differently, and only
  DDL legitimately returns no rows.
- MoR position deletes (`handle_delete_mor`): the raw, position-preserving scan
  stays as it is (positions address physical offsets), but the *count* now
  comes from `newly_deleted_row_count(raw_matches, live_matches)` per data
  file. `live_matches` is `Some(n)` from a second, delete-aware read of the
  same file — taken only when the file both carries delete files and had a raw
  match, so an ordinary MoR DELETE on a table with no deletes still reads each
  file once. `min` clamps a snapshot skew between the two reads. When the total
  is 0 the handler returns before writing a delete file at all, so re-running
  an idempotent DELETE no longer burns a snapshot re-marking hidden rows.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler merge_sql`,
112 passed / 0 failed)

- `write_handler::tests::newly_deleted_row_count_ignores_already_hidden_rows` —
  the counting function: no delete files → every raw match counts; a re-run
  where all matches are hidden → 0; partial overlap → only the visible rows;
  skew → clamped.
- `write_handler::tests::data_file_record_count_sums_manifest_counts`.

**Not covered by unit tests.** "`DELETE FROM t` on a multi-file table reports
the table row count" and "the same MoR position DELETE run twice reports N then
0" both need a real Iceberg table with committed data and delete files. There
is no in-process catalog harness in `write_handler.rs` (its tests drive the
per-batch helpers directly), so the unit tests sit at the counting-function
level as the finding allows, and the end-to-end pair belongs in
`crates/sqe-coordinator/tests/it/` (docker: Polaris + RustFS).
