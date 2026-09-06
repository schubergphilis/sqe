# 514: DML no-op paths return an empty result instead of `rows_affected = 0`, and MoR position deletes over-count already-deleted rows

- Scope: sqe-coordinator write path (write_handler DELETE/UPDATE/MERGE result shapes)
- Severity: low
- Category: reliability
- Effort: S (hours)
- Status: open
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
