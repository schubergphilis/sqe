# 501: MERGE with an empty source returns before evaluating WHEN NOT MATCHED BY SOURCE

- Scope: sqe-coordinator write path (write_handler MERGE, CoW and MoR)
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: open

## Problem

Both MERGE handlers short-circuit to "nothing to merge" when the source query
returned zero batches, before the clauses are classified.

`crates/sqe-coordinator/src/write_handler.rs:3510-3513` (CoW):

```rust
if source_batches.is_empty() {
    info!(table = %table_ident, "MERGE: source returned no data, nothing to merge");
    return Ok(vec![]);
}
```

`crates/sqe-coordinator/src/write_handler.rs:3955-3958` (MoR) is identical.
Clause classification happens later (`write_handler.rs:3562` and `:3992`), and
the `by_source` class is fully supported: `merge_sql.rs:65,153,201-205` and the
BY SOURCE loop at `write_handler.rs:4144-4204`.

A `WHEN NOT MATCHED BY SOURCE THEN DELETE` (or UPDATE) clause acts on target
rows with no source match. With an empty source every target row is
unmatched-by-source, so the correct result is to delete or update all of them.
The handler instead reports success with no change.

## Why it matters

Silent wrong result on a routine statement. The canonical use of NOT MATCHED
BY SOURCE is "make target equal to source" synchronisation; a source that
happens to be empty for one run (upstream extract returned nothing) should
empty the target and instead leaves it untouched while returning success and
`rows_affected` absent. Downstream consumers cannot detect it.

The early return also happens after the target has been fully read into the
pool-tracked buffer (`write_handler.rs:3436-3446`) and registered, so the cost
of the statement was already paid.

## Proposed change

1. Move the empty-source check after `classify_merge_clauses`. Return early only
   when `classified.by_source.is_empty()`.
2. When the source is empty but BY SOURCE clauses exist, register an empty
   `MemTable` for the source. The schema is not derivable from zero batches,
   so `handle_merge_dispatch` must receive the source plan's Arrow schema from
   `query_handler` (it already has `merge_source_logical`; use
   `plan.schema().as_arrow()`), or `execute_query` must return the schema next
   to the batches.
3. Apply the same fix to `handle_merge_equality`.

## Acceptance criteria

- Unit test (sqlite catalog or in-memory catalog): target with 3 rows, empty
  source, `WHEN NOT MATCHED BY SOURCE THEN DELETE` leaves 0 rows and returns
  `rows_affected = 3`, for both `write.merge.mode` values.
- Unit test: empty source with only MATCHED / NOT MATCHED clauses still
  returns without a commit (no new snapshot).
- `merge_sql` unit test asserting that classification runs before any
  emptiness shortcut (guard against regression by reordering).

## Related

- 514 (inconsistent `Ok(vec![])` vs `rows_affected` on no-op paths).
- 513 (test rig).
