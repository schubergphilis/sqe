# 501: MERGE with an empty source returns before evaluating WHEN NOT MATCHED BY SOURCE

- Scope: sqe-coordinator write path (write_handler MERGE, CoW and MoR)
- Severity: high
- Category: reliability
- Effort: S (hours)
- Status: fixed (branch `fix/data-consistency-p1`, 2026-09-05)
- Issue: #593

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

## Resolution (2026-09-05)

Both MERGE handlers now classify the clauses first and only shortcut an empty
source when the statement has no `WHEN NOT MATCHED BY SOURCE` clause.

**What changed**

- `crates/sqe-coordinator/src/merge_sql.rs`
  - `empty_source_is_no_op(&ClassifiedMerge)` — the shortcut predicate. It
    takes the *classification result*, so it cannot be evaluated before
    `classify_merge_clauses` has run; the ordering the fix depends on is
    carried by the type system rather than by a comment.
  - `empty_source_claim_count_sql(&ClassifiedMerge, &MergeNames, &str)` —
    `SELECT COUNT(*)` over the target relation filtered by the union of the BY
    SOURCE clause predicates plus the caller's row filter. With an empty source
    nothing matches and nothing is inserted, so BY SOURCE is the only class
    that can change a row and the union is exact under first-match-wins.
- `crates/sqe-coordinator/src/write_handler.rs`
  - `handle_merge` (CoW) and `handle_merge_equality` (MoR) take a new
    `source_schema_hint: Option<arrow_schema::SchemaRef>`. Zero batches carry
    no schema, so the hint is what lets an empty source be registered as a
    typed `MemTable` (`vec![vec![]]`) and the merge SQL be built at all. With
    no hint both handlers fall back to the historical no-op.
  - The `source_batches.is_empty()` early return moved below
    `classify_merge_clauses` in both handlers and now returns
    `affected_rows_batch(0)` (514) instead of `vec![]`.
  - CoW reports the claimed-row count for the empty-source case instead of
    `total_rows`: `total_rows` is the number of rows *written*, which is 0
    after a BY SOURCE DELETE emptied the table.
  - `scalar_count` was hoisted out of `check_merge_cardinality` to module
    scope (with a `label` argument; the cardinality messages are unchanged) so
    the empty-source count can reuse it.
  - `handle_merge_dispatch` threads the hint through all seven call sites.
- `crates/sqe-coordinator/src/query_handler.rs` — one added `let` in the
  `StatementKind::Merge` arm derives the hint from the already-captured source
  plan (`merge_source_logical.as_ref().map(|p| Arc::new(p.schema().as_arrow().clone()))`)
  and passes it to `handle_merge_dispatch`.

**Tests** (`cargo test -p sqe-coordinator --lib -- write_handler merge_sql`,
112 passed / 0 failed)

- `merge_sql::tests::empty_source_shortcut_requires_classification` — the
  regression guard the acceptance criteria asked for: classification succeeds
  with zero source columns, and the shortcut is false with a BY SOURCE clause,
  true without one.
- `merge_sql::tests::empty_source_by_source_delete_empties_target_end_to_end` —
  3-row target, empty source `MemTable`, `WHEN NOT MATCHED BY SOURCE THEN
  DELETE`: the CoW merge SELECT returns 0 rows (the table is emptied), the
  claim-count query returns 3, and the MoR anti-join shape claims the same 3
  rows. Both merge modes, at the SQL level.
- `merge_sql::tests::empty_source_claim_count_honours_predicates_and_row_filter`
  — a predicated BY SOURCE clause claims 2 of 3 rows; an EU-only caller claims
  0 (202 interaction).
- `merge_sql::tests::empty_source_claim_count_is_none_without_by_source_clauses`
  — no count query when the statement is a genuine no-op.

**Not covered by unit tests.** The commit-level assertion ("no new snapshot"
for an empty source with only MATCHED / NOT MATCHED clauses) is by
construction — both handlers return before the write and the commit — but
asserting the snapshot count needs a live catalog, so it belongs in the docker
e2e suite (`crates/sqe-coordinator/tests/it/`), as does an end-to-end
`rows_affected = 3` over a real Iceberg table. The in-process harness in
`merge_sql.rs` covers the SQL semantics only.
