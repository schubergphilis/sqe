# 202: MERGE applies policy to its source but not to the target rows it updates or deletes

- Scope: sqe-coordinator write path (write_handler MERGE), sqe-policy write_predicates
- Severity: high
- Category: security
- Effort: M (days)
- Status: fixed (branch `fix/authz-policy-enforcement-p1`, 2026-09-05)
- Issue: #555

## Problem

DELETE and UPDATE splice the caller's row filter and column masks into their
per-batch evaluator via `compute_write_predicates`:

```
crates/sqe-coordinator/src/write_handler.rs
:2101 handle_delete            -> :2128 compute_write_predicates
:2341 handle_delete_mor        -> :2371 compute_write_predicates
:2519 handle_delete_equality   -> :2559 compute_write_predicates
:2796 handle_update            -> :2822 compute_write_predicates
:3088 handle_update_equality   -> :3130 compute_write_predicates
```

The MERGE handlers do not:

```
:3317 handle_merge
:3702 handle_merge_dispatch
:3840 handle_merge_equality
```

A search of lines 3317 to 4300 for `policy`, `enforce` or
`compute_write_predicates` finds nothing. The only enforcement MERGE receives
is on its SOURCE query, which runs through `execute_query` in
`crates/sqe-coordinator/src/query_handler.rs:2050-2070` and therefore through
the plan rewriter.

INSERT and CTAS likewise run only the source through `enforce_source_plan`
(`write_handler.rs:1260`, `:1498`); there is no equivalent of PostgreSQL's
`WITH CHECK` on the rows written.

## Why it matters

A user with a row filter `region = 'EU'` on `sales.orders` can run

```sql
MERGE INTO sales.orders t USING (SELECT 42 AS id) s ON t.id = s.id
WHEN MATCHED THEN DELETE;
```

and delete a US row they cannot SELECT, because the target match is evaluated
over the unfiltered target file scan. `WHEN MATCHED THEN UPDATE SET amount = 0`
does the same for update. The same user can also INSERT rows into partitions
their filter hides, then never see them again. This is a write-side privilege
escalation the read-side model does not protect against, and it is
inconsistent with DELETE/UPDATE which already gate the target.

## Proposed change

1. In `handle_merge_dispatch` (`write_handler.rs:3702`), call
   `compute_write_predicates` for the target table exactly as `handle_update`
   does, and AND `row_filter_sql` into the `ON` match predicate before the
   target scan is built. Apply `column_mask_sqls` to target column references
   in the `ON` and `WHEN` conditions so masked values cannot be matched on.
2. Thread the returned `PolicySummary` into the MERGE audit entry alongside
   the source summary (currently only the source summary is recorded,
   `query_handler.rs:2066-2068`).
3. Decide and document INSERT semantics: either evaluate the target row filter
   over the incoming batches and reject rows that fail it (PostgreSQL `WITH
   CHECK` behaviour), or state in `docs/site/book/src/security/` that INSERT
   is not row-gated. Recommendation: enforce, since a hidden-row INSERT is
   indistinguishable from data loss to the user.

## Acceptance criteria

- Integration test (`make test-integration FILTER=merge_row_filter`): table
  with EU and US rows, user filtered to EU. `MERGE ... WHEN MATCHED THEN
  DELETE` targeting a US key leaves the US row intact; the same MERGE against
  an EU key deletes it. Same for UPDATE.
- The audit entry for the MERGE reports `row_filters_applied >= 1`.
- `grep -n compute_write_predicates crates/sqe-coordinator/src/write_handler.rs`
  shows a call inside the MERGE dispatch.

## Related

- 201 (the key used by `compute_write_predicates` must also be fixed).
- memory note "Write-path memory safety" tracks MERGE internals; this is the
  authorization gap, not the memory one.

## Resolution (2026-09-05)

MERGE now folds the target's row filter and column masks into the merge SQL (`crates/sqe-coordinator/src/merge_sql.rs`). `target_relation` wraps the target in a derived table that adds `__sqe_merge_admit` (the row filter) and `__sqe_masked_<col>` columns; `guarded_on` and `admit_guard` make hidden target rows unmatched for UPDATE/DELETE and for NOT MATCHED BY SOURCE, and `apply_target_masks` rewrites target column references in ON, WHEN conditions and SET expressions to the masked columns. Both the CoW path (`handle_merge`) and the MoR path (`handle_merge_equality`) compute the target policy and thread its summary into the audit record. Semantics follow PostgreSQL RLS: hidden rows are invisible to the statement and pass through unchanged. Tests: `target_policy_guards_on_by_source_and_masks_user_references`, `apply_target_masks_respects_boundaries_quotes_and_case` and the end-to-end `target_row_filter_protects_hidden_rows_end_to_end`.
