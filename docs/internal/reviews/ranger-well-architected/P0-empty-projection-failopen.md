# Empty mask/restrict projection returns the raw TableScan (fail-open)

- **ID:** empty-projection-failopen
- **Pillar:** Security
- **Severity:** P0/Critical
- **Status:** Resolved in commit 722d1b2
- **Files:** crates/sqe-policy/src/plan_rewriter.rs:285

## Problem
When every column of a table is in `restricted_columns`, the mask/restrict projection list comes out empty. The old `if !exprs.is_empty()` guard skipped the projection entirely and `builder.build()` returned the raw `TableScan`. The result exposed all columns instead of denying access.

The hole is reachable two ways: a single-column table where that column is restricted, or a table where all columns are unmappable-tagged. In both cases the user reads every column raw, which is the worst possible outcome for a column-restriction control.

## Proposed fix
When the projection expression list is empty, inject `builder.filter(lit(false))` so the plan denies (returns zero rows) rather than building over the raw scan.

## Acceptance criteria
A rewriter test where a fully-restricted table returns zero rows, not the raw scan. Added: `rewriter_integration.rs` test `all_columns_restricted_denies_instead_of_leaking`.
