# Restricted column produces a planner error on SELECT * / explicit reference (should be clean "permission denied")

- **ID:** restricted-column-select-star-ux
- **Pillar:** Operational Excellence
- **Severity:** Medium
- **Status:** Resolved. Decision: **restriction is a forced Nullify** (always-NULL). The rewriter now keeps a restricted column in the output schema and emits a typed NULL for it instead of dropping it. `SELECT *` and any explicit reference resolve and return NULL; the raw value is never returned; predicate pushdown on the real value is blocked (the value is a NULL expression, not a column). This removes the cryptic `FieldNotFound`, is Snowflake-aligned, and is BI-tool friendly. Restriction wins over a mask on the same column. Implemented in `crates/sqe-policy/src/plan_rewriter.rs`; tests in `rewriter_integration.rs` (`restricted_column_is_nulled_not_dropped`, `all_columns_restricted_returns_all_nulls_no_leak`, `unmappable_tag_restricts_column_fail_closed`) and `view_bypass_policy.rs` (`*_restricted_column_is_nulled`). Note: restriction is now semantically close to a Nullify mask; the distinction is that it is policy-assigned as a denial rather than a value transform.
- **Files:** `crates/sqe-policy/src/plan_rewriter.rs` (the restriction projection)

## Problem
A restricted column is dropped from the scan projection by the rewriter. If the user's query references that column, directly (`SELECT ssn`) or via `SELECT *` (which the SQL planner expands to include `ssn` before the rewriter runs), the outer reference no longer resolves and execution fails with a cryptic `type_coercion` / `FieldNotFound` error.

The behavior is fail-closed: no data leaks, the query errors. It matches PostgreSQL column-level security in spirit (you cannot `SELECT *` over a column you have no privilege on). But the error is opaque (`No field named ... ssn`) instead of a clear "permission denied for column ssn", and it surprises users who expect either the column to be silently omitted or a clean authorization error.

This was surfaced by the view-bypass regression suite (`crates/sqe-policy/tests/view_bypass_policy.rs`), which now asserts the fail-closed behavior rather than a silent drop.

## Proposed fix
Decide the intended semantics (a product decision) and implement one of:
1. **Clean error**: detect references to restricted columns above the scan during/after rewrite and return a `permission denied for column <name>` error instead of letting the planner fail with `FieldNotFound`.
2. **Silent omit for `SELECT *`**: make `*` expansion restriction-aware so a restricted column is excluded from `*` (requires policy knowledge at planning time, an architectural change since policy currently runs after planning).
3. **Forced NULL**: keep the column in the schema but always return NULL (treat restriction as a forced Nullify mask), so references resolve and yield NULL. Changes "invisible" to "always null".

Option 1 is the smallest honest improvement and keeps the current drop semantics.

## Acceptance criteria
`SELECT ssn FROM t` (ssn restricted) and `SELECT * FROM t` return a clear authorization error (or, per the chosen semantics, omit/NULL the column) rather than a `FieldNotFound` planner error. No raw value is ever returned.
