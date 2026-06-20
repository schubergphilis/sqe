# Views bypass policy: filters, masks, and restrictions skipped through a view

- **ID:** view-bypass-policy
- **Pillar:** Security
- **Severity:** P0/Critical
- **Status:** Resolved (WA review batch 5). The original diagnosis assumed DataFusion's `InlineTableScan` analyzer runs during optimization (after `evaluate`). On DataFusion 54 that rule was removed and view inlining moved into `LogicalPlanBuilder` at `ctx.sql` planning time, so `SELECT * FROM v` already produces `Projection -> SubqueryAlias(v) -> TableScan(base)` BEFORE `evaluate`. The rewriter therefore sees and governs the base `TableScan`. Verified by `crates/sqe-policy/tests/view_bypass_policy.rs` (row filter + mask apply through single, projecting, and nested views). Defense-in-depth added: if the rewriter ever encounters a `TableScan` whose provider is still a `ViewTable` (a hand-built scan or any residual un-inlined view), it now fails closed (deny `lit(false)`) instead of governing by the ungoverned view name. A separate UX gap (restricted column + `SELECT *` errors instead of a clean deny) is filed as MED-restricted-column-select-star-ux.
- **Files:** crates/sqe-coordinator/src/query_handler.rs:1749-1750; crates/sqe-catalog/src/schema_provider.rs:372; crates/sqe-policy/src/plan_rewriter.rs:91-100

## Problem
`PolicyEnforcer::evaluate` runs on `df.logical_plan()`, the UNOPTIMIZED plan, before DataFusion's `InlineTableScan` analyzer expands views. SQE views are DataFusion `ViewTable`s. At evaluate time `SELECT * FROM v` is a single `TableScan(ViewTable)`. The rewriter keys policy by the view name, which is usually ungoverned, and never descends into the view body. The base table's row filters, column masks, and column restrictions are ALL skipped.

This is a trivial governance bypass: an analyst with no grant on a governed table reads it raw through a view. THIS IS THE TOP PRIORITY.

## Proposed fix
Two options:
(a) Run the analyzer's `InlineTableScan` to expand `ViewTable`s into base scans before evaluate, then rewrite the inlined plan.
(b) In the rewriter, detect `TableScan` sources that are `ViewTable`, recurse into `ViewTable::logical_plan()`, rewrite, and rewrap.

Until fixed, consider denying queries whose resolved scan is a view over a governed table. This needs a dedicated branch plus view+policy integration tests because of the high blast radius on the core query path.

## Acceptance criteria
Base table `t` has a row filter plus a mask. After `CREATE VIEW v AS SELECT * FROM t`, running `SELECT * FROM v` as the policy-targeted user applies the same filter and mask as `SELECT * FROM t`.
