# 200: Plan rewriter never visits TableScans inside subqueries

- Scope: sqe-policy (plan_rewriter), every SQL entry point
- Severity: high
- Category: security
- Effort: S (hours)
- Status: open

## Problem

`PolicyPlanRewriter::evaluate` collects scans with `LogicalPlan::apply` and
rewrites them with `LogicalPlan::transform_down`:

```rust
// crates/sqe-policy/src/plan_rewriter.rs:129
plan.apply(|node| {
    if let LogicalPlan::TableScan(scan) = node { ... }
```

```rust
// crates/sqe-policy/src/plan_rewriter.rs:334
let rewritten = plan
    .transform_down(|node| {
        if let LogicalPlan::TableScan(ref scan) = node {
```

Both walk `LogicalPlan::inputs()` only. A subquery in an expression
(`WHERE x IN (SELECT ...)`, `WHERE EXISTS (...)`, a scalar subquery in the
select list or in a CASE) is a `LogicalPlan` embedded inside an `Expr`, not an
input. DataFusion provides separate walkers for exactly this reason:

```
~/.cargo/registry/src/.../datafusion-expr-55.0.0/src/logical_plan/tree_node.rs
:796  /// Similarly to [`Self::apply`], calls `f` on this node and all its inputs,
:797  /// including subqueries that may appear in expressions such as `IN (SELECT
:799  pub fn apply_with_subqueries(...)
:834  pub fn transform_down_with_subqueries(...)
```

The rewriter runs on `df.logical_plan()` straight out of `ctx.sql()`
(`crates/sqe-coordinator/src/query_handler.rs:3113` and `:3481`), which is
before the optimizer's `DecorrelatePredicateSubquery` and
`ScalarSubqueryToJoin` passes turn subqueries into joins. So at rewrite time
the inner scan is still hidden inside the predicate.

No test in `crates/sqe-policy/tests/rewriter_integration.rs` or
`view_bypass_policy.rs` builds a plan with a subquery, join, union or CTE
(see the test list; every case is a single `TableScan`).

## Why it matters

Any governed table can be read raw through a subquery:

```sql
SELECT * FROM public.dim WHERE 1 = (SELECT count(*) FROM hr.salaries WHERE salary > 500000);
SELECT id FROM public.dim WHERE id IN (SELECT ssn FROM hr.people);
```

Row filters, masks and restrictions on `hr.salaries` and `hr.people` are not
applied because their scans were never seen. Restricted columns are readable,
masked columns are unmasked, and a `lit(false)` deny (breaker open, unknown tag
state) is silently absent. The `PolicySummary` for the audit log also
under-reports. This defeats the "no information leakage" model from CLAUDE.md
on every entry point (Flight SQL, Trino, MCP, Quack) since all of them share
this rewriter.

## Proposed change

In `crates/sqe-policy/src/plan_rewriter.rs`:

1. Replace `plan.apply(` at line 129 with `plan.apply_with_subqueries(`.
2. Replace `.transform_down(` at line 334 with
   `.transform_down_with_subqueries(`. Keep the `TreeNodeRecursion::Jump`
   return so the injected wrappers are not re-entered.
3. Make `resources_from_plan` in
   `crates/sqe-coordinator/src/audit_resources.rs:45` use
   `apply_with_subqueries` too so the audit resource list matches what was
   enforced.
4. Add a debug assertion helper (test only) that walks the rewritten plan with
   `apply_with_subqueries` and fails if any `TableScan` with a non-empty
   policy is not directly wrapped by the policy `Filter`/`Projection`.

## Acceptance criteria

- New tests in `crates/sqe-policy/tests/rewriter_integration.rs`, each run
  end-to-end through `SessionContext` so DataFusion plans the subquery:
  - `IN (SELECT ...)` against a table with a row filter returns only the
    filtered subset.
  - `EXISTS (SELECT ...)` against a table with a restricted column that the
    subquery reads yields the same result as reading a NULL column.
  - Scalar subquery in the select list over a masked column returns the
    masked value.
  - A subquery over a table whose store `resolve` errors yields zero rows
    (deny-all propagates).
  - A two-table JOIN and a UNION ALL where only one side is governed.
- `cargo test -p sqe-policy` passes; `make test-integration FILTER=policy`
  passes.

## Related

- 201 (policy key drift), 211 (negative test gaps).
- docs/internal/reviews/2026-07-25-production-readiness-review.md, section
  "Security" test matrix lists "view/subquery/DML paths" as untested; still
  true.
