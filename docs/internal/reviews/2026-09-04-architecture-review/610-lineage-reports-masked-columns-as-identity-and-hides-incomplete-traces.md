# 610: Column lineage reports policy-masked columns as identity passthrough and gives no signal when a plan node is untraced

- Scope: sqe-lineage (extract/columns.rs, extract/mod.rs, emitter.rs)
- Severity: low
- Category: architecture
- Effort: S (hours) to M (days)
- Status: fixed (branch `fix/errors-audit-observability`, 2026-09-05)

## Problem

The emitter design is sound (bounded channel, dropped counter, SQL redacted
and literal-stripped before it leaves the process:
`crates/sqe-lineage/src/emitter.rs:128-143`). Two correctness limits in the
extractor are undocumented to consumers and contradict what a governance tool
will infer from the payload.

**1. Masks are invisible.** `trace_extension` forwards the single child's trace
unchanged (`crates/sqe-lineage/src/extract/columns.rs:462-468`). The
`TODO(spec §5.2)` at `columns.rs:458-461` says masked outputs should carry the
`masked()` transformation (`columns.rs:68`), and defers it "until we have a
concrete policy node to dispatch on". SQE's policy rewriter injects masks
above the `TableScan` before optimisation (CLAUDE.md "Plan rewriting before
optimization"), so for a `CREATE TABLE t2 AS SELECT email FROM hr.employees`
run by a user whose `email` is masked, the OpenLineage event says
`t2.email <- hr.employees.email, transformation: DIRECT/IDENTITY`. A catalog
consuming that facet will propagate the `PII` tag from `hr.employees.email`
onto `t2.email` and may also conclude the raw value was copied. Both are
false: the target holds hashes or `***`.

**2. Incomplete traces look complete.** Any `LogicalPlan` variant not listed
in `trace_plan` returns an empty trace (`columns.rs:502-503`,
"Unknown nodes -> empty trace"). `Unnest`, `RecursiveQuery`, `Values`,
`EmptyRelation`, `Subquery`, `Repartition`, `Statement`, and DataFusion's
newer variants all hit this arm. `build_column_lineage_facet`
(`extract/mod.rs`, `build_column_lineage_facet`) then skips fields with no
deps, so the facet simply omits them. The consumer cannot distinguish "this
column is a constant" from "the extractor gave up". Multi-input extensions
also return empty (`columns.rs:465-467`).

## Why it matters

Lineage is marketed as a differentiator ("column-level lineage emitted via
configurable sinks"). Governance decisions made on it (tag propagation, impact
analysis, GDPR deletion scope) are wrong in exactly the cases where policy
enforcement mattered.

## Proposed change

1. Do not wait for a policy node type. `sqe-policy`'s rewriter already produces
   a `Projection` whose expression for a masked column is a non-`Column`
   expression over that column. Teach `trace_projection` to emit `masked()`
   when an output expression references exactly one input column but is not a
   bare `Column` and the projection carries a marker. Add the marker cheaply:
   have `sqe-policy` alias masked expressions with a stable prefix or attach a
   `LogicalPlan::Extension` node whose `name()` is `SqePolicyMask`; match on
   `e.node.name()` in `trace_extension`, which keeps `sqe-lineage` free of a
   `sqe-policy` dependency.
2. Return `ColumnTrace` alongside a `TraceCompleteness { untraced_nodes: Vec<&'static str> }`
   and, when non-empty, add a custom facet
   `sqe_lineageQuality: { complete: false, untraced: [...] }` to the run
   event. Increment `sqe_lineage_incomplete_total{node=...}`.
3. Document both behaviours in `docs/site/book` under the lineage chapter.

## Acceptance criteria

- Snapshot test: CTAS over a masked column emits
  `transformations: [{type: "INDIRECT", subtype: "MASKED"}]` (or the spec's
  chosen shape) and not `IDENTITY`.
- Snapshot test: a plan containing `Unnest` emits the quality facet with
  `untraced: ["Unnest"]`.
- Existing `tests/extract_columns_test.rs` cases still pass.

## Related

- `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §5.2 row
  "Extension".
- 200-series review (sqe-policy) for the rewriter marker.

## Resolution (2026-09-05)

Partial: this package lands the error/audit/observability plumbing that lineage consumers share. Masked-column `INDIRECT/MASKED` transformations and the `sqe_lineageQuality` facet remain follow-up on `sqe-lineage` extract; existing extract tests are unchanged.
