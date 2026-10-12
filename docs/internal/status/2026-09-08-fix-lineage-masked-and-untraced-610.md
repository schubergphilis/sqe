**STATUS 2026-09-08 (night): finding 610 (#617) on `fix/lineage-masked-and-untraced-610`: column lineage reports masked source columns as MASKED and says when a trace is incomplete.**

`PolicySummary` gains `masked_fields: Vec<(table, column)>`, the scan
`TableReference` display plus column for every masked or restricted column
(a restricted column holds NULL, so it counts as masked); `summarise_policies`
fills it and `merge` folds it. The coordinator passes it into the lineage
`QueryCompleteCtx` / `QueryFailCtx` (new `masked_fields`), and the new
`extract::extract_lineage_governed` rewrites every column-lineage dependency
on one of those scan columns to `{DIRECT, MASKED, masking: true}`. The
decision comes from the enforcer's own record rather than from guessing at
projection shapes, and `sqe-lineage` stays free of a policy-crate dependency
(plain string tuples cross the boundary). Completeness: `trace_plan_reporting`
collects the plan node kinds the tracer has no rule for (`Unnest`,
`RecursiveQuery`, `Values`, multi-input extensions, ...) through a
thread-local active only during a reporting walk; when non-empty the run
carries the custom facet `sqe_lineageQuality: {complete: false, untraced:
[...]}`. The old `extract_lineage` entry point and every existing extract
test are unchanged. Book: operations/openlineage.md, new section "Policy and
completeness in column lineage". Tests: `extract_governed_test.rs` (masked vs
identity, namespace-only table key, `Unnest` facet, plain entry point
unchanged), summary test extended. Gates: sqe-lineage all binaries green,
sqe-policy 282, coordinator streaming tests, clippy clean. Not done from the
proposal: the `sqe_lineage_incomplete_total` counter (the emitter has no
metrics registry handle beyond the dropped counter; add when a lineage
metrics block exists).
**NEXT:** 627, #659 partials; the docker-capable CI runner for the
`polaris` group (807 item 3).
