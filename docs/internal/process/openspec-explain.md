# OpenSpec: EXPLAIN / EXPLAIN ANALYZE / EXPLAIN FULL

## Proposal

**Status:** Implemented
**Phase:** 2 (post-core)

Add three query-plan inspection commands that expose DataFusion's planning
and Iceberg's table statistics. All three are policy-aware: the plan shown
is the plan that actually executes after security enforcement.

## Variants

| Command | Executes? | Output |
|---|---|---|
| `EXPLAIN <query>` | No | `plan_type`, `plan` (logical + physical plan text) |
| `EXPLAIN ANALYZE <query>` | Yes | `step`, `operation`, `output_rows`, `elapsed_ms` |
| `EXPLAIN FULL <query>` | No | `step`, `operation`, `estimated_rows`, `estimated_bytes`, `files_scanned`, `files_total` |

## Design

### Parsing

`EXPLAIN` and `EXPLAIN ANALYZE` are parsed by sqlparser (classified as
`StatementKind::Utility` with the `analyze` flag extracted at routing time).
`EXPLAIN FULL` is pre-scanned before sqlparser and classified as
`StatementKind::ExplainFull(inner_sql)`.

### Policy Enforcement

All three handlers call `PolicyEnforcer::evaluate()` on the logical plan
before generating output. The plan shown reflects row filters and column
masks that will be applied at execution time.

### Iceberg Statistics (EXPLAIN FULL)

Statistics are read from the Iceberg snapshot summary (`total-records`,
`total-files-size`, `total-data-files`) without reading data files. Since
`IcebergScanExec` has no predicate-pushdown-to-file-level yet, `files_scanned`
equals `files_total`.

## Implementation

- `crates/sqe-sql/src/classifier.rs` — `StatementKind::ExplainFull`
- `crates/sqe-coordinator/src/explain.rs` — `ExplainHandler`
- `crates/sqe-coordinator/src/query_handler.rs` — routing wired up

## Specs

| Scenario | Expected |
|---|---|
| GIVEN `EXPLAIN SELECT …` WHEN executed THEN returns 2 rows: `logical_plan` and `physical_plan` | ✅ |
| GIVEN `EXPLAIN ANALYZE SELECT …` WHEN executed THEN returns ≥1 row with `output_rows ≥ 0` | ✅ |
| GIVEN `EXPLAIN FULL SELECT … FROM iceberg_table` WHEN executed THEN scan row has non-NULL `files_total` | ✅ |
| GIVEN policy enforcement active WHEN EXPLAIN runs THEN plan reflects enforced plan | ✅ |
| GIVEN Iceberg snapshot missing WHEN EXPLAIN FULL runs THEN stats are NULL, no error | ✅ |
