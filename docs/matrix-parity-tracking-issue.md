# Tracking issue body: Iceberg matrix parity 31% -> 83%

Copy-paste the content below into the tracking issue (GitLab/GitHub) when filing. Title:

> **Iceberg compatibility matrix parity: lift SQE from 31% to 83%**

---

## Summary

SQE scores 58/189 (31%) on the [public Iceberg compatibility matrix](https://icebergmatrix.org/) rubric, tied with AWS Athena and below DuckDB. The matrix is the de facto engineer reference when evaluating Iceberg engines, so the score is visible to every evaluator. The openspec change [`iceberg-matrix-parity`](../openspec/changes/iceberg-matrix-parity/proposal.md) lifts SQE to 156/189 (83%) across 8 phases in 4 to 6 months, and submits SQE as an OSS entry to the public matrix.

## Target scoreboard

| Tag                   | Score    | % | Phase |
|---|---:|---:|---|
| v0.15.0 (baseline)    | 58/189   | 31 | start |
| v0.16.0-catalogs      | ~82/189  | 43 | A: catalog sweep |
| v0.17.0-maintenance   | ~94/189  | 50 | B: CALL system.* procedures |
| v0.18.0-branching     | ~106/189 | 56 | C: branches and tags |
| v0.19.0-v3-types      | ~122/189 | 65 | D: nanosec + defaults |
| v0.20.0-row-deltas    | ~132/189 | 70 | E: equality deletes |
| v0.21.0-puffin        | ~137/189 | 73 | F: bloom filters + stats |
| v0.22.0-cdc           | ~139/189 | 75 | G: incremental scan |
| v0.23.0-mor           | ~156/189 | 83 | H: MoR UPDATE/MERGE |

## What this unblocks

- SQE as a first-class entry on icebergmatrix.org (evaluator visibility)
- dbt-sqe incremental materialization via CDC range scan (Phase G)
- SF100 `trade_result_update_holding` unblock via MoR (Phase H)
- Broader catalog support (Glue, HMS, Nessie, Unity, JDBC, Hadoop)
- Spark/Flink-parity DML via equality deletes (Phase E)

## What is deferred

Variant, shredded Variant, geometry, vector type, multi-argument partition transforms, and lineage. Reasons and effort estimates are in the proposal's `design.md` Decision 5 and deferred notes.

## Links

- Proposal: `openspec/changes/iceberg-matrix-parity/proposal.md`
- Design: `openspec/changes/iceberg-matrix-parity/design.md`
- Tasks: `openspec/changes/iceberg-matrix-parity/tasks.md` (168 tasks)
- Full roadmap with upstream research: `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md`
- Branch workflow: `docs/matrix-parity-workflow.md`
- Public matrix: https://icebergmatrix.org/
- Matrix data source: https://github.com/Neuw84/iceberg-matrix

## Upstream tracked

- apache/iceberg-rust #2203 (RowDeltaAction, Phase E cherry-pick target)
- apache/iceberg-rust #1939 (branch/tag transaction wrapper, Phase C upstream)
- apache/iceberg-rust #2145 (ExpireSnapshotsAction wrapper)
- apache/datafusion #21157 (StatisticsSource trait, Phase F consumer gate)
- apache/datafusion #20746 (MERGE INTO plan, Phase H simplification)
- RisingWave iceberg-rust fork rebases (affects Phase E conflict risk)

## Labels

`area/iceberg`, `area/catalog`, `tracking`, `phase-1`
