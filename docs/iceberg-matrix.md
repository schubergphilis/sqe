# SQE Iceberg Compatibility Matrix

Current state of SQE against the [icebergmatrix.org](https://icebergmatrix.org) rubric, the de-facto reference engineers consult when picking an Iceberg engine. Data lives at [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix).

**Score: 99/189 (52.4%)**  |  **Target: 156/189 (83%)**

Last generated: 2026-04-24T16:30:00Z  |  Source: `Phase H MoR UPDATE/MERGE`

Regenerate: `python3 scripts/render-iceberg-matrix.py`. Source of truth: `docs/iceberg-matrix-state.json`.

---

## Legend

| Symbol | Level | Meaning |
|:---:|---|---|
| F | full | Verified end-to-end; no significant limitations |
| P | partial | Some functionality works; caveats apply |
| ? | unknown | Library primitives exist; no end-to-end verification |
| . | none | Not implemented; planned or deferred |

Each feature is scored against V2 and V3 of the Iceberg spec (63 cells total). Aggregate score weights: F=3, P=2, ?=1, .=0. Max 189.

---

## Peer rankings

| Engine | Score | % |
|---|---:|---:|
| AWS EMR (Spark 7.12) | 180/189 | 95 |
| OSS Spark 4.1 | 175/189 | 93 |
| OSS Flink 2.2 | 153/189 | 81 |
| Snowflake | 134/189 | 71 |
| PyIceberg 0.11 | 130/189 | 69 |
| Databricks DBR 17.3 | 103/189 | 54 |
| **SQE (current)** | **99/189** | **52.4** |
| DuckDB 1.5 | 85/189 | 45 |
| Daft | 77/189 | 41 |
| Athena v3 | 59/189 | 31 |
| ClickHouse 26.1 | 46/189 | 24 |

Peer scores from icebergmatrix.org as of 2026-04-24.

---

## Feature matrix

### Row-level operations

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Position Deletes | F | ? | PositionDeleteFileWriter + FastAppendAction (Step 8d). | V3 path not tested end-to-end. |
| Equality Deletes | P | P | DELETE with write.delete.mode=merge-on-read writes equality-delete files via EqualityDeleteFileWriter and commits through RowDeltaAction. Un | Same writer path as V2; V3 roundtrip not yet exercised in integration tests. |
| Merge-on-Read | F | P | All three DML kinds (DELETE, UPDATE, MERGE) route through RowDeltaAction when write.*.mode = merge-on-read. UPDATE emits one data file per b | Same writer path and commit mechanism as V2. V3 round trip not yet exercised in integration tests. |
| Copy-on-Write | F | . | DELETE/UPDATE/MERGE via RisingWave iceberg-rust fork rewrite_files(). | V3 feature roundtrip untested. |

### Table management

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Schema Evolution | F | P | ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL (Step 7.3). | ADD COLUMN with DEFAULT uses initial_default; existing V2 evolution ops apply to V3 tables. |
| Type Promotion / Widening | F | P | int->long, float->double, decimal widening. | Existing V2 widening rules apply to V3 tables; nanosec-to-microsecond narrowing not attempted. |
| Column Default Values | n/a | F | V3-only feature | CREATE TABLE ... DEFAULT <literal> applies write_default; ALTER TABLE ADD COLUMN ... DEFAULT applies initial_default. Function-call defaults |
| Table Creation | F | P | CREATE TABLE, CTAS streaming. | CREATE TABLE emits format-version: 3 when any V3-only feature is used; V2-only schemas remain format-version: 2 for compat. |
| Time Travel / Snapshots | F | P | FOR SYSTEM_TIME AS OF + 6 metadata TVFs (Step 8c). | FOR SYSTEM_TIME AS OF works against V3 tables using the same snapshot walk. |
| Table Maintenance | P | P | CALL system.expire_snapshots / remove_orphan_files / rewrite_manifests wrap vendored actions. rewrite_data_files currently consolidates mani | Same procedures apply to format-version 3 tables; same rewrite_data_files caveat. |
| Branching & Tagging | F | F | Transaction::{create_branch, create_tag, drop_branch, drop_tag} in vendored iceberg-rust + ALTER TABLE ... CREATE BRANCH/TAG DDL + SET WRITE | Same transaction actions apply to V3 tables; ref semantics are format-version agnostic. |

### Partitioning

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hidden Partitioning | F | . | year/month/day/hour/bucket/truncate. | V3 type coverage missing. |
| Partition Evolution | F | . | Partition spec evolution supported. | V3 type coverage missing. |
| Multi-Argument Transforms | n/a | . | V3-only feature | Spec not stable upstream. |

### Read / write

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Read Support | F | P | DataFusion 53 scan + predicate pushdown + projection. | V3 tables produced by SQE are readable via existing iceberg-rust scan path (nanosec types supported, defaults materialised on read). |
| Write (INSERT) | F | P | Streaming INSERT and CTAS. | INSERT into V3 tables works when columns use nanosec timestamps or typed defaults; V3-only types (Variant, geometry) still unsupported. |
| Write (MERGE/UPDATE/DELETE) | F | . | All three DML ops via CoW rewrite_files(). | V3 untested. |
| Catalog Integration | F | . | Iceberg REST (Polaris primary). | V3 tables not exercised. |
| Statistics (Column Metrics) | F | . | Column metrics used for pruning and CBO. | V3 untested. |
| Bloom Filters & Puffin | P | P | write.parquet.bloom-filter-columns + write.parquet.bloom-filter-fpp honoured in the coordinator's CTAS/INSERT batch path. DataFusion reads b | Writer path is format-version agnostic; V3 tables produce bloom-enabled Parquet. V3-specific integration tests not run. |

### Catalog support

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hive Metastore | . | . | Config scaffolding only at backends/hms.rs; all calls return SqeError::Catalog. Functional wiring arrives with upstream iceberg-catalog-hms  | Blocked on V2. |
| AWS Glue Catalog | . | . | Config scaffolding only at backends/glue.rs; all calls return SqeError::Catalog. Functional wiring arrives with upstream iceberg-catalog-glu | Blocked on V2. |
| REST Catalog | F | . | Primary catalog type. | V3-enabled REST catalogs untested. |
| Nessie | F | . | Works via iceberg-catalog-rest against Nessie's REST adapter; documented in docs/deployment.md Section 5b. | Blocked on V3 catalog exercise. |
| Polaris | F | . | Primary target catalog. | V3 untested. |
| Unity Catalog | P | . | Compatible via REST; OIDC M2M auth provider landed in Phase A. | Blocked. |
| Snowflake Horizon | P | . | Horizon is Polaris-based; REST wiring compatible; no live test. | Blocked. |
| Hadoop Catalog | P | . | Storage-only scanner walks warehouse paths for metadata/v*.metadata.json (feature `hadoop`). Read path only; writes defer to REST/HMS. | Blocked. |
| JDBC Catalog | P | . | SQLite roundtrip implemented and tested (feature `sql`); PostgreSQL arrives via upstream iceberg-catalog-sql (task 2.11). | Blocked. |

### V3 data types

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Variant | n/a | . | V3-only feature | Deferred to next cycle. iceberg-rust #2188 not merged. |
| Shredded Variant | n/a | . | V3-only feature | Deferred. Blocked on arrow-rs #9790. |
| Geometry | n/a | . | V3-only feature | Deferred. Blocked on DataFusion UDT (#12644). |
| Vector / Embedding | n/a | . | V3-only feature | Deferred. Iceberg V3 vector spec not finalised. |
| Nanosecond Timestamps | n/a | F | V3-only feature | TIMESTAMP_NS(N) and TIMESTAMPTZ_NS(N) DDL route to PrimitiveType::TimestampNs/TimestamptzNs; Arrow bridge and INSERT/SELECT wiring covered b |

### V3 advanced

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Change Data Capture | n/a | ? | V3-only feature | Library primitives shipped: FOR INCREMENTAL BETWEEN SNAPSHOT parser (sqe-sql/src/time_travel.rs) and range-scan planner with delete reconcil |
| Lineage Tracking | n/a | . | V3-only feature | Deferred to next cycle. |

---

## Caveats

Cells marked `partial` or `unknown` have specific gaps documented in `docs/iceberg-matrix-state.json` under `caveats`. Key ones:

- **position-deletes (v3)**: No V3 integration test yet.
- **equality-deletes (v2)**: No cross-engine read test executed (Spark interop is #[ignore]).
- **equality-deletes (v2)**: RowDeltaOperation::delete_entries simplified to Ok(vec![]) against the RisingWave fork's SnapshotProducer; behaviour matches the fork's own CoW path but not independently verified against Java Iceberg.
- **equality-deletes (v3)**: V3 end-to-end test pending.
- **merge-on-read (v3)**: V3 end-to-end test pending.
- **table-maintenance (v2)**: rewrite_data_files does not re-encode Parquet payloads.
- **table-maintenance (v3)**: rewrite_data_files does not re-encode Parquet payloads.
- **read-support (v3)**: Variant/geometry not supported.
- **write-insert (v3)**: Variant/geometry writes not supported.
- **bloom-filters (v2)**: Worker / streaming write path (sqe-worker) not wired; distributed writes miss blooms.
- **bloom-filters (v2)**: Read-probe end-to-end verification deferred; needs docker-compose.test.yml run.
- **bloom-filters (v2)**: DataFusion StatisticsSource consumer for Puffin NDV is blocked on DF 54 (apache/datafusion#21157).
- **bloom-filters (v3)**: V3 integration coverage pending.
- **bloom-filters (v3)**: Same worker-path gap as v2.
- **unity-catalog (v2)**: No live integration test against Unity Catalog yet.
- **snowflake-horizon-catalog (v2)**: No live integration test.
- **hadoop-catalog (v2)**: Write path is race-prone on object stores without atomic rename; intentionally read-oriented.
- **jdbc-catalog (v2)**: PostgreSQL path pinned to apache/iceberg-rust adoption in task 2.11.
- **cdc-support (v3)**: Parser and planner unit-tested in isolation; no end-to-end SQL test exists yet.
- **cdc-support (v3)**: query_handler.rs has not been wired; SELECT ... FOR INCREMENTAL ... is not dispatched.

---

## SQE differentiation

Not captured in the rubric but material to picking SQE:

- **OIDC bearer-token passthrough.** Every query runs as the authenticated user. No service account. No engine on the matrix offers this.
- **Full SQL DML via CoW `rewrite_files()`.** DuckDB has MoR-only writes, no MERGE. SQE has all three operations in both CoW and MoR modes.
- **Arrow Flight SQL primary + Trino HTTP compat.** Matches the protocol surface of Spark and Flink without a JVM.
- **Benchmarks vs Trino 465.** 5 of 7 suites faster at SF1. Latest SF0.1 run: 2.3x faster across TPC-H, TPC-DS, SSB, ClickBench (177/177 SQE pass, 170/177 byte-match Trino). See `benchmarks/results/*2026-04-24*.json`.
- **Security audit.** 43 of 43 findings resolved before OSS release.

---

## Contributing

1. Make the change in code.
2. Update the matching entry in `docs/iceberg-matrix-state.json`.
3. Run `cargo xtask matrix-report` to verify the aggregate score.
4. Raise `MATRIX_MIN_PERCENT` in `.gitlab-ci.yml` if the new score clears the next 1% threshold.
5. Regenerate this file: `python3 scripts/render-iceberg-matrix.py`.

For the public matrix submission workflow see `openspec/changes/iceberg-matrix-parity/tasks.md` section 2.22 and beyond.

## See also

- [Full openspec change](../openspec/changes/iceberg-matrix-parity/proposal.md) with proposal, design, 8 spec files, and tasks
- [Source roadmap](./superpowers/plans/2026-04-24-iceberg-matrix-parity.md) with upstream research and deferral rationale
- [Matrix parity workflow](./matrix-parity-workflow.md) for per-phase branching conventions
- [Tracking issue body](./matrix-parity-tracking-issue.md)
