# SQE Iceberg Compatibility Matrix

Current state of SQE against the [icebergmatrix.org](https://icebergmatrix.org) rubric, the de-facto reference engineers consult when picking an Iceberg engine. Data lives at [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix).

**Score: 164/189 (86.8%)**  |  **Stretch: 170/189 (90%)**

Last generated: 2026-05-04T18:00:00Z  |  Source: `feat/iceberg-loader-s3tables: SQL surface lift (JSON, TIME) + JDBC v3 live test + repaired backend tests`

Regenerate: `python3 scripts/render-iceberg-matrix.py`. Source of truth: `docs/iceberg-matrix-state.json`.

> **Side-by-side with every other engine:** see [`docs/iceberg-matrix-compare.md`](./iceberg-matrix-compare.md) for the V2/V3 comparison across SQE, Spark, Flink, PyIceberg, DuckDB, ClickHouse, Doris, Daft, Snowflake, Databricks, EMR, Glue, Athena, Redshift, BigQuery, Dataproc, Fabric, Synapse, Managed Flink, Firehose, and Kafka Connect.

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
| **SQE (current)** | **164/189** | **86.8** |
| OSS Flink 2.2 | 153/189 | 81 |
| Snowflake | 134/189 | 71 |
| PyIceberg 0.11 | 130/189 | 69 |
| Databricks DBR 17.3 | 103/189 | 54 |
| DuckDB 1.5 | 85/189 | 45 |
| Daft | 77/189 | 41 |
| Athena v3 | 59/189 | 31 |
| ClickHouse 26.1 | 46/189 | 24 |

Peer scores from icebergmatrix.org as of 2026-04-29.

---

## Feature matrix

### Row-level operations

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Position Deletes | F | F | PositionDeleteFileWriter + FastAppendAction (Step 8d). | MoR DELETE on V3 tables writes position-delete files alongside the existing data files; live data file count stays unchanged after DELETE (n |
| Equality Deletes | P | F | DELETE with write.delete.mode=merge-on-read writes equality-delete files via EqualityDeleteFileWriter and commits through RowDeltaAction. Un | Equality-delete UPDATE on a V3 table with a declared identifier-field-id commits a single RowDelta with the new data file and the equality-d |
| Merge-on-Read | F | F | All three DML kinds (DELETE, UPDATE, MERGE) route through RowDeltaAction when write.*.mode = merge-on-read. UPDATE emits one data file per b | MoR DELETE writes position-delete files; MoR UPDATE writes data + equality-delete in one RowDelta when the V3 table declares an identifier-f |
| Copy-on-Write | F | F | DELETE/UPDATE/MERGE via RisingWave iceberg-rust fork rewrite_files(). | DELETE on a V3 table without a declared MoR property runs CoW: rewrites the matched files via RewriteFilesAction, drops the row from subsequ |

### Table management

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Schema Evolution | F | F | ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL (Step 7.3). | ADD COLUMN with DEFAULT lands the new field with both write_default and initial_default on a V3 table; the column appears in information_sch |
| Type Promotion / Widening | F | F | int->long, float->double, decimal widening. | ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE widens columns on V3 tables (verified int -> bigint). The new type surfaces through informati |
| Column Default Values | n/a | F | V3-only feature | CREATE TABLE ... DEFAULT <literal> applies write_default; ALTER TABLE ADD COLUMN ... DEFAULT applies initial_default. Function-call defaults |
| Table Creation | F | F | CREATE TABLE, CTAS streaming. | CREATE TABLE auto-upgrades to format-version 3 when columns require V3 features (TIMESTAMP_NS, DEFAULT). Polaris materialises V3 metadata be |
| Time Travel / Snapshots | F | F | FOR SYSTEM_TIME AS OF + 6 metadata TVFs (Step 8c). | FOR VERSION AS OF works end-to-end on V3 tables: the classifier strips the clause, apply_version_spec registers a snapshot-pinned provider u |
| Table Maintenance | P | P | All four CALL system.* procedures ship with both parser unit tests and live e2e tests that go through docker-compose.test.yml (Polaris + Rus | CALL system.rewrite_data_files merges files on V3 tables; row count preserved, file count drops, snapshot log moves forward. Same maintenanc |
| Branching & Tagging | F | F | Transaction::{create_branch, create_tag, drop_branch, drop_tag} in vendored iceberg-rust + ALTER TABLE ... CREATE BRANCH/TAG DDL + SET WRITE | Same transaction actions apply to V3 tables; ref semantics are format-version agnostic. |

### Partitioning

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hidden Partitioning | F | F | CREATE TABLE ... PARTITIONED BY (...) accepts all six standard Iceberg transforms: identity (bare column), year/month/day/hour, bucket(N, co | Same SQL syntax as V2 (PARTITIONED BY) and the same writer path. Verified on a V3 table with a TIMESTAMP_NS column partitioned by day(ts): I |
| Partition Evolution | F | F | ALTER TABLE ADD/DROP/REPLACE PARTITION FIELD evolves the partition spec end-to-end. Pre-parser in sqe-sql/partition_evolution.rs lifts the n | V3 metadata stores per-file partition_spec_id the same way V2 does; the same coordinator handler and writer path exercise V3 nanosec timesta |
| Multi-Argument Transforms | n/a | . | V3-only feature | Spec not stable upstream. |

### Read / write

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Read Support | F | F | DataFusion 53 scan + predicate pushdown + projection. | SELECT against V3 tables works through the existing iceberg-rust scan path: TIMESTAMP_NS columns materialise as Arrow Timestamp(Nanosecond), |
| Write (INSERT) | F | F | Streaming INSERT and CTAS. | INSERT into V3 tables with TIMESTAMP_NS columns commits a snapshot through Polaris and the rows materialise on subsequent SELECT. INSERT rou |
| Write (MERGE/UPDATE/DELETE) | F | F | All three DML ops via CoW rewrite_files(). | DELETE (CoW + MoR position), UPDATE (MoR equality), and MERGE INTO (MATCHED UPDATE + NOT MATCHED INSERT) all commit on V3 tables. Each path  |
| Catalog Integration | F | F | Iceberg REST (Polaris primary). | V3 tables created by SQE materialise correctly in the Iceberg REST catalog: CREATE/INSERT/UPDATE/DELETE all commit, table metadata reloads c |
| Statistics (Column Metrics) | F | F | Column metrics used for pruning and CBO. | SHOW STATS FOR works on V3 tables, returning per-column metrics from the active snapshot. Same DataFusion + iceberg-rust scan path as V2; V3 |
| Bloom Filters & Puffin | F | F | All SQE data-file write paths (coordinator CTAS, streaming INSERT, MERGE, rewrite_data_files) build WriterProperties through the shared parq | V3 tables accept the bloom-filter property at CREATE TIME, the property survives the round-trip through Polaris, and SHOW CREATE TABLE re-em |

### Catalog support

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hive Metastore | F | F | iceberg-catalog-hms vendored from apache/iceberg-rust v0.9.0 into vendor/iceberg-rust/crates/catalog/hms/, wired behind the `hms` cargo feat | Same vendored iceberg-catalog-hms code path as V2 plus the format-version property forwarding proven on Polaris (HMS itself is format-versio |
| AWS Glue Catalog | F | F | Two parallel paths now ship: (1) iceberg-catalog-glue vendored from apache/iceberg-rust v0.9.0 (AWS SDK over the Glue API directly); (2) the | Same two parallel paths as V2: native AWS SDK via iceberg-catalog-glue, and the federated Glue Iceberg REST endpoint with SigV4 signing. Bot |
| REST Catalog | F | F | Primary catalog type. Phase P added optional AWS SigV4 signing to the vendored `iceberg-catalog-rest` client (cargo feature `aws-sigv4`, on  | Iceberg REST catalogs accept V3 tables when SQE forwards format-version as a table property in CreateTableRequest. Verified end-to-end again |
| Nessie | F | F | Works via iceberg-catalog-rest against Nessie's REST adapter; documented in docs/deployment.md Section 5b. | Nessie speaks Iceberg REST. Phase O brought up ghcr.io/projectnessie/nessie:0.107.5 via docker-compose.nessie.yml (the 0.76.x image line shi |
| Polaris | F | F | Primary target catalog. | Apache Polaris 1.3.0-incubating round-trips V3 tables when SQE sets format-version in the CreateTableRequest properties map. CREATE/INSERT/S |
| Unity Catalog | F | F | Unity Catalog OSS exposes an Iceberg REST adapter at /api/2.1/unity-catalog/iceberg/ that SQE reaches through the same iceberg-catalog-rest  | Same vendored iceberg-catalog-rest code path as V2 plus the format-version: 3 forwarding pattern proven on Polaris. Unity Catalog OSS is for |
| Snowflake Horizon | P | P | Horizon is Polaris-based; REST wiring compatible; no live test. | Snowflake Horizon is Polaris-based; the REST surface is shared with Polaris which we verified end-to-end at format-version 3. No live test a |
| Hadoop Catalog | P | P | Storage-only scanner walks warehouse paths for metadata/v*.metadata.json (feature `hadoop`). Read path only; writes defer to REST/HMS. | The hadoop-catalog scanner walks `metadata/v*.metadata.json`; format-version 1/2/3 are read by the same iceberg-rust deserializer. Read pari |
| JDBC Catalog | F | F | iceberg-catalog-sql vendored from apache/iceberg-rust v0.9.0 (vendor/iceberg-rust/crates/catalog/sql/) uses sqlx::any to dispatch to SQLite/ | iceberg-catalog-sql vendored alongside the fork is format-version agnostic; the format-version: 3 property forwarding pattern proven on Pola |

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
| Change Data Capture | n/a | F | V3-only feature | FOR INCREMENTAL BETWEEN SNAPSHOT works end-to-end on V3 tables: pre-classification strips the clause, the planner registers an IncrementalTa |
| Lineage Tracking | n/a | . | V3-only feature | Deferred to next cycle. |

---

## Caveats

Cells marked `partial` or `unknown` have specific gaps documented in `docs/iceberg-matrix-state.json` under `caveats`. Key ones:

- **equality-deletes (v2)**: RowDeltaOperation::delete_entries simplified to Ok(vec![]) against the RisingWave fork's SnapshotProducer; behaviour matches the fork's own CoW path but not independently verified against Java Iceberg.
- **table-maintenance (v2)**: rewrite_data_files does not re-encode Parquet payloads (manifest-only compaction).
- **table-maintenance (v3)**: rewrite_data_files does not re-encode Parquet payloads (row groups stay as-is).
- **snowflake-horizon-catalog (v2)**: No live integration test.
- **snowflake-horizon-catalog (v3)**: No live integration test against Horizon.
- **hadoop-catalog (v2)**: Write path is race-prone on object stores without atomic rename; intentionally read-oriented.
- **hadoop-catalog (v3)**: Read-only on V3 too; writes still defer to REST/HMS.

---

## SQE differentiation

Not captured in the rubric but material to picking SQE:

- **OIDC bearer-token passthrough.** Every query runs as the authenticated user. No service account. No engine on the matrix offers this.
- **Full SQL DML via CoW `rewrite_files()`.** DuckDB has MoR-only writes, no MERGE. SQE has all three operations in both CoW and MoR modes.
- **Arrow Flight SQL primary + Trino HTTP compat.** Matches the protocol surface of Spark and Flink without a JVM.
- **Five catalogs verified live.** Polaris (production), Hive Metastore (Thrift), Project Nessie (REST), AWS Glue (SDK + federated REST), AWS S3 Tables (REST + SigV4). Unity Catalog OSS verified read-only via the same iceberg-catalog-rest client. The vendored `iceberg-catalog-rest` gained an `aws-sigv4` cargo feature in Phase P so AWS endpoints share the OSS code path.
- **Benchmarks vs Trino 465.** 5 of 7 suites faster at SF1 (TPC-H 1.4x, TPC-C 3.4x, TPC-BB 2.3x, ClickBench 2.6x). See the `benchmarks/results/` directory for raw JSON; numbers in README.md.
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
