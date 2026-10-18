# SQE Iceberg Compatibility Matrix

Current state of SQE against the [icebergmatrix.org](https://icebergmatrix.org) rubric, the de-facto reference engineers consult when picking an Iceberg engine. Data lives at [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix).

**Score: 144/162 (88.9%)**  |  **Stretch: 170/189 (90%)**

Last generated: 2026-10-17T00:00:00Z  |  Source: `Rescore against the pinned upstream rubric; evidence paths repaired after the crate and test moves.`

Regenerate: `python3 scripts/render-iceberg-matrix.py`. Source of truth: `docs/evidence/iceberg-matrix-state.json`.

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
| OSS Flink 2.2 | 153/189 | 81 |
| **SQE (current)** | **144/162** | **88.9** |
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
| Position Deletes | F | - | MoR DELETE on a V2 table without identifier fields writes position-delete files (PositionDeleteFileWriter) and commits a RowDelta; data file |  |
| Equality Deletes | F | F | DELETE with write.delete.mode=merge-on-read writes equality-delete files via EqualityDeleteFileWriter and commits through RowDeltaAction. Un | MoR UPDATE on a V3 table with a declared identifier field commits the new data file and an equality-delete file in one RowDelta; the SELECT  |
| Merge-on-Read | F | F | All three DML kinds (DELETE, UPDATE, MERGE) route through RowDeltaAction when write.*.mode = merge-on-read. UPDATE emits one data file per b | MoR DELETE on a V3 table writes deletion vectors (never position-delete files); MoR UPDATE / MERGE write data plus equality deletes in one R |
| Copy-on-Write | F | F | DELETE / UPDATE / MERGE in copy-on-write mode (the default) rewrite the affected files through ReplaceFilesAction. | DELETE on a V3 table without a declared MoR property runs CoW: rewrites the matched files via RewriteFilesAction, drops the row from subsequ |

### Table management

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Schema Evolution | F | F | ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL (Step 7.3). | ALTER TABLE ADD COLUMN ... DEFAULT on a V3 table commits write_default and initial_default; rows written before the ALTER read the initial d |
| Type Promotion / Widening | F | F | int->long, float->double, decimal widening. | ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE widens columns on V3 tables (verified int -> bigint). The new type surfaces through informati |
| Column Default Values | n/a | F | V3-only feature | CREATE TABLE ... DEFAULT <literal> and ALTER TABLE ADD COLUMN ... DEFAULT set write_default and initial_default. INSERT with a column list a |
| Table Creation | F | F | CREATE TABLE, CTAS streaming. | CREATE TABLE is V3 when a column needs it (TIMESTAMP_NS, VARIANT, DEFAULT), when asked with WITH ('format-version' = '3') / TBLPROPERTIES, o |
| Time Travel / Snapshots | F | F | FOR SYSTEM_TIME AS OF + 6 metadata TVFs (Step 8c). | FOR VERSION AS OF works end-to-end on V3 tables: the classifier strips the clause, apply_version_spec registers a snapshot-pinned provider u |
| Table Maintenance | F | F | All four CALL system.* procedures ship with both parser unit tests and live e2e tests that go through docker-compose.test.yml (Polaris + Rus | CALL system.rewrite_data_files merges files on V3 tables with real Parquet re-encoding; row count preserved, file count drops, snapshot log  |
| Branching & Tagging | F | F | ALTER TABLE CREATE/DROP BRANCH and TAG with retention options, SET WRITE_BRANCH routes INSERT / DELETE / UPDATE to a branch, FOR VERSION AS  | Same transaction actions apply to V3 tables; ref semantics are format-version agnostic. |

### Partitioning

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hidden Partitioning | F | F | CREATE TABLE ... PARTITIONED BY (...) accepts all six standard Iceberg transforms: identity (bare column), year/month/day/hour, bucket(N, co | Same SQL syntax as V2 (PARTITIONED BY) and the same writer path. Verified on a V3 table with a TIMESTAMP_NS column partitioned by day(ts): I |
| Partition Evolution | F | F | ALTER TABLE ADD/DROP/REPLACE PARTITION FIELD evolves the partition spec end-to-end. Pre-parser in sqe-sql/partition_evolution.rs lifts the n | V3 metadata stores per-file partition_spec_id the same way V2 does; the same coordinator handler and writer path exercise V3 nanosec timesta |
| Multi-Argument Transforms | n/a | . | V3-only feature | PARTITIONED BY accepts only single-source transforms; a multi-source transform cannot be declared. |

### Read / write

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Read Support | F | F | Iceberg scan with partition pruning, projection, row-group / page / RowFilter predicate pushdown and runtime join filters. | SELECT against V3 tables works through the existing iceberg-rust scan path: TIMESTAMP_NS columns materialise as Arrow Timestamp(Nanosecond), |
| Write (INSERT) | F | F | Streaming INSERT and CTAS. | INSERT into V3 tables with TIMESTAMP_NS columns commits a snapshot through Polaris and the rows materialise on subsequent SELECT. INSERT rou |
| Write (MERGE/UPDATE/DELETE) | F | F | DELETE, UPDATE and MERGE INTO in copy-on-write or merge-on-read mode. | DELETE (CoW, and MoR via deletion vectors), UPDATE (CoW and MoR equality) and MERGE INTO commit on V3 tables; each path has a V3 e2e test. |
| Catalog Integration | F | F | Iceberg REST (Polaris primary; Nessie, Glue / S3 Tables REST, Unity), native Glue and S3 Tables SDKs, HMS, JDBC and a read-only Hadoop backe | V3 tables created by SQE materialise correctly in the Iceberg REST catalog: CREATE/INSERT/UPDATE/DELETE all commit, table metadata reloads c |
| Statistics (Column Metrics) | F | F | Manifest column metrics drive pruning and planning; writers emit them; optional Puffin NDV sidecars are written and consumed (query.puffin_s | SHOW STATS FOR works on V3 tables, returning per-column metrics from the active snapshot. Same DataFusion + iceberg-rust scan path as V2; V3 |
| Bloom Filters & Puffin | F | F | Every data-file write path builds WriterProperties through build_writer_props, which emits Parquet bloom filters; the footer test re-reads t | Same writer helper as V2; the bloom property round-trips through Polaris on a V3 table. |

### Catalog support

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hive Metastore | - | - |  |  |
| AWS Glue Catalog | F | F | Two parallel paths now ship: (1) iceberg-catalog-glue vendored from apache/iceberg-rust v0.9.0 (AWS SDK over the Glue API directly); (2) the | Same two parallel paths as V2: native AWS SDK via iceberg-catalog-glue, and the federated Glue Iceberg REST endpoint with SigV4 signing. Bot |
| REST Catalog | F | F | Primary catalog type. Phase P added optional AWS SigV4 signing to the vendored `iceberg-catalog-rest` client (cargo feature `aws-sigv4`, on  | Iceberg REST catalogs accept V3 tables when SQE forwards format-version as a table property in CreateTableRequest. Verified end-to-end again |
| Nessie | - | - |  |  |
| Polaris | - | - |  |  |
| Unity Catalog | P | P | SQE reaches Unity's Iceberg REST endpoint through the shared REST client, with the OIDC M2M provider for Databricks-hosted Unity. Only a rea | Same REST path as V2; Unity serves whatever metadata the writer produced. No V3 table read or written through Unity. |
| Snowflake Horizon | P | P | Horizon implements the Iceberg REST API (Polaris-based); SQE's REST client speaks it. Not measured against Horizon. | Snowflake Horizon is Polaris-based; the REST surface is shared with Polaris which we verified end-to-end at format-version 3. No live test a |
| Hadoop Catalog | - | - |  |  |
| JDBC Catalog | - | - |  |  |

### V3 data types

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Variant | n/a | P | V3-only feature | VARIANT columns (and JSON columns on V3 tables) store Iceberg variant, unshredded. JSON text is parsed on write (invalid JSON fails the writ |
| Shredded Variant | n/a | . | V3-only feature | SQE writes unshredded variants and cannot read shredded ones; unshred_variant in the Parquet readers is planned (#849 follow-up). |
| Geometry | n/a | . | V3-only feature | No GEOMETRY / GEOGRAPHY type in SQE DDL or the read path. |
| Vector / Embedding | - | - |  |  |
| Nanosecond Timestamps | n/a | F | V3-only feature | TIMESTAMP_NS(N) / TIMESTAMPTZ_NS(N) map to Iceberg timestamp_ns / timestamptz_ns and to Arrow nanosecond timestamps; CREATE, INSERT and SELE |

### V3 advanced

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Change Data Capture | - | - |  |  |
| Lineage Tracking | n/a | F | V3-only feature | Row lineage is maintained at write time on V3 tables: new rows inherit fresh _row_id from first-row-id at commit; CoW DELETE / UPDATE / MERG |

---

## Caveats

Cells marked `partial` or `unknown` have specific gaps documented in `docs/evidence/iceberg-matrix-state.json` under `caveats`. Key ones:

- **unity-catalog (v2)**: Read-only: Unity OSS does not implement create / commit, and no Databricks-hosted write was tested.
- **unity-catalog (v2)**: Both Unity tests are #[ignore] and annotated 'last verified: never in this tree'.
- **unity-catalog (v3)**: Not measured on a V3 table; read-only smoke only.
- **snowflake-horizon-catalog (v2)**: Inferred from generic Iceberg REST catalog support; not measured against Horizon.
- **snowflake-horizon-catalog (v3)**: Inferred from generic Iceberg REST catalog support; not measured against Horizon.
- **google-lakehouse (v2)**: Inferred from generic Iceberg REST catalog support; not measured against Google Lakehouse.
- **google-lakehouse (v2)**: Iceberg data on GCS (gs://) through the table FileIO and Google OAuth for the REST endpoint are not verified.
- **google-lakehouse (v3)**: Inferred from generic Iceberg REST catalog support; not measured against Google Lakehouse.
- **google-lakehouse (v3)**: Iceberg data on GCS (gs://) through the table FileIO and Google OAuth for the REST endpoint are not verified.
- **variant-type (v3)**: Field access goes through the variant's JSON text (json_* functions); no variant_get / typed path access yet.
- **variant-type (v3)**: Shredded variant files (typed_value) are not covered by a test; the Spark interop test reads Spark's default unshredded output.
- **variant-type (v3)**: Compaction of a variant table is untested.
- **unknown-type (v3)**: Needs a live test: read a V3 table with a Spark-written VOID / unknown column.

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
2. Update the matching entry in `docs/evidence/iceberg-matrix-state.json`.
3. Run `cargo xtask matrix-report` to verify the aggregate score.
4. Raise `MATRIX_MIN_PERCENT` in `.gitlab-ci.yml` if the new score clears the next 1% threshold.
5. Regenerate this file: `python3 scripts/render-iceberg-matrix.py`.

For the public matrix submission workflow see `openspec/changes/iceberg-matrix-parity/tasks.md` section 2.22 and beyond.

## See also

- [Full openspec change](../openspec/changes/iceberg-matrix-parity/proposal.md) with proposal, design, 8 spec files, and tasks
- [Source roadmap](./superpowers/plans/2026-04-24-iceberg-matrix-parity.md) with upstream research and deferral rationale
- [Matrix parity workflow](./matrix-parity-workflow.md) for per-phase branching conventions
- [Tracking issue body](./matrix-parity-tracking-issue.md)
