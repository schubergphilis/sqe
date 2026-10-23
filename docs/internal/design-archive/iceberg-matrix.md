# SQE Iceberg Compatibility Matrix

SQE rated against the [icebergmatrix.org](https://icebergmatrix.org) rubric ([Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix)).

**Score: 150/162 (92.6%)** on 54 cells.

- SQE cells: `docs/evidence/iceberg-matrix-state.json`, last changed 2026-10-23.
- Rubric and peer scores: `docs/evidence/iceberg-matrix-rubric.json`, upstream `d14d0f1341` fetched 2026-10-23.
- Regenerate: `python3 scripts/render-iceberg-matrix-compare.py` (refreshes the rubric snapshot and [the engine comparison](./iceberg-matrix-compare.md)), then `python3 scripts/render-iceberg-matrix.py`.

---

## Legend

| Symbol | Level | Weight | Meaning |
|:---:|---|---:|---|
| F | full | 3 | Verified end-to-end; no significant limitations |
| P | partial | 2 | Some functionality works; caveats apply |
| ? | unknown | 1 | Library primitives exist; no end-to-end verification |
| . | none | 0 | Not implemented; planned or deferred |
| n/a | | | The rubric does not score this format version |

A feature is scored on each format version from `introducedIn` up to, not including, `removedIn`.

---

## Peer rankings

Every engine scored on the same 54 cells. SQE rates itself; the other engines are rated in the upstream repo. Top 12 of 36 upstream entries; the full list is in [the comparison](./iceberg-matrix-compare.md).

| Engine | Score | % |
|---|---:|---:|
| **SQE (self-rated)** | **150/162** | **92.6** |
| AWS EMR (8.1 Spark) | 147/162 | 90.7 |
| Starburst (482-e) | 147/162 | 90.7 |
| Dremio / SAP (26.x) | 146/162 | 90.1 |
| AWS Glue (6.0) | 145/162 | 89.5 |
| AWS EMR (8.1 Spark) (S3 Tables) | 143/162 | 88.3 |
| AWS Glue (6.0) (S3 Tables) | 141/162 | 87.0 |
| Google Managed Spark (3.0.13) | 139/162 | 85.8 |
| Snowflake | 138/162 | 85.2 |
| Snowflake (external) | 138/162 | 85.2 |
| Spark (4.1) | 138/162 | 85.2 |
| Trino (483) | 130/162 | 80.2 |
| OSS Flink (2.3) | 126/162 | 77.8 |

---

## Feature matrix

### Row-level operations

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Position Deletes | F | n/a | MoR DELETE on a V2 table without identifier fields writes position-delete files (PositionDeleteFileWriter) and commits a RowDelta; data files are not ... | Not scored on V3 |
| Equality Deletes | F | F | On a table with identifier fields, merge-on-read DELETE, UPDATE and MERGE write equality-delete files (EqualityDeleteFileWriter) and commit one RowDelta; no ... | Same as V2 on a V3 table: keyed merge-on-read DELETE, UPDATE and MERGE write equality-delete files in one RowDelta (summary asserts ... |
| Merge-on-Read | F | F | DELETE, UPDATE and MERGE route through RowDeltaAction when write.*.mode = merge-on-read. With identifier fields, UPDATE writes a data file plus an equality ... | MoR DELETE on a V3 table writes deletion vectors (never position-delete files); with identifier fields DELETE, UPDATE and MERGE write equality deletes in one ... |
| Copy-on-Write | F | F | DELETE / UPDATE / MERGE in copy-on-write mode (the default) rewrite the affected files through ReplaceFilesAction. | DELETE on a V3 table without a declared MoR property runs CoW: rewrites the matched files via RewriteFilesAction, drops the row from subsequent SELECTs, and ... |
| Lineage Tracking | n/a | F | V3-only feature | Row lineage is maintained at write time on V3 tables: new rows inherit fresh _row_id from first-row-id at commit; CoW DELETE / UPDATE / MERGE, MoR UPDATE / ... |
| Deletion Vectors | n/a | F | V3-only feature | MoR DELETE on a V3 table writes one Puffin deletion vector per data file, merges the file's existing deletes into it and retires the superseded vector in the ... |

### Table management

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Schema Evolution | F | F | ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL (Step 7.3). | ALTER TABLE ADD COLUMN ... DEFAULT on a V3 table commits write_default and initial_default; rows written before the ALTER read the initial default (#830) in ... |
| Type Promotion / Widening | F | F | ALTER COLUMN ... SET DATA TYPE accepts int->long, float->double and decimal precision widening, refuses every other change before commit, and reads files ... | ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE widens columns on V3 tables (int -> bigint, real -> double, decimal precision widening, unknown -> any ... |
| Column Default Values | n/a | F | V3-only feature | CREATE TABLE ... DEFAULT <literal> and ALTER TABLE ADD COLUMN ... DEFAULT set write_default and initial_default. INSERT with a column list and MERGE INSERT ... |
| Table Creation | F | F | CREATE TABLE, CTAS streaming. | CREATE TABLE is V3 when a column needs it (TIMESTAMP_NS, VARIANT, DEFAULT), when asked with WITH ('format-version' = '3') / TBLPROPERTIES, or by ... |
| Time Travel / Snapshots | F | F | FOR SYSTEM_TIME AS OF + 6 metadata TVFs (Step 8c). | FOR VERSION AS OF works end-to-end on V3 tables: the classifier strips the clause, apply_version_spec registers a snapshot-pinned provider under ... |
| Table Maintenance | F | F | All four CALL system.* procedures ship with both parser unit tests and live e2e tests that go through docker-compose.test.yml (Polaris + RustFS): ... | CALL system.rewrite_data_files merges files on V3 tables with real Parquet re-encoding; row count preserved, file count drops, snapshot log moves forward. Same ... |
| Branching & Tagging | F | F | ALTER TABLE CREATE/DROP BRANCH and TAG with retention options, SET WRITE_BRANCH routes INSERT / DELETE / UPDATE to a branch, FOR VERSION AS OF '<ref>' reads it. | Same transaction actions apply to V3 tables: on a format-version 3 table, SET WRITE_BRANCH routes INSERT / DELETE / UPDATE to a branch and FOR VERSION AS OF ... |

### Partitioning

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Hidden Partitioning | F | F | CREATE TABLE ... PARTITIONED BY (...) accepts all six standard Iceberg transforms: identity (bare column), year/month/day/hour, bucket(N, col), truncate(L, ... | Same SQL syntax as V2 (PARTITIONED BY) and the same writer path. Verified on a V3 table with a TIMESTAMP_NS column partitioned by day(ts): INSERT routes ... |
| Partition Evolution | F | F | ALTER TABLE ADD/DROP/REPLACE PARTITION FIELD evolves the partition spec end-to-end. Pre-parser in sqe-sql/partition_evolution.rs lifts the non-standard syntax ... | V3 metadata stores per-file partition_spec_id the same way V2 does; the same coordinator handler and writer path exercise V3 nanosec timestamp partitioning ... |
| Multi-Argument Transforms | n/a | . | V3-only feature | PARTITIONED BY accepts only single-source transforms; a multi-source transform cannot be declared. |

### Read / write

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Read Support | F | F | Iceberg scan with partition pruning, projection, row-group / page / RowFilter predicate pushdown and runtime join filters. | SELECT against V3 tables works through the existing iceberg-rust scan path: TIMESTAMP_NS columns materialise as Arrow Timestamp(Nanosecond), INSERT/SELECT ... |
| Write (INSERT) | F | F | Streaming INSERT and CTAS. | INSERT into V3 tables with TIMESTAMP_NS columns commits a snapshot through Polaris and the rows materialise on subsequent SELECT. INSERT round-trip verified ... |
| Write (MERGE/UPDATE/DELETE) | F | F | DELETE, UPDATE and MERGE INTO in copy-on-write or merge-on-read mode. | DELETE (CoW, and MoR via deletion vectors), UPDATE (CoW and MoR equality) and MERGE INTO commit on V3 tables; each path has a V3 e2e test. |
| Catalog Integration | F | F | Iceberg REST (Polaris primary; Nessie, Glue / S3 Tables REST, Unity), native Glue and S3 Tables SDKs, HMS, JDBC and a read-only Hadoop backend. | V3 tables created by SQE materialise correctly in the Iceberg REST catalog: CREATE/INSERT/UPDATE/DELETE all commit, table metadata reloads carry the right ... |
| Statistics (Column Metrics) | F | F | Manifest column metrics drive pruning and planning; writers emit them; optional Puffin NDV sidecars are written and consumed (query.puffin_statistics). | SHOW STATS FOR works on V3 tables, returning per-column metrics from the active snapshot. Same DataFusion + iceberg-rust scan path as V2; V3 metadata ... |
| Bloom Filters & Puffin | F | F | Every data-file write path builds WriterProperties through build_writer_props, which emits Parquet bloom filters; the footer test re-reads the file and finds ... | Same writer helper as V2; the bloom property round-trips through Polaris on a V3 table, and an SQE INSERT into a V3 table Spark configured with the Iceberg ... |

### Catalog support

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| AWS Glue Catalog | F | F | Two parallel paths now ship: (1) iceberg-catalog-glue vendored from apache/iceberg-rust v0.9.0 (AWS SDK over the Glue API directly); (2) the federated Glue ... | Same two parallel paths as V2: native AWS SDK via iceberg-catalog-glue, and the federated Glue Iceberg REST endpoint with SigV4 signing. Both are ... |
| REST Catalog | F | F | Primary catalog type. Phase P added optional AWS SigV4 signing to the vendored `iceberg-catalog-rest` client (cargo feature `aws-sigv4`, on by default in SQE), ... | Iceberg REST catalogs accept V3 tables when SQE forwards format-version as a table property in CreateTableRequest. Verified end-to-end against Polaris (Apache ... |
| Unity Catalog | P | P | SQE reaches Unity's Iceberg REST endpoint through the shared REST client, with the OIDC M2M provider for Databricks-hosted Unity. Only a read-only smoke ... | Same REST path as V2; Unity serves whatever metadata the writer produced. No V3 table read or written through Unity. |
| Snowflake Horizon Catalog | P | P | Horizon implements the Iceberg REST API (Polaris-based); SQE's REST client speaks it. Not measured against Horizon. | Snowflake Horizon is Polaris-based; the REST surface is shared with Polaris which we verified end-to-end at format-version 3. No live test against a Horizon ... |
| Google Lakehouse | P | P | Google Lakehouse runtime catalog exposes the Iceberg REST API, which SQE's REST client speaks. Rated partial because the pairing has not been measured. | Google Lakehouse runtime catalog exposes the Iceberg REST API, which SQE's REST client speaks. Rated partial because the pairing has not been measured. |

### V3 data types

| Feature | V2 | V3 | V2 notes | V3 notes |
|---|:---:|:---:|---|---|
| Variant Type | n/a | F | V3-only feature | VARIANT columns (and JSON columns on V3 tables) store Iceberg variant, unshredded. JSON text is parsed on write (invalid JSON fails the write), values read ... |
| Shredded Variant | n/a | F | V3-only feature | SQE reads and writes shredded variant files. Read: Spark 4.1 + Iceberg 1.12 writes a table with write.parquet.shred-variants=true (the test asserts typed_value ... |
| Geometry / Geo Types | n/a | . | V3-only feature | No GEOMETRY / GEOGRAPHY type in SQE DDL or the read path. |
| Nanosecond Timestamps | n/a | F | V3-only feature | TIMESTAMP_NS(N) / TIMESTAMPTZ_NS(N) map to Iceberg timestamp_ns / timestamptz_ns and to Arrow nanosecond timestamps; CREATE, INSERT and SELECT round-trip ... |
| Unknown Type | n/a | F | V3-only feature | Top-level unknown columns work across engines. SQE declares one as UNKNOWN or VOID, reads a Spark-written VOID column as NULLs, inserts around it and promotes ... |

---

## Open cells

Every cell below full, with its caveats. Each has a `[MATRIX]` issue.

- **multi-arg-transforms (v3)**, none: PARTITIONED BY accepts only single-source transforms; a multi-source transform cannot be declared.
- **unity-catalog (v2)**, partial: SQE reaches Unity's Iceberg REST endpoint through the shared REST client, with the OIDC M2M provider for Databricks-hosted Unity. Only a read-only smoke against Unity OSS exists; writes are not verified.
  - Read-only: Unity OSS does not implement create / commit, and no Databricks-hosted write was tested.
  - Both Unity tests are #[ignore] and annotated 'last verified: never in this tree'.
- **unity-catalog (v3)**, partial: Same REST path as V2; Unity serves whatever metadata the writer produced. No V3 table read or written through Unity.
  - Not measured on a V3 table; read-only smoke only.
- **snowflake-horizon-catalog (v2)**, partial: Horizon implements the Iceberg REST API (Polaris-based); SQE's REST client speaks it. Not measured against Horizon.
  - Inferred from generic Iceberg REST catalog support; not measured against Horizon.
- **snowflake-horizon-catalog (v3)**, partial: Snowflake Horizon is Polaris-based; the REST surface is shared with Polaris which we verified end-to-end at format-version 3. No live test against a Horizon endpoint.
  - Inferred from generic Iceberg REST catalog support; not measured against Horizon.
- **google-lakehouse (v2)**, partial: Google Lakehouse runtime catalog exposes the Iceberg REST API, which SQE's REST client speaks. Rated partial because the pairing has not been measured.
  - Inferred from generic Iceberg REST catalog support; not measured against Google Lakehouse.
  - Iceberg data on GCS (gs://) through the table FileIO and Google OAuth for the REST endpoint are not verified.
- **google-lakehouse (v3)**, partial: Google Lakehouse runtime catalog exposes the Iceberg REST API, which SQE's REST client speaks. Rated partial because the pairing has not been measured.
  - Inferred from generic Iceberg REST catalog support; not measured against Google Lakehouse.
  - Iceberg data on GCS (gs://) through the table FileIO and Google OAuth for the REST endpoint are not verified.
- **geometry-type (v3)**, none: No GEOMETRY / GEOGRAPHY type in SQE DDL or the read path.

## Caveats on full cells

- **merge-on-read (v2)**: UPDATE and MERGE are merge-on-read only on tables with identifier fields (ALTER TABLE ... SET IDENTIFIER FIELDS); without them they fall back to copy-on-write.
- **merge-on-read (v3)**: UPDATE and MERGE are merge-on-read only on tables with identifier fields (ALTER TABLE ... SET IDENTIFIER FIELDS); without them they fall back to copy-on-write.
- **type-promotion (v3)**: Nanosecond-to-microsecond narrowing not attempted (Iceberg spec rejects narrowing).
- **type-promotion (v3)**: date -> timestamp (a V3 promotion) is refused: Iceberg Java 1.12 refuses it too, so no other engine could read such a table.
- **read-support (v3)**: Geometry is not supported, and shredded variants are read unshredded (separate cells).
- **write-insert (v3)**: Geometry writes are not supported (separate cell).
- **bloom-filters (v2)**: write.parquet.bloom-filter-max-bytes is ignored: parquet-rs sizes a filter from the row count. Members of lists and maps get no bloom.
- **bloom-filters (v3)**: write.parquet.bloom-filter-max-bytes is ignored: parquet-rs sizes a filter from the row count. Members of lists and maps get no bloom.
- **aws-glue-catalog (v2)**: The live AWS tests are #[ignore] (need AWS credentials and a warehouse bucket) and run locally only; CI does not run them.
- **aws-glue-catalog (v3)**: The live AWS test is #[ignore] (needs AWS credentials and a warehouse bucket) and runs locally only; CI does not run it.
- **shredded-variant (v3)**: Shredded columns are unshredded on read, so typed_value gives no pruning or typed field access.
- **shredded-variant (v3)**: Only top-level variant columns are shredded on write; a variant nested in a struct, list or map is written unshredded.
- **lineage (v3)**: MoR DELETE is not a lineage case (deleted rows have no successor).

---

## Catalogs outside the rubric

The current Iceberg matrix rubric no longer scores catalog rows for Polaris, Nessie, Hive Metastore, JDBC or Hadoop. SQE still supports them; the table below says how each is tested today.

| Catalog | Support | Testing |
|---|---|---|
| Apache Polaris | Read + write | Primary target catalog: the Polaris + Keycloak and Ranger quickstarts run against it end to end. |
| Project Nessie | Read + write (Iceberg REST) | Runnable quickstart and a live namespace round-trip test, run manually. |
| Hive Metastore | Read + write | Live round-trip test against a standalone metastore container, run manually, not in CI. |
| JDBC (PostgreSQL) | Read + write | Live round-trip tests, including a V3 table, run manually against PostgreSQL, not in CI. |
| Hadoop (storage-only) | Read only | Auto-discovery unit test; object-store integration test run manually. Writes go through a real catalog. |

Source: `docs/site/web/iceberg.json`.

---

## SQE differentiation

Not captured in the rubric but material to picking SQE:

- **OIDC bearer-token passthrough.** Every query runs as the authenticated user, with no service account.
- **Fine-grained access control in the engine.** Row filters, column masks and column restriction are applied by rewriting the logical plan before optimization (Ranger, Polaris grants).
- **Full SQL DML.** DELETE, UPDATE and MERGE in both copy-on-write and merge-on-read modes.
- **Arrow Flight SQL first, Trino HTTP compatible.** No JVM.
- **Benchmarks.** See `README.md` and `benchmarks/results/` for the current numbers.

---

## Contributing

1. Make the change in code.
2. Update the matching entry in `docs/evidence/iceberg-matrix-state.json` (level, notes, evidence, caveats, `score`, `generatedAt`).
3. Run `cargo xtask matrix-report` to check the declared score. CI runs it with `--min-percent` in `ci/aikido-rust-extra.sh`.
4. Regenerate this file: `python3 scripts/render-iceberg-matrix.py`.

## See also

- [Engine comparison](./iceberg-matrix-compare.md)
- [Openspec change](../../../openspec/changes/iceberg-matrix-parity/proposal.md) with proposal, design, specs and tasks (the upstream submission is section 2.22 on)
- [Source roadmap](../plans/2026-04-24-iceberg-matrix-parity.md) with upstream research and deferral rationale
- [Matrix parity workflow](./matrix-parity-workflow.md)
