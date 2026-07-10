## Why

SQE scores 58/189 (31%) on the public [icebergmatrix.org](https://icebergmatrix.org) rubric, tied with AWS Athena and below DuckDB. The matrix is the de-facto reference engineers use to pick an Iceberg engine; under-scoring makes SQE invisible for evaluations even though it beats Trino on benchmarks. The delta between us and PyIceberg (69%) is almost entirely features that already exist in our vendored `iceberg-rust` tree but are not surfaced through SQL or the catalog layer.

Additionally, once the gap closes we can submit SQE to the matrix as a first-class OSS entry, which is a direct channel for reaching evaluators.

## What Changes

Eight workstreams lift SQE from 58/189 to ~156/189 (83%), landing between Flink (81%) and Spark (93%).

- **Phase A: Catalog adoption sweep.** Adopt `iceberg-catalog-glue`, `iceberg-catalog-hms`, `iceberg-catalog-sql`, `storage-only` backends from apache/iceberg-rust workspace. Wire Nessie via REST. Adds Unity Catalog OIDC-M2M auth flow. Overlaps with existing `pluggable-catalogs` change, which remains the canonical source for catalog trait design.
- **Phase B: Table maintenance SQL.** Add `CALL system.rewrite_data_files`, `CALL system.expire_snapshots`, `CALL system.remove_orphan_files`, `CALL system.rewrite_manifests` procedures on top of existing vendored actions.
- **Phase C: Branching and tagging.** Add `Transaction::create_branch`/`create_tag` wrappers to vendored iceberg-rust. Add SQL surface: `ALTER TABLE ... CREATE BRANCH/TAG`, `SELECT ... FOR VERSION AS OF 'branch'`.
- **Phase D: V3 type-exposure polish.** Expose nanosecond timestamps (`TIMESTAMP_NS`/`TIMESTAMPTZ_NS`) and column default values (`DEFAULT <expr>` in CREATE TABLE) through sqe-sql. Both are already in vendored iceberg-rust.
- **Phase E: Equality deletes + RowDeltaAction.** Cherry-pick [iceberg-rust#2203](https://github.com/apache/iceberg-rust/pull/2203). Build SQE planner path that chooses equality-delete writes vs CoW based on `write.update.mode` table property.
- **Phase F: Puffin bloom filters + stats hook.** Enable Parquet bloom filter write. Emit Puffin sidecar with NDV sketches on commit. Wait for DataFusion [#21157](https://github.com/apache/datafusion/issues/21157) to consume via `StatisticsSource` trait.
- **Phase G: CDC incremental scan.** Implement `SELECT ... FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y` via snapshot-range walk. Add `_change_type` / `_commit_snapshot_id` meta columns.
- **Phase H: MoR UPDATE/MERGE.** Once equality deletes land in Phase E, add MoR path for UPDATE and MERGE based on table property. Unblocks SF100 `trade_result_update_holding` (currently 120s timeout).

Deferred to next cycle with reasons documented: Variant type, shredded Variant, geometry type, vector type, multi-argument partition transforms, lineage.

After completion, submit SQE entry to Neuw84/iceberg-matrix.

## Capabilities

### New Capabilities

- `iceberg-v3-types`: Nanosecond timestamps and column default values surfaced end-to-end (DDL, scan, predicate pushdown).
- `iceberg-table-maintenance`: SQL `CALL system.*` procedures for compaction, snapshot expiry, orphan file removal, and manifest rewrite.
- `iceberg-branching-tagging`: Named branches and tags on table snapshots, with SQL DDL for create/drop and `FOR VERSION AS OF` query-time selection.
- `iceberg-row-deltas`: Equality delete writer commit path (RowDeltaAction) and MoR mode dispatch for UPDATE/MERGE/DELETE based on table properties.
- `iceberg-puffin-stats`: Parquet bloom filter write, Puffin NDV theta sketch emission, and DataFusion `StatisticsSource` consumer.
- `iceberg-cdc-scan`: Snapshot-range incremental scan and changelog view with `_change_type` / `_change_ordinal` / `_commit_snapshot_id` columns.
- `iceberg-matrix-submission`: Public submission of SQE to Neuw84/iceberg-matrix as an OSS engine entry.

### Modified Capabilities

- `pluggable-catalogs`: Extended to include AWS Glue, Hive Metastore, JDBC (SQL), and storage-only backends in addition to the Iceberg REST / Nessie / Unity path already in the active change. Unity Catalog OIDC-M2M auth is new.

## Impact

**Code:**
- `vendor/iceberg-rust/` - new transaction wrappers (branching/tagging), cherry-picked RowDeltaAction patch
- `crates/sqe-catalog/` - new backend modules, Puffin stats source, incremental scan planner
- `crates/sqe-sql/` - procedure parser, branching DDL, nanosec/default DDL, `FOR INCREMENTAL BETWEEN` syntax
- `crates/sqe-planner/` - mode dispatch for CoW vs MoR, row-delta plan
- `crates/sqe-coordinator/` - procedure handler, CDC meta column plumbing
- `crates/sqe-worker/` - Parquet bloom filter write hook, Puffin emit on commit

**Dependencies:**
- Add `iceberg-catalog-glue`, `iceberg-catalog-hms`, `iceberg-catalog-sql` (workspace)
- No new external crates beyond apache/iceberg-rust family

**Upstream monitored:**
- [iceberg-rust#2203](https://github.com/apache/iceberg-rust/pull/2203) RowDeltaAction (draft, cherry-pick target)
- [iceberg-rust#1939](https://github.com/apache/iceberg-rust/issues/1939) Tag in FastAppend (unblocked)
- [iceberg-rust#2145](https://github.com/apache/iceberg-rust/issues/2145) ExpireSnapshotsAction wrapper
- [datafusion#21157](https://github.com/apache/datafusion/issues/21157) StatisticsSource trait (blocks Phase F consumer side)
- [datafusion#20746](https://github.com/apache/datafusion/issues/20746) MERGE INTO native plan (unblocks Phase H simplification)
- RisingWave iceberg-rust fork rebases (affects Phase E cherry-pick conflict risk)

**Breaking changes:** None. All additions.

**External submission:** Neuw84/iceberg-matrix PR after Phase A completes.
