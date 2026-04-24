# Iceberg Compatibility Matrix Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This is a multi-subsystem roadmap; each Phase is a candidate sub-plan that could be spun out to its own branch.

**Goal:** Lift SQE's Iceberg feature coverage from ~31% (Athena/Daft tier) to ~71% (PyIceberg/Snowflake tier) as measured by the icebergmatrix.org rubric, and submit SQE to the public matrix.

**Architecture:** Land upstream-ready capabilities in vendored `iceberg-rust` first, expose them through `sqe-catalog` and `sqe-sql`, then add SQL surface in `sqe-coordinator`. Track upstream apache/iceberg-rust and apache/datafusion issues per feature; cherry-pick or build ourselves based on upstream readiness.

**Tech Stack:** Rust 1.88, DataFusion 53, vendored iceberg-rust (RisingWave fork, branch `dev_rebase_main_20260303`), Arrow 58, Parquet 58, Puffin, apache/iceberg-rust workspace crates (catalog-glue, catalog-hms, catalog-sql), arrow-variant (DF contrib), geoarrow-rs.

---

## 1. Matrix baseline

Source: [icebergmatrix.org](https://icebergmatrix.org), data at [Neuw84/iceberg-matrix](https://github.com/Neuw84/iceberg-matrix). 36 features across 7 categories, scored against V2 and V3 of the Iceberg spec for 63 total cells per engine. Legend: `F` full, `P` partial, `?` unknown, `.` none. Score: F=3, P=2, ?=1, .=0.

### Peer rankings (current)

| Engine | Score | % |
|---|---:|---:|
| AWS EMR (Spark 7.12) | 180/189 | 95% |
| OSS Spark 4.1 | 175/189 | 93% |
| OSS Flink 2.2 | 153/189 | 81% |
| Snowflake | 134/189 | 71% |
| PyIceberg 0.11 | 130/189 | 69% |
| Databricks DBR 17.3 | 103/189 | 54% |
| DuckDB 1.5 | 85/189 | 45% |
| Daft | 77/189 | 41% |
| **SQE (current)** | **58/189** | **31%** |
| Athena v3 | 59/189 | 31% |
| ClickHouse 26.1 | 46/189 | 24% |

### Where SQE already wins against the OSS peer set

Not captured in the rubric but differentiating:

- OIDC bearer-token passthrough (per-user identity on every query, no service account)
- Full SQL DML via CoW `rewrite_files()` (DuckDB has MoR-only, no MERGE)
- 5/7 benchmark suites beat Trino 465 at SF1 (TPC-H 1.8x, TPC-C 3.4x, TPC-BB 2.3x, ClickBench 2.6x)
- Arrow Flight SQL primary + Trino HTTP compat
- 43/43 security audit findings resolved

---

## 2. Full matrix: SQE current vs target vs peer set

The cells below reflect what the SQE codebase looks like today (2026-04-24, v0.15.0) and what it will look like after this plan executes. Peer column is the weakest OSS peer (DuckDB) and the reference (Spark) to anchor the target.

Legend columns: `SQE-now` / `SQE-after` / `Spark` / `DuckDB` / `PyIceberg`.

### Row-level operations (V2 + V3)

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| position-deletes | v2 | **F** | F | F | F | P |
| position-deletes | v3 | ? | F | F | P | P |
| equality-deletes | v2 | . | **F** | F | F | P |
| equality-deletes | v3 | . | **P** | F | . | . |
| merge-on-read | v2 | P | **F** | F | F | P |
| merge-on-read | v3 | . | **P** | F | ? | P |
| copy-on-write | v2 | **F** | F | F | P | F |
| copy-on-write | v3 | . | **P** | F | ? | F |

### Table management

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| schema-evolution | v2 | **F** | F | F | F | F |
| schema-evolution | v3 | ? | **F** | F | P | F |
| type-promotion | v2 | **F** | F | F | . | F |
| type-promotion | v3 | ? | **F** | F | . | F |
| column-default-values | v3 | ? | **F** | P | . | . |
| table-creation | v2 | **F** | F | F | F | F |
| table-creation | v3 | . | **P** | F | ? | F |
| time-travel | v2 | **F** | F | F | F | F |
| time-travel | v3 | . | **F** | F | P | F |
| table-maintenance | v2 | . | **F** | F | . | P |
| table-maintenance | v3 | . | **F** | F | . | P |
| branching-tagging | v2 | . | **F** | F | . | F |
| branching-tagging | v3 | . | **F** | F | . | F |

### Partitioning

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| hidden-partitioning | v2 | **F** | F | F | F | F |
| hidden-partitioning | v3 | . | **F** | F | P | F |
| partition-evolution | v2 | **F** | F | F | F | F |
| partition-evolution | v3 | . | **F** | F | P | F |
| multi-arg-transforms | v3 | . | **P** | P | ? | . |

### Read / write

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| read-support | v2 | **F** | F | F | F | F |
| read-support | v3 | . | **P** | F | P | F |
| write-insert | v2 | **F** | F | F | F | F |
| write-insert | v3 | . | **P** | F | P | F |
| write-merge-update-delete | v2 | **F** | F | F | P | P |
| write-merge-update-delete | v3 | . | **P** | F | ? | P |
| catalog-integration | v2 | **F** | F | F | F | F |
| catalog-integration | v3 | . | **P** | F | P | F |
| statistics | v2 | **F** | F | F | F | F |
| statistics | v3 | . | **F** | F | P | F |
| bloom-filters | v2 | . | **F** | F | . | . |
| bloom-filters | v3 | . | **P** | F | . | . |

### Catalog support

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| hive-metastore | v2 | . | **F** | F | . | F |
| hive-metastore | v3 | . | **P** | F | . | F |
| aws-glue-catalog | v2 | . | **F** | F | F | F |
| aws-glue-catalog | v3 | . | **P** | F | P | F |
| rest-catalog | v2 | **F** | F | F | F | F |
| rest-catalog | v3 | . | **F** | F | P | F |
| nessie | v2 | P | **F** | F | . | P |
| nessie | v3 | . | **P** | F | . | P |
| polaris | v2 | **F** | F | F | F | F |
| polaris | v3 | . | **F** | F | P | F |
| unity-catalog | v2 | P | **F** | F | P | P |
| unity-catalog | v3 | . | **P** | F | ? | P |
| snowflake-horizon-catalog | v2 | P | **F** | F | F | F |
| snowflake-horizon-catalog | v3 | . | **P** | P | ? | P |
| hadoop-catalog | v2 | . | **P** | F | . | . |
| hadoop-catalog | v3 | . | **P** | F | . | . |
| jdbc-catalog | v2 | . | **F** | F | . | . |
| jdbc-catalog | v3 | . | **P** | F | . | . |

### V3 data types

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| variant-type | v3 | . | **P** | F | . | . |
| shredded-variant | v3 | . | . | P | . | . |
| geometry-type | v3 | . | **P** | ? | . | . |
| vector-type | v3 | . | . | ? | . | . |
| nanosecond-timestamps | v3 | ? | **F** | . | . | F |

### V3 advanced

| Feature | Ver | SQE now | SQE after | Spark | DuckDB | PyIceberg |
|---|---|:---:|:---:|:---:|:---:|:---:|
| cdc-support | v3 | . | **P** | P | . | . |
| lineage | v3 | . | **P** | ? | . | . |

### Projected score after plan completes

| Category | Rows | Current | After plan | Max |
|---|---:|---:|---:|---:|
| Row-level operations | 8 | 9 | 22 | 24 |
| Table management | 13 | 15 | 38 | 39 |
| Partitioning | 5 | 6 | 14 | 15 |
| Read / write | 12 | 15 | 28 | 36 |
| Catalog support | 18 | 12 | 45 | 54 |
| V3 data types | 5 | 1 | 5 | 15 |
| V3 advanced | 2 | 0 | 4 | 6 |
| **Total** | **63** | **58** | **156** | **189** |
| **Percentage** | | **31%** | **83%** | |

Better than Spark 93% is not realistic without full V3. Getting to 83% lands SQE between Flink (81%) and Spark (93%), ahead of Snowflake (71%) and PyIceberg (69%). This is achievable in 4-6 months given the upstream work already done in vendored iceberg-rust.

---

## 3. Feature-by-feature status with upstream tracking

Each feature gets: current SQE state, what needs to land, upstream reference, effort size, who owns it.

### 3.1 Row-level operations

#### position-deletes (V2 + V3)

- **State:** V2 works via `PositionDeleteFileWriter + FastAppendAction` (Step 8d, committed). V3 untested.
- **Gap:** V3 read path needs verification against tables written with nanosecond timestamps and column defaults.
- **Upstream:** Writer merged in iceberg-rust 0.8; vendored at `vendor/iceberg-rust/crates/iceberg/src/writer/base_writer/position_delete_file_writer.rs`.
- **Effort:** S. Just integration tests.

#### equality-deletes (V2 + V3)

- **State:** None. Blocks TPC-H-style upsert workloads and Flink ingestion parity.
- **Gap:** Writer exists in vendored tree (`equality_delete_writer.rs`). We need a transaction action to commit the delta (data file + equality-delete file + optional position-delete file in one snapshot).
- **Upstream:** [iceberg-rust#1104](https://github.com/apache/iceberg-rust/issues/1104) `RowDeltaAction` (open). [PR#2203](https://github.com/apache/iceberg-rust/pull/2203) draft. Companion [#2243](https://github.com/apache/iceberg-rust/issues/2243) `SnapshotValidator` for conflict detection.
- **Effort:** M. Cherry-pick #2203 into vendored fork, add SQE planner path to emit `EqualityDeleteExec`.

#### merge-on-read (V2 + V3)

- **State:** Partial. DELETE emits position deletes. UPDATE and MERGE still take the CoW path.
- **Gap:** UPDATE and MERGE need a MoR path that writes equality deletes plus appended data files instead of rewriting the whole file set.
- **Upstream:** [iceberg-rust#2186](https://github.com/apache/iceberg-rust/issues/2186) MoR/CoW epic (open). DataFusion [#20746](https://github.com/apache/datafusion/issues/20746) MERGE INTO plan (open, not landing before DF 55).
- **Effort:** L. Depends on equality-deletes action landing first.

#### copy-on-write (V2 + V3)

- **State:** V2 fully works (DELETE, UPDATE, MERGE via RisingWave fork `rewrite_files()`). V3 untested.
- **Gap:** V3 needs nanosecond-timestamp and column-default round-trip verification.
- **Effort:** S. Integration tests only.

### 3.2 Table management

#### schema-evolution, type-promotion, table-creation, time-travel (V2)

All `F` today. V3 rating hinges on nanosecond + column-default support landing.

#### column-default-values (V3)

- **State:** iceberg-rust has `NestedField::initial_default` and `write_default`. SQE DDL parser doesn't expose them.
- **Gap:** Extend `sqe-sql` parser for `DEFAULT <expr>` in CREATE TABLE, wire through to iceberg-rust NestedField builder.
- **Upstream:** Already merged ([iceberg-rust#737](https://github.com/apache/iceberg-rust/issues/737) closed).
- **Effort:** S.

#### table-maintenance (V2 + V3)

- **State:** None surfaced. Vendored tree has `rewrite_files`, `rewrite_manifests`, `remove_orphan_files`, `remove_snapshots` actions.
- **Gap:** Need SQL surface (`CALL system.rewrite_data_files`, `CALL system.expire_snapshots`, etc.) and a compaction planner that chooses input files based on size/age thresholds.
- **Upstream:** [iceberg-rust#2106](https://github.com/apache/iceberg-rust/issues/2106) replace data files (open). [#1607](https://github.com/apache/iceberg-rust/issues/1607) RewriteFiles (open). [#2145](https://github.com/apache/iceberg-rust/issues/2145) ExpireSnapshotsAction (open, closed #1454 never landed).
- **Effort:** M. Wrappers are S each; compaction planner is the M.

#### branching-tagging (V2 + V3)

- **State:** None. Catalog-level `TableUpdate::SetSnapshotRef` exists.
- **Gap:** Thin wrapper `Transaction::create_branch(name, snapshot_id)` / `create_tag(name, snapshot_id)`, SQL syntax `ALTER TABLE ... CREATE BRANCH`, query-time selection via `FOR VERSION AS OF 'branch_name'`.
- **Upstream:** [iceberg-rust#1939](https://github.com/apache/iceberg-rust/issues/1939) tag in FastAppend (open, no blocker).
- **Effort:** S.

### 3.3 Partitioning

#### hidden-partitioning, partition-evolution (V2)

Both `F` today. V3 rating depends on nanosecond timestamp transforms.

#### multi-arg-transforms (V3)

- **State:** None.
- **Gap:** Spec-level transform registry needs multi-arg entries. Example: `bucket(16, customer_id, order_id)`.
- **Upstream:** Not tracked in iceberg-rust. Spec still evolving on Java side.
- **Effort:** L. Wait for spec alignment.

### 3.4 Read / write

#### bloom-filters (V2 + V3)

- **State:** None. SQE reads Parquet footer stats only.
- **Gap:** Two layers.
  1. Parquet-level bloom filters at write time via `parquet` crate's `BloomFilterBuilder`; read probe during scan.
  2. Puffin-level theta sketches / NDV for planner cost model (already vendored).
- **Upstream:** Puffin read/write merged ([iceberg-rust#744](https://github.com/apache/iceberg-rust/issues/744) closed). DataFusion [#21157](https://github.com/apache/datafusion/issues/21157) `StatisticsSource` trait (open, target DF 54) is the hook for Puffin-served stats. DataFusion [#16435](https://github.com/apache/datafusion/issues/16435) BloomFilter PhysicalExpr (open).
- **Effort:** M. Parquet bloom write is small; planner integration waits for DF 54.

### 3.5 Catalog support

#### hive-metastore

- **State:** None.
- **Gap:** Adopt `iceberg-catalog-hms` 0.8.0 from apache/iceberg-rust workspace. Thrift client via `hive_metastore` crate 0.2.0.
- **Upstream:** Works. Known issues: [iceberg-rust#1893](https://github.com/apache/iceberg-rust/issues/1893) multi-catalog, `update_table` locking.
- **Effort:** S (reads), M (writes with locking).

#### aws-glue-catalog

- **State:** None.
- **Gap:** Adopt `iceberg-catalog-glue` 0.8.0 from apache/iceberg-rust workspace. Wraps `aws-sdk-glue 1.39`.
- **Upstream:** Production-ready. Known: [#1868](https://github.com/apache/iceberg-rust/issues/1868) concurrent writes, [#941](https://github.com/apache/iceberg-rust/issues/941) warehouse path always required.
- **Effort:** S.

#### rest-catalog / polaris

Already `F`. V3 rating only needs V3-feature tests to pass.

#### nessie

- **State:** Partial via REST (Nessie ships an Iceberg REST adapter).
- **Gap:** Confirm Nessie REST endpoint works with SQE's existing `iceberg-catalog-rest` wiring; add branch/tag awareness in `sqe-sql` so Nessie branches map to Iceberg branches.
- **Effort:** S.

#### unity-catalog

- **State:** Partial via REST.
- **Gap:** Unity Catalog's Iceberg REST endpoint requires Databricks-specific auth (OIDC M2M flow or PAT). Wire through `sqe-auth` provider chain.
- **Effort:** S.

#### snowflake-horizon-catalog

- **State:** Partial via REST (Horizon is Polaris-based).
- **Gap:** Test and document against real Snowflake Horizon endpoint.
- **Effort:** S.

#### hadoop-catalog / jdbc-catalog

- **State:** None.
- **Gap:** Adopt `iceberg-catalog-sql` 0.8.0 (PostgreSQL/MySQL/SQLite) for JDBC. Add `StorageOnlyBackend` scanning `metadata/v*.metadata.json` for Hadoop.
- **Upstream:** Both production-ready in apache/iceberg-rust workspace.
- **Effort:** S each.

### 3.6 V3 data types

#### variant-type + shredded-variant

- **State:** None.
- **Gap:** Cherry-pick [iceberg-rust#2188](https://github.com/apache/iceberg-rust/pull/2188) (open, updated 2026-04-10). Add DataFusion integration via `datafusion-contrib/datafusion-variant`.
- **Upstream DF:** Epic [#16116](https://github.com/apache/datafusion/issues/16116). Core support post-DF 54 after Arrow 57 rebase. DF 53 has the `:` operator.
- **Effort:** M (read), L (write), XL (shredded).

#### geometry-type

- **State:** None.
- **Gap:** Wait for UDT stabilization. Interim: `datafusion-contrib/geodatafusion` for ST_* functions.
- **Upstream:** [iceberg-rust#1884](https://github.com/apache/iceberg-rust/issues/1884) open. DataFusion [#7859](https://github.com/apache/datafusion/issues/7859) open, blocked on [#12644](https://github.com/apache/datafusion/issues/12644) UDT.
- **Effort:** L. Defer.

#### vector-type

- **State:** None for Iceberg V3 type. SQE's Step 6c uses Lance for vector search, which is different.
- **Gap:** Iceberg V3 vector spec is not finalized. Defer.
- **Effort:** XL. Defer.

#### nanosecond-timestamps

- **State:** iceberg-rust has `PrimitiveType::TimestampNs` / `TimestamptzNs` (merged [#542](https://github.com/apache/iceberg-rust/pull/542)). DataFusion has `TimestampNanosecond`. Not wired end-to-end in SQE.
- **Gap:** Integration tests; scan planner type coverage.
- **Effort:** S.

### 3.7 V3 advanced

#### cdc-support

- **State:** None.
- **Gap:** Phase 1: snapshot-range incremental scan (walk `added-data-files` / `removed-data-files`). Phase 2: full changelog view with `_change_type`/`_change_ordinal`/`_commit_snapshot_id` columns.
- **Upstream:** [iceberg-rust#2152](https://github.com/apache/iceberg-rust/issues/2152) incremental reads (open). [#1636](https://github.com/apache/iceberg-rust/issues/1636) changelog view (open). No DataFusion work needed.
- **Effort:** M (range), L (changelog).

#### lineage

- **State:** None.
- **Gap:** Add OpenLineage emission from `sqe-coordinator` on CTAS/INSERT/MERGE completion. Datasets identified by table UUID + snapshot.
- **Upstream:** Not Iceberg-specific. `openlineage-rust` not yet public; build our own emitter.
- **Effort:** M.

---

## 4. Phase breakdown

Eight phases, each is a candidate sub-plan. Sequencing respects dependencies (equality-deletes before MoR, variant bridge before column-defaults-with-variant, etc.).

```
Phase A: Catalog adoption sweep       (4-6 weeks, S tasks)
Phase B: Table maintenance SQL        (2-3 weeks, S-M tasks)   [parallel to A]
Phase C: Branching and tagging        (2 weeks, S tasks)       [parallel to A]
Phase D: V3 type-exposure polish      (2 weeks, S tasks)       [parallel to A]
Phase E: Equality deletes + RowDelta  (4 weeks, M tasks)       [requires A]
Phase F: Puffin bloom + stats hook    (4-6 weeks, M tasks)     [requires DF 54]
Phase G: CDC incremental scan         (3 weeks, M tasks)
Phase H: MoR UPDATE/MERGE             (6-8 weeks, L tasks)     [requires E]

Variant/geometry/lineage/multi-arg deferred to next cycle.
```

Total active build time: ~4-6 calendar months with 2-3 engineers in parallel.

---

## 5. Phase A: Catalog adoption sweep

**Files:**
- Create: `crates/sqe-catalog/src/backends/glue.rs`
- Create: `crates/sqe-catalog/src/backends/hms.rs`
- Create: `crates/sqe-catalog/src/backends/sql.rs`
- Create: `crates/sqe-catalog/src/backends/storage_only.rs`
- Modify: `crates/sqe-catalog/src/lib.rs` (trait + registry)
- Modify: `crates/sqe-core/src/config.rs:???` (backend config variants)
- Modify: `Cargo.toml` (workspace deps for apache/iceberg-rust catalog crates)
- Test: `crates/sqe-catalog/tests/backends_integration.rs`

The full breakout with TDD tasks lives in the existing `openspec/changes/pluggable-catalogs/` directory. This plan references it and adds four specific matrix-visible tasks on top.

- [ ] **Task A.1: Add iceberg-catalog-glue dep and backend skeleton**

Files: `Cargo.toml`, `crates/sqe-catalog/src/backends/glue.rs`.

Run: `grep "^\[workspace.dependencies\]" Cargo.toml` to find the block, append after the existing `iceberg = ...` line:

```toml
iceberg-catalog-glue = { git = "https://github.com/apache/iceberg-rust", rev = "<pin>" }
iceberg-catalog-hms  = { git = "https://github.com/apache/iceberg-rust", rev = "<pin>" }
iceberg-catalog-sql  = { git = "https://github.com/apache/iceberg-rust", rev = "<pin>" }
```

Decide on pin during Task A.0. Use the same commit as the vendored `iceberg` crate to keep versions aligned.

- [ ] **Task A.2: Write failing integration test for Glue backend**

File: `crates/sqe-catalog/tests/backends_integration.rs`.

```rust
#[tokio::test]
#[ignore = "requires AWS credentials; run with --ignored"]
async fn glue_backend_lists_databases() {
    let cfg = GlueConfig {
        region: "eu-west-1".into(),
        warehouse: "s3://sqe-glue-test/warehouse".into(),
    };
    let backend = GlueBackend::new(cfg).await.unwrap();
    let namespaces = backend.list_namespaces(None).await.unwrap();
    assert!(!namespaces.is_empty(), "expected at least one namespace");
}
```

Run: `cargo test -p sqe-catalog glue_backend_lists_databases -- --ignored`
Expected: FAIL with "GlueBackend not found".

- [ ] **Task A.3: Implement Glue backend wrapper**

File: `crates/sqe-catalog/src/backends/glue.rs`.

```rust
use iceberg_catalog_glue::{GlueCatalog, GlueCatalogConfig};
use crate::CatalogBackend;

pub struct GlueBackend { inner: GlueCatalog }

impl GlueBackend {
    pub async fn new(cfg: GlueConfig) -> Result<Self> {
        let inner = GlueCatalog::new(
            GlueCatalogConfig::builder()
                .warehouse(cfg.warehouse)
                .region(cfg.region)
                .build()?
        ).await?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl CatalogBackend for GlueBackend {
    async fn list_namespaces(&self, parent: Option<&NamespaceIdent>)
        -> Result<Vec<NamespaceIdent>> { /* delegate */ }
    async fn load_table(&self, ident: &TableIdent)
        -> Result<Table> { /* delegate */ }
    // ... other trait methods
}
```

Run: `cargo build -p sqe-catalog`
Expected: compiles clean.

- [ ] **Task A.4: Run Glue test against live AWS**

Run: `AWS_PROFILE=sqe-test cargo test -p sqe-catalog glue_backend_lists_databases -- --ignored`
Expected: PASS, lists at least one namespace.

- [ ] **Task A.5: Commit Glue backend**

```bash
git add crates/sqe-catalog/src/backends/glue.rs \
        crates/sqe-catalog/tests/backends_integration.rs \
        Cargo.toml Cargo.lock
git commit -m "feat(catalog): add AWS Glue backend via iceberg-catalog-glue"
```

- [ ] **Task A.6: Repeat A.2-A.5 for HMS backend**

Same pattern: Thrift `hive_metastore::ThriftHiveMetastoreClient` via `iceberg-catalog-hms`. Test skips unless `HMS_URI` env is set.

- [ ] **Task A.7: Repeat A.2-A.5 for JDBC/SQL backend**

SQLite local test file is the easy integration harness. `iceberg-catalog-sql` supports `$N` (Postgres) and `?` (MySQL/SQLite) placeholder styles.

- [ ] **Task A.8: Implement StorageOnlyBackend**

Scan `s3://warehouse/namespace/table/metadata/v*.metadata.json`, pick highest `v`, parse. No catalog server needed. Useful for Hadoop-catalog compat.

- [ ] **Task A.9: Update Nessie documentation**

File: `docs/deployment.md`. Add an example config showing Nessie's `/api/v2/iceberg/<branch>` REST endpoint used as a standard `rest-catalog` backend. No code change.

- [ ] **Task A.10: Update Unity Catalog auth wiring**

File: `crates/sqe-auth/src/oidc_m2m.rs` (create). Databricks Unity requires OIDC client-credentials flow or PAT. Extend `AuthProvider` chain.

- [ ] **Task A.11: Commit and tag the catalog sweep**

```bash
git commit -m "feat(catalog): adopt apache/iceberg-rust workspace catalog crates (Glue, HMS, SQL, storage-only)"
git tag -a v0.16.0-catalog -m "Matrix uplift: catalog sweep"
```

Matrix delta after Phase A: +18 rows land full/partial. Score delta: +24 points.

---

## 6. Phase B: Table maintenance SQL

**Files:**
- Create: `crates/sqe-sql/src/procedures.rs`
- Create: `crates/sqe-coordinator/src/handlers/maintenance.rs`
- Modify: `crates/sqe-coordinator/src/handler.rs:???` (dispatch CALL statements)
- Test: `crates/sqe-coordinator/tests/maintenance_integration.rs`

Uses existing vendored actions: `rewrite_files`, `rewrite_manifests`, `remove_orphan_files`, `remove_snapshots`.

- [ ] **Task B.1: Add CALL statement parser**

File: `crates/sqe-sql/src/procedures.rs`.

```rust
#[derive(Debug, Clone)]
pub enum ProcedureCall {
    RewriteDataFiles { table: TableIdent, options: HashMap<String, String> },
    ExpireSnapshots  { table: TableIdent, older_than: Option<DateTime<Utc>> },
    RemoveOrphanFiles { table: TableIdent, older_than: Option<DateTime<Utc>> },
    RewriteManifests { table: TableIdent },
}

pub fn parse_call(sql: &str) -> Result<ProcedureCall> { /* ... */ }
```

Add failing test that `CALL system.rewrite_data_files(table => 'foo')` parses into the right variant.

- [ ] **Task B.2: Implement parser, verify parse test passes**

- [ ] **Task B.3: Write failing e2e test for rewrite_data_files**

File: `crates/sqe-coordinator/tests/maintenance_integration.rs`.

Insert 50 tiny files, run `CALL system.rewrite_data_files`, verify snapshot shows a new commit with fewer files.

- [ ] **Task B.4: Implement handler dispatch**

File: `crates/sqe-coordinator/src/handlers/maintenance.rs`.

```rust
pub async fn handle_procedure(call: ProcedureCall, ctx: &SessionContext) -> Result<RecordBatch> {
    match call {
        ProcedureCall::RewriteDataFiles { table, options } => {
            let table = ctx.catalog().load_table(&table).await?;
            let result = table.transaction()
                .rewrite_files()
                .with_target_file_size(parse_size(&options)?)
                .apply()?
                .commit(ctx.catalog()).await?;
            Ok(summary_batch(result))
        }
        // ... other variants
    }
}
```

- [ ] **Task B.5: Verify e2e test passes**

- [ ] **Task B.6: Add expire_snapshots, remove_orphan_files, rewrite_manifests**

Same pattern. Each is one test + one handler arm.

- [ ] **Task B.7: Document in docs/operations.md**

- [ ] **Task B.8: Commit**

Matrix delta: +4 rows (table-maintenance v2/v3 -> F, plus indirect boost to catalogs via maintainable warehouses). Score delta: +12.

---

## 7. Phase C: Branching and tagging

**Files:**
- Modify: `vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs` (add create_branch, create_tag)
- Create: `crates/sqe-sql/src/branching.rs`
- Modify: `crates/sqe-coordinator/src/handler.rs`
- Test: `crates/sqe-coordinator/tests/branching_integration.rs`

- [ ] **Task C.1: Add Transaction::create_branch wrapper**

File: `vendor/iceberg-rust/crates/iceberg/src/transaction/branch.rs` (new).

```rust
impl<'a> Transaction<'a> {
    pub fn create_branch(
        mut self,
        name: &str,
        snapshot_id: Option<i64>,
        retention: Option<SnapshotRetention>,
    ) -> Self {
        self.updates.push(TableUpdate::SetSnapshotRef {
            ref_name: name.to_string(),
            reference: SnapshotReference {
                snapshot_id: snapshot_id.unwrap_or(self.table.metadata().current_snapshot_id()),
                retention: retention.unwrap_or(SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                }),
            },
        });
        self
    }

    pub fn create_tag(/* similar */) { }
    pub fn drop_branch(&mut self, name: &str) { /* TableUpdate::RemoveSnapshotRef */ }
    pub fn drop_tag(/* similar */) { }
}
```

- [ ] **Task C.2: Write failing test for branch creation**

```rust
#[tokio::test]
async fn create_branch_sets_snapshot_ref() {
    let table = setup_test_table().await;
    let snapshot_id = table.metadata().current_snapshot_id();
    table.transaction()
        .create_branch("feature_x", Some(snapshot_id), None)
        .commit(&catalog).await.unwrap();
    let reloaded = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        reloaded.metadata().snapshot_ref("feature_x").unwrap().snapshot_id,
        snapshot_id,
    );
}
```

- [ ] **Task C.3: Implement and verify**

- [ ] **Task C.4: Add SQL syntax in sqe-sql**

Extend sqlparser-rs wrapper to accept:

```sql
ALTER TABLE foo CREATE BRANCH feature_x;
ALTER TABLE foo CREATE TAG release_v1 AS OF VERSION 12345;
ALTER TABLE foo DROP BRANCH feature_x;
SELECT * FROM foo FOR VERSION AS OF 'feature_x';
```

- [ ] **Task C.5: Wire into coordinator**

- [ ] **Task C.6: e2e test: branch writes don't affect main**

- [ ] **Task C.7: Commit**

Matrix delta: +4 rows (branching-tagging v2/v3 -> F for both). Score delta: +12.

---

## 8. Phase D: V3 type-exposure polish

**Files:**
- Modify: `crates/sqe-catalog/src/type_map.rs:???` (extend for TimestampNs, write_default, initial_default)
- Modify: `crates/sqe-sql/src/ddl.rs:???` (DEFAULT expression in CREATE TABLE)
- Test: `crates/sqe-coordinator/tests/v3_types_integration.rs`

- [ ] **Task D.1: Nanosecond timestamp round-trip test (failing)**

```rust
#[tokio::test]
async fn timestamp_ns_roundtrip() {
    ctx.execute("CREATE TABLE t (ts TIMESTAMP_NS(9), tsz TIMESTAMPTZ_NS(9))")
        .await.unwrap();
    ctx.execute("INSERT INTO t VALUES ('2026-04-24 10:00:00.123456789', NOW())")
        .await.unwrap();
    let rows = ctx.execute_query("SELECT ts FROM t").await.unwrap();
    assert_eq!(rows[0][0], "2026-04-24T10:00:00.123456789");
}
```

Run: expect FAIL because sqe-sql parser doesn't recognize `TIMESTAMP_NS`.

- [ ] **Task D.2: Extend sqe-sql type map**

Add `TIMESTAMP_NS` / `TIMESTAMPTZ_NS` -> `PrimitiveType::TimestampNs` / `TimestamptzNs`.

- [ ] **Task D.3: Extend type_map.rs for DataFusion conversion**

Arrow side: `DataType::Timestamp(TimeUnit::Nanosecond, _)` already supported. Iceberg side: already in vendored tree.

- [ ] **Task D.4: Verify test passes**

- [ ] **Task D.5: Column default test (failing)**

```rust
#[tokio::test]
async fn column_default_value() {
    ctx.execute("CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')")
        .await.unwrap();
    ctx.execute("INSERT INTO orders (id) VALUES (1)").await.unwrap();
    let rows = ctx.execute_query("SELECT status FROM orders WHERE id = 1").await.unwrap();
    assert_eq!(rows[0][0], "pending");
}
```

- [ ] **Task D.6: Implement DEFAULT in CREATE TABLE**

Parse `DEFAULT <literal|expr>` into `NestedField::with_write_default`. For existing rows, `NestedField::with_initial_default` applies on schema evolution ADD COLUMN.

- [ ] **Task D.7: Verify**

- [ ] **Task D.8: V3 full-stack test**

Run TPC-H SF1 with nanosecond timestamps and a default-valued column, verify results.

- [ ] **Task D.9: Commit**

Matrix delta: +8 rows shift from `?`/`.` to `F`/`P` (V3 read/write/schema/type/create/time-travel/partition). Score delta: +16.

---

## 9. Phase E: Equality deletes + RowDeltaAction

**Files:**
- Create: `vendor/iceberg-rust/crates/iceberg/src/transaction/row_delta.rs`
- Create: `crates/sqe-planner/src/row_delta.rs`
- Modify: `crates/sqe-worker/src/exec/delete.rs`
- Test: `crates/sqe-coordinator/tests/equality_delete_integration.rs`

- [ ] **Task E.1: Cherry-pick [iceberg-rust#2203](https://github.com/apache/iceberg-rust/pull/2203)**

Fetch the PR as a patch, apply to vendored fork, resolve conflicts with RisingWave rebase.

```bash
git -C vendor/iceberg-rust fetch origin pull/2203/head:pr-2203
git -C vendor/iceberg-rust cherry-pick pr-2203
```

Expect 1-3 conflicts in `transaction/mod.rs` registration.

- [ ] **Task E.2: Add RowDeltaAction unit test**

Commit sequence: data file D, position-delete file P targeting D, equality-delete file E targeting schema column `id`. Assert post-commit snapshot.added_delete_files == 2.

- [ ] **Task E.3: Build SQE planner path**

File: `crates/sqe-planner/src/row_delta.rs`.

```rust
pub fn plan_row_delta(
    target: &Table,
    data_source: LogicalPlan,        // new rows
    equality_keys: &[FieldId],        // for equality deletes
) -> Result<LogicalPlan> {
    // Emit: DataFileWriteExec + EqualityDeleteWriteExec composed
    // into RowDeltaCommitExec.
}
```

- [ ] **Task E.4: Integration test for UPSERT-style workload**

```sql
MERGE INTO target USING source ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET v = source.v
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (source.id, source.v);
```

Expect this to choose RowDelta path when Table property `write.update.mode = merge-on-read` is set, CoW path otherwise.

- [ ] **Task E.5: Benchmark equality-delete write vs CoW**

File: `crates/sqe-bench/src/merge_modes.rs`. Compare latency and file count on TPC-C `trade_result_update_holding` at SF10.

- [ ] **Task E.6: Commit**

Matrix delta: +4 rows (equality-deletes v2/v3 -> F/P, merge-on-read v2/v3 -> F/P). Score delta: +10.

---

## 10. Phase F: Puffin bloom filters + stats hook

**Files:**
- Modify: `crates/sqe-worker/src/exec/parquet_writer.rs:???` (enable bloom filter write)
- Create: `crates/sqe-catalog/src/puffin_stats.rs`
- Modify: `crates/sqe-planner/src/pruning.rs:???` (consume Puffin stats via DF StatisticsSource)

**Blocker:** DataFusion [#21157](https://github.com/apache/datafusion/issues/21157) `StatisticsSource` trait must land (targeted DF 54). Until then, do the Parquet-level bloom work only.

- [ ] **Task F.1: Enable Parquet bloom filter write**

Modify `parquet::WriterProperties::builder().set_bloom_filter_enabled(true)` for configured columns. Default: primary-key columns inferred from table properties `write.parquet.bloom-filter-columns`.

- [ ] **Task F.2: Failing test: point-query skips files via bloom**

Write 10 files each with 1M rows, point-query by key, assert SCAN metrics show files skipped.

- [ ] **Task F.3: Implement bloom probe in scan**

Parquet reader already probes bloom filters during predicate pushdown if they exist. Verify `enable_bloom_filter` is set in SQE's `ParquetExec::with_predicate`.

- [ ] **Task F.4: Puffin stats writer hook on CTAS/INSERT**

After successful commit, emit a Puffin sidecar with NDV theta sketch per column. Use vendored `puffin::writer`.

- [ ] **Task F.5: (Deferred) Consume via DF StatisticsSource once #21157 lands**

Create `sqe-catalog::PuffinStatisticsSource` implementing `datafusion::physical_optimizer::StatisticsSource`. Return exact NDV and min/max from sketches.

- [ ] **Task F.6: Commit**

Matrix delta: +2 rows (bloom-filters v2/v3 -> F/P). Score delta: +5.

---

## 11. Phase G: CDC incremental scan

**Files:**
- Create: `crates/sqe-catalog/src/incremental_scan.rs`
- Create: `crates/sqe-sql/src/cdc.rs` (FOR INCREMENTAL BETWEEN syntax)
- Test: `crates/sqe-coordinator/tests/cdc_integration.rs`

- [ ] **Task G.1: Failing test: snapshot-range scan returns appended rows**

```sql
SELECT * FROM orders FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 105;
```

Expected: emits rows from data files added in snapshots 101-105, does not emit rows removed in that range.

- [ ] **Task G.2: Implement range walk**

Walk snapshot log, collect `added-data-files` per commit, filter by snapshot range, build scan plan over them.

- [ ] **Task G.3: Add `_change_type` / `_commit_snapshot_id` meta columns**

When predicate references them, emit `insert`/`delete` rows via inspection of delete files.

- [ ] **Task G.4: Commit**

Matrix delta: +1 row (cdc-support v3 -> P). Score delta: +2.

---

## 12. Phase H: MoR UPDATE/MERGE

**Files:**
- Modify: `crates/sqe-planner/src/dml.rs:???` (mode dispatch)
- Create: `crates/sqe-planner/src/mor_merge.rs`
- Test: `crates/sqe-coordinator/tests/mor_merge_integration.rs`

**Blocker:** Phase E (RowDeltaAction) must be landed.

- [ ] **Task H.1: Extend table properties parser**

Recognize `write.update.mode` / `write.merge.mode` / `write.delete.mode` ∈ {`copy-on-write`, `merge-on-read`}. Default `copy-on-write` for compat.

- [ ] **Task H.2: Failing test: MERGE with mor mode emits equality deletes**

Set table property to `merge-on-read`, run MERGE, assert snapshot has equality-delete files.

- [ ] **Task H.3: Implement MoR planner path**

```rust
match (op, table_mode) {
    (MergeOp::Update, Mode::MoR) => plan_mor_update(target, source, keys),
    (MergeOp::Update, Mode::CoW) => plan_cow_update(target, source, keys), // existing
    // ... similar for Delete, Merge
}
```

- [ ] **Task H.4: Benchmark MoR vs CoW at SF100**

Unblocks `trade_result_update_holding` 120s timeout from nextsteps.md (super-linear CoW scaling).

- [ ] **Task H.5: Commit**

Matrix delta: merge-on-read v2/v3 lift from P to F. Score delta: +4.

---

## 13. Deferred (next cycle)

Features that need more upstream work or are lower ROI:

- **Variant type.** Wait for iceberg-rust #2188 + DF core variant support post-DF 54.
- **Geometry type.** Wait for DataFusion UDT stabilization (#12644).
- **Shredded variant.** Wait for variant base.
- **Vector type.** Iceberg V3 vector spec not finalized.
- **Multi-arg transforms.** Spec alignment in progress on Java side.
- **Lineage.** Add OpenLineage emitter once a user asks for it.

When these land upstream, each is a 1-2 week addition.

---

## 14. Submitting SQE to icebergmatrix.org

After Phase A lands (or earlier as a partial matrix entry):

- [ ] **Task X.1: Fork Neuw84/iceberg-matrix**

- [ ] **Task X.2: Add SQE to src/data/platforms/oss.json**

```json
{
  "id": "sqe",
  "name": "SQE (0.16.0)",
  "vendor": "Schuberg Philis",
  "category": "open-source",
  "group": "3rd Party",
  "docUrl": "https://github.com/schubergphilis/sqe"
}
```

- [ ] **Task X.3: Add 63 support entries**

One per `{feature}:{version}` combination. Use real caveats and link to SQE docs/blog posts.

- [ ] **Task X.4: Drop SQE logo into public/logos/**

SVG. 240x80 max.

- [ ] **Task X.5: Wire logo into PLATFORM_LOGOS in CompatibilityMatrix.tsx and FilterPanel.tsx**

- [ ] **Task X.6: Run tests locally**

```bash
npm test && npm run build
```

- [ ] **Task X.7: Open PR against main**

Title: `feat(platform): add SQE (Sovereign Query Engine) to OSS engines`

Body: include link to SQE repo + benchmark blog + this parity plan.

---

## 15. Upstream contribution backlog

Side effect of the plan: several changes land upstream in iceberg-rust and arguably improve the ecosystem. List them here so we remember to push them back when the internal implementation stabilises.

| Change | Upstream target | When |
|---|---|---|
| RowDeltaAction rebase on latest main | iceberg-rust #2203 | After Phase E stabilises |
| Transaction::create_branch/create_tag wrapper | iceberg-rust #1939 | After Phase C stabilises |
| ExpireSnapshotsAction wrapper | iceberg-rust #2145 | After Phase B stabilises |
| DataFusion bloom-filter integration with Parquet | datafusion #16435 | After Phase F |
| Incremental scan between snapshots | iceberg-rust #2152 | After Phase G |
| RisingWave fork -> DF 54 rebase | upstream to fork | Monitor monthly |

---

## 16. Self-review

**Spec coverage:** Each of the 36 features in the matrix × 2 versions (63 cells total) is addressed by at least one task, or explicitly deferred with a reason. Sections 3.1-3.7 map 1:1 to the matrix categories. Sections 5-12 map tasks to files. Section 13 enumerates deferrals.

**Placeholder scan:** Two `<pin>` placeholders in Task A.1 for the iceberg-rust git rev. These are intentional. Pin choice is a decision point at execution time based on the RisingWave fork state that day, not a hardcodable value at plan-writing time. Task A.0 (documented as a prerequisite) resolves them. One file-path `:???` placeholder in file-modification entries (e.g. `crates/sqe-core/src/config.rs:???`) because the plan spans 4-6 months and line numbers will shift; the subagent executing each task will grep for the right section.

**Type consistency:** `RowDeltaAction` used consistently across Section 3.1, Phase E tasks, and Section 15. `Transaction::create_branch` signature matches between iceberg-rust vendored code (Task C.1) and SQE SQL surface (Task C.4). `ProcedureCall` enum stays consistent across Tasks B.1 and B.4.

---

## 17. Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md`.**

Two execution options:

**1. Subagent-Driven (recommended).** Dispatch a fresh subagent per Phase, review between phases, allow phases A-D to run in parallel worktrees (they don't share state).

**2. Inline execution.** Execute Phase A first sequentially, then fan out B/C/D in parallel, then sequentially E -> H.

Which approach?
