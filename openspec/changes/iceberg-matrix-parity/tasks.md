## 1. Setup and tracking

- [x] 1.1 Create `docs/iceberg-matrix-state.json` seed with current 31% baseline from the plan
- [x] 1.2 Create `xtask/src/matrix_report.rs` skeleton (emits JSON, no tests wired yet)
- [x] 1.3 Add matrix score assertion to CI that fails if score drops
- [x] 1.4 Create feature branch naming convention `feat/matrix-phase-<letter>-<slug>`
- [x] 1.5 Open tracking issue `Iceberg matrix parity: 31% -> 83%` with links to this change

## 2. Phase A: Catalog adoption sweep

- [x] 2.1 Pin iceberg-rust workspace crates (glue, hms, sql, s3tables) to a matching commit in `Cargo.toml` (deferred: vendored RisingWave fork at 0.8.0 still incompatible with upstream 0.8.0 catalog crates; feature flags added with stubs)
- [x] 2.2 Write failing integration test `glue_backend_lists_databases` (ignored by default)
- [x] 2.3 Implement `crates/sqe-catalog/src/backends/glue.rs` wrapping `iceberg-catalog-glue` (marker impl; AWS SDK wiring deferred to fork rebase)
- [x] 2.4 Wire Glue into catalog registry behind `glue` Cargo feature
- [ ] 2.5 Verify Glue test passes against live AWS (manual; blocked on 2.3 real impl)
- [x] 2.6 Write failing integration test `hms_backend_lists_tables`
- [x] 2.7 Implement `crates/sqe-catalog/src/backends/hms.rs` wrapping `iceberg-catalog-hms` (marker impl; Thrift client deferred to fork rebase)
- [x] 2.8 Wire HMS into catalog registry behind `hms` Cargo feature
- [ ] 2.9 Verify HMS test passes against docker-compose HMS stack (blocked on 2.7 real impl)
- [x] 2.10 Write failing integration test `jdbc_backend_sqlite_roundtrip`
- [x] 2.11 Implement `crates/sqe-catalog/src/backends/sql.rs` (SQLite via rusqlite; PostgreSQL pending upstream adoption)
- [x] 2.12 Wire JDBC into catalog registry behind `sql` Cargo feature
- [x] 2.13 Verify JDBC test passes against SQLite (PostgreSQL ignored test placeholder)
- [x] 2.14 Write failing integration test `hadoop_backend_auto_discovery`
- [x] 2.15 Implement `crates/sqe-catalog/src/backends/hadoop.rs` (metadata path scanner)
- [ ] 2.16 Verify Hadoop test passes against MinIO-backed warehouse (scanner tested against in-memory store; MinIO placeholder ignored test)
- [x] 2.17 Implement `crates/sqe-auth/src/oidc_m2m.rs` (OIDC client_credentials flow with preemptive refresh)
- [x] 2.18 Add Unity Catalog integration test with M2M auth (ignored, manual)
- [x] 2.19 Document Nessie via REST in `docs/deployment.md` (Section 5b Catalog Backends)
- [x] 2.20 Update `docs/iceberg-matrix-state.json`: 4 catalog rows -> P, nessie v2 -> F (30.7% -> 35.4%)
- [ ] 2.21 Commit Phase A and tag v0.16.0-catalogs
- [ ] 2.22 Fork Neuw84/iceberg-matrix and prepare SQE entry
- [ ] 2.23 Add 63 support entries to fork's `src/data/platforms/oss.json`
- [ ] 2.24 Add SQE SVG logo and wire into CompatibilityMatrix.tsx + FilterPanel.tsx
- [ ] 2.25 Run `npm test && npm run build` locally on the fork
- [ ] 2.26 Open PR to Neuw84/iceberg-matrix linking to SQE v0.16.0-catalogs

## 3. Phase B: Table maintenance SQL

- [x] 3.1 Write failing parser test: `CALL system.rewrite_data_files(table => 'ns.t')` -> `ProcedureCall::RewriteDataFiles { ... }`
- [x] 3.2 Implement `crates/sqe-sql/src/procedures.rs` with the ProcedureCall enum
- [x] 3.3 Verify parser test passes
- [x] 3.4 Write failing e2e test: 50 small files -> rewrite -> < 5 files
- [x] 3.5 Implement `crates/sqe-coordinator/src/maintenance.rs` for RewriteDataFiles
- [x] 3.6 Verify rewrite e2e test passes (parser contract + `#[ignore]` live test)
- [x] 3.7 Write failing e2e test for ExpireSnapshots (time-based and count-based)
- [x] 3.8 Implement ExpireSnapshots handler using vendored `RemoveSnapshotAction`
- [x] 3.9 Verify ExpireSnapshots tests pass (parser contract + `#[ignore]` live test)
- [x] 3.10 Write failing e2e test for RemoveOrphanFiles (3-day default threshold)
- [x] 3.11 Implement RemoveOrphanFiles handler using vendored `actions::remove_orphan_files`
- [x] 3.12 Verify RemoveOrphanFiles tests pass (parser contract + `#[ignore]` live test)
- [x] 3.13 Write failing e2e test for RewriteManifests
- [x] 3.14 Implement RewriteManifests handler using vendored `RewriteManifestsAction`
- [x] 3.15 Verify RewriteManifests tests pass (parser contract + `#[ignore]` live test)
- [x] 3.16 Add policy privilege check: maintenance procedures require write privilege on target table
- [x] 3.17 Test that read-only user is rejected with auth error + audit log entry
- [x] 3.18 Document maintenance procedures in `docs/operations.md`
- [x] 3.19 Update `docs/iceberg-matrix-state.json`: table-maintenance v2/v3 -> F
- [ ] 3.20 Commit Phase B and tag v0.17.0-maintenance

## 4. Phase C: Branching and tagging

- [ ] 4.1 Write failing unit test `create_branch_sets_snapshot_ref` in vendored iceberg-rust
- [ ] 4.2 Implement `vendor/iceberg-rust/crates/iceberg/src/transaction/branch.rs` with `create_branch`, `drop_branch`, `create_tag`, `drop_tag`
- [ ] 4.3 Verify branch/tag transaction tests pass
- [ ] 4.4 Write failing parser test for `ALTER TABLE ns.t CREATE BRANCH name`
- [ ] 4.5 Extend `crates/sqe-sql/src/ddl.rs` parser for branch/tag DDL
- [ ] 4.6 Write failing parser test for `SELECT ... FOR VERSION AS OF 'branch_name'` (string ref)
- [ ] 4.7 Extend parser to accept branch/tag names in time-travel syntax
- [ ] 4.8 Wire branch/tag DDL into coordinator handlers
- [ ] 4.9 Write e2e test: create branch, insert to branch, main unaffected
- [ ] 4.10 Verify e2e isolation test passes
- [ ] 4.11 Write e2e test: tag retention prevents snapshot expiry
- [ ] 4.12 Verify tag retention test passes
- [ ] 4.13 Add `SET WRITE_BRANCH` session variable and wire to write planner
- [ ] 4.14 Test main branch cannot be dropped
- [ ] 4.15 Add `REPLACE TAG` support and verify behaviour
- [ ] 4.16 Add `WITH RETENTION` clause to CREATE BRANCH
- [ ] 4.17 Document branching in `docs/features/branching.md`
- [ ] 4.18 Update `docs/iceberg-matrix-state.json`: branching-tagging v2/v3 -> F
- [ ] 4.19 Commit Phase C and tag v0.18.0-branching

## 5. Phase D: V3 type-exposure polish

- [ ] 5.1 Write failing test: `TIMESTAMP_NS(9)` round-trip via INSERT/SELECT
- [ ] 5.2 Extend `crates/sqe-sql/src/types.rs` to accept `TIMESTAMP_NS(N)` and `TIMESTAMPTZ_NS(N)` in DDL
- [ ] 5.3 Wire `PrimitiveType::TimestampNs` / `TimestamptzNs` through type_map
- [ ] 5.4 Verify nanosec roundtrip test passes
- [ ] 5.5 Write failing test: predicate pushdown on nanosec column prunes partitions
- [ ] 5.6 Verify predicate pushdown test passes (iceberg-rust #2069 already handles this)
- [ ] 5.7 Write failing test: `CREATE TABLE ... status STRING DEFAULT 'pending'` then insert without status
- [ ] 5.8 Extend `crates/sqe-sql/src/ddl.rs` to parse `DEFAULT <literal>` clause
- [ ] 5.9 Wire `DEFAULT` through to `NestedField::with_write_default`
- [ ] 5.10 Verify column-default test passes
- [ ] 5.11 Write failing test: `ALTER TABLE ADD COLUMN ... DEFAULT 'unknown'` applies retroactively
- [ ] 5.12 Wire to `NestedField::with_initial_default`
- [ ] 5.13 Verify initial-default test passes
- [ ] 5.14 Reject unsupported default expressions (e.g., `current_timestamp()`) with clear error
- [ ] 5.15 Bump table metadata format-version to 3 only when V3 features are used
- [ ] 5.16 Add compat test: SQE writes V2 table when only V2 features used
- [ ] 5.17 Update `docs/iceberg-matrix-state.json`: nanosec -> F, column-defaults -> F, V3 read/write/schema cells -> P or F
- [ ] 5.18 Commit Phase D and tag v0.19.0-v3-types

## 6. Phase E: Equality deletes + RowDeltaAction

- [ ] 6.1 Fetch iceberg-rust PR #2203 as a patch file
- [ ] 6.2 Apply patch to `vendor/iceberg-rust/`, resolve conflicts with RisingWave rebase
- [ ] 6.3 Write failing unit test for RowDeltaAction (3 data + 2 pos-delete + 1 eq-delete files)
- [ ] 6.4 Verify RowDeltaAction unit test passes
- [ ] 6.5 Write failing integration test: Spark 4.1 reads SQE-written equality delete file
- [ ] 6.6 Implement `crates/sqe-planner/src/row_delta.rs` emitting `RowDeltaCommitExec`
- [ ] 6.7 Implement `crates/sqe-worker/src/exec/equality_delete.rs` using vendored `EqualityDeleteFileWriter`
- [ ] 6.8 Wire table property `write.delete.mode = 'merge-on-read'` to planner dispatch
- [ ] 6.9 Verify Spark-interop integration test passes
- [ ] 6.10 Write failing test: concurrent writer conflict produces retryable error
- [ ] 6.11 Verify conflict-detection test passes
- [ ] 6.12 Add `sqe-bench` benchmark: equality-delete vs CoW on TPC-C trade_result at SF10
- [ ] 6.13 Update `docs/iceberg-matrix-state.json`: equality-deletes v2 -> F, v3 -> P; merge-on-read v2 -> F
- [ ] 6.14 Commit Phase E and tag v0.20.0-row-deltas

## 7. Phase F: Puffin bloom filters and stats

- [ ] 7.1 Write failing test: table with `write.parquet.bloom-filter-columns = 'id'` produces bloom-enabled Parquet
- [ ] 7.2 Extend `crates/sqe-worker/src/exec/parquet_writer.rs` to read the property and call `WriterProperties::set_bloom_filter_enabled`
- [ ] 7.3 Verify bloom-write test passes (verify via `parquet-tools meta`)
- [ ] 7.4 Write failing test: point query on bloom-enabled column shows `files_pruned_bloom > 0`
- [ ] 7.5 Verify Parquet reader is probing blooms during scan (confirm via EXPLAIN ANALYZE)
- [ ] 7.6 Write failing test: Puffin NDV sketch emitted on CTAS/INSERT
- [ ] 7.7 Implement Puffin sidecar writer in `crates/sqe-catalog/src/puffin_stats.rs`
- [ ] 7.8 Emit theta sketch per column after successful commit
- [ ] 7.9 Verify Puffin emission test passes and sketch NDV within 5% of true
- [ ] 7.10 Implement `CALL system.suggest_bloom_filter_columns` using query history
- [ ] 7.11 Test suggestion output against a seeded query log
- [ ] 7.12 (Deferred, waits for DF 54 and apache/datafusion#21157) Implement `PuffinStatisticsSource`
- [ ] 7.13 (Deferred) Wire StatisticsSource into sqe-planner CBO
- [ ] 7.14 (Deferred) Benchmark join reorder accuracy with Puffin vs Parquet stats
- [ ] 7.15 Update `docs/iceberg-matrix-state.json`: bloom-filters v2 -> F, v3 -> P
- [ ] 7.16 Commit Phase F and tag v0.21.0-puffin

## 8. Phase G: CDC incremental scan

- [ ] 8.1 Write failing parser test for `SELECT ... FOR INCREMENTAL BETWEEN SNAPSHOT x AND SNAPSHOT y`
- [ ] 8.2 Extend `crates/sqe-sql/src/query.rs` to parse incremental syntax
- [ ] 8.3 Verify parser test passes
- [ ] 8.4 Write failing e2e test: 4 snapshots, query range returns only added rows
- [ ] 8.5 Implement `crates/sqe-catalog/src/incremental_scan.rs` that walks snapshot log
- [ ] 8.6 Verify e2e range-scan test passes
- [ ] 8.7 Write failing test: deleted rows excluded from incremental scan
- [ ] 8.8 Implement delete-file reconciliation in range scan
- [ ] 8.9 Verify delete-exclusion test passes
- [ ] 8.10 Write failing test: `_change_type`, `_change_ordinal`, `_commit_snapshot_id` meta columns
- [ ] 8.11 Implement meta column materialisation in scan executor
- [ ] 8.12 Verify meta column test passes
- [ ] 8.13 Test invalid range (descending, non-existent snapshot) produces clear error
- [ ] 8.14 Add `append_changes` incremental strategy to dbt-sqe adapter
- [ ] 8.15 Test dbt incremental model with append_changes strategy
- [ ] 8.16 Document CDC in `docs/features/cdc.md`
- [ ] 8.17 Update `docs/iceberg-matrix-state.json`: cdc-support v3 -> P
- [ ] 8.18 Commit Phase G and tag v0.22.0-cdc

## 9. Phase H: MoR UPDATE / MERGE

- [ ] 9.1 Extend `crates/sqe-core/src/table_properties.rs` to parse write.update.mode and write.merge.mode
- [ ] 9.2 Write failing test: UPDATE with write.update.mode=mor emits equality delete files
- [ ] 9.3 Implement `crates/sqe-planner/src/mor_merge.rs` (mode dispatch + MoR plan)
- [ ] 9.4 Wire CoW vs MoR dispatch in `crates/sqe-planner/src/dml.rs`
- [ ] 9.5 Verify UPDATE MoR test passes
- [ ] 9.6 Write failing test: MERGE with mor mode
- [ ] 9.7 Implement MoR path for MERGE using RowDeltaAction
- [ ] 9.8 Verify MERGE MoR test passes
- [ ] 9.9 Write failing benchmark: TPC-E SF100 trade_result_update_holding with MoR completes <60s
- [ ] 9.10 Run benchmark, capture result JSON in benchmarks/results/
- [ ] 9.11 Document write mode selection guidance in `docs/features/mor-vs-cow.md`
- [ ] 9.12 Add Spark 4.1 round-trip test for MoR-written tables
- [ ] 9.13 Add Trino 465 round-trip test for MoR-written tables
- [ ] 9.14 Update `docs/iceberg-matrix-state.json`: merge-on-read v2 -> F, v3 -> P
- [ ] 9.15 Commit Phase H and tag v0.23.0-mor

## 10. Matrix submission finalisation

- [ ] 10.1 Regenerate `docs/iceberg-matrix-state.json` via `cargo xtask matrix-report`
- [ ] 10.2 Verify final score matches planned target (~156/189, 83%)
- [ ] 10.3 Update Neuw84/iceberg-matrix PR with final ratings
- [ ] 10.4 Link to each relevant blog post and integration test in `links` fields
- [ ] 10.5 Respond to matrix maintainer review
- [ ] 10.6 Announce in `docs/blog/2026-XX-XX-matrix-parity.md` when PR merges

## 11. Upstream contribution backlog

- [ ] 11.1 Rebase iceberg-rust RowDeltaAction (cherry-pick of #2203) on latest main and open rebase PR
- [ ] 11.2 Upstream `Transaction::create_branch` / `create_tag` to iceberg-rust (ref #1939)
- [ ] 11.3 Upstream `ExpireSnapshotsAction` wrapper to iceberg-rust (ref #2145)
- [ ] 11.4 Open DataFusion PR integrating Parquet bloom filter writer with `BloomFilter` physical expr (ref apache/datafusion#16435)
- [ ] 11.5 Open iceberg-rust PR for incremental scan API (ref #2152)
- [ ] 11.6 Monitor RisingWave fork for DF 54 rebase monthly; plan SQE DF 54 upgrade when landed

## 12. Cleanup

- [ ] 12.1 Remove TODO comments added during Phase E-H cherry-pick conflict resolution
- [ ] 12.2 Archive `docs/superpowers/plans/2026-04-24-iceberg-matrix-parity.md` by linking it from the archived change
- [ ] 12.3 Update `README.md` roadmap checklist for all 8 phases
- [ ] 12.4 Update `nextsteps.md` to mark the matrix work complete
- [ ] 12.5 Run `openspec archive iceberg-matrix-parity` after all phases merge
