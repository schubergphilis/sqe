# Enterprise Iceberg Compaction for SQE

Status: approved (design)
Date: 2026-07-20
Branch: `worktree-feat-enterprise-compaction` (peels into stacked MRs)
Author: Jacob Verhoeks (with Claude)

## Motivation

SQE already ships a working `CALL system.*` maintenance surface, including
`rewrite_data_files` (greedy bin-pack + atomic swap), `expire_snapshots`,
`remove_orphan_files`, `rewrite_manifests`, and snapshot rollback. It is more
complete than most engines start with, but three things are missing for an
enterprise, high-performance, resilient story:

1. **Correctness.** `rewrite_data_files` is not delete-aware. On a Merge-on-Read
   table (data files + position/equality deletes) it reads raw Parquet without
   applying deletes (`maintenance.rs:read_parquet_file`), and the rewritten data
   files get new paths and a new sequence number. The surviving position deletes
   still reference the old file paths, so they no longer match the rewritten
   data; equality deletes at a lower sequence number also stop applying. The
   referenced-data-file dangling check that would otherwise drop those deletes is
   an unimplemented TODO in the vendored fork
   (`vendor/iceberg-rust/crates/iceberg/src/transaction/manifest_filter.rs:420`;
   `remove_dangling_deletes_for` at :457 only flags that filtering is needed, it
   does not drop the deletes). No error path fires. The result is silent
   resurrection of logically-deleted rows. The published book
   (`docs/site/book/src/design-notes/mor-vs-cow.md:97`) currently *recommends*
   running this procedure on MoR tables, so the docs point users at the
   corrupting path.
2. **Performance.** Bin-pack is partition-blind (a cross-partition group fans
   back out to ~1 file per partition, near-zero consolidation), single-strategy
   (no sort, no z-order clustering), and buffers whole groups in coordinator
   memory.
3. **Automation.** All maintenance is manual `CALL`. There is no detection or
   scheduling.

Of the 2026 compaction landscape (Spark, Databricks, Glue, S3 Tables, Dremio,
Flink, LakeOps), only LakeOps is Rust-on-DataFusion like SQE. SQE can be its
own in-engine LakeOps: a sovereign, self-compacting lakehouse with no external
Spark maintenance dependency.

## Goals mapped to requirements

- **Enterprise**: delete-aware correctness (no data loss), audit trail on every
  maintenance action, opt-in autonomous maintenance that preserves the
  no-service-account query path.
- **High performance (multi-stream)**: parallel file-group rewrite streams,
  partition-aware grouping, sort and z-order clustering for query-time pruning.
- **Resilient**: partial-progress commits, conflict-aware retry with
  sequence-number pinning, memory-safe spill (degrade, never OOM).

## Non-goals

- No changes to the vendored iceberg-rust fork. Every API needed
  (`RewriteFilesAction` with delete-file routing and
  `set_new_data_file_sequence_number`, ArrowReader delete application,
  `RemoveOrphanFilesAction`, `rewrite_manifests`, `expire_snapshot`,
  `client_credentials` auth) already exists.
- No standalone `rewrite_position_delete_files` procedure in this program;
  deletes are applied inside `rewrite_data_files` (Trino/Spark semantics).
  A dedicated delete-consolidation procedure is a later nice-to-have.
- No CTAS `PARTITIONED BY` parser fix (tracked separately); sort compaction is
  the route to sorted benchmark tables in the meantime.

## Architecture

All work lands in `crates/sqe-coordinator/src/maintenance.rs`,
`crates/sqe-sql/src/procedures.rs`, and new coordinator modules
(`zorder.rs`, `maintenance_scheduler.rs`, and a rewrite-execution helper).
The `CALL system.*` chain is unchanged end to end:

```
sqe-sql/procedures.rs (parse -> ProcedureCall)
  -> classifier.rs:770 (StatementKind::Procedure)
  -> query_handler.rs:1248 (MaintenanceHandler::handle)
  -> maintenance.rs (per-procedure dispatch)
  -> vendored RewriteFilesAction / scan / actions
```

### Phase 1 - Correctness foundation (MR #1)

Begin with a **failing integration test** proving the resurrection bug: create a
MoR table, delete rows (position deletes), run `rewrite_data_files`, assert the
deleted rows stay gone. It must fail on the current code.

- **P0 delete guard.** Extend `collect_live_data_files` (maintenance.rs:1214) to
  also surface live delete-file manifest entries (`content_type() != Data`). If
  any exist, `rewrite_data_files` returns a `summary_batch` with
  `status = skipped: table has live delete files; delete-aware rewrite lands in
  phase 2` instead of proceeding. Correct `mor-vs-cow.md:97`.
- **P1 partition-aware grouping.** Before `pack_file_groups` (maintenance.rs:1268),
  pre-group live data files by `(partition_spec_id, partition_struct)` and pack
  within each partition. `min_input_files` applies per partition. Keying: use the
  partition struct's serialized/`Debug` form if `Struct` is not `Hash`. Files
  from older specs still re-emit under the current default spec (matches Spark);
  document this.

Tests: partition-grouping unit tests; the resurrection test stays red (fixed in
phase 2); the guard returns `skipped` on MoR tables (green).

### Phase 2 - Delete-applying, multi-stream, resilient engine (MR #2)

Replaces the P0 guard with a correct implementation.

- **P2 delete-applying rewrite.** Replace `read_parquet_file` (maintenance.rs:1344)
  with a table scan: `table.scan().select_all().build()?.plan_files()` yields
  `FileScanTask`s carrying applicable delete files; the vendored ArrowReader
  applies position deletes (arrow/reader.rs:977) and equality deletes
  (arrow/reader.rs:804). Filter tasks to the group's data-file paths.
- **Invariant change.** Replace `added_rows == removed_rows`
  (maintenance.rs:624-631) with `added_rows == rows_read` and
  `rows_read <= sum(record_count)`; record `deletes_applied` in the summary.
  This is the one deliberately-relaxed safety net. Add a scan-side cross-check
  for position-delete-only tables:
  `rows_read + position_deletes_applied == sum(record_count)`.
- **Delete-file removal.** Compute delete files fully covered by the rewritten
  data set and pass them to `.delete_files(...)`; the action routes delete-content
  files into `removed_delete_files` (rewrite_files.rs:83-108). Leave ambiguous
  equality deletes; let `drop_delete_files_older_than` age them out.
- **Sequence-number pin.** Call `set_new_data_file_sequence_number(seq_at_start)`
  (rewrite_files.rs:162) so a concurrent writer committing newer equality
  deletes mid-compaction still has them apply. Conflict-correctness keystone.
- **Multi-stream execution.** Generalize `max_concurrent_file_group_rewrites`
  into bounded parallel rewrite streams; each streams through
  `write_data_files_streaming` (writer.rs:496) instead of buffering the group as
  `Vec<RecordBatch>`. Pool-register the pipeline on the coordinator RuntimeEnv.
- **Resilience.** `partial_progress => true` commits per group (not one
  transaction for all groups, maintenance.rs:658), shrinking the conflict window;
  conflict-aware retry via `classify_commit_error` (maintenance.rs:1188) with
  bounded backoff; gate on the coordinator `MemoryPressure` signal.
- **Parser.** Add optional `delete_file_threshold => N` and `rewrite_all => true`
  to the `RewriteDataFiles` variant (procedures.rs); a file at target size but
  carrying deletes above threshold becomes eligible.

Tests: the phase-1 resurrection test goes **green**; integration matrix over
CoW / MoR-position / MoR-equality; partial-progress and conflict-retry tests;
memory-bounded streaming test.

### Phase 3 - Sort and z-order strategies (MR #3)

- **Parser.** `strategy => 'binpack'|'sort'`, `sort_order => 'col ASC, ...'` or
  `'zorder(a,b)'`, mirroring Spark's `rewrite_data_files` signature. If
  `strategy => 'sort'` with no `sort_order`, use the table's declared sort order;
  error if none. Validate in the handler (schema available), not the parser.
- **Sort execution.** New rewrite-execution helper builds a DataFusion
  `SessionContext` on the shared `FairSpillPool` + `DiskManager` runtime
  (runtime.rs:10-59). Feed the delete-applying scan as a streaming source (not a
  `MemTable`), `df.sort(...)` -> spillable `SortExec`, stream into
  `write_data_files_streaming`. Under sort, group = all eligible files within one
  partition (follows from P1), size-capped by `max_sort_group_bytes`.
  Optionally stamp `sort_order_id` when the applied order equals the table's.
- **Z-order.** New `crates/sqe-coordinator/src/zorder.rs` (~250-300 lines):
  session-local `ScalarUDF __sqe_zvalue(cols...) -> FixedSizeBinary` computing an
  order-preserving Morton key (ints: XOR sign bit; floats: IEEE-754
  sign-magnitude flip; strings/binary: first 8 bytes; dates/timestamps as ints;
  NULLs sort-first sentinel; then bit-interleave). Project the z-value, sort on
  it, drop before write. No Iceberg sort-order metadata for z-order (Iceberg's
  SortOrder cannot express it; matches Spark).

Benchmark payoff: load fast/unsorted, then
`CALL system.rewrite_data_files(strategy => 'sort')` once - sidesteps the CTAS
`PARTITIONED BY` parse gap and the sort-on-write OOM, and feeds row-group
pruning (SSB SF10 story).

Tests: Morton kernel order-preservation unit tests; sort spill test under a tiny
memory budget; sort/z-order integration on a partitioned table.

### Phase 4 - Advisory + opt-in autonomous scheduler (MR #4)

Advisory by default; autonomous only when an operator configures a principal.

- **Where.** New `crates/sqe-coordinator/src/maintenance_scheduler.rs`, spawned
  from `main.rs` as a supervised `TaskGuard` task (same pattern as the health and
  authenticator-refresh tasks, main.rs:171-211). It reuses `MaintenanceHandler`
  as a caller; it is not a new engine.
- **Advisory (no credentials).** Per tick (default 5-15 min, jittered), for each
  table read the current snapshot summary from `TableMetadataCache`
  (`total-data-files`, `total-delete-files`, `total-records`,
  `total-position-deletes`, `total-equality-deletes`). Apply thresholds (small-file
  count, avg file size vs target, delete ratio, snapshot count/age). Expose
  Prometheus gauges (`sqe_table_small_files`, `sqe_table_delete_ratio`, ...) and a
  `system.table_health` query surface that suggests the exact `CALL` to run.
- **Autonomous (opt-in).** A `[maintenance]` config section names a dedicated
  principal using the existing sqe-auth `client_credentials` backend
  (authenticator.rs:22-50). Polaris grants it MODIFY only on tables with property
  `sqe.maintenance.enabled = 'true'` (checked each tick). Per-table sequencing:
  `rewrite_data_files` -> `rewrite_manifests` (if manifest count high) ->
  `expire_snapshots` -> `remove_orphan_files` (age-guarded, 3-day default,
  maintenance.rs:801). Single-flight per table; skip tables whose latest snapshot
  is younger than a quiet-window threshold; gate on `MemoryPressure`. The query
  path stays 100% user-identity; maintenance is operator-plane.
- **Audit.** Emit maintenance events (new `AuditKind::Maintenance` or existing
  `AdminDdl`) attributed to the principal, with the triggering rule in the
  payload, via the `AuditLogger` the handler already holds (maintenance.rs:49).

Tests: threshold-detection unit tests; advisory-gauge integration; opt-in gating
(a table without the property is never touched); sequencing order test.

## Cross-cutting concerns

### Testing strategy

- TDD per phase: write the failing test first, then implement.
- Integration matrix over CoW, MoR-position, MoR-equality tables, run on the
  live-stack harness (`rewrite_data_files_real.rs` pattern).
- Unit tests for pure functions: partition grouping, Morton kernel, threshold
  logic, conflict classification.

### Risks and mitigations

- **Invariant relaxation (P2)** is the scariest change: the strict row-count
  equality is what makes bin-pack trustworthy today. Mitigate with the scan-side
  cross-check and the CoW/MoR test matrix; the phase-1 resurrection test is the
  regression guard.
- **Memory.** The current buffered `Vec<RecordBatch>` group read can allocate
  multiple GB untracked (512 MiB Parquet decodes 3-10x, times concurrency). The
  streaming write path + pool registration lands no later than phase 2; sort
  without spill would be a new instance of the known sort-on-write OOM.
- **Commit conflicts.** Single-transaction-for-all-groups is fine for manual
  quiet-window runs, wrong for automation. Partial progress ships before the
  scheduler (phase 2, used by phase 4).
- **Spec evolution.** Post-P1, files under old specs re-emit under the current
  spec (matches Spark). Keep, document; add a `use_starting_spec` escape hatch
  only on request.
- **Scheduler sequencing.** compact -> manifests -> expire -> orphans, never
  interleaved per table; orphan removal stays age-guarded to avoid racing
  in-flight multipart uploads.
- **Sovereignty.** Autonomous execution requires an explicit operator-configured
  principal; the default posture is advisory-only, so no deployment gains a
  service identity by accident.

## Implementation surface

| Phase | procedures.rs | maintenance.rs | New modules | Vendored | Config |
|---|---|---|---|---|---|
| 1 | - | ~+120 (guard + partition grouping + tests) | - | none | - |
| 2 | ~+40 (2 args) | ~+350 (scan read, invariant, delete removal, seq pin, multi-stream, partial progress, retry) | rewrite-exec helper | none | optional |
| 3 | ~+80 (strategy/sort_order/zorder) | ~+280 (sort groups, DF pipeline) | `zorder.rs` ~300 | none | `max_sort_group_bytes` |
| 4 | - | ~+50 (partial_progress plumbing) | `maintenance_scheduler.rs` ~400-600 | none | `[maintenance]` in sqe-core |

Docs updated each phase (`docs/site/book/src/sql-reference/procedures.md`);
`mor-vs-cow.md:97` corrected in phase 1.

## Success criteria

- MoR table + deletes + `rewrite_data_files` never resurrects rows (phase-1 test
  green after phase 2).
- Partitioned-table compaction consolidates within partitions (file count drops).
- `strategy => 'sort'` completes under a memory budget smaller than the table via
  spill, and produces query-time pruning improvement on a benchmark suite.
- Advisory scheduler reports table health with zero configured credentials;
  autonomous execution runs only against opted-in tables under the configured
  principal, with audit events.
- Each phase is an independently reviewable, mergeable MR.
