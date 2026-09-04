# Handover: DataFusion 55 and Iceberg backports

Date: 2026-09-04  
Status: DataFusion migration and planned correctness/performance backports are
merged; statistics adoption is implemented behind a flag on
`feat/puffin-ndv-statistics` (SF1 gate pending); baseline refresh remains open.

## Current repository state

`main` is at `17c85f0f`, matches `origin/main`, and includes the Arrow/Parquet
59.3 merge. The dependency graph resolves to one DataFusion/Arrow generation:

- DataFusion 55.0.0;
- Arrow and Parquet 59.3.0;
- `object_store` 0.13.2, retained because DataFusion 55 uses that API;
- the locally vendored `datafusion-functions-json` 0.54.2 port, compiled against
  DataFusion 55.

The worktree also contains unrelated untracked paths
`docs/internal/research/2026-09-01-fulltext-search-es-replacement.md` and
`playground/`. Do not overwrite or remove them.

## Completed work

The DataFusion 55 / Arrow 59 migration is complete. The recorded A/B found no
correctness regression and lower mean runtime for TPC-H (-26.3%), SSB (-43.1%),
and TPC-DS (-45.3%). Arrow and Parquet were subsequently advanced from 59.2 to
59.3 in `82543f5f`.

Every correctness backport in Phase 2 of the upgrade program is on `main`:

- Apache #2781: equality-delete NULL correctness (`d9840b3f`);
- Apache #2797: reject absent required fields without defaults (`3c455d3c`);
- Apache #2859: positional-delete lost-wakeup fix (`fe29b31a`);
- RisingWave #196: publish and cache equality-delete load failures
  (`6dfe2232`);
- RisingWave #186 and Apache #2867: typed truncated-data and malformed-Puffin
  failures (`2cb7ab4d`, with #2867 already present in the vendor baseline);
- Apache #2509: FastAppend path de-duplication (`0f711f51`);
- Apache #3062: SQL catalog commit-error propagation (`9ec92b26`).

Every bounded-memory/performance item in Phase 3 was also accepted:

- RisingWave #181/#185: reuse manifests across retries and bound rewrite
  memory (`76fcdb73`);
- RisingWave #177: stream orphan-file manifest loading (`ecf09b80`);
- Apache #3015: buffer positional-delete runs (`6516b552`);
- Apache #3080: avoid repeated constant-column clones (`bc8e4714`);
- RisingWave #208: cache OpenDAL operators per bucket (`6ac0d867`).

Additional Apache fixes landed alongside the program, including temporal
partition formatting (#3022). The vendor ledger currently records ten focused
RisingWave backports and fifteen Apache backports.

## Still open

### RisingWave baseline refresh

The vendor declaration still names RisingWave `813e54419b43` as its baseline.
Wanted changes through audited tip `1657c184` were applied as focused backports,
but Phase 4's clean reconstruction on `1657c184` and comparison against the
incremental tree has not been done.

Do not take RisingWave `c3ac742` (transaction unification) without running the
full Copy-on-Write, Merge-on-Read, conflict, and compaction regression suite.
DeleteVector trait/export commits `08a18f41` and `ac90a10d` remain intentionally
deferred until a consumer needs them.

### Official Apache migration

Apache iceberg-rust v0.10.1 and the audited Apache `main` cannot replace the
RisingWave-derived vendor tree. Required gaps include
`PositionDeleteFileWriter`, rewrite/overwrite and RowDelta transaction actions,
`DeletionVectorWriter`, dynamic predicates, and SQE-specific scan extensions.
Apache PRs #2185 and #2203 were still open and conflicted at the last audit.

### Puffin statistics

Phase 6 is implemented behind a disabled flag on `feat/puffin-ndv-statistics`
(commits `217d5f49`, `5b44b671`; unsigned, re-sign before pushing). The
worktree the earlier draft of this handover called empty held the consumer
code uncommitted under `cargo fmt` noise; the noise was dropped and the real
work committed.

What is on the branch:

- `PuffinNdvCache` / `parse_ndv_properties` (`sqe-catalog/src/puffin_stats.rs`)
  read the `ndv` blob property from the snapshot `StatisticsFile` metadata, so
  a request never opens the sidecar;
- `SqeTableProvider::scan_with_args` answers `StatisticsRequest::DistinctCount`
  as `Precision::Inexact`, capped at the row count;
- `PuffinStatisticsRequestRule` (`sqe-planner/src/stats.rs`) requests NDV for
  every projected scan column;
- `query.puffin_statistics` (env `SQE_QUERY__PUFFIN_STATISTICS`, default
  false) registers the rule and sets
  `datafusion.optimizer.use_statistics_registry = true`;
- bench lever `BENCH_PUFFIN_STATS=1` loads with `write.puffin.stats` and starts
  the coordinator with the flag on.

Two defects found while validating, both fixed on the branch:

- The sidecar writer never ran on the default path. The coordinator routes
  CTAS and INSERT through the streaming handlers, which did not call the
  emitter; a full SF0.1 load produced zero `.stats` files.
  `ThetaSketchAccumulator` now taps the batch stream.
- Type coverage: Decimal128 and Date32 were admitted as sketchable with no
  handler (`ndv = 0`, one WARN per batch); `Utf8View`, which DataFusion 55
  produces for Parquet strings, was not admitted, so string columns had no
  blob. Both handled; a unit test now asserts every admitted type yields an
  estimate.

Evidence so far (SF0.1, dev-release, local stack): Trino compare matched
22/22 TPC-H and 11/13 SSB (the two SSB misses are the known vacuous pair) with
the flag on. NDV blobs cover all columns (orders: o_orderkey 148715 vs 150000
true, o_clerk 1000 exact). EXPLAIN A/B in `docs/evidence/perf/puffin-ndv/`:
q03/q05/q07/q08/q18 unchanged, q09 flips the orders/lineitem build side.

Task 7.14 ran the same afternoon (`docs/evidence/perf/puffin-ndv-ab-2026-09-04.md`).
The flag was split in two: `query.puffin_statistics` (NDV requests) and
`query.statistics_registry` (DataFusion's registry). NDV-only is plan-neutral
except q17 and runtime-neutral within noise. The registry arm flips TPC-H
build sides onto lineitem in q03/q05/q17/q18/q21 and reverses runtime-filter
direction; q17 measured 1.40x slower in both counterbalanced rounds. Both
flags and `write.puffin.stats` stay off. Peak memory was not captured.

### Validation and later phases

The broad reusable Phase 1 safety-suite milestone is not explicitly recorded
as complete, although individual backports carry focused tests. This handover
verified the resolved dependency graph only; it did not rerun the full test,
catalog matrix, memory, or benchmark suites.

DataFusion 56 work waits for a published release and must move DataFusion,
Arrow/Parquet, `object_store`, sqlparser, physical-plan proto users, the Iceberg
integration, and JSON functions as one coherent stack.

## Recommended next actions

1. Open the MR for `feat/puffin-ndv-statistics` (re-sign the commits first),
   then repeat the `on` vs `off` A/B on the clean rig and decide whether the
   SQE join rules should read `distinct_count` or an SQE `StatisticsProvider`
   should sit ahead of DataFusion's built-in join provider.
2. Decide whether the clean RisingWave `1657c184` reconstruction is still worth
   doing now that all wanted deltas are focused backports.
3. Complete or explicitly reconcile the Phase 1 cross-catalog safety-suite
   checklist.
4. Continue the monthly Apache-main shadow audit; do not de-vendor until the
   missing writer and transaction APIs exist.

## Read first

- `docs/internal/plans/2026-09-03-datafusion-iceberg-upgrade-program.md`
- `docs/internal/research/2026-09-02-datafusion-55-followups.md`
- `vendor/iceberg-rust/README.md`

