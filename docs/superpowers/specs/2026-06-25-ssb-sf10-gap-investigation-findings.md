# SSB SF10 vs Trino: Gap Investigation Findings

**Date:** 2026-06-25
**Status:** Investigation closed (no full SSB fix; cause pinned to a tracked engine-maturity item)
**Branch / MR:** `fix/issue-235-parallel-scan-hash-exchange` / MR !427 (draft, default-off)
**Related issues:** #235 (single-node scan single-threaded), #132 (skip Tier-1 dynamic filter on unclustered fact tables), #242 (page-index dropped), #131 (q72 regression that pinned the scan to 1 partition)

## Question

Why does SQE trail Trino ~2.5x on SSB at SF10, while winning or tying every other suite?

## Current SF10 scores (SQE vs Trino, clean-slate compare-trino run)

Single coordinator, fresh Polaris + RustFS stack + Trino 481, BENCH_SCALE=10. Speedup = Trino_total / SQE_total (>1 means SQE faster). Run was with the experimental `parallel_probe_scan` flag on, which is perf-neutral (see below), so these are representative of default behavior.

| Suite | SQE | Trino | Speedup | Notes |
|---|---|---|---|---|
| TPC-E | 8.1s | 105.8s | **13.0x** | SQE dominant |
| TPC-BB | 205.3s | 637.4s | **3.11x** | SQE wins big |
| ClickBench | 6.2s | 12.7s | **2.06x** | decode-heavy, no joins; SQE wins |
| TPC-H | 62.9s | 72.1s | **1.15x** | SQE wins (median 1.22x) |
| TPC-DS | 152.1s | 167.5s | **1.10x** | SQE wins (median 1.30x); no q72 regression |
| TPC-C | 5.2s | 4.5s | 0.88x | ~parity (median 1.46x) |
| **SSB** | **34.1s** | **13.2s** | **0.39x** | **the lone loss (Trino ~2.5x)** |

All suites passed 222/222 SQE-side with zero row diffs. Absolute times carry some host-memory noise; the cross-suite pattern is the robust signal.

## What we ruled out (with evidence)

1. **Scan parallelism (#235) is NOT the SSB lever.** Built an opt-in `ParallelProbeScanRule` that parallelizes the probe-side Iceberg scan of `CollectLeft` joins. Result: SSB flag-on 34.1s vs flag-off 31.7s. **Perf-neutral.** The fact-table decode was already parallel within one partition (intra-file row-group split), so adding probe output partitions changed nothing.

2. **It is not generic parquet decode throughput.** ClickBench is decode-heavy and SQE *wins* it 2.06x. So SQE's reader is competitive; SSB's loss is specific.

3. **It is not caching / IO.** Per-operator profiles show the S3 fetch is ~30-60ms per query; the data arrives fast (warm). Trino is not winning on IO.

4. **It is not join or aggregation kernels.** Summed `elapsed_compute` for `HashJoinExec` + `AggregateExec` + `FilterExec` is ~300-400ms of a ~6.3s SSB query.

## What it actually is

SSB query wall time is ~93-97% **non-operator-compute**: the async parquet decode plus the per-batch dynamic-filter (Tier-2) evaluation over ~60M `lineorder` rows. The combination unique to SSB:

- SSB joins a large fact table with dimensions whose filters land on **uniformly-distributed (unclustered)** FK columns. The dynamic-filter Tier-1 registration (manifest / row-group / RowFilter pruning) therefore prunes **zero** row groups, but still pays per-file bind + per-batch RowFilter eval, and Tier-2 re-evaluates the same predicate. That is "we do too much" on SSB specifically (tracked as #132).
- TPC-DS / TPC-H fact tables are clustered (e.g. sorted by date), so the same Tier-1 machinery is a large win (q82 16x). ClickBench has no joins, so no dynamic filter at all.

So the residual SSB gap is a **vectorized decode + dynamic-filter throughput** difference versus Trino on a large unclustered fact table. It is a genuine engine-maturity gap, not a config flip.

## Mitigation tested

`clustering_skip_enabled` (issue #132's gate: skip Tier-1 when the planned files are uniform on every filter column) exists in code under `[catalog.runtime_filters]`, default `false`. Enabling it for an SSB run:

| SSB SF10 | SQE | vs Trino |
|---|---|---|
| gate off (default) | 34.1s | 0.39x |
| gate on | **30.4s** | 0.46x |

A real but modest **~11%** win. It does not close the gap, and enabling it by default needs the TPC-DS no-regression check (the discriminator that broke the naive heuristics in #132). Left default-off.

## Bonus: a real correctness bug found and fixed (the 4x)

The #235 rule's correctness smoke caught **silent data corruption**: with parallel scan on, every aggregate was 4x too large and `count(*) FROM lineorder` returned 240M vs 60M. Root cause: `IcebergScanExec`'s `to_arrow` path called `table.scan()` (the whole table) and ignored the per-partition file slice, so each partition holding a file re-scanned all files. Fixed in `f847356` (plan once, filter the `FileScanTask` stream to this partition's files).

Scope check (does this hurt SF100?): **no.** `with_target_partitions` is called only by the default-off rule; the table provider pins scans to 1 partition everywhere else. The distributed path is a separate, slice-safe code path: each worker receives a `ScanTask` with an explicit disjoint `data_file_paths` list and reads only those (crates/sqe-worker/src/executor.rs). SF100 scaling is bounded by the in-memory shuffle (no spill), a separate issue.

## Disposition

- **#235 rule:** correct, no q72 regression, but perf-neutral for SSB. Keep **default-off**; MR !427 stays draft. The keeper from that branch is the 4x scan-correctness fix.
- **SSB lever:** #132 (clustering-skip, ~11%) + #242 (page-index), and the remainder is vectorized decode/filter throughput. That is the real, harder work, and it needs a clean dedicated rig to measure against.
- **SQE at SF10 is otherwise competitive or dominant** versus Trino across all other suites.

## Result artifacts

This run's JSON reports are committed under `benchmarks/results/` (`*-sf10-flight-2026-06-25T*.json` and `compare-*-sf10-2026-06-25T*.json`), including the `clustering_skip_enabled` SSB A/B (`compare-ssb-sf10-2026-06-25T12:40:23.json`).
