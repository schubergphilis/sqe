> Status 2026-07-07: adopted as phase A of `openspec/changes/scan-throughput-memory-safety/` (adds a memory clamp on the partition count and pool-registered decode buffers on top of this design). Execute via that change's tasks; this folder is retired when it lands.

## Why

A single-node Iceberg scan runs on one core. `IcebergScanExec` defaults to `DEFAULT_TARGET_PARTITIONS = 1` (`crates/sqe-catalog/src/iceberg_scan.rs:75`) and `target_partitions` is deliberately not auto-wired (`crates/sqe-catalog/src/table_provider.rs:229-246`). On a multi-core coordinator a scan-bound query leaves most cores idle.

The wiring is absent on purpose. The code comment records exactly why (`crates/sqe-catalog/src/table_provider.rs:229-240`): setting `target_partitions` to the session value made `IcebergScanExec` advertise `Partitioning::UnknownPartitioning(N)`, which is the worst possible signal for DataFusion's `EnforceDistribution` rule. With `UnknownPartitioning(N)`, the planner cannot promote a downstream `HashJoinExec` to `Partitioned` mode (which needs `HashPartitioning` on the join key). It falls back to `CollectLeft` and inserts a `CoalescePartitionsExec` immediately above the scan to gather the N streams back into one. Net effect: parallel I/O, then immediate serialization, then a single-threaded hash build fragmented into many tiny round-robin batches. TPC-DS q72 SF1 regressed 5-6x (~17s -> ~100s) until the wiring was removed; issue #131.

The root cause is not parallelism. It is parallelism announced with the wrong partitioning, so `EnforceDistribution` undoes it with a redundant exchange. The fix is to emit partitioning that the optimizer can use, or to place repartitions explicitly so the optimizer does not insert wasteful ones.

This change re-introduces parallel single-node scan correctly, behind a config flag, gated on q72 not regressing.

## What Changes

Recommendation: **emit proper `Partitioning` and place `RepartitionExec` explicitly** so `EnforceDistribution` stops inserting the redundant `CoalescePartitionsExec` + round-robin. The `with_target_partitions` setter on `IcebergScanExec` is already kept for this purpose (`crates/sqe-catalog/src/table_provider.rs:242-245`).

1. Parallelize the scan into N file-group partitions (reusing the bin-packing already used for distributed `ScanTask`s).
2. Announce partitioning the optimizer can consume. Where the scan feeds a hash join or hash aggregate, hash-partition on the join / group key so `EnforceDistribution` promotes the join to `Partitioned` mode instead of `CollectLeft`. Where the scan feeds a pipeline operator with no distribution requirement (filter, project), `RoundRobinBatch(N)` is safe and triggers no exchange.
3. Place any required `RepartitionExec` deliberately, rather than letting the optimizer guess from `UnknownPartitioning`.
4. Gate the whole thing behind `execution.parallel_scan` (default off) until the q72 benchmark gate is green.

## Capabilities

### New Capabilities
- `scan-parallel-roundrobin`: parallelize scans that feed pipeline operators with `RoundRobinBatch(N)` and no added exchange.
- `scan-parallel-hash`: hash-partition the scan on join / group keys so `EnforceDistribution` keeps the partitioned plan.

### Modified Capabilities
- `iceberg-scan`: `IcebergScanExec` can advertise meaningful partitioning instead of forcing `UnknownPartitioning(1)`, under the flag.

## Impact

- `sqe-catalog`: `table_provider.rs` conditionally wires `target_partitions` and chooses a partitioning scheme based on the consuming operator; `iceberg_scan.rs` emits the chosen `Partitioning`.
- `sqe-planner` / `sqe-coordinator`: a planner pass that inspects the operator above the scan to pick round-robin vs hash, and places `RepartitionExec` where needed.
- No SQL-surface, catalog, or wire-protocol change.

## Rollback

`execution.parallel_scan` defaults to `false`, which is exactly today's behaviour (`target_partitions = 1`). The flag flips to `true` only after the q72 gate passes. Rolling back is a config change with no plan-format migration. The distributed path is unaffected: it has its own file-group splitting and does not read `target_partitions`.

## Success Criteria

- TPC-DS q72 SF1 single-node does not regress against the committed baseline `compare-tpcds-sf1-2026-05-28T14:19:18.json` (q72 = 756ms). Gate: parallel-scan q72 time <= 1.1x baseline. The `~17s -> ~100s` figures above are the historical #131 regression delta; q72 has since improved to sub-second through later optimization, and the gate is against that current 756ms baseline, not the old numbers.
- A scan-bound query (e.g. TPC-H Q1, TPC-H Q6, a `SELECT count(*) ... WHERE` over a large table) speeds up at least 2x on a 4+ core coordinator versus the single-partition baseline.
- The TPC-H SF1 suite does not regress against `tpch-sf1-flight-2026-04-02T14:16:27.json` (22/22, 37.5s).
- The plan for q72 under the flag contains no `CoalescePartitionsExec` directly above the scan and the join stays `Partitioned` (not `CollectLeft`).
