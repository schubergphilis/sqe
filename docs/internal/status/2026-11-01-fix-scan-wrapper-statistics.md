**STATUS 2026-11-01: scan wrappers pass statistics through (`fix/scan-wrapper-statistics`).**

Found with `quickstart/engine-bench` at 150M rows: `q2_join_agg` (sales joined
to a 50,000-row products file) took 4.9 s and 4.0 GiB in SQE against 1.5 s
and 0.39 GiB in DuckDB. `EXPLAIN` with `datafusion.explain.show_statistics`
showed `ScopedFileScan` at `Rows=Absent` over a `DataSourceExec` at
`Rows=Exact(50000)`, and the join as `mode=Partitioned`.

Cause: DataFusion 55 computes statistics with `StatisticsContext::compute`,
which asks each node for `child_stats_requests` (default `ChildStats::Skip`,
so the child's stats are replaced by unknown) and calls
`statistics_from_inputs`. The deprecated `partition_statistics` is no longer
called, so overriding it does nothing. `JoinSelection` with no byte size and
no row count never chooses `CollectLeft`. `IcebergScanExec` already used the
new API; three wrappers created inside a provider's `scan()` did not.

Fix: `ScopedExec` (`scoped_table.rs`), `HiveScanExec` (`hive/scan_exec.rs`)
and `HiveLocalScanExec` (`hive/scan_mode.rs`) request their child's stats at
the same partition and return them unchanged. No SQE code calls
`partition_statistics` directly.

Verified:
- Unit test `scoped_scan_reports_the_inner_row_count`, plus the `hive` and
  `scoped_table` unit tests in `sqe-catalog`.
- Release `sqe-cli --embedded`: q2 plan is `HashJoinExec: mode=CollectLeft`
  with products as the build side.
- q2 at 150M: 1.5 s and 0.14 GiB peak RSS (was 4.9 s and 4.0 GiB). At 10M:
  0.12-0.17 s. The checksum matches DuckDB (18356, 138001076,
  172855341241.0, -21357.443). Timings were taken while the machine was busy
  after a reboot (load average 60+), so the plan shape is the firm evidence.

Not covered: `HiveScanExec` under a `LIMIT` wraps `LazyPartitionScanExec`,
a leaf with no statistics, so it still reports nothing (it reads lazily, so
there is nothing cheap to report). `dim_build_swap`, `star_schema_reorder`
and `join_strategy` now see real row counts for file TVFs and Hive tables;
intended, but no test pins their choices on those inputs.

Also measured, not changed: `datafusion.optimizer.enable_window_topn` with
`rn <= 1` on `q3_window_latest` is 5x slower at 10M (3.8-4.1 s against
0.7 s, 2.4 GiB against 0.85 GiB) and fails at 150M with "Resources
exhausted" in `PartitionedTopK` against the embedded default 1 GiB pool (it
does not spill). Leave it off on DataFusion 55; retest on 56 (#25730,
#26133).

**NEXT:** commit the embedded Iceberg parallel-scan fix on its own branch;
then cherry-pick the iceberg-rust read correctness fixes.
