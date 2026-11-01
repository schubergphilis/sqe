**STATUS 2026-11-01: the two gaps left by !1604 (`fix/scan-stats-gaps`).**

!1604 made `ScopedFileScan`, `HiveScanExec` and `HiveLocalScanExec` pass
their input's statistics through under DataFusion 55. It left two gaps.

1. `LazyPartitionScanExec`, the leaf a Hive scan uses under `LIMIT` with more
   than one partition, reported nothing. It lists nothing at plan time, so
   there are no file statistics, but its stream is hard-capped at `limit`
   rows. It now reports `num_rows = Inexact(limit)`; `HiveScanExec` passes
   that through. DataFusion's join selection uses inexact counts, so a
   `LIMIT` below 131,072 rows can now be the broadcast side.
2. No test pinned what the coordinator's plan-shape rules (`dim_build_swap`,
   `star_schema_reorder`, `join_strategy`, after DataFusion's
   `JoinSelection`) choose once file TVFs carry real row counts. New IT
   `tvf_join_broadcast_e2e::a_small_read_parquet_file_is_the_broadcast_build_side`
   runs EXPLAIN through the coordinator handler for a 300,000-row file joined
   to a 100-row file, in both textual orders, and asserts
   `HashJoinExec: mode=CollectLeft` with the 100-row file as the build side,
   then checks the count and sum.

Verified: `hive_provider_test::a_limited_lazy_scan_reports_its_limit_as_the_row_bound`
(new) and the existing lazy-scan test pass; the new coordinator IT passes on
the integration stack.

**NEXT:** embedded Iceberg parallel scan on its own branch; then the
iceberg-rust read correctness cherry-picks.
