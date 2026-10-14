**STATUS 2026-10-13: partitioned sorts spill instead of failing under a tight query quota (`fix/sort-merge-headroom-788`, #788 item 2).**

Repro (unit, 512-row batches, 1M rows of ~100 bytes): `ORDER BY s` and `row_number() OVER (ORDER BY s)` at 8 partitions under a 64 MB quota, and at 16 partitions under 128 MB, all failed with "Not enough memory to continue external sort". Two causes, each enough on its own:

1. Every `ExternalSorterMerge[i]` pre-reserves `sort_spill_reservation_bytes` (10 MB), non-spillable: 8 x 10 MB is more than the 64 MB quota.
2. Under the greedy quota the first sorters to fill take every spillable byte; a sorter refused while its in-memory batches are empty has nothing to spill and fails the query.

Matrix: greedy + 10 MB, fair share + 10 MB and greedy + 256 KB all failed; only a share plus a smaller merge reservation passed.

Fix:
- `HeadroomBudget::with_sort_share` caps each sort consumer (`ExternalSorter[` / `ExternalSorterMerge[`) at `spillable_limit / sort consumers`, first reservation exempt. It is on for every per-query pool. The join retry keeps the stricter fair share on top. Aggregates and other consumers stay greedy, so wide TPC-DS plans keep their aggregate budget (the reason the coordinator pool is greedy).
- `join_fallback::sort_task_context` sizes `sort_spill_reservation_bytes` from the quota (`merge_reservation`: a quarter of the cap over the sorters, 1 MB floor, configured ceiling). Both coordinator execution paths (`open_stream`, `execute_query`), `EXPLAIN ANALYZE` and the sort-merge retry execute through it. The limit is read from the context's pool, so it follows whatever quota the query runs under.

Trade: the denominator counts both consumers of each sort partition (`ExternalSorter[i]` and `ExternalSorterMerge[i]`), so the sorters together buffer at most half the spillable bytes before they spill. At the default 1 GB quota with 20% headroom, a 16-partition sort's sorters buffer 16 x 25 MB = 400 MB, where before they could take the whole 800 MB spillable line (merges are non-spillable and do not count against it). A one-partition sort gets 400 MB instead of 800 MB. Every sort under a query quota spills up to 2x sooner. Counting only `ExternalSorter[` would keep 800 MB, but it is unsafe: at 16 partitions under 128 MB, 15 sorters at 6.4 MB plus 16 x 2 MB merge reservations fill the quota and the 16th sorter gets nothing. Half the spillable bytes plus a quarter for merges always fits.

Not covered: the write path (`sqe-write` `WriteHandler` / `hive_write`, e.g. CTAS ... ORDER BY) executes on the shared coordinator pool with no per-query quota, so neither the share nor the smaller merge reservation applies there; `SortMemoryRule` gates it at plan time as before.

Tests: `a_partitioned_sort_spills_within_a_tight_quota` (join_fallback; fails with either half reverted, checked), `sorts_share_the_spillable_bytes_and_everything_else_stays_greedy` (memory_headroom). `cargo test -p sqe-coordinator --lib`: 840 passed.

Gate: TPC-H / TPC-DS SF10 at a 16 GB pool and `max_query_memory = 1GB` (the strict case; dev-release builds of main c127a759 and this branch, same data, back to back):

| | main | branch |
|---|---|---|
| TPC-H | 21/22, 53.1 s | 21/22, 50.1 s |
| TPC-DS | 98/99, 172.1 s | 98/99, 128.8 s |

Per-query times sit within run-to-run noise (TPC-DS q67 9.9 s -> 5.3 s). TPC-H q09 and TPC-DS q09 fail on both binaries: a scan-decode refusal at 1 GB. TPC-DS q14 sits on the cap and is nondeterministic on both binaries. In 5 runs each, main passed 3/5 and the branch 2/5, and every failure was the same refusal: `iceberg-scan-decode:tpcds_sf10.store_sales` lost the race against a hash build. A hash-build refusal is retried with sort-merge joins (~33 s). A scan-decode refusal is not: `ScanDecodeGate` (`sqe-catalog/src/scan_memory.rs`) stringifies the DataFusion error into `iceberg::ErrorKind::Unexpected`, so `JoinFallback::applies` never sees a `ResourcesExhausted` root. That limitation predates this change; fixed by `fix/scan-decode-fallback-788` (2026-10-14).

Merge order with !1418: both edit the line that builds the query budget. !1418 moves it into `with_quota_over`; whichever merges second rebases, and `.with_sort_share()` belongs in `with_quota_over`.

Open:
- With !1418, a read admitted at red runs under `pressure_admission_memory` (64 MB). Before this change no spilling sort at 7+ partitions could finish under that cap.
- `SortMemoryRule` grants from `query.max_query_memory`, not the quota a query actually runs under. It is a plan-time gate only; follow-up.
- An aggregate can still starve a sort inside one query (both spill, greedy between them); pre-existing.

**NEXT:** rerun the SF10 concurrency probe (!1418 and this change are both merged); then #755.
