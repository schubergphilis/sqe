**STATUS 2026-10-14: a join query refused memory at Iceberg scan decode retries with sort-merge joins (`fix/scan-decode-fallback-788`, #788).**

Found in the SF10 gate for the sort share (`2026-10-13-fix-sort-merge-headroom-788.md`). At a 1 GB quota, TPC-DS q14 failed in about half its runs, and TPC-H q09 failed in every run on main. In each failure the scan's decode gate was the consumer refused (`iceberg-scan-decode:<table>`); when a hash build was refused first, the join fallback (#784) retried and the query passed.

Cause: `ScanDecodeGate`'s `DecodeGate` impl (`sqe-catalog/src/scan_memory.rs`) turned the `DataFusionError::ResourcesExhausted` into an `iceberg::Error` holding only its text. The scan wraps that as `DataFusionError::External`, and `find_root` stops at an `External` it cannot downcast, so `JoinFallback::applies` never saw a memory refusal. The doc on `applies` already named q09's scan-decode refusal as a case it covered.

Fix:
- `scan_memory::refused` keeps the DataFusion error as the `iceberg::Error`'s source (`with_source`). Its display still carries the "Resources exhausted" wording that the text classifier in `sqe-core` matches.
- `query_memory::is_resources_exhausted` walks the `std::error::Error` source chain, downcasting each link to `DataFusionError` and testing its `find_root`. `JoinFallback::applies` and `execution_error` (the client's `RESOURCE_EXHAUSTED` code) both use it.

Tests: `a_scan_decode_refusal_behind_an_iceberg_error_is_resources_exhausted` (query_memory: the wrapped shape, the same under `.context`, the error code, and the text-only wrap stays false); `decode_gate_trait_denial_carries_exhausted_wording` now also asserts the typed source. `cargo test -p sqe-coordinator --lib`: 844 passed; `sqe-catalog` scan_memory: 12 passed.

Live (SF10, 16 GB pool, `max_query_memory = 1GB`, dev-release):
- TPC-H q09: refused at `iceberg-scan-decode:tpch_sf10.lineitem`, retried, and passed in 62 s and 82 s (175 rows). On main it fails within about a second every time. The bench's default 60 s per-query timeout cuts it off, so `BENCH_QUERY_TIMEOUT_SECS` has to be raised to see it pass.
- TPC-DS q14: 5/5 passed (33-43 s, all retried). Main passed 3/5 and the previous branch 2/5. In these 5 runs every first refusal was a hash build, so q14 did not itself exercise the new path; q09 did.
- TPC-DS q09 is not covered: it has no hash join to switch, so the fallback does not apply.

SF10 concurrency probe (#788 workload: TPC-H SF10, greedy pool, 4 GB node pool and 2 GB per query; main with !1418 and !1419, plus this change):

| | 2026-10-07 (!1317) | 2026-10-14 |
|---|---|---|
| sort alone (`row_number() OVER (ORDER BY l_comment)`) | fails in the merge | ok, 18.9 s |
| agg alone | ok, 8.1 s | ok, 7.0 s |
| join alone (q09) | ok | ok, 9.6 s |
| 2 sort + 2 agg + 2 join concurrently | 2/6 ok | 3/6 ok (both joins, one agg) |
| light `count(*) FROM nation` every 0.5 s | 2/41 rejected | **0/263 rejected**, p50 135 ms, p95 1443 ms |

- Peak RSS was 3.6 GB, and no spill files were left.
- In the concurrent run, one join was refused at scan decode, retried through the path this change opens, and completed after 230 s.
- The three failures were all `iceberg-scan-decode:tpch_sf10.lineitem` refusals against the full 4 GB node pool, while spillable aggregates held about 117 MB per partition. Sorts and aggregates have no fallback.
- An earlier probe run (q09 mistyped by the probe script) also had 0/68 light rejections.
- #788 item 3 (zero light-query rejections) holds.
- Light-query p95 rose from 535 ms to 1.4 s. They are now admitted while heavy queries hold the pool, so they wait behind them instead of being refused.

Open:
- Scan-decode admission fails against the node pool before other queries' spillable operators spill (#788 open item 1). It is now the only failure mode in the probe.
- On the second q09 run the coordinator logged `Query completed` with 376 s elapsed, while the bench had all its rows at 82 s. Completion seems to be logged when the stream is released; not investigated.

**NEXT:** rerun the SF10 concurrency probe; then #755.
