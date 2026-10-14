**STATUS 2026-10-14: an Iceberg scan refused memory by a full node pool waits for other queries to release it instead of failing (`fix/scan-decode-backpressure-788`, #788 item 1).**

Follows `2026-10-14-fix-scan-decode-fallback-788.md`. After that change, every failure in the SF10 concurrency probe was a scan-decode refusal (`iceberg-scan-decode:tpch_sf10.lineitem`) against the full 4 GB node pool, while the failing query's own 2 GB quota still had room. Other queries' spillable aggregates and sorts would have freed that memory a few seconds later.

Fix:
- `ScanDecodeGate::reserve` (`sqe-catalog/src/scan_memory.rs`) checks which pool refused. When the query's own quota has room for the bytes (`reserved + bytes <= memory_limit`), the refusal came from the node pool, so the gate polls with backoff (10 ms, doubling to 200 ms) until memory frees or the wait budget runs out. It then fails with the same typed `ResourcesExhausted` as before.
- A refusal by the query's own quota still fails at once. Waiting there could deadlock the query on itself, and the #784 join fallback needs the fast error: q09 still retried with sort-merge joins after 1.3 s and 1.5 s at a 1 GB quota (2/2 passed, 175 rows).
- A pool without a quota (the worker's fair pool) never waits.
- The budget is `[catalog] scan_memory_wait_ms` (default 30000; 0 restores fail-fast). It is the time slept in total per scan partition (one gate per `execute(partition)`), not a deadline per admission. A long scan that waits early keeps the rest of its budget for later refusals, and a scan never waits more than the budget in total, however many pieces it admits.
- The vendored reader pipeline (`vendor/iceberg-rust/.../reader/pipeline.rs`, SQE PATCH sqe#788) races each admission against the output channel closing. A cancelled query stops waiting and its dispatcher and workers exit.

Tests: `scan_memory` 19 passed. New: a node-pool denial waits for another query to release (staged and unstaged), fails typed when the wait runs out, the budget is one per gate rather than one per admission, a later denial keeps the budget an earlier wait left, a quota denial fails without waiting, and a pool without a quota fails without waiting. `clippy -p sqe-catalog --tests` and `clippy -p iceberg` are clean; `cargo check --workspace --tests` passes.

SF10 concurrency probe (TPC-H SF10, greedy pool, 4 GB node pool and 2 GB per query, dev-release; two runs):

| | 2026-10-14 fallback (!1421) | this change, run 1 | this change, run 2 |
|---|---|---|---|
| sort alone | ok, 18.9 s | ok, 14.5 s | ok, 18.3 s |
| agg alone | ok, 7.0 s | ok, 4.8 s | ok, 5.9 s |
| join alone (q09) | ok, 9.6 s | ok, 6.8 s | ok, 9.0 s |
| 2 sort + 2 agg + 2 join concurrently | 3/6 ok | **4/6 ok** (both joins, both aggs), 31.3 s | **4/6 ok** (both joins, both aggs) |
| light `count(*) FROM nation` every 0.5 s | 0/263 rejected, p50 135 ms, p95 1443 ms | 0/58 rejected, p50 23 ms, p95 193 ms | 0/120 rejected, p50 49 ms, p95 401 ms |
| peak RSS | 3.6 GB | 4.6 GB | 4.6 GB |

- No scan-decode refusals were left. Both remaining failures in each run are the sorts, refused in their merge: `ExternalSorterMerge` could not get 10 MB (or 38 KB) from the full 4 GB node pool while other queries' `FinalHashAggregateStream`s held about 87-174 MB per partition. They failed after 10-17 s, not after minutes of waiting, because the merge reservation does not go through the scan gate.
- Run 2's joins reported 151 s and 186 s because the laptop clamshell-slept for 117 s mid-run (pmset). The sorts had already failed and the aggregates completed before the sleep; one join retried with sort-merge joins (#784).
- An earlier attempt was discarded: the laptop slept, and the queries came back with S3 403s from clock skew.
- Peak RSS rose from 3.6 to 4.6 GB, because more queries now hold their memory at the same time instead of failing. The 4 GB pool cap held; the RSS above it was not broken down.
- No spill files were left.

Cancellation: with the pool at 3.3 GB under the same concurrent load, `kill -9` of the clients brought the pool to 0 bytes by the next query 20 s later, with no `iceberg-scan-decode` consumers left.

Not run: the SF10 TPC-H / TPC-DS pair against main. The wait only starts after a refusal, so a run without memory pressure does not reach it.

Open:
- The sort-merge reservation (`ExternalSorterMerge`) is refused by the node pool while other queries' spillable aggregates hold memory. It is now the only failure mode in the probe. A sort has no fallback and the merge cannot spill.
- `Query completed` elapsed still sometimes exceeds the client's time (from the fallback status), not investigated.

**NEXT:** the sort-merge node-pool refusal under concurrency (#788), then #755.
