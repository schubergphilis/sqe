**STATUS 2026-10-14: a read query refused by a full coordinator pool waits for room and runs its own plan again (`fix/memory-rerun-788`, #788).**

Follows `2026-10-14-fix-scan-decode-backpressure-788.md` (!1422). After the scan wait, the SF10 concurrency probe (4 GB pool, 2 GB quota; two sorts, two aggregates and two q09 joins at once, light `count(*)` every 0.5 s) still lost two of six heavy queries. The sorts failed when they took their merge reservation back after a spill. A join refused at its hash build went to the sort-merge retry, which does not help when other queries hold the memory. Every refusal showed the pool at about 4.28 of 4.29 GB, held mostly by the aggregates' `FinalHashAggregateStream` partitions.

Three approaches were measured and set aside:
- A merge reserve in the node pool (local branch `fix/sort-merge-reserve-788`, not pushed): 2/6, 3/6 and 4/6 over three runs. The sorts failed in every run, since the pool really was full. One run had a merge's own final growth refused to keep another merge's slot, and joins were pushed onto the sort-merge retry (86 s against 6.7 s alone).
- `coordinator.unspillable_headroom_percent = 30` on main: 3/6 (main at 20: 4/6).
- Committing each query's full quota (local branch `feat/committed-quota-admission-788`, #788 note 493521).

Change (`join_fallback.rs`, `query_memory.rs`, `query.memory_rerun`, default true):
- `query_memory::is_quota_refusal` tells a refusal by the query's own quota (its message starts with the setting it names) from one by the coordinator pool.
- `FallbackStream` arms for every read query that has a `JoinFallback`. On a pool refusal before the first batch, it drops the attempt and takes the coordinator's `RerunGate` (one rerun waits or starts at a time). It then waits, polling every 100 ms, until `reserved + min(quota, pool / 2) <= pool`, and plans and runs the same logical plan on the coordinator under a fresh quota. The gate is held until the rerun's first batch.
- If no room comes before `query.timeout_secs`, the rerun gives up (`rerun_timeout`). A rerun refused again goes to the sort-merge retry when the plan has a switchable hash join, and fails otherwise.
- A quota refusal goes straight to the sort-merge retry, unchanged.
- Without spill files only the rerun is armed. Under-pressure admissions get neither.
- Metrics: `sqe_join_fallback_total{outcome="rerun"|"rerun_timeout"}`.

Results (dev-release, on AC power, `caffeinate -ims`):

| run | heavy ok | sorts | joins | aggs | light p95 | peak RSS |
|---|---|---|---|---|---|---|
| main | 4/6 | 1 failed | 1 failed | ok | 214 ms | 4.2 GB |
| rerun, 1 | 6/6 | 47 s, 61 s | 30 s, 34 s | 25 s | 125 ms | 5.3 GB |
| rerun, 2 | 6/6 | 40 s, 57 s | 35 s, 42 s | 21 s | 111 ms | 5.2 GB |

Alone: sort 13 s, aggregate 5 s, join 7 s. Each rerun run logged three reruns (two sorts and one join) and no sort-merge retry. q09 alone under a 1 GB quota (the small-quota gate) still took the sort-merge retry and completed in 40 s, with no rerun.

Limits:
- The rerun repeats the abandoned attempt's work (the sorts lost 8 to 15 s each).
- The rerun is not distributed.
- Peak RSS rose about 1 GB over main, from more queries running to the end.
- A query refused after its first batch still fails.

Tests: coordinator lib 848 passed. New tests: a pool refusal reruns the hash plan once the other query lets go; the rerun gives up at the query timeout; reruns take the gate one at a time and room means a full quota fits; quota and pool refusals are told apart. Clippy clean in the touched files.

**NEXT:** SF10 TPC-H / TPC-DS pair against main on this branch, then #755.
