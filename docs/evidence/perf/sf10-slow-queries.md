# SF10 slow / problem queries - SQE vs Trino EXPLAIN comparison

Captured 2026-06-15 on the post-fix rig (all session fixes merged: q28 dialect,
q95 self-join strip, sort-on-write + OOM-failover, CTAS `PARTITIONED BY`
transforms; TPC-H loaded **partitioned by `month(date)`**). SQE = single-node
host coordinator (16 GB pool, no workers); Trino 465 = single node, same Iceberg
tables. SQE plans/metrics from the query profiler (`EXPLAIN ANALYZE` equivalent);
Trino from `EXPLAIN ANALYZE`.

> Caveat: the host was moderately loaded (browser + other processes), so absolute
> wall-clock is indicative. The **plan shapes, scan byte/row counts, and join
> strategies** below are the trustworthy signals.

## VALIDATED 2026-06-16: clean-rig run confirms the fix and the honest crossover

Re-ran the whole comparison on a dedicated 8-core / 31 GB box (nothing else on it),
both engines containerized against the same Iceberg store, query cache off,
single-node. This removes the contention caveat above: the suite totals here are
trustworthy because both engines share the box, so the hardware cancels in the
ratio. Trino is 465.

The dynamic-filter fix holds at scale. The four queries that ran 160 to 300
seconds are now 4.6s (q10), 4.9s (q12), 13.5s (q17), 3.1s (q20). No query explodes.

| Suite | SQE | Trino | Ratio | Match |
|---|---|---|---|---|
| TPC-H SF1 | 15.8s | 36.4s | 2.3x | 22/22 |
| SSB SF1 | 4.8s | 7.3s | 1.5x | 13/13 |
| TPC-DS SF1 | 46.6s | 111.3s | 2.4x | 92/99 |
| TPC-H SF10 | 126.4s | 108.8s | **0.86x** | 21/22 |
| SSB SF10 | 31.8s | 16.8s | **0.53x** | 13/13 |
| TPC-DS SF10 | 374s | 455s | **1.22x** | 95/99 |

SQE wins every suite at SF1. At SF10 there is a real scaling crossover: SQE wins
TPC-DS on breadth (q01/q02/q04/q05/q08/q11/q14/q22/q23 by 2 to 4x) even while
losing the single biggest query q72 (0.7x, 140s of SQE's 374s total), but trails
TPC-H on the heavy joins (q09 0.3x, q18 0.6x) and trails SSB across the board.
The contended-Mac read of "SQE wins TPC-H SF10" did not survive a clean box. It is
not a cache effect: the compare runs each query once, so the result cache cannot
inflate a single sweep. On large data Trino's vectorized and distributed hash
joins scale better. That is the work ahead.

All three SF10 suites were measured under one memory budget (SQE 12 GB pool,
Trino heap capped at 32 percent with 7 GB per query) after a host OOM-kill of the
SQE process showed that a 16 GB pool plus Trino's default heap did not fit 31 GB.
TPC-H and SSB were re-run at this budget to confirm the numbers did not move
(both within a percent of the 16 GB run), so the TPC-DS win is not a Trino memory
handicap.

Loading SF10 also surfaced a write-path gap: a partitioned CTAS with a
sort-on-write hint fans the sort into one non-spillable merge buffer per output
partition, exhausting the pool (TPC-H lineitem at 60M rows across ~84 monthly
partitions failed where the unpartitioned SSB lineorder of the same size sorted
fine). The bench loader now skips the redundant sort on already-partitioned
tables; the engine-level fix (bounded or spillable partition writers) is open.

## RESOLVED 2026-06-15: the q12/q17/q10/q20 explosions were a dynamic-filter snapshot bug

The first cut of this doc (below) blamed the q12/q17/q10/q20 blow-ups on partition
over-scan, single-node `CollectLeft`, and a "q95 aggregated-subquery sibling".
That was wrong. Root-causing with a CPU profile and per-operator timers found the
real cause, which is the same for all four and has nothing to do with join
distribution or partitioning.

**Root cause.** On a `mode=Partitioned` hash join, DataFusion's build-side
dynamic filter is a single `CASE hash_repartition % N WHEN i THEN <partition i's
IN-list> ...` expression. Each per-partition branch carries that partition's
build keys as an `IN (SET)` list, so the whole expression holds tens of thousands
of literal nodes (q12 SF10: 11 branches × ~28K orderkeys ≈ 300K nodes). SQE's
probe-side scan applies this filter per batch and called
`DynamicFilterPhysicalExpr::current()` **once per batch** to re-sample it.
`current()` rebuilds the entire expression every call (it walks the tree via
`transform_up` to remap children), costing ~10 ms for a tree this size. Over a
15M-row / ~14,600-batch probe scan that is ~150 s spent rebuilding the filter,
versus ~0.2 s actually evaluating it. Per-operator timers (cumulative, dead
linear): `current()` 41,156 ms vs `evaluate()` 399 ms at 4,000 batches.

The `IN (SET)` hash membership already works; the cost was never the inlist
evaluation. Lowering `runtime_filter_inlist_max_values` (the q95 lever) only
"fixed" it by suppressing the inlist entirely, which is why it looked like the
q95 family.

**Fix** (`crates/sqe-catalog/src/iceberg_scan.rs`): cache the first sealed
(non-`lit(true)`) snapshot of each dynamic filter per scan stream and reuse it,
instead of calling `current()` every batch. The build side seals once and never
reverts; while pending, the snapshot is the tiny `lit(true)` placeholder (cheap
to re-sample), so only not-yet-sealed slots keep sampling. The dynamic filter is
a probe-reduction optimization the hash join re-checks, so a cached (slightly
stale) snapshot can only pass extra rows, never drop a match - correctness is
unaffected.

**Validation** (same rig, default `runtime_filter_inlist_max_values = 65536`, no
threshold change):

| Query | Before | After fix | Speedup |
|---|---:|---:|---:|
| tpch q12 | 161 s | 2.7 s | 60× |
| tpch q17 | 176 s | 7.1 s | 25× |
| tpch q10 | 300 s (FAIL) | 3.3 s | ~90× |
| ssb q4.1 | 11.6 s | 6.8 s | 1.7× |
| ssb q3.1 | 5.6 s | 3.1 s | 1.8× |

q12 result rows are byte-identical before/after (MAIL 154,379 / SHIP 154,144).
SSB improves too at the default threshold - the per-batch `current()` was taxing
every Partitioned-join probe scan, just less visibly than the multi-minute
TPC-H cases. This retires the SSB-vs-TPC-H threshold tradeoff: keep the 65536
default; the fix makes it safe.

The partition over-scan analysis below (root cause #1) is still valid for
q08/q09/q19, which do not explode - they trail ~0.4× from reading all 84
`lineitem` partition files. Root causes #2 (single-node CollectLeft) and #3 (the
"q95 sibling" story) are **superseded** by the above for q12/q17/q10/q20.

---

## Where SQE trails at SF10 (the rest of the suite SQE wins)

SQE wins TPC-DS (1.30×), ClickBench (1.45×), and q95 (3.3×) at SF10. The losses
cluster in **TPC-H** (dragged by a few exploding queries) and the long-standing
**SSB scan-bound** gap. TPC-E/TPC-C are transactional and SQE wins them at SF1.

| Query | SQE | Trino | Ratio | Root cause (summary) |
|---|---:|---:|---:|---|
| **tpch q12** | **>60s (timeout)** | 2.2s | **<0.04×** | simple `orders⋈lineitem`; SQE over-scans all 84 `lineitem` partitions + single-node join - should be trivial |
| **tpch q10** | **>60s / 300s FAIL** | 3.6s | ~0.01× | 4-way join + group-by; Trino uses 2 PARTITIONED joins, SQE broadcasts single-node |
| **tpch q17** | **>60s** | 8.4s | <0.14× | correlated `AVG(l_quantity)` subquery -> Trino CrossJoin+PARTITIONED; SQE materializes (q95 sibling) |
| **tpch q20** | 28.7s | 2.5s | 0.1× | nested `IN`-subqueries (LeftSemi/RightSemi); 4 PARTITIONED joins in Trino vs SQE CollectLeft |
| **tpch q09** | 11.1s | 4.5s | 0.4× | full `lineitem` (60M) + `orders` scan, 3.15M join; Trino 4 PARTITIONED vs SQE CollectLeft |
| **tpch q08** | 8.4s | 3.0s | 0.4× | 7-way join; SQE over-scans all 84 `lineitem` partitions (863 MB) for 395K rows |
| **tpch q19** | 5.6s | 2.4s | 0.4× | OR-predicate on `lineitem`; SQE scans all 84 partitions (528 MB) to return 5K rows |
| **tpch q06** | 0.3-0.7s | 0.2-0.5s | 0.6× | date-range; partition pruning **works** (12/84 files) but vectorized decode loses |
| **ssb q4.1** | 0.8-2.9s | 0.4-5.2s | 0.5× | 4 dim joins, all CollectLeft; 335 MB `lineorder` scan-bound |
| **ssb q3.3** | 0.4-2.8s | 0.3-2.3s | 0.6× | scan-bound (same shape) |
| **ssb q2.3** | 0.5-2.1s | 0.4-2.6s | 0.8× | scan-bound (same shape) |

## Three root causes

### 1. Partition over-scan (the TPC-H partitioning is net-negative)
Partitioning `lineitem` by `month(l_shipdate)` (84 partitions) **helps date-range
queries** - q06 and q20 prune to **12 of 84** files. But for queries with **no
date filter on `lineitem`** it forces opening **all 84 small partition files**:

| Query | `lineitem` files scanned | bytes scanned | rows decoded |
|---|---:|---:|---:|
| q08 | **84 (all)** | 863 MB | 395 K |
| q09 | **84 (all)** | 908 MB | 60 M (full) |
| q19 | **84 (all)** | 528 MB | 5 K |
| q06 | 12 (pruned) | 50 MB | 9 M |
| q20 | 12 (pruned) | 70 MB | 9 M |

q19 is the clearest waste: **528 MB scanned across 84 files to return 5 K rows.**
The small-file open/footer overhead outweighs any benefit when the query doesn't
filter on the partition key. **Recommendation: back out TPC-H month-partitioning**
(or gate partitioning to large single-partition files), and do **not** roll
lexicographic date-partitioning out to the other suites blindly. Partition
pruning is a win only for queries that filter the partition column.

### 2. Heavy joins: Trino distributes (PARTITIONED), SQE broadcasts single-node (CollectLeft)
For the big fact-to-fact / multi-way joins, **Trino hash-partitions the join
across its workers** (q09: 4 PARTITIONED, q10: 2, q17: 2, q20: 4), while this
SQE run is a **single-node coordinator** that builds every join as `CollectLeft`
(broadcast). At SF10 the large intermediates (q09 -> 3.15M, q20 -> 86K via
RightSemi) are processed on one node. This is the known "TPC-H heavy joins" SF10
gap - and the README's level-rig numbers show SQE **distributed** (2 workers)
closes much of it (tpch 130s single -> 95s distributed). The single-node host
coordinator used here exaggerates the loss; a worker-backed run is the apples-to-
apples comparison.

### 3. Subquery explosions (q10 / q12 / q17 / q20) - the q95 sibling
These four are subquery/aggregate-heavy and **explode on SQE** while Trino
dispatches them in 2-8 s:
- **q17**: correlated `WHERE l_quantity < (SELECT 0.2*AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)`. Trino decorrelates to CrossJoin + PARTITIONED join (8.4 s). SQE times out >60 s.
- **q20**: nested `IN (SELECT ... FROM lineitem ... )` over partsupp/supplier - LeftSemi + RightSemi (28.7 s).
- **q12 / q10**: join + group-by; q12 is *only* `orders⋈lineitem` yet SQE exceeds 60 s vs Trino 2.2 s - the most alarming, since it is structurally simple.

These are the **same subquery-decorrelation family as the q95 regression**, but
they involve an **aggregated** sub-select (q17's `AVG`). The shipped q95 fix
(`strip_self_join_dynamic_filters`) deliberately **skips joins with an
`AggregateExec` between the scan and the join** (to avoid over-firing on benign
year-over-year CTEs) - so these aggregated-subquery self-joins are **not** covered
and can still hit the inlist/materialization blow-up. Compounded by the partition
over-scan (#1) and single-node execution (#2).

> SQE plans for q10/q12/q17 are absent here: they exceed the 60 s bench per-query
> timeout, so the profiler never recorded a completed plan. That a 2-table join
> (q12) cannot finish in 60 s while Trino does it in 2.2 s is itself the finding.

## SSB (scan-bound, long-standing)
q2.1-q4.3 share one shape: 3-4 dimension joins, **all `CollectLeft`**, over a
`lineorder` scan of **335 MB / 150 K decoded rows** (4 files). SQE and Trino are
close (0.5-0.8×); the residual is raw scan/decode throughput on the uniform-FK
`lineorder` where row-group pruning can't help (no clustering benefit on the
filtered columns). This is the known SSB gap - the candidate fixes are bloom
filters on the join keys and/or faster vectorized decode, not plan changes.

## Recommended next levers (in priority order)
1. **Back out / gate TPC-H month-partitioning** - net-negative at SF1 and SF10 for the non-date-filtered majority (root cause #1). Re-confirm the suite total without it.
2. **Investigate q12** - a simple 2-table join exceeding 60 s is a likely bug (partition over-scan + a join pathology); cheapest high-impact fix.
3. **Extend the q95 self-join/inlist fix to aggregated subqueries** (q17/q20/q10 family) - the `AggregateExec` barrier currently excludes them.
4. **Re-run the heavy TPC-H joins distributed** (2 workers) for a fair comparison - single-node exaggerates q08/q09 (root cause #2).
5. **SSB scan throughput** - bloom-on-join-keys / vectorized decode (long-standing, lower urgency since the gap is small).
