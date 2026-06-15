# SF10 slow / problem queries — SQE vs Trino EXPLAIN comparison

Captured 2026-06-15 on the post-fix rig (all session fixes merged: q28 dialect,
q95 self-join strip, sort-on-write + OOM-failover, CTAS `PARTITIONED BY`
transforms; TPC-H loaded **partitioned by `month(date)`**). SQE = single-node
host coordinator (16 GB pool, no workers); Trino 465 = single node, same Iceberg
tables. SQE plans/metrics from the query profiler (`EXPLAIN ANALYZE` equivalent);
Trino from `EXPLAIN ANALYZE`.

> Caveat: the host was moderately loaded (browser + other processes), so absolute
> wall-clock is indicative. The **plan shapes, scan byte/row counts, and join
> strategies** below are the trustworthy signals.

## Where SQE trails at SF10 (the rest of the suite SQE wins)

SQE wins TPC-DS (1.30×), ClickBench (1.45×), and q95 (3.3×) at SF10. The losses
cluster in **TPC-H** (dragged by a few exploding queries) and the long-standing
**SSB scan-bound** gap. TPC-E/TPC-C are transactional and SQE wins them at SF1.

| Query | SQE | Trino | Ratio | Root cause (summary) |
|---|---:|---:|---:|---|
| **tpch q12** | **>60s (timeout)** | 2.2s | **<0.04×** | simple `orders⋈lineitem`; SQE over-scans all 84 `lineitem` partitions + single-node join — should be trivial |
| **tpch q10** | **>60s / 300s FAIL** | 3.6s | ~0.01× | 4-way join + group-by; Trino uses 2 PARTITIONED joins, SQE broadcasts single-node |
| **tpch q17** | **>60s** | 8.4s | <0.14× | correlated `AVG(l_quantity)` subquery → Trino CrossJoin+PARTITIONED; SQE materializes (q95 sibling) |
| **tpch q20** | 28.7s | 2.5s | 0.1× | nested `IN`-subqueries (LeftSemi/RightSemi); 4 PARTITIONED joins in Trino vs SQE CollectLeft |
| **tpch q09** | 11.1s | 4.5s | 0.4× | full `lineitem` (60M) + `orders` scan, 3.15M join; Trino 4 PARTITIONED vs SQE CollectLeft |
| **tpch q08** | 8.4s | 3.0s | 0.4× | 7-way join; SQE over-scans all 84 `lineitem` partitions (863 MB) for 395K rows |
| **tpch q19** | 5.6s | 2.4s | 0.4× | OR-predicate on `lineitem`; SQE scans all 84 partitions (528 MB) to return 5K rows |
| **tpch q06** | 0.3–0.7s | 0.2–0.5s | 0.6× | date-range; partition pruning **works** (12/84 files) but vectorized decode loses |
| **ssb q4.1** | 0.8–2.9s | 0.4–5.2s | 0.5× | 4 dim joins, all CollectLeft; 335 MB `lineorder` scan-bound |
| **ssb q3.3** | 0.4–2.8s | 0.3–2.3s | 0.6× | scan-bound (same shape) |
| **ssb q2.3** | 0.5–2.1s | 0.4–2.6s | 0.8× | scan-bound (same shape) |

## Three root causes

### 1. Partition over-scan (the TPC-H partitioning is net-negative)
Partitioning `lineitem` by `month(l_shipdate)` (84 partitions) **helps date-range
queries** — q06 and q20 prune to **12 of 84** files. But for queries with **no
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
(broadcast). At SF10 the large intermediates (q09 → 3.15M, q20 → 86K via
RightSemi) are processed on one node. This is the known "TPC-H heavy joins" SF10
gap — and the README's level-rig numbers show SQE **distributed** (2 workers)
closes much of it (tpch 130s single → 95s distributed). The single-node host
coordinator used here exaggerates the loss; a worker-backed run is the apples-to-
apples comparison.

### 3. Subquery explosions (q10 / q12 / q17 / q20) — the q95 sibling
These four are subquery/aggregate-heavy and **explode on SQE** while Trino
dispatches them in 2–8 s:
- **q17**: correlated `WHERE l_quantity < (SELECT 0.2*AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)`. Trino decorrelates to CrossJoin + PARTITIONED join (8.4 s). SQE times out >60 s.
- **q20**: nested `IN (SELECT ... FROM lineitem ... )` over partsupp/supplier — LeftSemi + RightSemi (28.7 s).
- **q12 / q10**: join + group-by; q12 is *only* `orders⋈lineitem` yet SQE exceeds 60 s vs Trino 2.2 s — the most alarming, since it is structurally simple.

These are the **same subquery-decorrelation family as the q95 regression**, but
they involve an **aggregated** sub-select (q17's `AVG`). The shipped q95 fix
(`strip_self_join_dynamic_filters`) deliberately **skips joins with an
`AggregateExec` between the scan and the join** (to avoid over-firing on benign
year-over-year CTEs) — so these aggregated-subquery self-joins are **not** covered
and can still hit the inlist/materialization blow-up. Compounded by the partition
over-scan (#1) and single-node execution (#2).

> SQE plans for q10/q12/q17 are absent here: they exceed the 60 s bench per-query
> timeout, so the profiler never recorded a completed plan. That a 2-table join
> (q12) cannot finish in 60 s while Trino does it in 2.2 s is itself the finding.

## SSB (scan-bound, long-standing)
q2.1–q4.3 share one shape: 3–4 dimension joins, **all `CollectLeft`**, over a
`lineorder` scan of **335 MB / 150 K decoded rows** (4 files). SQE and Trino are
close (0.5–0.8×); the residual is raw scan/decode throughput on the uniform-FK
`lineorder` where row-group pruning can't help (no clustering benefit on the
filtered columns). This is the known SSB gap — the candidate fixes are bloom
filters on the join keys and/or faster vectorized decode, not plan changes.

## Recommended next levers (in priority order)
1. **Back out / gate TPC-H month-partitioning** — net-negative at SF1 and SF10 for the non-date-filtered majority (root cause #1). Re-confirm the suite total without it.
2. **Investigate q12** — a simple 2-table join exceeding 60 s is a likely bug (partition over-scan + a join pathology); cheapest high-impact fix.
3. **Extend the q95 self-join/inlist fix to aggregated subqueries** (q17/q20/q10 family) — the `AggregateExec` barrier currently excludes them.
4. **Re-run the heavy TPC-H joins distributed** (2 workers) for a fair comparison — single-node exaggerates q08/q09 (root cause #2).
5. **SSB scan throughput** — bloom-on-join-keys / vectorized decode (long-standing, lower urgency since the gap is small).
