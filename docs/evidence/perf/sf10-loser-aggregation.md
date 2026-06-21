# SF10 slower queries: EXPLAIN ANALYZE aggregation (clean rig, 2026-06-16)

Where SQE trails Trino 465 at SF10, and why. Captured on the dedicated 8-core/31GB
box, both engines containerized against the same Iceberg store, query cache off,
single-node. For every query SQE loses (ratio < ~0.85x) we ran `EXPLAIN ANALYZE` on
both engines; raw plans are in `docs/perf/sf10-explains/<suite>_<q>.{sqe,trino}.txt`.

The point of this doc is not 30 plans. It is: **what do we fix, and what is it
worth.** Each loser gets one root-cause bucket; buckets are ranked by total
seconds lost (sum of SQE minus Trino across that bucket's queries).

## Result: two buckets, and one dominates

| Bucket | Root cause | Queries | Seconds lost | Share |
|---|---|---|---|---|
| **A** | Single-partition fact-table scan / decode throughput | 27 | **114.3s** | **87%** |
| **B** | Single-node join / aggregate on full fact cardinality | 3 | 16.9s | 13% |

Total measured loss across all SF10 losers: 131.3s. The headline is blunt:
**87 percent of everything we lose at SF10 is scan and decode throughput, not join
strategy.** One query, TPC-DS q72, is 28 percent of the total all by itself.

## Bucket A: scan / decode throughput (the real problem)

SQE's `IcebergScanExec` advertises `UnknownPartitioning(1)`. A fact-table scan
funnels into one output stream, then a `RepartitionExec(RoundRobinBatch(11))` spreads
it across cores for the joins and aggregates above. So the joins parallelize but the
scan and its decode do not. Trino runs the same scan as a split-level SOURCE pipeline:
many splits decode in parallel with a vectorized reader, and the rows fan out before
any single thread becomes the bottleneck.

The EXPLAIN ANALYZE evidence: in every Bucket-A loser the dominant operator by
elapsed time is the `IcebergScanExec`, and Trino's matching SOURCE fragment shows the
same input row count spread across parallel work.

| Query | SQE bottleneck (elapsed) | SQE / Trino | Lost |
|---|---|---|---|
| tpcds q72 | IcebergScanExec 124.3s (38.6M rows; Trino scans 1.17B) | 140.6 / 103.7s | 36.9s |
| tpcds q50 | IcebergScanExec 8.5s for 34k output rows | 11.0 / 2.7s | 8.4s |
| tpcds q64 | IcebergScanExec 3.2s | 12.7 / 5.2s | 7.5s |
| tpcds q88 | 8 subquery scans of store_sales, serialized | 8.4 / 0.9s | 7.5s |
| tpch q09 | IcebergScanExec 11.9s (3.15M rows) | 17.3 / 5.7s | 11.6s |
| tpch q17 | IcebergScanExec 8.8s (correlated subquery rescans lineitem) | 13.4 / 7.9s | 5.5s |
| tpcds q09 | scan-fed, 28.8M store_sales | 10.6 / 5.7s | 4.9s |
| tpch q08/q10/q12/q20 | IcebergScanExec dominant | - | 8.6s combined |
| tpcds q24/q31/q90/q93/q96 | scan / IO bound | - | 5.5s combined |
| SSB (all 10 losers) | lineorder 52-60M decoded per query | - | 15.3s combined |

SSB is the purest case: its uniform foreign-key distribution defeats row-group
pruning, so every star query decodes 52 to 60 million lineorder rows. SQE's compute
per operator is tiny (single-digit to low-hundreds of milliseconds); the wall-clock
sits in scan I/O and decode that one stream cannot keep up with. q88 and q96 are the
extreme-ratio cases (0.1x, 0.2x) but low absolute: each is a small query where SQE's
serialized scan setup dominates a sub-second Trino result, not a separate pathology.

Fix direction: real scan-level parallelism. The #131 work split row-group decode
inside a file but kept `UnknownPartitioning(1)`; the scan still presents one partition
to the plan. The lever is to have `IcebergScanExec` emit N partitions (one per
split / row-group band) so decode runs wide, the way Trino's SOURCE pipeline does.
This single change addresses 87 percent of the SF10 gap.

## Bucket B: single-node join / aggregate on full fact cardinality

A smaller, distinct gap. Trino PARTITION-distributes a large join or aggregate across
the cluster; SQE runs it on one node.

| Query | SQE bottleneck | SQE / Trino | Lost | Trino |
|---|---|---|---|---|
| tpch q18 | HashJoinExec 25.2s over 60M rows | 30.4 / 17.0s | 13.3s | PARTITIONED join |
| tpch q01 | AggregateExec 5.3s over 59M lineitem rows | 4.3 / 2.5s | 1.9s | distributed aggregate |
| tpcds q44 | AggregateExec on 28.8M store_sales | 3.9 / 2.1s | 1.8s | distributed aggregate |

q18 is the one query where join strategy, not scan, is the headline: a 60M-row hash
join that Trino splits across PARTITIONED fragments and SQE builds on a single node.
This is the natural place to ask whether SQE's own distributed mode (the
hash-shuffle ShuffleHashJoinPlan) closes the gap. That is a separate experiment, not
part of this EXPLAIN pass; noting it here because the plans point straight at it.

## What changed from the pre-fix read

The earlier version of `sf10-slow-queries.md` blamed three causes including a
"subquery explosion" bucket. That bucket is gone: the dynamic-filter snapshot fix
(!371) removed the q10/q12/q17/q20 blow-ups, confirmed on this clean rig (all now
single-digit-to-teens seconds, no explosions). What remains is not pathology, it is
throughput: SQE decodes one fact stream where Trino decodes many. Fix the scan
parallelism and the SF10 crossover narrows or closes for everything except the
genuinely distribution-bound joins (q18).
