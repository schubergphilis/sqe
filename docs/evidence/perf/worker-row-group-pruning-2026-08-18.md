# Worker-side row-group pruning (#369, statistics half)

Date: 2026-08-18. Branch: `perf/worker-row-group-pruning-369`.

## The gap

A distributed scan pruned strictly worse than the same scan run locally, and
nothing in the code or the metrics said so.

The coordinator's local path prunes four ways before it decodes anything:
manifest bounds, row-group min/max statistics, the page index, and the #369
bloom probe for sealed membership sets. All of it lives in the vendored
iceberg-rust reader. The worker does not use that reader. It reads parquet
directly with `ParquetRecordBatchStreamBuilder`, selects row groups **only** by
the ticket's morsel row-group range and byte range, and uses the pushed
predicate for exactly one thing: a two-phase `RowFilter`.

So every distributed fragment decoded every row group its morsel covered. The
sealed min/max bounds that already ride every ticket, which are precisely what
statistics pruning consumes, were being ignored.

## Upstream check first

- DataFusion 54.1.0 is the latest 54. arrow/parquet 59 exists, but DF 54 pins
  arrow 58, so parquet cannot move on its own. No version bump is available.
- `RowGroupAccessPlanFilter::prune_by_statistics` is **public** in
  `datafusion-datasource-parquet` and reachable through the umbrella crate. Used
  as-is, so min/max semantics, null-count handling and type coercion stay
  upstream's problem instead of becoming a second implementation to keep in sync
  with the vendored one.
- `prune_by_bloom_filters` and `BloomFilterStatistics` are `pub(crate)`. They
  cannot be reused, which is why the vendored patch hand-rolled
  `SbbfRowGroupEvaluator`, and why the bloom half is not in this change.
- `Sbbf` and `ParquetRecordBatchStreamBuilder::get_row_group_column_bloom_filter`
  ARE public, so a worker-side bloom probe is buildable later with no vendor
  patch.

## Measured: SSB SF10, two-worker rig, one binary, interleaved

Both arms are the same binary; `SQE_WORKER_ROW_GROUP_PRUNING=0` is the OFF arm.
Distribution forced, shipped InList cap (65,536), two reps each, interleaved
OFF/ON/OFF/ON.

| arm | suite (ms) | mean | rows emitted | pruning fired |
|---|---|---|---|---|
| OFF | 45,700 / 43,955 | 44,828 | 6,968,724 | 0 |
| ON | 38,135 / 40,336 | **39,236** | 6,968,724 | 83 |

**-12.5%**, and the gap is about 2.5x the largest per-arm rep-to-rep spread.
All four runs are 13/13 with 13 queries distributed.

**Rows emitted are identical to the row in all four runs.** That is the
correctness result: pruning removed only row groups whose rows would all have
been discarded by the `RowFilter` anyway. Nothing was lost, and 13/13 passing
says the same thing from the other end.

### Per query

| q | OFF | ON | delta | % | max spread |
|---|---|---|---|---|---|
| q4.3 | 4,834 | 2,578 | **-2,256** | -46.7% | 972 |
| q4.2 | 4,206 | 2,426 | **-1,780** | -42.3% | 333 |
| q3.3 | 3,605 | 3,114 | -491 | -13.6% | 326 |
| q3.1 | 4,078 | 3,611 | -467 | -11.5% | 232 |
| q1.3 | 1,508 | 1,132 | -376 | -24.9% | 204 |
| q3.4 | 2,044 | 1,740 | -304 | -14.9% | 122 |
| q3.2 | 3,538 | 3,258 | -280 | -7.9% | 255 |
| q2.3 | 3,856 | 3,687 | -168 | -4.4% | 165 |
| q1.1 | 2,054 | 1,966 | -89 | -4.3% | 409 |
| q2.1 | 3,987 | 4,096 | +108 | +2.7% | 547 |
| q1.2 | 1,226 | 1,359 | +132 | +10.8% | 526 |
| q4.1 | 5,953 | 6,134 | +182 | +3.0% | 207 |
| q2.2 | 3,938 | 4,134 | +196 | +5.0% | 579 |

Every apparent regression is inside its own arm's rep-to-rep spread. The two
large wins are 2x to 7x outside it.

## Two metrics that cannot answer this question

Worth writing down, because both nearly produced a wrong conclusion.

- `sqe_worker_bytes_read_total` is fed by each file's SIZE (`known_size` or
  `store.head().size`), summed per fragment. It is Σ file sizes touched, not
  bytes fetched, so it is invariant no matter how many row groups are skipped.
  It reads 110,378,809,632 in every arm of every experiment today.
- `sqe_worker_rows_scanned_total` counts rows EMITTED, after the `RowFilter`,
  not rows decoded. It is the right instrument for "how many rows crossed
  Flight" (which is how it was used for the #415 membership measurement) and
  the wrong one for "how much decode work was avoided". Identical values across
  these arms is the correctness signal, not a null result.

Neither is a defect in this change, but a future measurement wanting decode
volume needs a new counter for row groups and rows actually decoded.

## What this does not cover

The bloom half. `BENCH_BLOOM_FILTER` defaults to off, so the loaded SF10 tables
carry no parquet bloom filters and a bloom probe would ship unmeasured. It needs
a bloom-enabled reload first, and it is the piece that would let large sealed
membership sets prune (see the #415 measurement: transporting an exact
membership set as an `InList` is a 16% net LOSS at SF10 precisely because it
prunes rows after row-group selection instead of pruning row groups).

## The bloom half was built, measured, and NOT shipped

Written, run against bloom-enabled data, and then removed from this branch. The
result is a clean negative and worth keeping.

`BENCH_BLOOM_FILTER=1` reload of SSB SF10, then a worker-side probe: extract
`col IN (literal, ...)` conjuncts from the pushed predicate, resolve each column
to its parquet leaf, load that row group's SBBF via the public
`get_row_group_column_bloom_filter`, and prune the row group when every key
tests bloom-negative.

**It never probed a single row group.** Across four runs, `probed = 0`,
`no_bloom = 0`, `pruned = 0`, with 234 probe attempts logged.

The reason is not that the keys fail to arrive. They do, and more than expected:
of the fragments carrying membership conjuncts, 84 carried
`[lo_orderdate, lo_custkey, lo_suppkey]`, 36 carried
`[lo_orderdate, lo_partkey, lo_suppkey]`, and 14 carried all four. High
cardinality scattered keys reach the worker as `InList`s under the shipped cap.

The reason is that **statistics pruning has already emptied those exact
fragments**. Of 71 pruning events on one worker, 61 ended with `kept: 0`: the
sealed bounds eliminate every row group the fragment owned. Ten kept one or two
groups. So the candidate set the bloom probe inherits is empty precisely where
the membership sets are richest.

Which follows from where both filters come from. Bounds and membership are two
halves of the same sealed dynamic filter. If the build side has finished, the
worker gets both, and at SF10 SSB the bounds alone are selective enough to
eliminate whole files. If it has not finished, it gets neither. There is no
regime in this workload where membership is available and bounds are not.

Reversing the order would not help: probing first would prune row groups that
statistics would have pruned anyway, at the cost of a ranged read per
(row group, column) to load each SBBF.

So the condition for revisiting is specific, and this workload does not meet it:
a join whose keys are scattered across the probe side's key domain AND whose
sealed bounds are unselective, i.e. a dimension key with no correlation to the
fact table's physical ordering. SSB's `lo_orderdate` correlates strongly with
file layout, which is exactly why bounds win.

Shipping the probe anyway would have added a per-task key materialization and a
ranged read for zero measured benefit. That is the same mistake as shipping an
unmeasured guard, so it stays out.

### A rig caveat that outlives this experiment

The bloom-enabled reload put the rig into a slower, high-variance regime:
the same distributed SSB SF10 suite measured 76-109 s afterwards against 44 s
before, with one arm spreading 40 s across two reps. Absolute times either side
of that reload are not comparable, and the -12.5% statistics result above was
taken before it on a rig whose per-arm spread was about 2 s. Any follow-up
timing work needs a fresh stable baseline first.

## Follow-up: how far the -12.5% claim actually extends

!879 shipped this default-on, measured on SSB SF10 only, so the next question was
whether it holds or regresses on other suites. Answered, and the answer bounds
the claim rather than extending it.

**At SF1 the pruning never fires.** Instrumented count of
`Row-group statistics pruning applied` per suite, one settled run, workers at
debug:

| suite | scale | result | pruning events |
|---|---|---|---|
| tpch | 1 | 22/22, 34,672 ms | **0** |
| tpcds | 1 | 99/99, 43,951 ms | **0** |
| ssb | 10 | 13/13, 44,930 ms | 87 |

So a TPC-H / TPC-DS SF1 A/B measures nothing: at SF1 the files are small enough
that a fragment's morsel covers a single row group, `kept.len()` equals
`candidates.len()`, and the pruner returns `None` without touching the
selection. An earlier forced-distribution SF1 A/B appeared to show tpch -18% and
tpcds -21%; **those numbers are noise and must not be quoted.** Instrumenting
the arm is what caught it, the same lesson as the vacuous distributed suite
earlier in this branch's history.

Two things follow. The change carries no risk at SF1 because it does not
engage. And "validated beyond SSB" is NOT established: validating on TPC-H needs
TPC-H at SF10, which is not loaded.

### A 60-second timeout that was not this change

One tpcds SF1 arm came back 97/99 with q38 and q54 erroring at exactly 60,002 ms
and 60,001 ms, against 99/99 in the other three arms. Two independent lines of
evidence clear the pruning change:

- **q38 and q54 in isolation, eight alternating arms:** 0.4 s to 1.3 s every
  time, no timeouts, no broken pipes, and `pruning_fired = 0`. The pruning code
  is not even active on those two queries.
- **`PruningPredicate::try_new` build cost is bounded**, so a per-file rebuild
  cannot stall a query: 251 us at 200 literals, 331 us at 3,200, 640 us at
  6,400, 2.4 ms at 25,600. Even 80 files in one fragment is about 200 ms. This
  matters because the InList cap checks per-node length, so a CASE-of-InLists
  (the shape behind the older q21 16s-to-24s regression) passes through with
  thousands of literals and no single oversized node. It is cheap anyway.

The actual cause was transport churn: the coordinator logged repeated
`do_get failed ... BrokenPipe` and both workers logged
`error trying to set TCP_NODELAY: Invalid argument (os error 22)` 101 times.
The workers were advertising the host's LAN address (192.168.1.229), so every
fragment crossed the LAN stack. Setting `advertise_url = "http://127.0.0.1:5005N"`
in the rig's worker configs removes that path, and the rig returned to the ~44 s
SSB SF10 regime afterwards.
