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
