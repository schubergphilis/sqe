**STATUS 2026-11-09: #785 criterion (b) at SF1 and SF10; the candidate is exact again after the decimal type changes (`feat/tpcbb-q01-preagg-785`).**

The decimal work of 11-04/11-05 (half-up rounding, then Trino's result types)
changed what q01 returns: `AVG(DECIMAL(7,2))` is `DECIMAL(7,2)` and `SUM` is
`DECIMAL(38,2)`. The candidate still cast to `DECIMAL(17,2)` and
`DECIMAL(11,6)`, so its top 20 differed (1642.608659 against 1642.61).

Fix: `benchmarks/experiments/tpcbb-q01-preaggregated.sql` returns the new
types. The average is not `CAST(sum / count AS DECIMAL(7,2))`: the division
rounds half-up at scale 6 and the cast rounds again. For 0.0049999975 that
gives 0.005000, then 0.01, where the original's AVG gives 0.00. It needs a
weighted count near 10^6 per group, which SF100 reaches (about 1.7M joined
rows per item). The candidate divides in whole cents: exact quotient
(`(cents - cents % n) / n`) plus one cent away from zero when
`2 * |rest| >= n`.

`benchmarks/experiments/tpcbb-preaggregation.py` gains that group (one click,
2,000,001 sales, 1,000,000 of them 0.01). Result: 943 groups equal, types
Int64 / Decimal128(38,2) / Decimal128(7,2). Mutation: the naive cast returns
0.01 for that group and the harness fails. sqe-cli JSON prints NULL as
`null` now, so last time's NULL finding is gone; the harness copes with both.

Criterion (b), `docs/evidence/perf/tpcbb-q01-preaggregation-2026-11-09.json`
(new `benchmarks/experiments/tpcbb-q01-scale.py`, M3 Pro, embedded over
local Parquet, three alternating repetitions, medians):

| scale | original join rows | original | candidate | peak RSS orig / cand | top 20 |
|---|---|---|---|---|---|
| SF1 | 384M | 7.5 s | 54 ms | 188 / 116 MB | equal |
| SF10 | 6.78B | 127 s | 538 ms | 885 / 310 MB | equal |

Both inputs collapse to one row per item (18,000 and 102,000). The join
output grows with clicks x sales per item, so SF100 (204,000 items, about
1,176 x 1,411 per item) is roughly 340B rows for the original. Not run.

Left open on #785: SF30/SF100 on the SF100 box (`--candidate-only` there,
or `--timeout` for the original), and whether to build a general eager
aggregation rule. The applicability conditions are in
`2026-11-03-test-tpcbb-q01-preagg-785.md`; the average needs this cents
form, not a plain division.

**NEXT:** #788 write path per-query quota (`sqe-write` has none), then SF30/SF100
for #785 when the box is ready.
