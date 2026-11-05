**STATUS 2026-11-03: the TPCBB q01 pre-aggregation candidate is exact against the original, types included (`test/tpcbb-q01-preagg-785`, #785 criteria a and c).**

The prototype (691c2fdc4, d1bfc5521) compared a DOUBLE fixture within
`isclose`. It now covers criterion (a):

- `benchmarks/experiments/tpcbb-q01-preaggregated.sql` casts the measures to
  the original's types. Probed with sqe-cli: `SUM(DECIMAL(7,2))` is
  `Decimal128(17,2)` and `AVG` is `Decimal128(11,6)`, truncated (1.676666 for
  5.03/3). `BIGINT * DECIMAL(17,2)` is `Decimal128(38,2)` and the division is
  `Decimal128(38,6)`, also truncated. Without the casts the candidate returns
  wider types.
- `benchmarks/experiments/tpcbb-preaggregation.py` builds the fixture with
  `DECIMAL(7,2)` amounts that include NULLs and refunds (every fifth key is
  negative only). It also has an item whose amounts are all NULL, NULL item
  keys and keys with no item row on both facts, keys with clicks but no
  sales, sales past the click range, and pairs of `i_item_sk` that share an
  `i_item_id`. It compares every group with `==` (no tolerance) plus explicit
  `IS NULL` flags and `arrow_typeof` of the three measures, and fails if the
  fixture loses its NULL or negative groups.

Result (`uv run --no-project --with duckdb python3
benchmarks/experiments/tpcbb-preaggregation.py --sqe-cli target/release/sqe-cli`):
942 groups equal, 1 all-NULL group, 189 negative groups, types Int64 /
Decimal128(17,2) / Decimal128(11,6). Mutations caught: an average cast to
`DECIMAL(12,6)` (types differ), and `paid_count` in place of `sale_count`
(values differ).

Criterion (c), scope of a general rewrite (eager aggregation). It is valid
when:

- The join is inner and on equality of one key K, and the GROUP BY columns
  come from a side joined on K (here `item`), so every pre-aggregated K
  falls into exactly one group.
- Every aggregate decomposes with weights. `COUNT(x)` becomes the sum of
  `count_x * other_side_rows`, `SUM(x)` the sum of `sum_x * other_side_rows`,
  and `AVG(x)` the weighted `SUM / NULLIF(weighted COUNT(x), 0)`. MIN and MAX
  need no weight. `COUNT(DISTINCT)` and order-sensitive aggregates do not
  qualify.
- The measures are exact numerics. A float SUM is reassociated and is not
  bit-equal; a decimal SUM is. The weighted products widen to
  `DECIMAL(38,s)`, so the candidate cannot overflow before the original. The
  final cast to the original type fails exactly where the original sum would
  exceed its precision. No fixture reproduces that, because it needs about
  10^10 rows of maximal amounts.
- Outer joins need NULL-extended weights (a missing side counts one row of
  NULLs) and are out of scope.

Finding: `sqe-cli -f json` prints NULL as `""`, so it cannot tell NULL from an
empty string. `QueryResult.rows` is `Vec<Vec<String>>` for every backend
(`crates/sqe-cli/src/client.rs:3`, `display.rs:90`). The harness works around
it with the `IS NULL` flags.

Left open on #785: (b) join input/output rows, memory and runtime at
SF1/SF10/SF30 on data regenerated with the item-domain fix
(`fix/tpcbb-item-domain-785`). Join output rows are exact and cheap at any
scale through the candidate's CTEs: `SUM(click_count * sale_count)`. The
original at SF30 is projected, not run.

**NEXT:** the sqe-cli JSON NULL finding, then the shared SF10 pass for #785 (b)
and #787, then the embedded join-fallback move (#786) and the engine-bench
150M rerun and MR.
