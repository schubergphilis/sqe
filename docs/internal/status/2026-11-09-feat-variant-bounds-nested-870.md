# 2026-11-09 feat/variant-bounds-nested-870

Part of #870: variant manifest bounds for shredded double paths, written and
read.

## What changed

- Vendor `variant_shred.rs` (`SQE PATCH (v3-shredded-variant)`): `Scalar`
  gains `Double(TotalF64)`, ordered by `f64::total_cmp` as DataFusion
  compares floats.
  - A NaN at a path drops its bound, through the same `None` slot a value
    of another type uses. Iceberg Java does the same
    (`ParquetMetrics$MetricsVariantVisitor` returns null on `NaNUtil.isNaN`;
    a Spark probe showed no bound for a file holding a NaN).
  - A zero lower bound is written -0.0, a zero upper bound +0.0.
- Reader `sqe-catalog/src/variant_prune.rs`: `Target::Float64` (`'double'`,
  `'float64'`) and `Bound::Double(TotalF64)`.
  - Manifest bounds only. `bounds()` has no Float64 arm, so row-group
    statistics never prune a double field (Parquet leaves NaN out of them).
  - `widen_zeros` turns a zero min into -0.0 and a zero max into +0.0:
    Iceberg Java 1.12 wrote +0.0 as the lower bound of a file holding -0.0
    (probe hex kept as `SPARK_DOUBLE_LOWER`).
  - A NaN literal, a NaN bound, a `float` target and an integer or decimal
    bound read as a double are refused.
- Nested variants: Iceberg Java writes bounds only for top-level variant
  columns (probe: a table with `s STRUCT<v VARIANT>`, `ARRAY<VARIANT>`,
  `MAP<STRING, VARIANT>` and a top-level `v` got bounds for `id` and `v`
  only). SQE matches it, so no nested bounds and no `get_field` resolution.
- Docs: `limitations.md` and `ddl.md` no longer say only top-level variants
  are shredded (stale since nested shredding); `ddl.md` gets a "Variant
  bounds" item. Matrix caveat and notes updated, score unchanged (158/162).

## Tests

- Vendor `cargo test -p iceberg --lib variant_shred` -> 9 passed;
  `doubles_bound_with_zeros_widened_and_nan_dropping_the_path` (NaN first,
  later, negative NaN, across batches; ±0 at either end) and
  `quoted_keys_get_no_bound` replace `doubles_and_quoted_keys_get_no_bound`.
- `cargo test -p sqe-catalog --lib variant_prune` -> 15 passed, new
  `manifest_bounds_read_doubles` (Spark's real bound bytes) and
  `double_filters_never_prune_a_row_group`.
- Integration `sqe_prunes_on_double_variant_bounds`: SQE-written and
  Spark-written tables, with -0.0 and a NaN file. `-0.0 < 0` is false in
  SQE and Spark alike, so `< 0` finds no row but keeps the zero file;
  `= CAST('-0.0' AS DOUBLE)` against Spark's +0.0 lower bound proves the
  widening. `make test-integration FILTER=variant_spark_interop` -> 9 passed.

## Found, not fixed here

- `double_col > 9.0` compares in `Decimal128(30, 15)`: `9.0` is a decimal
  under `parse_float_as_decimal`, and DataFusion casts the double side to
  decimal. A NaN fails the query (`Cannot cast to Decimal128(30, 15).
  Overflowing on NaN`), and so would a value above 1e15. Trino casts the
  decimal to double instead. Such a filter does not prune either.
- Under `EXPLAIN FULL` an exponent literal (`25e-1`) still compares in
  decimal and the same NaN error fires, while the plain SELECT rewrites
  it to a double. The rewrite does not reach the `EXPLAIN FULL` path
  (`StatementKind::ExplainFull` -> `Explainer::full` -> `plan_sql`); the
  exact cause is not traced. The test uses `CAST(2.5 AS DOUBLE)` instead.

## Left open (#870)

- Typed read of shredded variants without unshredding.

**NEXT:** file the decimal-vs-double comparison issue, then typed read of
shredded variants (#870).
