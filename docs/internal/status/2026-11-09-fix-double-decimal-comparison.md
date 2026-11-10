# 2026-11-09 fix/double-decimal-comparison

Fixes the decimal-vs-double coercion found during #870 (stacked on
`feat/variant-bounds-nested-870`, !1697).

## What changed

- New analyzer rule `FloatOverDecimalRule`
  (`sqe-catalog/src/session_tuning/float_decimal_comparison.rs`), inserted
  before `type_coercion`, after `DecimalArithmeticRule`. DataFusion's
  `comparison_coercion` and `type_union_coercion` both pick a decimal for
  float vs decimal (Float64 -> `Decimal128(30,15)`, Float32 ->
  `Decimal128(14,7)`); Trino picks the double. The rule casts the decimal
  operands to the widest float among them in:
  - `=, <>, <, <=, >, >=, IS [NOT] DISTINCT FROM`, `BETWEEN`, `IN (..)`,
    `IN (SELECT ..)` (the subquery column through `cast_subquery`), the
    `CASE x WHEN ..` operand;
  - `CASE` results and `coalesce`, `nvl`, `ifnull`, `nullif`, `greatest`,
    `least`, `make_array` (also `[a, b]`), `nvl2` (not its first argument);
  - `UNION` columns: a decimal input is read through a projection that casts
    the column, then `Union::try_new_with_loose_types`;
  - `VALUES` columns. The planner (`LogicalPlanBuilder::values` ->
    `type_union_resolution`) fixes the column type and casts each row
    before any analyzer rule runs. The rule strips a row's cast to the
    column type, replays that inference on the rows as written, and
    retypes the column to the float only when the replay lands on the
    column type again, so an `INSERT` target type is never touched. The
    planner's cast and a user's `CAST` are the same expression: a row cast
    by hand to exactly the inferred decimal (`DECIMAL(30,15)` for a double)
    is read as the double it came from.
  Decimal vs decimal and decimal vs integer are untouched. Arithmetic
  already gives Float64 in DataFusion.
- `decimal_arithmetic.rs`: `type_plan` generalised to
  `rewrite_plan_exprs(plan, node)`, and `coerced_type` made `pub(super)`, so
  both rules share the per-node typing walk.
- 870 IT: `> 9.0` now prunes (files 1/3, was 3/3); new `> 5.0` on the
  Spark table with the NaN file (failed before, now `1|7 files 1/3`).
- Docs: `limitations.md`, `ddl.md` and the matrix caveat drop the "`9.0`
  is a decimal, write `9`" advice.

## Tests

- `cargo test -p sqe-catalog --lib` -> 798 passed; new
  `values_keep_decimals_that_were_not_inferred` (decimal-only rows, a cast
  to another decimal, `INSERT` into a decimal column),
  `doubles_compare_with_decimals_in_floating_point`,
  `doubles_meet_decimals_as_doubles` (checks the executed type is Float64;
  `df.schema()` is the pre-coercion type) and
  `the_float_column_stays_uncast`.

- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --lib` -> 805
  passed. Without the larger stack
  `flight_sql::tests::do_get_ddl_kind_yields_schema_then_completes`
  overflows its stack with this rule disabled too, so the overflow was
  already there.

**NEXT:** typed read of shredded variants (#870).
