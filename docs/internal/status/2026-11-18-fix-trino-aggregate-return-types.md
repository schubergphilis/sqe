**STATUS 2026-11-18: Trino return types for real statistics and decimal round (`fix/trino-aggregate-return-types`, openspec trino-sql-parity 3.3, 3.4 in part).**

`corr`, `covar_pop`, `covar_samp`, `regr_slope` and `regr_intercept` wrap DataFusion's aggregates (`sqe-trino-functions/src/real_aggregates.rs`): `(real, real)` returns `real`, everything else `double`. Trino has no other `regr_*`, so those are untouched. Nothing re-aggregates these by name (`is_reaggregatable` and DataFusion's single-distinct rule admit only sum/min/max/count). `round` over `decimal` takes Trino's result types (`decimal_math.rs`). Decimal `truncate` types come with the math-functions change (MR !1836). `sum`, `avg` and `abs` over decimal were already right and are pinned in `sqe-catalog` (`session_tuning/trino_decimal_types.rs`).

Not done: `avg(real)` still returns `double` (Trino: `real`); other `real` aggregates in task 3.4 are unchecked. `round(decimal(p,0))` returns `decimal(p,0)` per Trino's constraint expression, to be confirmed by the parity run.

**NEXT:** run the parity harness against Trino 484 for these types; then `avg(real)` and the remaining `real` aggregates.
