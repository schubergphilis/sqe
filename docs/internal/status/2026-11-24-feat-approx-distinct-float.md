# approx_distinct over double and real

**Branch:** `feat/approx-distinct-float`

`approx_distinct(double)` and `approx_distinct(real)` now run instead of failing with "not implemented". DataFusion rejects floats on purpose (upstream issue 26985 and the open PR 27064 make the rejection consistent for nested floats), so no DataFusion upgrade adds this.

`sqe-trino-functions/src/approx_distinct.rs` wraps the context's `approx_distinct`. A float input is widened to `double`, NaN and `-0.0` are made canonical, and the bits go to the wrapped aggregate as `bigint`. Other types pass through unchanged. `Float16` is not taken (Trino has no such type).

Tests: `tests/it/approx_distinct_float_test.rs`, seen failing first with the "not implemented" error. They cover double and real, with and without `GROUP BY`; rows split over four partitions so NaN payloads and both zeros meet only in the final merge; and a skipped partial aggregation (`skipped_aggregation_rows=4` asserted), where `-0.0` is aggregated and `0.0` and a second NaN reach the merge through `convert_to_state`. Dropping the float mapping in `convert_to_state` makes that test fail (4 against 3).

No `approx_count_distinct` alias exists in SQE or DataFusion; Spark, Databricks, Snowflake and DuckDB use that name.

**NEXT:** clients that cast float columns to VARCHAR before `approx_distinct` can drop the cast once this ships; decide separately whether to add `approx_count_distinct` as an alias for Spark and Databricks SQL.
