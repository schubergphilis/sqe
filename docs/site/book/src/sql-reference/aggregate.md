# Aggregate functions

Functions used in `GROUP BY` queries and `OVER` clauses. SQE inherits ~40 aggregates from DataFusion plus 12 Trino UDAFs from `sqe-trino-functions`.

## Standard aggregates

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `count(expr)` | `datafusion-builtin` | Counts non-NULL rows. | `count` | `count` | `count` | `count` |
| `count(*)` | `datafusion-builtin` | Counts all rows. | `count` | `count` | `count` | `count` |
| `count(distinct expr)` | `datafusion-builtin` | Distinct non-NULL count. | `count(distinct ...)` | `count(distinct ...)` | `count(distinct ...)` | `count(distinct ...)` |
| `sum(expr)` | `datafusion-builtin` | Sum. NULL-skipping. | `sum` | `sum` | `sum` | `sum` |
| `sum(distinct expr)` | `datafusion-builtin` | Distinct sum. | `sum(distinct ...)` | `sum(distinct ...)` | `sum(distinct ...)` | `sum(distinct ...)` |
| `avg(expr)` / `mean(expr)` | `datafusion-builtin` | Arithmetic mean. NULL-skipping. | `avg` | `avg` | `avg` / `mean` | `avg` / `mean` |
| `min(expr)` | `datafusion-builtin` | Minimum. | `min` | `min` | `min` | `min` |
| `max(expr)` | `datafusion-builtin` | Maximum. | `max` | `max` | `max` | `max` |
| `median(expr)` | `datafusion-builtin` | Exact median. Slower than `approx_median` on big inputs. | - | `median` | `median` | `median` |

## Statistical / regression

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `variance(x)` / `var_samp(x)` | `datafusion-builtin` | Sample variance. | `variance` / `var_samp` | `variance_samp` | `variance` / `var_samp` | `variance` / `var_samp` |
| `var_pop(x)` | `datafusion-builtin` | Population variance. | `var_pop` | `variance_pop` | `var_pop` | `var_pop` |
| `stddev(x)` / `stddev_samp(x)` | `datafusion-builtin` | Sample stddev. | `stddev` / `stddev_samp` | `stddev_samp` | `stddev` / `stddev_samp` | `stddev` |
| `stddev_pop(x)` | `datafusion-builtin` | Population stddev. | `stddev_pop` | `stddev_pop` | `stddev_pop` | `stddev_pop` |
| `corr(y, x)` | `datafusion-builtin` | Pearson correlation. | `corr` | `corr` | `corr` | `corr` |
| `covar_samp(y, x)` / `covar_pop(y, x)` | `datafusion-builtin` | Sample / population covariance. | `covar_samp` / `covar_pop` | `covar_samp` / `covar_pop` | `covar_samp` / `covar_pop` | `covar_samp` / `covar_pop` |
| `regr_slope(y, x)` | `datafusion-builtin` | Linear regression slope. | `regr_slope` | `regr_slope` | - | `regr_slope` |
| `regr_intercept(y, x)` | `datafusion-builtin` | y-intercept. | `regr_intercept` | `regr_intercept` | - | `regr_intercept` |
| `regr_r2(y, x)` | `datafusion-builtin` | R-squared. | `regr_r2` | `regr_r2` | - | `regr_r2` |
| `regr_count`, `regr_sxx`, `regr_syy`, `regr_sxy`, `regr_avgx`, `regr_avgy` | `datafusion-builtin` | Regression sums and counts. | yes | yes | - | yes |

## Distinct and approximation

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `approx_distinct(expr)` | `datafusion-builtin` | HyperLogLog distinct count. ~1% error. | `approx_distinct` | `approx_count_distinct` | `approx_count_distinct` | `approx_count_distinct` |
| `approx_median(expr)` | `datafusion-builtin` | Median estimate via t-digest. | - | - | `approx_percentile(0.5)` | `approx_quantile(0.5)` |
| `approx_percentile_cont(expr, p)` | `datafusion-builtin` | Percentile estimate via t-digest. `p` in `[0, 1]`. | `approx_percentile` | `approx_percentile` | `approx_percentile` | `approx_quantile` |
| `approx_percentile(expr, p)` | `sqe-trino-functions` | Trino-named alias of `approx_percentile_cont`. `trino_functions.rs:164` | `approx_percentile` | - | - | - |

## Boolean and bitwise

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `bool_and(x)` | `datafusion-builtin` | True only if every row is true. | `bool_and` | `booland_agg` | `bool_and` / `every` | `bool_and` |
| `bool_or(x)` | `datafusion-builtin` | True if any row is true. | `bool_or` | `boolor_agg` | `bool_or` / `any` | `bool_or` |
| `every(x)` | `sqe-trino-functions` | Trino-named alias of `bool_and`. `trino_functions.rs:170` | `every` | - | `every` | - |
| `bit_and(x)` | `datafusion-builtin` | Bitwise AND of all values. | `bitwise_and_agg` | `bitand_agg` | `bit_and` | - |
| `bit_or(x)` | `datafusion-builtin` | Bitwise OR. | `bitwise_or_agg` | `bitor_agg` | `bit_or` | - |
| `bit_xor(x)` | `datafusion-builtin` | Bitwise XOR. | `bitwise_xor_agg` | `bitxor_agg` | `bit_xor` | - |
| `bitwise_and_agg(x)` | `sqe-trino-functions` | Trino name for `bit_and`. `trino_functions.rs:144` | `bitwise_and_agg` | - | - | - |
| `bitwise_or_agg(x)` | `sqe-trino-functions` | Trino name for `bit_or`. `trino_functions.rs:150` | `bitwise_or_agg` | - | - | - |
| `bitwise_xor_agg(x)` | `sqe-trino-functions` | Trino name for `bit_xor`. `trino_functions.rs:158` | `bitwise_xor_agg` | - | - | - |

## Positional

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `first_value(expr [order by ...])` | `datafusion-builtin` | First row's value. Most useful with `OVER` ordering. | `first_value` | `first_value` | `first_value` | `first_value` |
| `last_value(expr [order by ...])` | `datafusion-builtin` | Last row's value. | `last_value` | `last_value` | `last_value` | `last_value` |
| `nth_value(expr, n [order by ...])` | `datafusion-builtin` | Nth row's value. | `nth_value` | `nth_value` | `nth_value` | `nth_value` |
| `max_by(value, key)` | `sqe-trino-functions` | `value` from the row with the max `key`. `trino_functions.rs:177` | `max_by` | `max_by` | - | `arg_max` |
| `min_by(value, key)` | `sqe-trino-functions` | `value` from the row with the min `key`. `trino_functions.rs:178` | `min_by` | `min_by` | - | `arg_min` |
| `arbitrary(expr)` | `sqe-trino-functions (ext)` | Any one non-NULL value. Trino-named alias of `any_value`. `trino_functions_ext.rs:68` | `arbitrary` | `any_value` | `any_value` | `any_value` |

## Collection-building

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `array_agg(expr)` | `datafusion-builtin` | Collect into an array. NULLs included. | `array_agg` | `array_agg` | `collect_list` | `array_agg` / `list` |
| `array_agg(distinct expr)` | `datafusion-builtin` | Distinct array. | `array_agg(distinct)` | `array_agg(distinct)` | `collect_set` | `list_distinct` |
| `string_agg(expr, sep)` | `datafusion-builtin` | Concatenate with separator. SQL standard. | `array_join`/`listagg` | `listagg` | - | `string_agg` |
| `listagg(expr, sep)` | `sqe-trino-functions` | Same as `string_agg`; Snowflake / Trino name. `trino_functions.rs:138` | `listagg` | `listagg` | - | - |
| `histogram(expr)` | `sqe-trino-functions` | Map of value -> count. `trino_functions.rs:188` | `histogram` | - | - | `histogram` |
| `map_agg(key, value)` | `sqe-trino-functions` | Build a map by aggregating key-value pairs. Last write wins. `trino_functions.rs:189` | `map_agg` | `object_agg` | `map_from_arrays` | `map` |
| `multimap_agg(key, value)` | `sqe-trino-functions` | Map where each value is an array (collects duplicates). `trino_functions.rs:190` | `multimap_agg` | - | - | - |
| `map_union(map_col)` | `sqe-trino-functions` | Aggregate already-built maps into one. `trino_functions.rs:191` | `map_union` | - | - | - |

## Modifiers

| Modifier | Notes |
|---|---|
| `agg_func(expr) FILTER (WHERE pred)` | Filter rows before aggregation. Cleaner than `agg_func(CASE WHEN pred THEN expr END)`. |
| `agg_func(distinct expr)` | Distinct values only. |
| `agg_func(expr) OVER (...)` | Window form. Uses `PARTITION BY`, `ORDER BY`, frame clauses. See [Window functions](./window.md). |
| `agg_func(expr) WITHIN GROUP (ORDER BY ...)` | Ordered aggregate (e.g. `listagg`). |

Example using `FILTER`:

```sql
SELECT
    region,
    count(*) AS total_orders,
    count(*) FILTER (WHERE status = 'cancelled') AS cancelled,
    sum(amount) FILTER (WHERE status = 'shipped') AS shipped_revenue
FROM orders
GROUP BY region;
```

## GROUP BY extensions

| Construct | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `GROUP BY GROUPING SETS ((a, b), (a), ())` | `datafusion-builtin` | Multiple grouping levels in one query. | yes | yes | yes | yes |
| `GROUP BY CUBE (a, b, c)` | `datafusion-builtin` | All 2^N grouping combinations. | yes | yes | yes | yes |
| `GROUP BY ROLLUP (a, b, c)` | `datafusion-builtin` | Hierarchical: `()`, `(a)`, `(a, b)`, `(a, b, c)`. | yes | yes | yes | yes |
| `GROUPING(col)` | `datafusion-builtin` | Returns 1 if `col` was rolled up in this row, else 0. | yes | yes | yes | yes |

```sql
SELECT
    region,
    product,
    sum(amount) AS revenue,
    GROUPING(region) AS region_rolled_up,
    GROUPING(product) AS product_rolled_up
FROM orders
GROUP BY ROLLUP (region, product)
ORDER BY region, product;
```

## Approximation vs exact: when to choose

- **`count(distinct)` exact**. sub-second on millions of rows; avoid above ~1B distinct values.
- **`approx_distinct` HyperLogLog**. order of magnitude faster on huge inputs. ~1% relative error.
- **`median` exact**. sorts the entire group; expensive on big partitions.
- **`approx_median` / `approx_percentile_cont` t-digest**. sub-percent error, much cheaper memory profile.

For dashboards over multi-billion-row tables, default to approximations. For audit queries that need exact counts, default to exact.
