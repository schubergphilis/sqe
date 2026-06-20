# Window functions

Window functions compute a value per row using a "window" of related rows. Unlike aggregates, they do not collapse rows; the input row count is preserved.

All window functions in SQE come from `datafusion-functions-window` (DataFusion's built-in window crate). No SQE-specific window functions exist; the SQL surface matches DataFusion exactly.

## Syntax

```sql
window_function(args) OVER (
    [PARTITION BY col1, col2, ...]
    [ORDER BY col1 [ASC|DESC] [NULLS FIRST|LAST], ...]
    [frame_clause]
)
```

The frame clause has three forms:

```text
ROWS BETWEEN <start> AND <end>
RANGE BETWEEN <start> AND <end>
GROUPS BETWEEN <start> AND <end>
```

Bounds:

```text
UNBOUNDED PRECEDING
N PRECEDING
CURRENT ROW
N FOLLOWING
UNBOUNDED FOLLOWING
```

Default frame:

- With `ORDER BY`: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
- Without `ORDER BY`: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

## Functions

### Ranking

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `row_number()` | `datafusion-builtin` | 1-based unique rank. | `row_number` | `row_number` | `row_number` | `row_number` |
| `rank()` | `datafusion-builtin` | Standard rank with gaps after ties. | `rank` | `rank` | `rank` | `rank` |
| `dense_rank()` | `datafusion-builtin` | Rank with no gaps. | `dense_rank` | `dense_rank` | `dense_rank` | `dense_rank` |
| `percent_rank()` | `datafusion-builtin` | `(rank - 1) / (rows - 1)` in `[0, 1]`. | `percent_rank` | `percent_rank` | `percent_rank` | `percent_rank` |
| `cume_dist()` | `datafusion-builtin` | Cumulative distribution: rows <= current / total. | `cume_dist` | `cume_dist` | `cume_dist` | `cume_dist` |
| `ntile(n)` | `datafusion-builtin` | Bucket rows into N equal-size groups. | `ntile` | `ntile` | `ntile` | `ntile` |

### Offset

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `lag(expr [, offset [, default]])` | `datafusion-builtin` | Value `offset` rows back. Default offset 1, default value NULL. | `lag` | `lag` | `lag` | `lag` |
| `lead(expr [, offset [, default]])` | `datafusion-builtin` | Value `offset` rows forward. | `lead` | `lead` | `lead` | `lead` |
| `first_value(expr)` | `datafusion-builtin` | First row's value within frame. | `first_value` | `first_value` | `first_value` | `first_value` |
| `last_value(expr)` | `datafusion-builtin` | Last row's value within frame. | `last_value` | `last_value` | `last_value` | `last_value` |
| `nth_value(expr, n)` | `datafusion-builtin` | Nth row's value within frame. | `nth_value` | `nth_value` | `nth_value` | `nth_value` |

### Aggregates as windows

Every aggregate function from [Aggregate functions](./aggregate.md) also works as a window function:

```sql
SELECT
    customer_id,
    order_date,
    amount,
    sum(amount) OVER (PARTITION BY customer_id ORDER BY order_date) AS running_total,
    avg(amount) OVER (PARTITION BY customer_id) AS customer_avg
FROM orders;
```

## Frame examples

### Running total

```sql
SELECT
    order_date,
    amount,
    sum(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM orders;
```

### Trailing 7-day average

```sql
SELECT
    order_date,
    amount,
    avg(amount) OVER (
        ORDER BY order_date
        RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
    ) AS trailing_7d_avg
FROM orders;
```

`RANGE` with an `INTERVAL` works on date / timestamp ordering keys and respects time gaps. `ROWS` would just count rows regardless of time.

### Top N per group via row_number

```sql
WITH ranked AS (
    SELECT
        category,
        product,
        revenue,
        row_number() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM products
)
SELECT * FROM ranked WHERE rn <= 5;
```

### Rolling difference with LAG

```sql
SELECT
    order_date,
    amount,
    amount - lag(amount, 1, 0) OVER (ORDER BY order_date) AS day_over_day
FROM orders;
```

The `, 0` argument fills the first row (where there is no predecessor) with zero instead of NULL.

## Frame variants compared

| Form | What "between -1 and +1" means |
|---|---|
| `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` | Three rows by position: previous, current, next. |
| `RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING` | Rows within `[order_key - 1, order_key + 1]` of the current order key value. |
| `GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING` | Three peer groups: rows tied with the current key, plus the previous and next tied groups. |

`GROUPS` is rare but useful when ordering on a low-cardinality key produces many ties and you want "the previous distinct value group" semantics.

## What is NOT supported (DataFusion blocked)

- `QUALIFY` clause (filtering on window-function output without a subquery). DataFusion's parser does not accept `QUALIFY`. Workaround: wrap the SELECT and filter in an outer query, as in the "Top N per group" example above.

The audit row lives in [`features.md`](../../../features.md). Tracked upstream as a parser enhancement.

## Performance notes

- `PARTITION BY` enables parallelism: each partition runs on its own thread / worker. Without partitioning, the window runs single-threaded against the global ordering.
- `ROWS` frames are cheaper than `RANGE` frames when the ordering key has many ties; `RANGE` may need a binary search per row.
- A `unbounded preceding ... unbounded following` frame on a sorted input lets DataFusion stream-compute aggregates without materialising the partition. Other frames require partition-buffering.

The `EXPLAIN ANALYZE` output shows partition counts and frame mode per WindowAgg node; use it when a window query is slower than expected.
