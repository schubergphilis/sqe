# Array, map, struct

DataFusion's `datafusion-functions-nested` crate ships ~40 array and map helpers. SQE adds Trino-named aggregate constructors (`map_agg`, `histogram`, `multimap_agg`).

## Array construction

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `[1, 2, 3]` (literal) | `datafusion-builtin` | Element type from common supertype. | yes | yes | yes | yes |
| `make_array(a, b, ...)` | `datafusion-builtin` | Function form of literal. | - | yes | yes | yes |
| `array(...)` | `datafusion-builtin` | Alias for `make_array`. | yes | yes | yes | yes |
| `range(start, stop)` | `datafusion-builtin` | Half-open integer array. | - | - | - | yes |
| `range(start, stop, step)` | `datafusion-builtin` | With step. | - | - | - | yes |
| `array_repeat(elem, n)` | `datafusion-builtin` | Array of n copies. | yes | - | - | yes |

## Array inspection

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `array_length(a)` / `cardinality(a)` | `datafusion-builtin` | Number of elements. | yes | yes | yes | yes |
| `array_dims(a)` | `datafusion-builtin` | Array of per-dimension sizes (for nested arrays). | - | - | - | yes |
| `array_ndims(a)` | `datafusion-builtin` | Nesting depth. | - | - | - | yes |
| `array_position(a, elem)` | `datafusion-builtin` | 1-based offset of first match; 0 if missing. | yes | yes | yes | yes |
| `array_positions(a, elem)` | `datafusion-builtin` | Array of all matching offsets. | - | - | - | yes |
| `array_contains(a, elem)` / `array_has(a, elem)` | `datafusion-builtin` | Boolean membership. | yes | yes | yes | yes |
| `array_has_all(a, sub)` | `datafusion-builtin` | All of `sub` are in `a`. | - | - | - | yes |
| `array_has_any(a, sub)` | `datafusion-builtin` | Any of `sub` is in `a`. | yes | - | - | yes |

## Array transformation

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `array_append(a, elem)` | `datafusion-builtin` | Add to end. | yes | yes | yes | yes |
| `array_prepend(elem, a)` | `datafusion-builtin` | Add to start. | yes | - | yes | yes |
| `array_concat(a1, a2, ...)` | `datafusion-builtin` | Variadic concat. | yes | yes | yes | yes |
| `array_remove(a, elem)` | `datafusion-builtin` | Remove first occurrence. | - | - | - | yes |
| `array_remove_all(a, elem)` | `datafusion-builtin` | Remove all occurrences. | yes | - | - | yes |
| `array_replace(a, from, to)` | `datafusion-builtin` | Replace first match. | yes | - | yes | yes |
| `array_replace_all(a, from, to)` | `datafusion-builtin` | Replace all matches. | - | - | - | yes |
| `array_reverse(a)` | `datafusion-builtin` | Reverse order. | yes | yes | yes | yes |
| `array_sort(a)` | `datafusion-builtin` | Ascending sort. NULLs last. | yes | yes | yes | yes |
| `array_distinct(a)` | `datafusion-builtin` | Deduplicate. Preserves first occurrence. | yes | yes | yes | yes |
| `array_slice(a, start, end)` | `datafusion-builtin` | 1-based, inclusive. Negative indexes count from end. | yes | yes | yes | yes |
| `array_pop_front(a)` / `array_pop_back(a)` | `datafusion-builtin` | Remove first / last. | yes | - | - | yes |
| `array_resize(a, n [, fill])` | `datafusion-builtin` | Truncate or pad to length n. | - | - | - | yes |
| `array_flatten(a)` / `flatten(a)` | `datafusion-builtin` | One level of flattening. | yes | yes | yes | yes |

## Array set operations

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `array_intersect(a, b)` | `datafusion-builtin` | Common elements (set-style). | yes | yes | yes | yes |
| `array_union(a, b)` | `datafusion-builtin` | Distinct combination. | yes | yes | yes | yes |
| `array_except(a, b)` | `datafusion-builtin` | In `a` but not in `b`. | yes | - | yes | yes |

## Array reductions

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `array_min(a)` | `datafusion-builtin` | Minimum element. | yes | yes | yes | yes |
| `array_max(a)` | `datafusion-builtin` | Maximum. | yes | yes | yes | yes |
| `array_sum(a)` | `datafusion-builtin` | Sum of numeric elements. | yes | - | - | yes |
| `array_mean(a)` | `datafusion-builtin` | Average. | - | - | - | - |
| `array_any_value(a)` | `datafusion-builtin` | First non-NULL element. | - | - | - | - |

## Array unnesting (lateral)

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `unnest(a)` | `datafusion-builtin` | One row per element. Used in FROM. | yes | yes | yes (`explode`) | yes |
| `unnest(a) WITH ORDINALITY` | `datafusion-builtin` | Adds 1-based offset column. | yes | - | - | - |

```sql
-- One row per (order, item) pair
SELECT order_id, item
FROM orders, UNNEST(items) AS t(item);

-- Numbered
SELECT order_id, item, idx
FROM orders, UNNEST(items) WITH ORDINALITY AS t(item, idx);
```

## Map functions

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `map(keys_array, values_array)` | `datafusion-builtin` | Build a map from two parallel arrays. | yes | - | yes (`map_from_arrays`) | yes |
| `map_keys(m)` | `datafusion-builtin` | Array of keys. | yes | yes | yes | yes |
| `map_values(m)` | `datafusion-builtin` | Array of values. | yes | yes | yes | yes |
| `map_extract(m, key)` | `datafusion-builtin` | Lookup. NULL if missing. Also accessible via `m[key]`. | yes (`element_at`) | yes (`get`) | yes (`element_at`) | yes (`element_at`) |
| `cardinality(m)` | `datafusion-builtin` | Number of keys. | yes | yes | yes | yes |
| `m['key']` | `datafusion-builtin` | Subscript syntax for map lookup. | yes | yes | yes | yes |

## Aggregates that build maps / arrays

See [Aggregate functions](./aggregate.md) for `array_agg`, `map_agg`, `histogram`, `multimap_agg`, `map_union`. The names differ slightly across engines:

| SQE | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|
| `array_agg(x)` | `array_agg` | `array_agg` | `collect_list` | `array_agg` / `list` |
| `map_agg(k, v)` | `map_agg` | `object_agg` | `map_from_arrays` | `map` |
| `histogram(x)` | `histogram` | - | - | `histogram` |
| `multimap_agg(k, v)` | `multimap_agg` | - | - | - |

## Struct / row

| Construct | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `struct(a, b, ...)` | `datafusion-builtin` | Anonymous record. | - | yes (`object_construct`) | yes | yes |
| `named_struct('a', x, 'b', y)` | `datafusion-builtin` | Named-field record. | yes (`row(...)`) | yes (`object_construct`) | yes | yes |
| `s.field` | `datafusion-builtin` | Field access. | yes | yes | yes | yes |
| `(a, b, ...)` (row literal) | `datafusion-builtin` | Anonymous tuple. | yes | - | yes | yes |

```sql
SELECT named_struct('host', host, 'port', port) AS endpoint
FROM servers;

SELECT endpoint.host, endpoint.port FROM ...;
```

## Examples

### Tag-set membership

```sql
-- Find products with both 'sale' and 'new' tags
SELECT * FROM products
WHERE array_has_all(tags, ARRAY['sale', 'new']);

-- Find products with any of the listed tags
SELECT * FROM products
WHERE array_has_any(tags, ARRAY['sale', 'clearance']);
```

### Top-K frequencies via histogram

```sql
SELECT k, v
FROM events, UNNEST(map_keys(histogram(event_type)), map_values(histogram(event_type))) AS t(k, v)
ORDER BY v DESC
LIMIT 10;
```

### Build a map from joined tables

```sql
SELECT
    user_id,
    map_agg(setting_key, setting_value) AS preferences
FROM user_settings
GROUP BY user_id;
```

`map_agg` errors on duplicate keys. For multimap-style behaviour use `multimap_agg`.

### Lateral pattern: filter then unnest

```sql
SELECT order_id, tag
FROM orders, UNNEST(tags) AS t(tag)
WHERE order_id > 100 AND tag LIKE 'priority_%';
```

## Lambda functions

DataFusion's parser does not support lambda syntax (`x -> x + 1`). Trino, Spark, DuckDB do. The audit rows in the [feature comparison](https://getsqe.com/compare/features) note this. Workarounds:

- Pre-compute via a CTE plus `unnest`.
- Use `map_filter` / `transform` from `datafusion-functions-nested` once parser support lands upstream.

## What is NOT registered

- **`zip(a, b)`** (parallel-iterate two arrays). Use `unnest` against an indexed pair instead.
- **`reduce(a, init, lambda, finish)`**. Aggregate within a CTE instead.
- **Snowflake `flatten` table function** (with PATH and OUTER options). Use `UNNEST` directly.

These are tracked but blocked on lambda support in DataFusion.
