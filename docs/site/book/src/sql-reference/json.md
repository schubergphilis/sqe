# JSON

Two layered JSON surfaces. Both are registered in every session.

1. **Trino-named layer** in `sqe-trino-functions`: `json_extract`, `json_extract_scalar`, `json_array_length`, `json_parse`, `json_format`, `json_object`, `is_json_scalar`, `json_array_contains`, `json_size`, `json_array_get`, `to_json`. Maps directly to dbt-trino model expectations.
2. **DataFusion JSON layer** via `datafusion-functions-json` (registered in `session_context.rs:361` and `embedded.rs:172`): `json_get`, `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`, `json_get_json`, `json_get_array`, `json_contains`, `json_as_text`, `json_length`. Type-specific extractors that avoid an outer CAST.

JSON columns are stored as `VARCHAR` (Iceberg has no native JSON type yet). Both layers parse on every call, so for hot paths consider extracting once into a typed column.

## Trino-named layer

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `json_parse(s)` | `sqe-trino-functions` | Parse JSON text. Returns the same `VARCHAR` after validation; primarily used to fail fast on malformed input. `trino_functions.rs:92` | yes | `parse_json` | `from_json` | `json` |
| `json_format(j)` | `sqe-trino-functions` | Format / re-emit JSON. `trino_functions.rs:60` | yes | - | `to_json` | `to_json` |
| `json_object(k1, v1, k2, v2, ...)` | `sqe-trino-functions` | Build a JSON object from key-value pairs. `trino_functions.rs:59` | yes | yes | yes | yes |
| `json_array_length(j)` | `sqe-trino-functions` | Number of elements in a JSON array. NULL if not an array. `trino_functions.rs:91` | yes | yes | yes | `json_array_length` |
| `json_extract(j, '$.path')` | `sqe-trino-functions` | JSONPath extraction. Returns JSON-encoded value. `trino_functions.rs:89` | yes | yes | yes | yes |
| `json_extract_scalar(j, '$.path')` | `sqe-trino-functions` | Same path syntax; returns plain text scalar (or NULL). `trino_functions.rs:90` | yes | - | `get_json_object` | `json_extract_string` |
| `is_json_scalar(j)` | `sqe-trino-functions (ext)` | True for JSON null / boolean / number / string. False for objects and arrays. `trino_functions_ext.rs:30` | yes | - | - | - |
| `json_array_contains(j, v)` | `sqe-trino-functions (ext)` | True if JSON array contains the value. `trino_functions_ext.rs:31` | yes | - | - | - |
| `json_size(j, '$.path')` | `sqe-trino-functions (ext)` | Cardinality at the path. Object key count for objects; array length for arrays; `0` for primitives. `trino_functions_ext.rs:72` | yes | - | - | - |
| `json_array_get(j, idx)` | `sqe-trino-functions (ext)` | 0-based element from a JSON array. `trino_functions_ext.rs:73` | yes | - | - | - |
| `to_json(any)` | `sqe-trino-functions (ext)` | Serialize a SQL value (struct, array, map) as JSON. `trino_functions_ext.rs:80` | - | yes | yes | yes |

## DataFusion JSON layer

These return typed scalars directly, which means no outer `CAST` and DataFusion can push predicates through.

| Function | Origin | Notes | Returns |
|---|---|---|---|
| `json_get(j, key_or_index)` | `datafusion-functions-json` | Generic typed accessor. The argument can be a string key or integer index. | `Union` of possible types |
| `json_get_str(j, key)` | `datafusion-functions-json` | Force string return. | `VARCHAR` |
| `json_get_int(j, key)` | `datafusion-functions-json` | Force integer. | `BIGINT` |
| `json_get_float(j, key)` | `datafusion-functions-json` | Force float. | `DOUBLE` |
| `json_get_bool(j, key)` | `datafusion-functions-json` | Force boolean. | `BOOLEAN` |
| `json_get_json(j, key)` | `datafusion-functions-json` | Re-emit nested JSON. | `VARCHAR` (JSON) |
| `json_get_array(j, key)` | `datafusion-functions-json` | Force array. | `VARCHAR` (JSON array) |
| `json_contains(j, '$.path')` | `datafusion-functions-json` | True if path exists. | `BOOLEAN` |
| `json_as_text(j)` | `datafusion-functions-json` | Re-emit as JSON text (round-trip). | `VARCHAR` |
| `json_length(j)` | `datafusion-functions-json` | Number of object keys / array elements at root. | `BIGINT` |

## Path syntax

The Trino layer uses **JSONPath** (`$.foo.bar[0]`).

The DataFusion JSON layer uses **single-step keys**: a string for object access or an integer for array access.

```sql
-- Trino-style: full JSONPath
SELECT json_extract_scalar(payload, '$.user.id')
FROM events;

-- DataFusion: chained single-step
SELECT json_get_str(json_get_json(payload, 'user'), 'id')
FROM events;
```

Both compose. Both work on the same VARCHAR column. Choose by ergonomics:

- Trino-style is one call per path; cleaner SQL.
- DataFusion JSON layer returns native types; no outer CAST.

For a deeply nested path used in a hot WHERE clause, the DataFusion layer wins on speed (no JSON re-encoding between steps).

## Comparison

| Operation | SQE | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|
| Parse / validate | `json_parse` | `json_parse` | `parse_json` | `from_json` (typed) | `json()` cast |
| Path extract (text) | `json_extract_scalar` | `json_extract_scalar` | `:` syntax (`payload:user.id`) | `get_json_object` | `json_extract_string` |
| Path extract (JSON) | `json_extract` | `json_extract` | `path` (object navigation) | nested `from_json` | `json_extract` |
| Typed extract | `json_get_str/int/float/bool` | `cast(... AS ...)` | `:value::TYPE` | `from_json` schema | `json_extract_*` |
| Build object | `json_object(k, v, ...)` | `json_object(k, v, ...)` | `object_construct` | `to_json(struct(...))` | `json_object` |
| Build array | `json_array(...)` (Trino style; not registered yet) | `json_array(...)` | `array_construct` | `to_json(array(...))` | `json_array` |
| Length | `json_array_length` / `json_length` | `json_array_length` | `array_size` | `json_array_length` | `json_array_length` |
| Contains key/value | `json_contains` / `json_array_contains` | `json_array_contains` | `array_contains` | `array_contains` | `json_contains` |
| To JSON text | `to_json` | `cast(x AS json)` | `to_json` | `to_json` | `to_json` |

## Examples

### Extract a typed field for filtering

```sql
-- Slowpath: parse JSON, cast to int, compare
SELECT * FROM events
WHERE CAST(json_extract_scalar(payload, '$.user_id') AS BIGINT) = 12345;

-- Faster: typed extractor, no CAST
SELECT * FROM events
WHERE json_get_int(json_get_json(payload, 'user'), 'id') = 12345;
```

### Project several fields at once

```sql
SELECT
    json_extract_scalar(payload, '$.user.id')        AS user_id,
    json_extract_scalar(payload, '$.user.email')     AS email,
    json_extract_scalar(payload, '$.session.locale') AS locale,
    CAST(json_extract_scalar(payload, '$.amount') AS DECIMAL(18, 2)) AS amount
FROM events;
```

### Path-existence filter

```sql
SELECT * FROM events
WHERE json_contains(payload, '$.metadata.experimental_flag') = true;
```

### Build a structured field for downstream consumers

```sql
SELECT json_object(
    'id', id,
    'host', url_extract_host(url),
    'path', url_extract_path(url),
    'received_at', cast(occurred_at AS VARCHAR)
) AS event_doc
FROM events;
```

## Performance tips

- **Decode once at the boundary**: when a JSON column is queried in many downstream views, consider materialising the relevant subfields into typed columns at ingest time. Repeated parsing is the biggest JSON cost.
- **Path index pruning**: the DataFusion JSON layer can push `json_get_str(payload, 'user_id') = 'X'` through to a manifest min/max statistic when the underlying JSON has been pre-extracted. The Trino layer is opaque.
- **Avoid round-trip**: `json_format(json_parse(j))` is a no-op semantically but burns CPU. Skip the round-trip unless you need re-canonicalisation.

## Iceberg JSON column type

Iceberg V3 has no native JSON primitive yet. SQE accepts `JSON` in CREATE TABLE and stores it as `VARCHAR` underneath. Predicates and projections work as text. When Iceberg adds a JSON primitive, the storage layout will change but the SQL surface stays the same.

## What is not registered

- **`json_array(...)`** (Trino name for build-an-array). Use `cast(make_array(...) AS VARCHAR)` followed by `json_format`, or `to_json(make_array(...))`.
- **`json_size`** at the root: covered. With a path: covered.
- **JSONPath wildcards** (`$..foo`, `$[*]`): not supported by DataFusion's path parser. Use `unnest(json_get_array(...))` for array-level wildcarding.
