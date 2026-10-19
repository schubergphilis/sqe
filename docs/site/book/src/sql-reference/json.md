# JSON

Two layered JSON surfaces. Both are registered in every session.

1. **Trino-named layer** in `sqe-trino-functions`: `json_extract`, `json_extract_scalar`, `json_array_length`, `json_parse`, `json_format`, `json_object`, `is_json_scalar`, `json_array_contains`, `json_size`, `json_array_get`, `to_json`. Maps directly to dbt-trino model expectations.
2. **DataFusion JSON layer** via `datafusion-functions-json` (registered in `session_context.rs:361` and `embedded.rs:172`): `json_get`, `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`, `json_get_json`, `json_get_array`, `json_contains`, `json_as_text`, `json_length`. Type-specific extractors that avoid an outer CAST.

A `JSON` column is stored as Iceberg `VARIANT` on a V3 table (the default for new tables) and as a string on a V2 table; either way it reads back as JSON text (see [DDL, VARIANT](./ddl.md#variant)). Both layers parse on every call, so for hot paths consider extracting once into a typed column.

## Trino-named layer

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `json_parse(s)` | `sqe-trino-functions` | Parse JSON text and return it in Trino's canonical form: compact, object keys sorted (`{"b": 1, "a": 2}` becomes `{"a":2,"b":1}`). Malformed input returns the text `null` (Trino raises an error). `trino_functions.rs:92` | yes | `parse_json` | `from_json` | `json` |
| `json_format(j)` | `sqe-trino-functions` | Format / re-emit JSON. `trino_functions.rs:60` | yes | - | `to_json` | `to_json` |
| `json_object(k1, v1, k2, v2, ...)` | `sqe-trino-functions` | Build a JSON object from key-value pairs. `trino_functions.rs:59` | yes | yes | yes | yes |
| `json_array_length(j)` | `sqe-trino-functions` | Number of elements in a JSON array. NULL if not an array. `trino_functions.rs:91` | yes | yes | yes | `json_array_length` |
| `json_extract(j, '$.path')` | `sqe-trino-functions` | JSONPath extraction. Returns JSON-encoded value. `trino_functions.rs:89` | yes | yes | yes | yes |
| `json_extract_scalar(j, '$.path')` | `sqe-trino-functions` | Same path syntax; returns plain text scalar (or NULL). `trino_functions.rs:90` | yes | - | `get_json_object` | `json_extract_string` |
| `is_json_scalar(j)` | `sqe-trino-functions (ext)` | True for JSON null / boolean / number / string. False for objects and arrays. `trino_functions_ext.rs:30` | yes | - | - | - |
| `json_array_contains(j, v)` | `sqe-trino-functions (ext)` | True if JSON array contains the value. `trino_functions_ext.rs:31` | yes | - | - | - |
| `json_size(j, '$.path')` | `sqe-trino-functions (ext)` | Cardinality at the path. Object key count for objects; array length for arrays; `0` for primitives. `trino_functions_ext.rs:72` | yes | - | - | - |
| `json_array_get(j, idx)` | `sqe-trino-functions (ext)` | 0-based element from a JSON array. `trino_functions_ext.rs:73` | yes | - | - | - |
| `to_json(any)` | `sqe-trino-functions (ext)` | Serialize a SQL value (struct, array, map) as JSON; the target of `CAST(x AS JSON)`, see below. NULL in, NULL out. `json_cast.rs` | - | yes | yes | yes |

## CAST(x AS JSON)

`CAST(x AS JSON)` is rewritten to `to_json(x)` and returns the text Trino 476 returns for the same cast:

| Input | Result |
|---|---|
| `ARRAY[1, NULL, 3]` | `[1,null,3]` |
| `ARRAY['a', 'b"c']` | `["a","b\"c"]` (strings escaped, control characters as `\u0001`) |
| `ARRAY[1.5, 2.25]` (decimal) | `[1.50,2.25]` (the declared scale is kept) |
| `ARRAY[DOUBLE '1e300', DOUBLE '0.0001']` | `[1.0E300,1.0E-4]` (Java `Double.toString` notation) |
| `ARRAY[DOUBLE 'NaN']` | `["NaN"]` (also `"Infinity"`, `"-Infinity"`) |
| `ARRAY[DATE '2024-01-02']` | `["2024-01-02"]` |
| `ARRAY[TIMESTAMP '2024-01-02 03:04:05.123']` | `["2024-01-02 03:04:05.123"]` |
| `MAP(ARRAY['b', 'a'], ARRAY[1, 2])` | `{"a":2,"b":1}` (keys stringified and sorted) |
| `MAP(ARRAY[10, 9], ARRAY['x', 'y'])` | `{"10":"x","9":"y"}` |
| `CAST(ROW(1, 'x') AS ROW(a INTEGER, b VARCHAR))` | `{"a":1,"b":"x"}` |
| `'abc'` | `"abc"` |
| `NULL` | SQL `NULL` |

A cast of a value that is already JSON (`json_parse`, `json_extract`, `json_array_get`, a `JSON '...'` literal, a nested `CAST(.. AS JSON)`) returns the value unchanged. `JSON '<text>'` is read as `json_parse('<text>')`, so the literal comes back canonical too.

Known differences from Trino:

- **Anonymous rows**: `CAST(ROW(1, 'x') AS JSON)` gives `{"c0":1,"c1":"x"}`; Trino gives `{"":1,"":"x"}`. Name the fields with `CAST(ROW(..) AS ROW(a .., b ..))` to get the same output.
- **JSON columns and JSON inside an array or map**: on a V2 table SQE stores JSON as `VARCHAR` with no type marker, so only a cast of a JSON-returning function is recognised as already JSON. `CAST(payload AS JSON)` on a stored `JSON` column encodes the text as a JSON string, `"{\"a\":1}"`, where Trino returns it unchanged, `{"a":1}`; likewise `CAST(ARRAY[json_parse('{"a":1}')] AS JSON)` gives `["{\"a\":1}"]`, where Trino embeds it, `[{"a":1}]`. Use the column directly, or `json_parse(payload)`, instead of casting it.
- **Timestamp precision**: fractional seconds print without trailing zeros; Trino pads to the declared precision. A zoned timestamp ends in an offset (`+00:00`), not a zone name (`UTC`).
- **Types Trino refuses**: varbinary, time and interval values are written as their quoted text; Trino raises "Cannot cast ... to json". Likewise `CAST(json AS VARCHAR)` returns the JSON text, where Trino raises an error.

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

Iceberg has no JSON primitive. SQE accepts `JSON` in CREATE TABLE and stores it as `VARIANT` on a V3 table (the default) and as a string on a V2 table. Either way it reads back as JSON text, and the `json*` functions accept both. On a variant, `variant_get(j, '$.a.b', 'int')` reads a typed field without rendering the value as JSON text (it does not take a string column). Existing string columns are not converted. See [DDL, VARIANT](./ddl.md#variant).

## What is not registered

- **`json_array(...)`** (Trino name for build-an-array). Use `cast(make_array(...) AS VARCHAR)` followed by `json_format`, or `to_json(make_array(...))`.
- **`json_size`** at the root: covered. With a path: covered.
- **JSONPath wildcards** (`$..foo`, `$[*]`): not supported by DataFusion's path parser. Use `unnest(json_get_array(...))` for array-level wildcarding.
