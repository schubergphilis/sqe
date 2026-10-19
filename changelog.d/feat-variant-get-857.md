### Added

- `variant_get(v, '$.path', 'type')` and `try_variant_get` read a typed field of a VARIANT column (#857)

    The field is read straight from the variant encoding with arrow's `variant_get` kernel, not through the value's JSON text as the `json*` functions do. The path is `$` followed by `.field`, `['field']` or `[index]` steps. The type is a Spark/SQL name (`int`, `bigint`, `double`, `decimal(p,s)`, `string`, `date`, `timestamp`, ...); without one the result is a variant. A missing path is NULL. A value that does not convert is an error for `variant_get` and NULL for `try_variant_get`, as in Spark. `rewrite_data_files` on a variant table is now covered by a test and keeps every value. Iceberg matrix `variant-type:v3` moves to full (148/162).
