### Fixed

- Trino function leftovers (trino-sql-parity wave 3)

    `sha256` now returns `varbinary` and accepts `varbinary` or `varchar` (the DataFusion built-in; use `to_hex` for text); HASH column masks are unaffected because they use the internal keyed function. `concat` with an array argument prepends, appends or concatenates like Trino. `json_array_contains` returns NULL for a non-array. `EXTRACT(... FROM ...)` and `POSITION(x IN y)` return `bigint`. `avg(real)` and `sum(real)` return `real`. New: `json_scalar`, `combinations`, `trim_array`, `title_case`, `char2hexint`. Breaking for callers that treated `sha256(x)` as hex text: wrap it in `to_hex`. In embedded mode (`sqe-cli`) `[policy] mask_key` no longer keys `sha256()`; the CLI warns when it is set, and `hmac_sha256(x, key)` gives a keyed hash.
