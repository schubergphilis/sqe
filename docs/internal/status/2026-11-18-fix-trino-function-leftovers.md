**STATUS 2026-11-18: Trino function leftovers (`fix/trino-function-leftovers`, openspec trino-sql-parity 3.1, 3.4).**

`sha256` is the varbinary built-in. The policy crate no longer exports a user-callable hash; HASH masks keep the internal keyed UDF, and custom `MASKED WITH` expressions were already parsed against the built-in. Embedded mode used to key `sha256()` with `[policy] mask_key` (issue #37); it now warns that the key has no effect there and points to `hmac_sha256`.

`concat` handles array arguments. `json_array_contains` gives NULL for non-arrays. `EXTRACT` and `POSITION` return bigint through an expression planner. `avg(real)` and `sum(real)` return real: both wrap the context's own aggregate (the engine's decimal `avg` and `sum`), delegate every other input type to it, and forward the sliding-window accumulator, so `sum(real) OVER (ROWS ...)` still retracts. Added `json_scalar`, `combinations`, `trim_array`, `title_case`, `char2hexint`. `parse_presto_data_size` does not exist in Trino 484 and was dropped. `min`, `max` and the deviation aggregates already match over real.

Known difference: next to a `count(DISTINCT ...)`, the companion rule re-applies `sum` over its partials, so `sum(real)` rounds each partial to real before the outer sum. The type matches Trino; the last digits can differ.

**NEXT:** integer literal typing for 3.2 and 3.5 (task 7).
