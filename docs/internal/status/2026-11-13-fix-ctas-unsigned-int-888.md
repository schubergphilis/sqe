**STATUS 2026-11-13: CTAS and Flight ingest create map `UInt64` to `long` (`fix/ctas-unsigned-int-888`, #888).**

A downstream dbt test project hit "Failed to build Iceberg schema: UInt64 is not supported" on every model that stores `row_number()`, `rank()`, `dense_rank()` or `ntile()`; it worked around it with `CAST(... AS BIGINT)` in each model.

Changed:

- `timestamps_for_format_version` is now `write_schema_for_format_version` (`crates/sqe-write/src/write_handler.rs`) and also maps a top-level `UInt64` field to `Int64`. Both create sites use it: streaming CTAS and the Flight SQL ingest create (`write_handler/ingest.rs`). iceberg-rust already maps `UInt8`/`UInt16` to `int` and `UInt32` to `long`.
- No plan-level `CAST`: DataFusion's cast is the safe kind and turns an overflow into NULL. The sink's checked cast (`apply_stamped_schema`, #834) fails the write instead: "Can't cast value 18446744073709551615 to type Int64".

Tests:

- unit `test_write_schema_maps_uint64_to_long_and_checks_overflow` (sqe-write).
- `ctas_write_modes_e2e::ctas_ranking_functions_make_bigint_columns_888`: CREATE and CREATE OR REPLACE with the four ranking functions give `bigint` columns with the right values; an overflowing replace fails and the previous table keeps its rows.
- `flight_ingest_e2e::ingest_create_maps_uint64_to_long`: create from a `UInt64` stream, then an overflowing append is refused and the table is unchanged.

Gate: `make test-integration FILTER=888` and `FILTER=uint64`, 1 passed each; `cargo test -p sqe-write --lib write_schema_` and `timestamps_for` passed.

Left open: unsigned fields nested in a struct, list or map still fail CTAS (top-level fields only, as for timestamps).

**NEXT:** the dbt test project can drop its `CAST(... AS BIGINT)` workarounds once it runs a build that carries this commit.
