# fix/trino-type-names-info-schema

Follow-up to !1491 (#848, #867): its review found that `information_schema.columns` named types with a string translation (`arrow_display_to_trino_type`) that disagreed with the wire mapper `encode::arrow_to_trino_type`, and that literal typing differs from Trino.

What changed:

- `encode.rs` has one mapper with three spellings: wire (`json` for a variant), declared (`variant` at any depth: typeof, DESCRIBE, information_schema) and DDL (declared, plus timestamp precision from the unit, for SHOW CREATE TABLE).
- `info_schema_compat::arrow_display_to_trino_type` parses the Arrow display string back with `DataType::from_str` (field metadata stripped first) and calls the declared mapper. The old string fallback is gone. A string Arrow cannot parse passes through unchanged. Proof: unit test `display_string_matches_the_wire_type_name` builds UInt8, UInt64, Timestamp, Decimal128, List/Struct/Map with field-id metadata and an odd field name, and asserts display -> name equals the wire name.
- `typeof` uses the declared mapper by storage shape: `typeof(make_array(v))` is `array(variant)` (was `array(json)`).
- SHOW CREATE TABLE prints Trino types through the DDL mapper (`integer`, `timestamp(9)`, lowercase `variant`) and fails with "table not found" for a missing table (was an empty `CREATE TABLE t (\n)`).
- `1e0` is a double (Trino) rather than `decimal(1,0)`: `trino_compat` rewrites an exponent number literal to `CAST('<text>' AS DOUBLE)`.

Decided, not changed:

- Integer literals stay `bigint` (Trino: `integer`). Live probe: `CAST(2147483647 AS INTEGER) + CAST(1 AS INTEGER)` returns -2147483648 and `9223372036854775807 + 1` wraps negative, where Trino fails. Typing literals as `integer` would turn correct sums into silent wraparound.
- String literals stay `varchar` (Trino: `varchar(1)`); SQE has no bounded varchar.

Known limit: the Arrow parser keeps `\"` escapes in a struct field name, so a field name containing a double quote does not round-trip through the display string and falls through unchanged.

Gate:
- `make test-integration FILTER=show_create` -> 2 passed (`show_create_table_round_trips_its_column_types`, `show_create_table_right_after_ctas_lists_columns_850`)
- `make test-integration FILTER=variant_e2e` -> 9 passed, 0 failed
- Unit: sqe-trino-protocol, sqe-trino-compat, sqe-trino-functions, sqe-sql lib all pass.

The round-trip test (`show_create_table_round_trips_its_column_types`) covers scalar and row columns. Arrays and maps are left out: CREATE TABLE refuses `ARRAY(..)`, `ARRAY<..>` and `MAP(..)`, and ROW with parameterized field types (`sql_type_to_arrow_for` in `sqe-write/src/write_handler.rs` only handles STRUCT/ROW).

Findings (confirmed on the stack, not fixed here):
- CREATE TABLE lacks ARRAY/MAP and parameterized ROW field types: next branch.
- `CAST(TIMESTAMP '..' AS TIMESTAMP WITH TIME ZONE)` yields `Timestamp(ns)` without a zone.
- Integer arithmetic wraps silently on overflow.
- DESCRIBE on the coordinator/Flight path still returns Arrow names.
- SHOW CREATE TABLE does not quote column names (pre-existing).

**NEXT:** CREATE TABLE support for ARRAY/MAP/parameterized ROW, then extend the round-trip test to arrays and maps; file the tz-cast and overflow issues on OK.
