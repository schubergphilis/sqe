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

Second commit (review findings, fixed here):

- CREATE TABLE / ALTER TABLE ADD COLUMN take nested types. `sqe-sql/src/ddl_types.rs` rewrites Trino column types in the column list (`array(T)` -> `ARRAY<T>`, `map(K,V)` -> `MAP(K, V)`, `row(f T)` -> `STRUCT<f T>`, recursively) before parsing; it runs on parse failure and on a parsed CREATE/ALTER (a flattened `row(t timestamp with time zone)` parses but loses the field types). `sql_type_to_arrow_for` maps `Array` and `Map`. ADD COLUMN assigns nested field ids from `max_field_id` (`arrow_column_to_iceberg_type`).
- SHOW CREATE TABLE quotes names that need it (`sqe-sql/src/quote.rs`: reserved words, uppercase, anything not `[a-z_][a-z0-9_]*`), resolves the table for TBLPROPERTIES through `resolve_table_ident` instead of splitting on `.`, and escapes quotes in property values.
- DESCRIBE / SHOW COLUMNS on the coordinator return Trino names (Iceberg: `with_column_comments`; Hive: `describe_columns`). The Trino front end maps again; the mapping is idempotent (unit test `trino_type_mapping_is_idempotent`). The Trino reshape carries the comment column (was always "").
- Hive SHOW CREATE TABLE backtick-quotes names, renders column comments, and the table comment as TBLPROPERTIES `comment` (sqlparser has no COMMENT clause on CREATE EXTERNAL TABLE; the Hive metastore stores it as that parameter). `try_extract_hive_table` lifts `comment` back into the spec. Round trip pinned by the embedded test `show_create_table_renders_athena_compatible_ddl`.

Deliberate: Flight SQL `information_schema.columns` keeps Arrow display names (DataFusion built-in): the Arrow schema is the Flight type system and dbt-sqe's `SQEColumn` reads those names.

Gate (after the last edit):
- `make test-integration FILTER=nested_type_ddl` -> 1 passed (`nested_type_ddl_round_trips_through_show_create`: Trino and Spark nested types, INSERT + read, SHOW CREATE -> CREATE copy -> same DESCRIBE, ADD COLUMN array/row then INSERT + read, quoted `"select"` / `"My Col"`)
- `FILTER=show_create` 3 passed, `FILTER=describe` 4 passed, `FILTER=variant_e2e` 9 passed, `FILTER=trino_metadata` 3 passed, `FILTER=ctas_write_modes` 10 passed
- Hive: `cargo test -p sqe-coordinator --test it -- hive_`, `cargo test -p sqe-catalog --test it -- hive_`, `cargo test -p sqe-cli --bin sqe-cli -- show_create_table_renders_athena`
- Unit: sqe-sql, sqe-trino-compat, sqe-write (`test_sql_type_to_arrow_array_and_map`, `test_arrow_schema_to_iceberg_assigns_nested_list_and_map_field_ids`)

Next branches of this batch (different concern, worked right after this MR):
- `fix/timestamptz-cast-zone`: `CAST(.. AS TIMESTAMP WITH TIME ZONE)` yields a zone-less timestamp because `datafusion.execution.time_zone` is unset.
- `fix/integer-overflow-checked`: integer `+ - *` wraps silently; Trino fails. Once checked, revisit the bigint-literal rationale above.

**NEXT:** the two branches above.
