**STATUS 2026-10-26: `table_files` returns valid JSON (`fix/table-files-bounds-json`).**

Found while building geospatial writes (!1548): `lower_bounds` / `upper_bounds` were built from `Datum`'s Display, which is not JSON for most types.

- `crates/sqe-catalog/src/iceberg_metadata_tvf.rs`: `datum_json` renders each bound by type. Booleans, ints, longs, decimals and finite floats stay bare (unchanged output, so `rewrite_data_files_real`'s `parse_bound` keeps working). Strings are JSON-escaped. Date, time, timestamp(tz)(_ns), UUID, binary / fixed (uppercase hex, as before) and NaN / inf become JSON strings of their Display text.
- `partition` was `format!("{v:?}")` of the Literal (Rust Debug). Now `partition_value_json` uses the vendored `Literal::try_into_json` with the spec's partition type (Iceberg single-value JSON: dates as text, binary lowercase hex, decimals as strings). A value whose type is unknown or mismatched falls back to its Debug form as a JSON string. `table_partitions` groups by and prints the same key, so its `partition` column is JSON too and its grouping is unchanged.
- !1548's geo IT strips value quotes in its `file_bounds` helper, so it passes before and after this change.

Gates:
- `make test-integration FILTER=table_files_` -> 1 passed (`table_files_columns_are_json_for_every_type`: BIGINT, DATE partition, STRING with `"` and `\`, BYTEA, TIMESTAMP; all three columns parse with serde_json).
- `cargo test -p sqe-catalog --lib iceberg_metadata_tvf` -> 20 passed (new `test_bounds_json_is_valid_for_every_type`, `test_partition_values_render_as_json`). Clippy clean on sqe-catalog.

Open:
- Bounds use uppercase hex (Datum Display), the partition column lowercase (spec JSON). Left as is: both are what their source defines, and changing the bounds' case would break existing readers of the column.

**NEXT:** rebase !1548 on main after !1546 merged; then GEOMETRY / GEOGRAPHY DDL (#863).
