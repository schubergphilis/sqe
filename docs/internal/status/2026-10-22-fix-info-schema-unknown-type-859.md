**STATUS 2026-10-22: `information_schema` names unknown columns `unknown` (`fix/info-schema-unknown-type-859`).**

The last open "Done when" item of #859 besides nested promotion (!1502).
DataFusion's built-in `information_schema.columns` prints `data_type` with
Arrow's display, so a V3 `unknown` column (Arrow `Null`) showed `Null` on
the native path (Flight SQL, embedded CLI). DESCRIBE, SHOW COLUMNS and the
Trino endpoint already mapped it to `unknown` through
`arrow_display_to_trino_type`.

- `crates/sqe-catalog/src/file_tvf_prepare.rs`: `name_unknown_types` wraps
  every `information_schema.columns` scan in `plan_sql`, `plan_parsed` and
  `plan_sql_without_policy` (subqueries included) in a projection that maps
  `data_type = 'Null'` to `unknown`. It runs before the optimizer, so a
  user filter on `data_type` sees the new value. Nested types keep their
  Arrow display. DESCRIBE passes `unknown` through unchanged (Arrow cannot
  parse it, `arrow_display_to_trino_type` returns the input).
- Unit test `information_schema_names_a_null_column_unknown` (projection,
  filter, subquery). The interop test now asserts `u|unknown`.
- Matrix: the `unknown-type:v3` information_schema caveat is gone. Score
  unchanged (150/162; the cell was already full).

Gate:
- `make test-integration FILTER=unknown_type_spark_interop` (Spark 4.1 +
  Iceberg 1.12 stack up), rebased on main with !1502: 6 passed, 0 skips.
- `make test-integration FILTER=describe`: 4 passed; one
  access-control test skips without `SQE_AC_E2E=1` (unrelated).
- `cargo clippy -p sqe-catalog --lib --tests`: clean.
- `cargo test -p sqe-catalog --lib file_tvf_prepare`: 12 passed;
  `cargo test -p sqe-coordinator --lib` (describe / show_columns /
  info_schema): 26 passed; `cargo test -p sqe-trino-compat info_schema`: 4
  passed.

Other paths checked: the MCP `describe_schema` tool reads `SHOW COLUMNS`
(already `unknown`); Flight `GetTables(include_schema)` returns an Arrow
IPC schema, where `Null` is the correct type.

Left open on #859: promoting a list element or a map value, and
date -> timestamp. Nested struct members promote since !1502 (merged).

**NEXT:** split list/map/date promotion into its own issue and close #859.
