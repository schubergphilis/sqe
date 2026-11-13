**STATUS 2026-11-13: in-place CREATE OR REPLACE TABLE sets the statement's TBLPROPERTIES (`fix/ctas-replace-tblproperties-894`, #894).**

Found while adding table properties to dbt-sqe (#891): a `table` model re-run with changed `tblproperties` kept the old ones. Reproduced live on main: create with `write.merge.mode=merge-on-read, a.b=1`; a same-schema replace with `copy-on-write, a.b=2` still showed the old values, while a schema-changing replace (staged path) applied its own.

Changed:

- `crates/sqe-write/src/write_handler.rs`: `ctas_replace_in_place` takes the statement's properties and commits them with the content swap, through `commit_rewrite_and_properties_with_retry` (the old `commit_rewrite_with_retry` now delegates to it with none). The empty-to-empty replace, which commits no rewrite, sets them in a commit of their own. The doc comment that called the old behaviour deliberate is replaced.
- `crates/sqe-write/src/write_handler/table_props.rs`: new module. The property helpers (`create_table_options_as_slice`, `create_table_properties`, `record_json_columns`, `declared_json_columns`, `merge_user_table_properties`, `sql_expr_to_property_string`) moved here unchanged (write_handler.rs was 9 lines under its budget), plus `replace_in_place_properties` (user keys only; reserved keys and the `format_version` option dropped, since the table and its format version stay), `with_properties` and `commit_properties`.
- `crates/sqe-coordinator/src/query_handler.rs`: `sets_reserved_table_property` also matches a `CREATE OR REPLACE TABLE` naming `sqe.column-tags`, so it needs admin like `ALTER TABLE ... SET TBLPROPERTIES`. An in-place replace keeps the old snapshots those tags mask; letting any writer clear the tags would unmask history through time travel.

Semantics: keys the statement names are set, other live keys stay (Iceberg's replace transaction does the same). The staged path is unchanged: it creates a new table with exactly the statement's properties.

Tests: unit `write_handler::table_props::tests::replace_in_place_properties_keep_user_keys_and_drop_reserved_ones`; integration `ctas_write_modes_e2e::ctas_replace_in_place_sets_tblproperties_894` (set, kept, one snapshot, data swapped, empty-to-empty replace) and `grant_introspection_gate_test::create_or_replace_with_reserved_tblproperties_requires_admin`.

Gate: `make test-integration FILTER=ctas_` 18 passed (exit 0), including the new test; with the fix disabled the new test fails on `missing 'write.merge.mode' = 'copy-on-write'`. `cargo test -p sqe-coordinator --test it -- tblproperties` 3 passed (the admin-gate tests are plain tests, so the `--ignored` gate does not run them); `cargo test -p sqe-write --lib table_props` 1 passed; `cargo clippy -p sqe-write -p sqe-coordinator --all-targets -- -D warnings` clean; `cargo xtask budgets --check` ok. The move into `table_props.rs` was checked as byte-identical apart from `pub(super)` on `sql_expr_to_property_string`.

**NEXT:** dbt-sqe #891 relies on this for `tblproperties` on re-runs of `table` models.
