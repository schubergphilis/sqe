**STATUS 2026-11-13: a schema-changing `CREATE OR REPLACE TABLE ... AS SELECT` keeps the table's properties and column tags (`fix/ctas-replace-keeps-properties-896`, #896).**

A replace whose SELECT changes the schema or partitioning is staged as `t__sqe_replace_<uuid>`, then swapped in with DROP + RENAME. The staged table was created with the statement's options only, so the live table's properties were lost, column tags (`sqe.column-tags`) included. The same-schema replace keeps them since #894.

Changed:

- `crates/sqe-write/src/write_handler/table_props.rs`: `carry_live_properties` adds every live property the statement does not set (case-insensitive) to the staged table's properties. It skips:
  - the reserved Iceberg keys;
  - the old table's storage paths (`write.data.path`, `write.metadata.path`, `write.object-storage.path`, `write.folder-storage.path`);
  - `sqe.json-columns`, which `record_json_columns` recomputes from the SELECT.

  `sqe.column-tags` keeps the entries whose column, or whose top-level segment for a nested path, is still in the new schema (ASCII case-insensitive). A value that does not parse is carried as it is, so the policy rewriter still fails closed on it.
- `crates/sqe-write/src/write_handler.rs`: the staged branch of the streaming CTAS calls it when a live table exists.

Tests:

- Unit, `cargo test -p sqe-write --lib carry_live_properties`, 3 passed: unset keys kept and bound keys skipped; tags filtered to the remaining columns; a malformed tag value carried verbatim.
- Integration, `make test-integration FILTER=ctas_write_modes_e2e`, 15 passed. The new test is `ctas_replace_with_a_new_schema_keeps_properties_and_tags_896`. It:
  - tags two columns;
  - replaces the table with one of them dropped and a column added;
  - reads `SHOW TAGS`: one row left, on the remaining column;
  - checks `SHOW CREATE TABLE`: the plain property is still there and the statement's value wins.

  Before the fix it failed with `only email keeps a tag: []`.
- Also green: `FILTER=information_schema_listing_race_e2e` (1 passed) and `cargo test -p sqe-coordinator --test it grant_introspection_gate_test` (16 passed).

Budget: sqe-write's line ceiling goes from 28100 to 28250.

Left open: the staged replace still drops the old table's snapshot history and UUID. The atomic replace (#892) keeps the table and fixes that as well.

**NEXT:** embedded CLI `DROP SCHEMA ... CASCADE`, then the #892 OpenSpec change (atomic schema-changing CREATE OR REPLACE).
