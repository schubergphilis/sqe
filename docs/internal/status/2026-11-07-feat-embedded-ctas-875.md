**STATUS 2026-11-07: embedded CTAS on the cluster's CTAS handler (`feat/embedded-ctas-875`, part of #875).**

What changed:

- `sqe-write/src/write_handler.rs`: `handle_ctas_streaming` is split. The wrapper resolves
  the target `SessionCatalog` and the REST flag from the config; the body is
  `handle_ctas_streaming_on(session, stmt, session_catalog, rest_catalog, ...)`, like
  `handle_insert_streaming_on`. One ordering change on the cluster: the target catalog
  is resolved before the SELECT is planned (it was after), the same order INSERT uses.
- `sqe-cli/src/embedded.rs`: `embedded_handle_iceberg_ctas` calls
  `handle_ctas_streaming_on` with `SessionCatalog::local`, then refreshes the namespace
  listing as `CREATE TABLE` does. Gone: the `LIMIT 0` CREATE + INSERT pair, the OR
  REPLACE refusal, the PARTITIONED BY / options refusal, and `widen_coarse_timestamps`
  (the handler's `timestamps_for_format_version` widens s/ms to µs). CTAS now returns
  the `rows_affected` batch instead of an empty result.
- Docs: `features/embedded.md` CTAS paragraph, the CREATE TABLE paragraph and the
  support table row.

Note on the issue's wording: "one commit on the cluster, two in embedded" was not quite
right. Both create the table (a catalog call, no snapshot) and then append once. The
real gap was that embedded CTAS ran DataFusion's CREATE, so it refused PARTITIONED BY,
TBLPROPERTIES and OR REPLACE.

Tests:

- `cargo test -p sqe-cli --bins ctas` -> 3 passed, new
  `ctas_is_one_commit_and_keeps_partitioning_and_replace` (row count 3, one snapshot,
  SHOW CREATE has PARTITIONED BY + the property, OR REPLACE in place keeps history (2
  snapshots), OR REPLACE with a new schema swaps). The existing rollback-on-failed-cast
  and ms-timestamp tests still pass.
- `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib committing`
  -> 36 passed (incl. `create_or_replace_in_place_keeps_table_identity`,
  `create_or_replace_with_new_schema_swaps_tables`,
  `overwrite_and_replace_refused_under_target_row_filter`).
- Gate: `make test-integration FILTER=ctas` -> 16 passed (`ctas_write_modes_e2e::*`).
  No integration test covers CTAS OR REPLACE; the sqlite rig above does.

Left open on #875: `CALL system.*` in embedded mode (needs the maintenance handler out of
sqe-coordinator).

**NEXT:** #875 `CALL system.*` in embedded mode, or #812 / #848 / #870.
