**STATUS 2026-10-29: embedded ALTER TABLE (`feat/embedded-alter-table`).**

Next embedded gap from the Databricks interop probe. Embedded mode answered
every `ALTER TABLE` with DataFusion's "unsupported" error.

- `sqe-cli/src/embedded.rs`: the SQE `ALTER` statement kinds (rename,
  schema change, properties, nested drop / retype, ref DDL, partition
  evolution, identifier fields) go to `CatalogOps` from `sqe-write`, bound to
  the target catalog's `SessionCatalog` through the new
  `CatalogOps::with_session_catalog`. The changed namespaces are refreshed
  afterwards. `RENAME TO` refuses a destination in another catalog.
- `SessionCatalog::commit_schema_update` was REST-only (raw JSON POST). On a
  non-REST handle it now builds a `TableCommit` and calls the trait's
  `update_table`; the SQLite catalog's `apply` checks the requirements.
  `TableCommit`'s builder is crate-private upstream, so the vendored
  iceberg-rust gains `TableCommit::from_parts` (`SQE PATCH
  (embedded-alter)`, listed in `vendor/iceberg-rust/README.md`).
- Smoke test `embedded_warehouse_alter_table` runs every ALTER kind on
  `iceberg.s.t` and checks the rows plus the metadata JSON.

Correction to the `fix/insert-values-array-field-ids` status: the "MAP
columns fail in embedded CREATE TABLE" gap is false. The repro used an
unqualified `s.t`, which resolves to DataFusion's in-memory `default`
catalog. `CREATE TABLE iceberg.s.t (m MAP(VARCHAR, INT))` works.

Still cluster-only in embedded mode: `SET TAGS`, masking policies,
`SET WRITE_BRANCH`.

**NEXT:** embedded Iceberg `SHOW CREATE TABLE` (move the renderers from
`query_handler.rs` into `sqe-write`), then time travel and metadata tables.
