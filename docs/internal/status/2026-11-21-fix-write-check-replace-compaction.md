**STATUS 2026-11-21: write check on CREATE OR REPLACE AS SELECT, new-table CTAS and rewrite_data_files (`fix/write-check-replace-compaction`).**

`SessionCatalog::check_write_access` (#880) ran on INSERT, DELETE, UPDATE, MERGE, TRUNCATE, Flight ingest and snapshot-property commits, but not on three paths that write data files:
- `CREATE OR REPLACE TABLE ... AS SELECT` on an existing table (both the in-place and the shape-changing replace): now probed on the live table in `handle_ctas_streaming_on`, before the policy check.
- New-table CTAS: probed at the top of `post_create`, so a refusal rolls back the create. A Polaris `TABLE_CREATE` grant alone does not let a caller write data.
- `CALL system.rewrite_data_files` (and `ALTER TABLE ... OPTIMIZE`): the role check in `authorize_or_deny` lets a reader with no roles through, so the procedure now probes before either execution mode. `create_catalog_bridge` was split so the procedure gets the `SessionCatalog`.

Not changed: Hive INSERT (no Polaris commit to probe; needs its own design), `rewrite_manifests` (manifests only), `remove_orphan_files` (deletes only), the maintenance scheduler (runs as the maintenance identity).

Gate: `make test-integration FILTER=dml_write_precheck` -> 2 passed (`a_reader_is_refused_dml_before_any_file_is_written`, now also covering both replace shapes and `rewrite_data_files`; new `a_creator_without_write_is_refused_ctas_before_any_file_is_written`).

Finding: with vended credentials Polaris refuses a create-only principal's CTAS at the create (`CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION`), so the new-table probe matters for static engine keys. That 403 surfaces as a `Catalog` error, not `ACCESS_DENIED`.

**NEXT:** Hive INSERT write check needs a design (storage-only probe on the table location).
