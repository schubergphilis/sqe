**STATUS 2026-10-20: `ALTER TABLE ... SET | DROP IDENTIFIER FIELDS` (`feat/iceberg-identifier-fields-ddl`).**

SQE can now declare an Iceberg table's identifier fields, which merge-on-read UPDATE and MERGE need to write equality deletes. Before this, only another engine (Spark) could set them.

- Parser: `crates/sqe-sql/src/identifier_fields.rs`, Spark syntax, parentheses optional, dotted names for struct members, unquoted parts ASCII-folded. Classified as `StatementKind::IdentifierFields` (an `AlterTable` change, refused inside a transaction, on the MCP change surface).
- Commit: `CatalogOps::apply_identifier_fields` builds a fresh schema (same fields, new set), so iceberg-rust's schema validation refuses nullable, float/double, nested-in-list/map and unknown columns. Requirements: `LastAssignedFieldIdMatch` and `CurrentSchemaIdMatch`. When the set goes back to an earlier schema, the catalog reuses that schema and rejects `SetCurrentSchema { -1 }`, so the existing schema is selected by id, and setting the current set again is a no-op.
- Finding fixed: `write.identifier-field-ids` / `WITH (identifier_field_ids = 'id')` are not read by anything. Four tests set them and believed they exercised equality deletes, but ran copy-on-write: `v3_e2e::equality_delete_update_on_v3_table`, `v3_e2e::row_lineage_survives_mor_update_and_merge_on_v3_table`, `rewrite_data_files_deletes::rewrite_applies_equality_deletes`, `rewrite_data_files_distributed_parity::equality_delete_parity`. They now declare `NOT NULL` + `SET IDENTIFIER FIELDS id` and assert `added-equality-delete-files`. Stale "SQE has no DDL for identifier fields" comments are corrected.
- Matrix: the equality-delete caveats are gone, the merge-on-read caveat now names the DDL. Score unchanged (the cells were already `full`).

Gate: `make test-integration FILTER="identifier_fields equality_delete_update_on_v3_table row_lineage_survives_mor rewrite_applies_equality_deletes ctas_update_mode_mor mor_spark_interop"` (15 tests, includes all 10 Spark MoR interop tests), plus `equality_delete_parity` against two native workers on :50052/:50053. The Docker `make test-distributed` rig cannot run that test from the host: its workers advertise `http://worker-N:50052` and refuse compaction requests addressed to the host-mapped port ("compaction request audience does not match this worker").

Not changed, on purpose: SHOW CREATE TABLE prints one CREATE statement and renders neither the partition spec nor identifier fields (both would need a trailing ALTER). `mor_spark_interop` still lets Spark set identifier fields, since that file tests the Spark-written direction.

**NEXT:** #859 nested unknown (write prune + read reconcile), then #858 shredded-variant write.
