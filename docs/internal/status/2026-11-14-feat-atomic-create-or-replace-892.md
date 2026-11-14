**STATUS 2026-11-14: schema-changing `CREATE OR REPLACE` is one commit on the live table (#892, `feat/atomic-create-or-replace-892`).**

Built from the OpenSpec change `atomic-create-or-replace` with the default answers to U1-U10.

Changed:

- Vendor `ReplaceTableShapeAction` (`SQE PATCH (sqe#892)`): plans `AddSchema` / `SetCurrentSchema`, `AddSpec` / `SetDefaultSpec`, a sort order that names only surviving columns and a format upgrade. It sends explicit ids for a shape seen before, and pins the ids it planned against. Field ids follow column names with the Iceberg promotions; a field stays required only when it keeps the id of a required field.
- `TableMetadata::schema_for_partition_spec`: an old spec whose source column was dropped binds to the newest schema that still holds it. Without it, a replace that dropped the partition column failed to write the manifest of the files it removed.
- CTAS: the staged create + drop + rename is gone. The replace writes through the evolved table view and commits the shape updates and the overwrite snapshot together. Properties, tags, json columns, policy refusals, snapshot properties and lineage work as before.
- Result cache: a query that reads `information_schema` or the `system` catalog is no longer cached. A repeated `information_schema.columns` query after a replace returned the old column list, because the cache evicts by the written table's name only. Found by the partition-drop integration step.
- Refused before writing: a format downgrade, and a shape change under `WRITE_BRANCH` or in a transaction.

Probe findings, recorded in the design:

- Polaris needs `spec-id` on `add-spec` and otherwise returns a bare 400.
- A rollback moves `main`, not the schema.

Gate (Polaris 1.7.0, local stack):

- `make test-integration FILTER=ctas_`
- `make test-integration FILTER=information_schema_columns_survive`
- `cargo test -p sqe-cli --bins`
- vendor `cargo test -p iceberg --lib -- replace_shape snapshot`
- `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib` (the races, the downgrade refusal and the snapshot properties, in `write_handler/tests/committing/shape_replace.rs`)

The crate line ceilings of sqe-write (28250 -> 28650) and sqe-coordinator (68250 -> 68300) are raised for this feature's code and tests; the new unit tests live in their own file so `write_handler.rs` stays under its ceiling.

Results are in the MR.

Left open:

- The manifests and manifest list of a refused commit stay on storage, unreferenced, on this and every other write path. The data files are removed.
- Not tested end to end: catalog grants surviving the replace, and the privileges Polaris checks for the one-commit replace compared with create + drop + rename (U5).

**NEXT:** #897 (Trino table properties through the Trino door), then #886 and #884.
