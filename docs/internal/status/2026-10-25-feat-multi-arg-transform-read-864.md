**STATUS 2026-10-25: Tables with multi-argument or unknown transforms are readable; writes refused (`feat/multi-arg-transform-read-864`, part of #864).**

The V3 spec asks readers to read a table whose partition or sort fields use a transform they do not know, ignoring those fields for filtering, and forbids writers to commit data under such a spec. A field may list several `source-ids` (a multi-argument transform). PyIceberg #3630 keeps the original name and sources on round trip; we do the same.

- **Vendor (`SQE PATCH (sqe#864)`)**: `PartitionSpec` and `SortOrder` deserialize through raw field forms. A field with several `source-ids`, or a transform name that does not parse, becomes `Transform::Unknown` with its first source in `source_id`; the original name and every source id are kept in a side table (`unknown_transforms`, keyed by partition field id or sort position) and written back. `bucket[N]` / `truncate[W]` parse only in their exact form. `TableMetadataBuilder::build` keeps the records across the rebind, so a metadata commit (`SET TBLPROPERTIES`) preserves them. Manifest metadata parses the same way, and manifests decode Unknown partition values with the type their Avro writer schema records (the spec's `partition_type` would guess string). `Transform::project` / `strict_project` return no predicate for Unknown, so scans keep every file instead of failing. `SnapshotProducer::validate_added_files` refuses any data or delete file under a spec with an unknown transform, which covers every commit path (append, row delta, rewrite, compaction, DVs).
- **SQE**: `write_data_files_streaming` refuses before uploading (`reject_unknown_transform_write`, next to the geo refusal). Partition evolution refuses to carry an Unknown field into a new spec (the unbound copy would rewrite it as a single-source `unknown`); dropping that field by name should make the table writable (verified from code: `parse_partition_transform` turns a bare identifier into the field name; no gate covers it). `$files` / `$partitions` hide a multi-argument field's value when any source column is masked or restricted (was: first source only). Sort-order and identity-partition consumers already skip non-identity transforms.
- `SortOrder::new(order_id, fields)` added because struct literals no longer compile (the side table field).

Gates:
- Vendor `cargo test -p iceberg --lib` -> 2148 passed. New: `test_scan_ignores_multi_argument_partition_field` (identity spec prunes `x=1` to 0 rows; the same manifests relabelled zorder over `[1,2]` return 2048 rows, and the manifest keeps the long partition values), `test_multi_argument_transforms_survive_a_metadata_commit`, `test_partition_spec_keeps_unknown_transforms`, `test_parse_tolerant_transform`, `test_fast_append_refuses_unknown_transform_spec`; `test_table_metadata_v3_partition_source_ids` now expects multi-source metadata to load.
- `cargo test -p sqe-catalog -p sqe-compaction --lib` -> 685 + 121 passed (new `test_multi_argument_partition_field_governed_by_any_source`, `unknown_transforms_refuse_writes`).
- `make test-integration FILTER=partition_evolution_e2e` -> 7 passed.
- Clippy clean on sqe-catalog, sqe-compaction, sqe-coordinator; vendor clippy reports nothing in the changed lines. `cargo xtask matrix-report --print-only` -> 154/162 (multi-arg-transforms:v3 none -> partial).

Open:
- No engine writes a multi-argument transform yet; evidence is hand-built metadata and manifests. Re-check against Iceberg Java / PyIceberg once one ships.
- No catalog on our rigs can hold such a table: Polaris (Iceberg Java parses only `source-id`) and Lakekeeper (pins its own iceberg-rust fork, which refuses several `source-ids` like upstream) both reject it, so there is no end-to-end test.
- Standalone `PartitionField` serde (v1 `partition-spec`, REST AddSpec input) still refuses several `source-ids`.

**NEXT:** geospatial writes with bounding-box bounds (#863), pending the user's go-ahead.
