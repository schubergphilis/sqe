**STATUS 2026-10-24: V3 `source-ids` read on partition fields (`fix/partition-source-ids-864`, #864).**

`PartitionField` and `UnboundPartitionField` in `vendor/iceberg-rust` now deserialize through a repr that accepts `source-id` or `source-ids` (`SQE PATCH (sqe#864)` in `spec/partition.rs`). A one-element list reads as `source_id`. Several ids, an empty list, both keys, or neither key give an error that names the field. Serialization still writes `source-id`, so nothing SQE commits changes shape.

A multi-element field is refused, never read through its first id. Reading it through the first id would prune on the wrong column, and the next commit would rewrite the spec with one id and lose the rest.

Matrix: `sqe:multi-arg-transforms:v3` stays `none`. The issue's "Done when" asks SQE to read a Spark-written table with a multi-argument transform and to declare one. The V3 spec reserves `source-ids` but defines no multi-argument transform, so no such table exists to test against. The notes now say so.

Gates:

- Vendor lib: `cargo test -p iceberg --lib` in `vendor/iceberg-rust` -> 2098 passed, 0 failed. The new tests are `spec::partition::tests::test_partition_field_source_ids` and `spec::table_metadata::tests::test_table_metadata_v3_partition_source_ids`.
- `make test-integration FILTER=v3_e2e` -> 19 passed, 0 failed, gate=0.

Finding, not fixed here: `TableMetadata` deserializes through the untagged `TableMetadataEnum`, so any field error (this one included) reaches the user as "data did not match any variant of untagged enum TableMetadataEnum". The behaviour is upstream's, and five upstream tests pin that message. Dispatching on `format-version` instead would give named errors for every malformed metadata file.

**NEXT:** #866 catalog CI job, then #870 shredded variant pruning.
