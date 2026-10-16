**STATUS 2026-10-16: Iceberg metadata TVFs answer under Ranger again (`fix/metadata-tvf-tags-846`, #846).**

`scan_column_tags` returns `Known(empty)` for an `IcebergMetadataProvider` (new marker `sqe_catalog::iceberg_metadata_tvf::is_metadata_table_provider`, matched by type, not scan name). Before, the scan was `NotRecognised`, `CacheTagSource` looked up `table_snapshots()`, missed, and the statement was refused with "tag state unknown" on every handler.

The target table's tags were already enforced inside the provider (#750 `MetadataPolicy`): `table_files` / `table_partitions` withhold the stats of governed columns, and all of them on unreadable tags. The open question in the issue (stats-bearing TVFs) is therefore answered by the existing design; the residual whole-table totals in `table_snapshots` / `table_manifests` stay as documented.

New AC test `metadata_tvfs_answer_and_withhold_tag_masked_stats`: carol, alice and bob list snapshots; with a PII tag mask on `ssn` for role engineer, alice sees the ssn bounds in `table_files`, bob does not, and bob still sees the `region` bounds.

**NEXT:** #812 or #815.
