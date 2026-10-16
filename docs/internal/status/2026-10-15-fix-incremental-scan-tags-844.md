**STATUS 2026-10-15: CDC incremental reads carry their table's column tags, and resolve table names like a plain read (`fix/incremental-scan-tags-844`, #844, #845).**

`IncrementalTableProvider` now holds the Iceberg table's properties (`with_table_properties`), and `scan_column_tags` reads `sqe.column-tags` from it, as #842 does for `SqeTableProvider`. A provider built without properties is `NotRecognised` (the cache decides), never known-untagged.

`apply_incremental_spec` now resolves the table through `SessionContext::table_provider`, the path a plain read takes, and downcasts to `SqeTableProvider` (#845).

Also found: metadata TVFs (`table_snapshots`, ...) are refused with "tag state unknown" under Ranger, even for an admin on a warm handler (#846).

Verified: `scripts/access-control-test.sh unknown_tag_state` (fails with the #844 branch disabled), `make test-integration FILTER=incremental` 3/3, `scan_column_tags_tests` 8/8.

**NEXT:** #846 (metadata TVFs under Ranger), then #812 or #815.
