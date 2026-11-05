**STATUS 2026-11-05: `$files` lists delete files with a `content` column (`fix/files-delete-content`).**

Gap from the Snowflake Horizon interop run: `table_files` (and `t$files`)
skipped every non-data manifest entry, so Snowflake's position deletes and
deletion vectors showed only in the `$snapshots` summary. Checked against
the sources: the Iceberg spec defines `content` (data_file field 134: 0
data, 1 position deletes, 2 equality deletes; a V3 DV is content 1 in
Puffin with `referenced_data_file`, `content_offset`,
`content_size_in_bytes`) but no metadata tables; Spark's `files` and
Trino's `$files` list data and delete files with `content` first; Spark
also has `data_files` / `delete_files`.

Change (`sqe-catalog/src/iceberg_metadata_tvf.rs`): every live entry is
listed; appended columns `content`, `referenced_data_file`,
`content_offset`, `content_size_in_bytes`, `equality_ids`, `spec_id` with
Trino's names. Appended, not first, so positional readers keep working.
Governance unchanged: delete-file stats keyed by reserved field ids are
withheld under any column governance, everything under a row filter.
`table_partitions` stays data-only (Spark's `record_count` is data too).
No `data_files` / `delete_files` TVFs: each new TVF needs the MCP allowlist
and five registration sites; `WHERE content = 0` covers it.

Tests: schema unit tests; `v3_e2e::position_deletes_on_v3_table` asserts
the DV row (Puffin, referenced file, offset, size); 20 integration-test
queries that count or read data files gained `WHERE content = 0`. Live
probe against Horizon: Snowflake MoR V2 shows a content-1 parquet
position delete with its referenced file, MoR V3 a content-1 Puffin DV
with offset, SQE's own MoR V3 table a Puffin DV. The harness metadata
probes (`sqe-probes.sql`) now group `$files` by content.

**NEXT:** after !1663 merges, the iceberg.md gap list and the blog bullet
for `$files` may need a trivial conflict resolve (both branches touch the
Snowflake gap list).
