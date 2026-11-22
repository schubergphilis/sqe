## 1. Table level (first MR)

- [x] 1.1 `object_tags::inherit` / `inherit_onto`: own entries first, parent entries whose key the column lacks; unit tests (D1)
- [x] 1.2 `tag_source_impl::effective_column_tags`; `scan_column_tags` and `CacheTagSource` use it; a malformed `sqe.table-tags` is unknown (D3)
- [x] 1.3 `SHOW STATS` reads effective tags: it drops the row count under an inherited row filter (and restricted columns, as for own tags)
- [x] 1.4 Write authority and the Ranger projection stay on own tags (D2, D5)
- [x] 1.5 IT (in-process policy store): table-tag mask and UNSET, row filter on SELECT / UPDATE / DELETE / SHOW STATS (MERGE in `merge_row_filter_e2e`, tag cases), inherited conflict refused, malformed table tags refused
- [x] 1.6 Docs: fine-grained access control page

## 2. Schema (second MR), catalog later

- [x] 2.0 `policy.inherit_schema_tags` (default false), TOML and env; config docs (D8)
- [x] 2.1 Read `sqe.schema-tags` with the table load, no cache (D8); a namespace cache is a later, measured change. Catalog tags: TODO (#924)
- [x] 2.2 A 403, error or malformed schema property refuses the table (D3, D8); the provider constructor takes the schema tag state, no default
- [x] 2.3 Effective tags = column, then table, then schema (nearest wins); catalog later
- [x] 2.4 IT: flag off ignores schema tags; a schema tag masks a table created after the tag and a table another session already read; UNSET restores; malformed refuses; UPDATE/DELETE skip hidden rows

## 3. Follow-ups

- [x] 3.1 Ranger tag projection of table tags, for Spark parity (D5, #935 part 1): DDL, `reproject_column_tags` and the tag load observer; never shrink on a malformed value
- [x] 3.1b Ranger tag projection of schema tags (#935 part 2): `ALTER SCHEMA`, `reproject_column_tags` and the tag load observer on the schema-wide resource; never shrink on an unreadable schema
- [x] 3.2 Ranger e2e: `spark_mask_parity_e2e::table_tag_masks_every_column_in_both_engines` (tag mask and tag row filter on a table tag, both engines) and `schema_tags_mask_and_filter_every_table_in_both_engines` (the same on a schema tag, plus the repair)
