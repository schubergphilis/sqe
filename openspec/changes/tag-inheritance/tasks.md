## 1. Table level (first MR)

- [x] 1.1 `object_tags::inherit` / `inherit_onto`: own entries first, parent entries whose key the column lacks; unit tests (D1)
- [x] 1.2 `tag_source_impl::effective_column_tags`; `scan_column_tags` and `CacheTagSource` use it; a malformed `sqe.table-tags` is unknown (D3)
- [x] 1.3 `SHOW STATS` reads effective tags: it drops the row count under an inherited row filter (and restricted columns, as for own tags)
- [x] 1.4 Write authority and the Ranger projection stay on own tags (D2, D5)
- [x] 1.5 IT (in-process policy store): table-tag mask and UNSET, row filter on SELECT / UPDATE / DELETE / SHOW STATS (MERGE in `merge_row_filter_e2e`, tag cases), inherited conflict refused, malformed table tags refused
- [x] 1.6 Docs: fine-grained access control page

## 2. Schema and catalog (second MR)

- [ ] 2.1 Read `sqe.schema-tags` and `sqe.catalog-tags` through a cache with TTL <= the policy refresh interval; invalidate on SQE's own SET/UNSET TAGS (D6)
- [ ] 2.2 A 403, error or malformed parent property refuses the table (D3)
- [ ] 2.3 Effective tags = column, then table, schema, catalog (nearest wins)
- [ ] 2.4 IT: a schema tag masks a table created after the tag

## 3. Follow-ups

- [ ] 3.1 Ranger tag projection of inherited tags, for Spark parity (D5, #935); #924 is not done without it
- [ ] 3.2 Ranger e2e in `access_control_e2e.rs` once the Ranger stack is free
