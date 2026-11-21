## 1. Resolver (sqe-core)

- [x] 1.1 `column_tags::ids`: `TagIds { tags_sha256, ids }` with parse / to_json, `IDS_PROP_KEY = "sqe.column-tag-ids"`, digest of a map value
- [x] 1.2 `resolve(raw_tags, raw_ids, id_of_name, name_of_id) -> Resolved { applied, orphaned }` per D2, D5, D6; unit tests: rename followed, drop+re-add orphaned, no ids by name, digest mismatch by name, malformed ids an error, collision union, nested path
- [x] 1.3 `record(resolved_map, id_of_name, orphans) -> (tags_json, ids_json)` for writers

## 2. Read path

- [x] 2.1 `scan_column_tags` resolves against the provider's scan schema (snapshot schema for time travel)
- [x] 2.2 `CacheTagSource` resolves against the cached current schema
- [x] 2.3 `SHOW TAGS` adds `applied`; `information_schema.column_tags` lists applied entries under current names
- [x] 2.4 Ranger tag projection projects resolved names

## 3. Write path (after !1907 merges; removal comparisons run on resolved maps)

- [x] 3.1 SET/UNSET TAGS, masking policy, raw writes: read resolved, write map + ids + digest in the same commit (raw writes: checked on the resolved map after the write; their old ids stop binding, so the map applies by name)
- [x] 3.2 RENAME/DROP COLUMN: `rewrite_tag_keys` carries ids; digest recomputed
- [x] 3.3 CREATE OR REPLACE / CTAS removes `sqe.column-tag-ids`
- [x] 3.4 Raw SET/UNSET of `sqe.column-tag-ids` admin-only (SQE has no raw UNSET of table properties)

## 4. Acceptance

- [x] 4.1 IT: foreign rename and foreign drop+re-add committed through the catalog directly (not SQE's ALTER), then masking and SHOW TAGS checked (SHOW TAGS here; the masking read path is covered by the `scan_column_tags` unit tests)
- [x] 4.2 IT: legacy name-only fixture keeps its tags; a foreign map write without ids applies by name
- [x] 4.3 Docs: tag reference page, `SHOW TAGS` column
