**STATUS 2026-11-21: column tags follow the field id, not the name (`feat/column-tags-field-id-920`, #920).**

Design: `openspec/changes/column-tags-field-id/` (D1-D7).

- `sqe-core` `column_tags::ids`: the `sqe.column-tag-ids` value (`{"v":1,"tags_sha256":..,"ids":{name:id}}`), `resolve` (applied under current names plus orphans) and `record`. Ids bind only to the map whose digest they carry; a mismatch applies the map by name, malformed ids are an unknown tag state (fail closed).
- Read path: `scan_column_tags` resolves against the provider's scan schema (time travel reads the snapshot schema), `CacheTagSource`, SHOW TAGS (new `applied` column, orphans listed false), `information_schema.column_tags`, SHOW STATS, the metadata TVF governance check and the Ranger projection resolve against the schema they read.
- Write path: `sqe-write/src/live_tags.rs` reads the resolved map and records map + ids in the same commit. SET TAGS pins the schema id it resolved against; RENAME / DROP COLUMN records ids from the new schema; a rollback restores the exact property bytes. CREATE OR REPLACE removes the ids and keeps tags under current names.
- Raw writes: the coordinator's per-column diff and the ALTER re-check under the table lock compare the applied map before and after the write (the written map read with the ids the commit leaves), and the commit pins the schema checked. A raw `sqe.column-tag-ids` write is admin-only in every mode.
- `.config/budgets.json`: sqe-core 26000 -> 26300 (the new resolver and its tests), sqe-coordinator 71000 -> 71250 (the acceptance tests).

Gate:
- `make test-integration FILTER=column_tag_ids_920` -> 3 passed (foreign rename, foreign drop + re-add, map without ids + raw ids refused)
- `FILTER=tag` -> 41 passed; `FILTER=ctas_replace_shape` -> 4 passed; `FILTER=rename` -> 5 passed
- unit: sqe-write `live_tags tag replace_shape` 66, sqe-core `column_tags` 14, sqe-catalog `tag` 19, sqe-coordinator `tags_batch` 2
- clippy on sqe-core, sqe-catalog, sqe-write, sqe-coordinator clean; `cargo xtask budgets --check` ok

Not run: `make test-access-control` (the Ranger suite). The Ranger stack running on this machine belongs to another checkout. The raw-write diff under `ranger-delegate` changed, so run it before merging.

Left open: Hive tables have no field ids and still match by name. An SQE raw property write leaves the old ids in place; they no longer bind, so the map applies by name.

**NEXT:** #924 / #930 (track 1.14).
