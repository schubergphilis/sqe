## Why

`sqe.column-tags` is keyed by column name (#920). Iceberg identifies a column by its field id: a rename keeps the id, a drop retires it. SQE's own `ALTER TABLE` rewrites the tag keys in the same commit, but Spark, PyIceberg and other clients commit schema changes to the same catalog without touching the property:

- after a foreign rename, the tag stays on the old name and the renamed column is unmasked;
- after a foreign drop and an add under the same name, the new column inherits the old column's tags.

Unity Catalog keys tags by column identity, and a data service built on SQE has to reconcile renames itself; between a rename and its next pass the column is unmasked.

## What changes

- A sibling table property `sqe.column-tag-ids` records, for each key of `sqe.column-tags`, the Iceberg field id the key named when SQE last wrote the map, bound to that map by a SHA-256 of its exact value: `{"v":1,"tags_sha256":"<hex>","ids":{"email":3,"addr.zip":12}}`. `sqe.column-tags` keeps its shape, so older SQE versions and the Ranger tag projection keep reading it by name.
- Readers resolve the map against the schema the scan exposes:
  - an entry whose recorded id is in that schema applies to the column's current name (a foreign rename is followed);
  - an entry whose recorded id is not in that schema is **orphaned**: kept and shown, not applied (a foreign drop, or a drop and re-add under the same name);
  - an entry with no recorded id applies by name, as today (a table written by an older SQE);
  - a digest that does not match the live `sqe.column-tags` value means another writer changed the map; the ids are ignored and every entry applies by name, as today.
- SQE's tag writers read the resolved map, apply their change, and write the map back under current names with ids taken from the table's current schema and a fresh digest, in the same commit.
- `SHOW TAGS` gains an `applied` column, false for an orphaned entry.
- A raw write or removal of `sqe.column-tag-ids` is an admin's only, under every tag authority mode.

## Out of scope

- Hive tables: no field ids; unchanged.
- Ranger's tag projection (#899) stays keyed by name; it projects the resolved current name.
- A key that names no column of the current schema and has no recorded id (a typo, or a legacy entry for a dropped column) applies by name as today, so a column later added under that name picks it up. The first SQE write records no id for it; it stays legacy until a column of that name exists at a write.
