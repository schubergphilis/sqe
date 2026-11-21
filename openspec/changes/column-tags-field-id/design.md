## Context

`sqe.column-tags` (`sqe-core::column_tags`) maps a column path (`email`, `addr.zip` for a nested field) to its tags. It is read by:

- the policy rewriter (`tag_source_impl::scan_column_tags` off the scan provider, `CacheTagSource` off the metadata cache);
- `SHOW TAGS`, `information_schema.column_tags`, `$properties`-style metadata TVFs;
- the Ranger tag projection (`catalog_ops/tag_projection.rs`);
- the read-modify-write paths in `catalog_ops.rs` (SET/UNSET TAGS, masking policy, ALTER RENAME/DROP via `rewrite_tag_keys`), `table_props.rs` (CREATE OR REPLACE carry-over) and the tag authority's live read.

## Decisions

### D1. Sibling property, not a new shape

A field-id-keyed `sqe.column-tags` would make every older SQE fail closed on every tagged table (the parse fails, the tag state is unknown) and breaks the Ranger projection and external readers that read names. A sibling property keeps the map readable by name everywhere; only an SQE that knows the sibling follows renames.

### D2. The ids are bound to the map by digest

Any writer that rewrites `sqe.column-tags` without `sqe.column-tag-ids` (an older SQE in a rolling upgrade, Spark `SET TBLPROPERTIES`, a data service) would leave ids that no longer describe the map. Trusting them could orphan an entry a newer write meant to apply: for example, `email` dropped and re-added, then tagged by an old writer, keeps the stale id and the new tag would be orphaned, unmasking the column.

**Invariant: a write of `sqe.column-tags` that does not also write matching ids never turns an entry into an orphan.** The ids property carries `tags_sha256`, the SHA-256 (lower-case hex) of the exact `sqe.column-tags` string it was computed for. A mismatch means the ids are ignored and the map applies by name, which is today's behaviour.

A malformed `sqe.column-tag-ids` (not JSON, wrong shape, unknown `v`) is treated like a malformed `sqe.column-tags`: the tag state is unknown and the rewriter fails closed.

### D3. Ids come from the committed schema only

The catalog assigns field ids at commit for CREATE, REPLACE and ADD COLUMN. SQE stamps ids only in commits that do not change the schema (SET/UNSET TAGS, masking policy, raw property writes), from the table's current schema under the write lock. Commits that change the schema:

- `ALTER TABLE RENAME COLUMN` / `DROP COLUMN` (SQE): ids carry over by key (`rewrite_tag_keys` renames or drops the id entry with the tag entry); the digest is recomputed;
- `CREATE OR REPLACE` / CTAS: the ids property is removed and the map applies by name until the next tag write records ids.

### D4. Resolve against the scan's schema

A time-travel or incremental scan exposes a historical schema. Resolution uses the scan's schema (the provider's snapshot schema), so an id resolves to the name the column had at that snapshot. `CacheTagSource` (fallback for providers the scan hook does not recognise, and the audit writer) resolves against the cached table's current schema.

### D5. Collisions take the union

A legacy entry `mail` and an id-resolved entry `email -> mail` both land on `mail`; the column gets the union of the keys (more masking, never less). A writer merges them into one `mail` entry.

### D6. Orphans

An orphaned entry is kept under its stored name with its stored id. A writer drops it only when a live column now has that name and the write sets that column's tags (the new column's entry replaces it). `SHOW TAGS` lists it with `applied = false`; `information_schema.column_tags` lists only applied entries (it is a view of columns).

### D7. Authority

`sqe.column-tag-ids` decides which entries apply: pointing an id elsewhere orphans the entry and unmasks its column, and removing the property after a foreign rename does the same. A raw `SET TBLPROPERTIES` or `UNSET TBLPROPERTIES` of the key (any case, any spelling including Trino `extra_properties`) is an admin's only.

## Risks

- Older SQE writers keep writing name-keyed maps without ids; D2 makes that safe (falls back to by-name). Mixed fleets lose rename-following until a new SQE writes the map.
- Hash cost: one SHA-256 of a small string per resolution; negligible next to the scan.
