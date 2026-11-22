## Context

Tag enforcement reads a column's tags in two places, both in `tag_source_impl` (`sqe-write`, re-exported by `sqe-coordinator`):

- `scan_column_tags` reads the tags off the scan's Iceberg provider, at the snapshot the scan reads;
- `CacheTagSource` reads them from the shared metadata cache. It serves the DML policy probe (`write_predicates::extract_write_predicates` rewrites a synthetic scan that has no provider) and scans the provider hook does not recognise.

Both return a column -> tag keys map, or "unknown" (the rewriter then refuses the read). Object tags live in `sqe.table-tags`, `sqe.schema-tags` and `sqe.catalog-tags`, in the same entry format (`sqe_core::object_tags`).

## Decisions

### D1. Effective tags: own tags, then inherited, own key wins

A column's effective tags are its own entries followed by each parent entry whose key the column does not carry itself. A key carried by both keeps the column's value. Enforcement matches keys only, so the override matters for values shown to users and for future value-matching policies, not for which policy applies.

`object_tags::inherit` / `inherit_onto` implement this; `tag_source_impl::effective_column_tags` applies it to a table's properties and schema. Every column of the schema gets the table tags, including columns that carry no own tag.

### D2. Enforcement reads effective tags; write authority reads own tags

Masks, row filters, conflict detection and `SHOW STATS` use effective tags. The tag authority (SET/UNSET TAGS on columns and tables, the removal check of #927, the raw-property guards) stays on the tags written at that level. Otherwise untagging a column would require authority over a table tag the statement does not touch.

### D3. Unreadable parent tags fail closed

A malformed `sqe.table-tags` makes `effective_column_tags` an error, and both tag sources report the tag state as unknown. The rewriter refuses every read of the table. This is the same rule as a malformed `sqe.column-tags`: SQE never runs a query without a policy it could not read. `information_schema` tag views may show no rows for such a table; that is not enforcement.

For the second MR the same rule covers schema and catalog tags: a 403, any other error or a malformed property refuses the table.

### D4. Conflicts are refused at read time

A table tag adds the table's masks to every column. Under `mask_precedence = "refuse"`, a column whose own tag has a different mask now holds two masks and the read is refused (#918). No write-time check covers table tags: the write-time conflict guard is Ranger-specific and checks column tag writes. The read refusal is the backstop.

### D5. The Ranger tag projection stays on own tags

**Inherited tags are enforced by SQE only; the Ranger tag projection (and so Spark) still sees own tags.** This change gives no two-engine guarantee.

`catalog_ops/tag_projection.rs` and the tag load observer project `sqe.column-tags` into Ranger's tag store, so Spark's Ranger plugin masks the same columns. They keep projecting own tags only. A table tag therefore masks in SQE and not in Spark until the projection learns inheritance, with the same override as D1 (#935, part of #924). This is no regression for Spark: table tags never masked columns before. It does keep #924 open.

### D6. Freshness follows the table metadata

Table tags are read from the same table metadata as column tags: the provider's snapshot for a scan, the metadata cache for the DML probe. SQE's own SET/UNSET TAGS on a table invalidates the cached table for every token, so the next statement sees the change. A tag written by another client is seen when the cached metadata expires, as for column tags.

For schema and catalog tags (second MR), the parent properties are read through a cache whose TTL is no longer than the policy refresh interval, and SQE's own SET/UNSET TAGS on a schema or catalog invalidates it.

### D7. Views and Hive tables are out of scope

`SET TAGS` has no view form (#922 tags tables, schemas and catalogs). A view is expanded into its base tables before the rewriter runs, so a query of a view inherits through the base tables' tags. Hive tables refuse object tags, so they have nothing to inherit.

### D8. Schema tags: behind a flag, read with the table, three states

Schema tag inheritance ships behind `policy.inherit_schema_tags`, default `false`. With the flag off SQE makes no extra catalog call and enforcement is exactly the table-level behaviour above; a deployment turns it on once the tools that manage tags understand that a schema tag now masks data.

With the flag on, every table load that builds a scan provider (the schema provider, time travel, incremental, prepared statements, the write paths) reads the namespace's `sqe.schema-tags` with the caller's token in the same step, and the provider carries a three-state value: not inherited (flag off), known tags, or unknown (a 403, any other error, or a malformed value). The provider constructor takes the value as an argument, so a construction site that forgets it does not compile; there is no default that means "no schema tags". Unknown refuses every read of the table (D3).

There is no namespace cache in this MR: one `get_namespace` per table load, only with the flag on. That keeps freshness exact (a schema tag set by any client applies to the next statement, for every user) and avoids a cross-token cache whose eviction would have to reach other users' entries. The cache of task 2.1 is a later performance change, measured first.

`CacheTagSource`, the fallback for a scan whose provider the hook does not recognise, cannot see schema tags; with the flag on it reports the tag state as unknown for an Iceberg table rather than enforce table tags alone.

Effective tags are column, then table, then schema: the nearest key wins (D1 applied twice). Catalog tags stay out: `ALTER CATALOG ... SET TAGS` is refused today, and their inheritance is left to #924.

## Risks

- Tagging a table with a key that a row filter matches filters every query of it. That is the intent, but a broad key (`gold`) with a filter now has a wider effect than before. Release notes call it out.
- Spark and SQE differ on table tags until D5's follow-up (#935) lands.
