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

### D5. Table and schema tags are projected into Ranger

The first MR kept the projection on own tags, so a table tag masked in SQE and not in Spark. #935 part 1 projects table tags too.

- `sqe.table-tags` goes into Ranger's tag store on two resources of the tag service, in one import document: `{database, table, column: *}` and `{database, table}`. Kyuubi's Ranger plugin does not apply a tag on a bare table resource to a column mask request (checked live on the Spark stack: the bare resource left every column raw, the `column: *` one masked within seconds), so the every-column resource carries masks. The bare table resource carries tag row filters, which are table-level requests. A column is never named `*`, so the every-column resource cannot clash with a column's own resource.
- Order, as for column tags: what a `SET TAGS` adds is in Ranger before the Iceberg commit (a failure changes nothing); what an `UNSET TAGS` removes is deleted after it (a failure leaves Spark over-masking, logged).
- Ranger's tag import replaces: `add_or_update` sets a resource's tag list to exactly the list sent, and `delete` drops the resource whatever tags it names (checked live). A table-level change that leaves tags is therefore sent as one `add_or_update` with nothing deleted first; a delete followed by an add would leave Spark unmasked between the two calls. Only a change to no tags deletes. The column-level projection still deletes then adds (a follow-up in sqe-policy).
- **Never shrink on an error.** In Spark a table without projected tags is not masked, so "nothing projected" fails open. When the tag load observer finds a malformed `sqe.table-tags`, it logs an error and leaves the table-level resources as they were; own column tags still project. There is no metric or audit event for projection; the error log is the signal.
- `CALL system.reproject_column_tags` projects table tags as well, add-only.
- Spark cannot be told to refuse a table whose projection is stale. Between a tag written outside SQE and SQE's next load of the table, Spark enforces the old projection.
- Where a column's own tag and the table tag both carry a mask, SQE applies the own tag (D1). Kyuubi rendered the same value on the live stack (portable own-tag mask over a `MASK_NULL` table-tag mask: both engines showed the own-tag value). That is Ranger's tag policy order, not a rule SQE controls, so the parity e2e prints the pair rather than asserting it.

Part 2 projects schema tags onto the schema-wide resources `{database, table: *, column: *}` and `{database, table: *}`, only with `inherit_schema_tags` on. Kyuubi applies both: a `MASK_NULL` tag mask and a tag row filter on them reached `ac.orders` in Spark on the live stack. Catalog tags are not inherited and not projected.

- `ALTER SCHEMA ... SET|UNSET TAGS` keeps the order above: before the namespace commit Ranger gets the union of what it holds, the old list and the new one (a failure refuses the statement and changes nothing); after it, the exact new list, or a delete when the schema has no tags left (a failure over-masks, logged). The projection is serialised per catalog and schema with the load observer.
- The load observer reads the schema's tags in the same step as the table (D8) and only grows the schema-wide resource: a key the schema carries that Ranger does not hold yet is added, as one replace of the held list plus the new keys. It never removes a key; `ALTER SCHEMA` and `reproject_column_tags` do. When the schema cannot be read (`Unknown`), the observer logs an error and leaves the schema-wide resource as it is, while the table's own column and table tags still project.
- `CALL system.reproject_column_tags` writes each target's schema-wide resource exactly, as a row named `<schema>.*`. An unreadable schema is an error row and Ranger keeps what it holds.
- What is held is remembered per process. A coordinator that has not projected a schema yet sends what it reads, so with two coordinators a key removed on one can come back from the other's memory until a statement or a repair writes the schema again: Spark over-masks, it does not leak. Turning the flag off or dropping a schema leaves the schema-wide resources in Ranger (over-mask) until they are deleted by hand.
- Which of a column's own tag, the table's and the schema's wins when they carry different masks is #942; the parity e2e covers a schema tag alone.

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
- Spark and SQE differ on a table or schema whose tags were written outside SQE until SQE next loads a table of it, and on a removed schema tag that another coordinator still remembers (D5); both over-mask or under-mask only in that window, and Spark cannot be told to refuse a stale projection.
