# Tags on tables and schemas (#922)

## Why

SQE keeps tags on columns only. Databricks also tags tables, schemas and catalogs (`ALTER TABLE t SET TAGS ('k' = 'v')`), and Unity Catalog clients read them back from `information_schema.table_tags`, `schema_tags` and `catalog_tags`. !1890 adds those views, but they return no rows because nothing writes the tags. Today `ALTER TABLE t SET TAGS ('k' = 'v')` is refused with "tags on a table are not supported yet".

## What Changes

- `ALTER TABLE <t> SET TAGS ('k' = 'v', 'k2')` and `ALTER TABLE <t> UNSET TAGS ('k')` tag the table itself. They are stored in the table property `sqe.table-tags`.
- `ALTER SCHEMA|DATABASE <s> SET TAGS (...)` and `UNSET TAGS (...)` tag a schema. They are stored in the namespace property `sqe.schema-tags`, written with the caller's token.
- Both properties hold a JSON list in the entry format `sqe.column-tags` uses for one column: a key is a string, a key with a value is a one-entry object.
- `ALTER CATALOG <c> SET|UNSET TAGS` tags a catalog. The tags are stored in the Polaris catalog property `sqe.catalog-tags`, written through the management API with the caller's token and the entity version it read, so Polaris decides who may write (catalog admins, as Unity Catalog asks for ownership or `MANAGE`). A catalog that is not Polaris REST is refused.
- Object tags are metadata. They do not drive masks, are not projected into Ranger, and do not change what a query returns. (Superseded for table tags by the change `tag-inheritance`: a table's tags are inherited by its columns.)
- Authority follows `[access_control] tag_authority`:
  - Admins pass.
  - Under `admin-role`, only admins.
  - Under `ranger-delegate`, a table tag needs the configured `tag_privilege` on the table, and a schema tag needs it on every table of the database (a Ranger item whose table resource is `*`).
  - `tag_names = "ranger-tagdefs"` applies to the keys a statement adds, with the same `<catalog>.<tag>` rule under `catalog_scoped`.
- Writing `sqe.table-tags` through `SET TBLPROPERTIES`, Trino `extra_properties` or `CREATE ... TBLPROPERTIES`, in any case, is admin-only. Without that guard, a plain ALTER would skip the gate.
- Vendor patch: the vendored iceberg-rust `RestCatalog::update_namespace` returns `FeatureUnsupported`, as upstream does. SQE implements it as a `POST .../namespaces/{namespace}/properties` that sends only the given keys.

## Capabilities

### New Capabilities

- `object-tags`: tags on tables and schemas, how they are stored, and who may write them.

### Modified Capabilities

- `tag-authority`: the raw property gate also covers `sqe.table-tags`.

## Impact

- `crates/sqe-core/src/object_tags.rs`: the property keys, and parsing, serialising and applying the list.
- `crates/sqe-sql/src/object_tags.rs`: the parser; classifier variant `SetObjectTags`.
- `crates/sqe-coordinator/src/query_handler/tag_authority.rs`: the gate.
- `crates/sqe-write/src/object_tags.rs`: the writes.
- `vendor/iceberg-rust/crates/catalog/rest/src/catalog.rs`: `update_namespace`.
- !1890's `table_tags` and `schema_tags` read these properties through `sqe_core::object_tags`.

## Not in this change

- Tags on views.
- `SHOW TAGS` with values and with object tags (a follow-up).
- Masks or row filters keyed on object tags.
