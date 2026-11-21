# Design: tags on tables and schemas

## D1. One statement kind for object tags

Column tag statements (`SetTagsStatement`) carry per-column ops, and every consumer assumes columns: the gate, the write, the Ranger projection. Object tags get their own `ObjectTagsStatement { object: TagObject, action, tags, values }` and their own `StatementKind::SetObjectTags`, so every exhaustive `match` on the statement kind has to handle it.

The native column form starts its list with an identifier (`SET TAGS (col = ('pii'))`). The Databricks object form starts with a string literal (`SET TAGS ('k' = 'v')`). A leading `'` picks the object form. A list that mixes the two is refused.

## D2. Storage

| Object | Where | Key |
|---|---|---|
| Table | Iceberg table property | `sqe.table-tags` |
| Schema | Iceberg namespace property | `sqe.schema-tags` |

The value is a JSON list in the `sqe.column-tags` entry format: `["pii", {"owner": "finance"}]`. An SQE that predates object tags ignores the key. Tags compare and are removed by key; setting a key again replaces its value, as Databricks does.

UNSET of the last tag writes `[]` instead of removing the property. The `Catalog` trait's `update_namespace` takes no removals, so both objects behave the same, and readers treat `[]` and a missing key alike.

`sqe_core::object_tags` owns the keys and the parse, serialise and apply functions. !1890's views read through it, so the two MRs cannot drift on the format.

## D3. Metadata only

Object tags are not passed to `TagSource`, the plan rewriter or the tag projector. A tag policy on `pii` masks a column tagged `pii`, and does not mask a table tagged `pii`. The docs say so. Databricks also uses table tags for governance tooling rather than for masking.

## D4. Authority

| Mode | Table tag | Schema tag | Catalog tag |
|---|---|---|---|
| admin | yes | yes | refused (not supported) |
| `admin-role` | admins only | admins only | refused |
| `ranger-delegate` | `tag_privilege` on the table | `tag_privilege` on the database with table `*` | refused |

The table check uses `grants_table_privilege` with the policy key derived as the column check derives it, so the resource checked is the resource written. A schema check asks for the table `*`, which only an item covering every table of the database matches. A store error grants nothing.

`tag_names = "ranger-tagdefs"` checks the keys a statement adds. A key already on the object is not re-checked. `require_defined_tags` takes the catalog rather than a table name, so tables and schemas both get the `<catalog>.<tag>` rule.

## D5. The raw property back door

`ALTER TABLE t SET TBLPROPERTIES ('sqe.table-tags' = ...)` would write the same property and skip D4. Any statement that writes `sqe.table-tags` in any case, through raw options or `extra_properties`, is admin-only in every mode. Unlike column tags, no per-key diff is needed: no client writes the property this way, and the error points at `SET TAGS`. `CREATE SCHEMA` sends no properties, so `sqe.schema-tags` has no raw writer.

## D6. Concurrency

A tag write is a read-modify-write of one property. Neither an Iceberg table commit nor a namespace property update can compare-and-set a property. Table writes hold the per-table lock `set_column_tags` uses and evict every token's cached copy afterwards. Schema writes hold a per-namespace lock keyed by catalog and namespace. SQE does not cache namespace properties.

## D7. Catalogs

Hive tables are refused, as column tags are. A catalog without `update_namespace` (Hive, unconfigured) returns its own error.
