## Why

Stock dbt-trino talks to SQE through the Trino HTTP port. Its table materialization sends the model's `properties` config as a Trino `WITH (...)` list:

```sql
create table <rel>
  comment '<description>'          -- with persist_docs.relation
  WITH (format_version = 3, partitioning = ARRAY['day(ts)'], sorted_by = ARRAY['id'])
as (<model sql>);
```

Before #897 SQE stored every `WITH` item as a plain Iceberg table property and acted on none of them. The table came out at the server default format version, unpartitioned and unsorted, and nothing reported a problem. A model with both `persist_docs` and `properties` failed to parse. Trino refuses a property it does not know; SQE kept it silently. So a typo, or a property SQE cannot honour, went unreported.

## What Changes

- **Done in the first MR (format version and comment):**
  - CREATE TABLE and CTAS take the format version from `format_version` (a number or a string), `'format-version'` or `TBLPROPERTIES ('format-version' = ...)`.
  - `ALTER TABLE ... SET PROPERTIES format_version = N` upgrades the table and refuses a downgrade.
  - The front door folds `comment '...'` beside an options list into it as the `comment` property.
  - A `WITH` list beside `TBLPROPERTIES` joins it; sqlparser kept only the last list.
- **This change:** the rest of the Trino Iceberg connector's table property vocabulary. Each property is either honoured or refused with an error naming it. None is stored and ignored. See `design.md` for the mapping.
- An unknown unquoted `WITH` key is an error. Quoted keys and `TBLPROPERTIES` stay Iceberg properties, passed through as written.
- `ALTER TABLE ... SET PROPERTIES` takes the same vocabulary. `partitioning` becomes partition evolution and `sorted_by` replaces the sort order.
- The Trino compatibility docs list every supported property and what it maps to.

## Capabilities

### New Capabilities
- `trino-table-properties`: Trino `WITH (...)` table properties on CREATE TABLE, CTAS, CREATE OR REPLACE and ALTER TABLE SET PROPERTIES.

### Modified Capabilities
- none

## Impact

- `crates/sqe-write`: create-table option handling (`write_handler/table_props.rs` and a new module for the vocabulary), `catalog_ops::set_table_properties`.
- `crates/sqe-sql`: `partition.rs` (clause reordering, comment fold), the classifier's `SET PROPERTIES` rewrite.
- **Behaviour change:** `WITH (foo = 'bar')` with an unquoted unknown key used to store `foo`, and is now an error. Write `WITH ('foo' = 'bar')` or `TBLPROPERTIES` to set an arbitrary property.
- dbt-trino `on_table_exists = replace` needs the atomic schema-changing replace (#892) to keep a format upgrade. The default `rename` mode creates a fresh table and needs only this change.
