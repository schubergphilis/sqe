### Added

- `ALTER TABLE t SET|UNSET TAGS ('k' = 'v', ...)` and
  `ALTER SCHEMA s SET|UNSET TAGS (...)` tag a table or schema itself (#922)

    The Databricks object form. Table tags are stored in the `sqe.table-tags`
    table property, schema tags in the `sqe.schema-tags` namespace property,
    both as a JSON list in the column-tag format. They are metadata only: no
    policy matches them and nothing is projected into Ranger. The write takes
    the `tag_authority` gate (under `ranger-delegate`, the privilege on the
    table, or on the database with table `*` for a schema; a column grant does
    not count) and the `tag_names` check on the keys a `SET` adds. Writing
    `sqe.table-tags` through `SET TBLPROPERTIES` is admin-only in every mode.
    `ALTER CATALOG ... SET TAGS` and tags on Hive tables or roots are refused.
