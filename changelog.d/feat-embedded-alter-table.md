### Added

- `ALTER TABLE` in embedded mode

    On the `--warehouse` catalog and on attached Iceberg catalogs,
    `sqe-cli --embedded` now runs `ALTER TABLE` on the coordinator's DDL
    handler: `ADD`, `DROP`, `RENAME` and `ALTER COLUMN ... SET DATA TYPE`
    (nested `STRUCT` fields included), `SET TBLPROPERTIES`, partition
    evolution, `SET IDENTIFIER FIELDS`, `CREATE BRANCH` / `CREATE TAG` and
    `RENAME TO`. On a non-REST catalog such as the embedded SQLite one, the
    schema commit goes through the catalog's own `update_table` instead of
    failing with "requires the REST catalog backend". `SET TAGS` and masking
    policies still need a cluster.
