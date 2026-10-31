### Added

- `SHOW CREATE VIEW` on the coordinator renders the view's SQL

    `SHOW CREATE VIEW cat.ns.v` returns `CREATE VIEW cat.ns.v AS` and the
    stored body of the Iceberg view, in a column named `Create View`. It
    printed the view's columns as a `CREATE TABLE` before.

### Changed

- `SHOW CREATE TABLE` on a view fails, as in Trino

    `SHOW CREATE TABLE` on a view fails with `is a view, not a table`, and
    `SHOW CREATE VIEW` on a table with `is a table, not a view`. The MCP
    tool `get_table_ddl` returns the `SHOW CREATE VIEW` output for a view.
