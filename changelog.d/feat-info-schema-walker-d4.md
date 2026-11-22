### Changed

- `information_schema` reads walk only the catalogs, schemas and tables the query names (#939)

    `information_schema.tables`, `columns`, `views` and `schemata` no longer list and load every table in every catalog before filtering. The walk is limited by the reference's catalog qualifier and by the query's `table_catalog` / `catalog_name`, `table_schema` and `table_name` filters: equality, exact `LIKE`, a literal `IN` list, or an `OR` of those on one column. dbt-sqe's `get_columns_in_relation` now loads one table, and `get_catalog` lists only the schemas it asks for.

    **Behaviour change:** a qualified read returns only its own catalog's rows, as in Trino. `"a".information_schema.tables` used to list every registered catalog. A bare `information_schema` read with a session catalog set is limited to that catalog. A qualifier that contradicts a `table_catalog` filter returns no rows.

### Fixed

- A table the user may not read no longer fails an `information_schema` listing (#939)

    While walking, a table whose load the catalog refuses (403, access denied) is left out of `information_schema`, as Trino does. Any other failure still fails the query (#890): an unreachable catalog (5xx) or a schema SQE cannot convert. A `SELECT` on a denied table still reports access denied.
