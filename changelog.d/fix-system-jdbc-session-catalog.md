### Fixed

- DBeaver and other Trino JDBC clients can browse tables and views

    `system.jdbc` now serves every table the Trino JDBC driver reads:

    - `table_types` (`TABLE`, `VIEW`);
    - empty `procedures`, `procedure_columns`, `udts`, `super_types`, `super_tables`, `attributes` and `pseudo_columns` tables.

    Before, `getTableTypes()`, `getProcedures()` and similar metadata calls failed. `system.jdbc.tables` also lists views, as `VIEW`.

- Bare `current_user` works in SQL

    `SELECT current_user` failed with "No field named current_user", which also broke the Trino JDBC driver's `getUserName()`.

- `PreparedStatement.getMetaData()` works with the Trino JDBC driver before the statement runs

    `DESCRIBE OUTPUT` returns empty strings for Catalog, Schema and Table, as Trino does, instead of NULL. The driver threw a `NullPointerException` on NULL.
