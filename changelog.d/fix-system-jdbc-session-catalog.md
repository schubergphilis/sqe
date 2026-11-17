### Fixed

- DBeaver and other Trino JDBC clients can browse tables and views

    `system.jdbc` now serves every table the Trino JDBC driver reads:

    - `table_types` (`TABLE`, `VIEW`);
    - empty `procedures`, `procedure_columns`, `udts`, `super_types`, `super_tables`, `attributes` and `pseudo_columns` tables.

    Before, `getTableTypes()`, `getProcedures()` and similar metadata calls failed. `system.jdbc.tables` also lists views, as `VIEW`.
