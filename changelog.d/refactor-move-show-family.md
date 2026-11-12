### Changed

- The SHOW / DESCRIBE statement handlers moved from `query_handler.rs` into `query_handler/show.rs` (no behaviour change)

    The second statement family moved into its own child module: SHOW CREATE TABLE/VIEW/SCHEMA,
    SHOW CATALOGS, SCHEMAS, COLUMNS, TABLES, VIEWS, FUNCTIONS and ROLES, with the hidden-table
    check they share. `query_handler.rs` goes from 13,062 to 12,236 lines and its size ceiling
    drops from 13,500 to 12,500.
