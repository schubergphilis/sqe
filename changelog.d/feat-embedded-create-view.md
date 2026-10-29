### Added

- Iceberg views in `sqe-cli --embedded`

    `CREATE [OR REPLACE] VIEW` and `DROP VIEW [IF EXISTS]` on a catalog
    attached with `TYPE iceberg_rest` store an Iceberg view through the
    coordinator's view code: the body's analyzed schema, an `sqe` SQL
    representation, `default-catalog` = the warehouse, and `OR REPLACE` as
    one `replaceView` commit. `SELECT` and `DESCRIBE` read through the view;
    its body resolves the catalog under the attach name or the warehouse
    name. Views need a bearer token on
    the attach; an OAuth2 attach refuses them. `SHOW TABLES` does not list
    views yet.

### Fixed

- Embedded `CREATE VIEW` on an Iceberg catalog no longer creates an empty table

    On the SQLite warehouse, `CREATE VIEW iceberg.s.v`
    registered an empty Iceberg table named `v`. The SQLite catalog stores no
    views, so the statement now fails with `stores no Iceberg views`; `DROP
    VIEW IF EXISTS` there is a no-op.
