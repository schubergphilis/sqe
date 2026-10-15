### Added

- `CALL system.drop_table(table => 'ns.t', purge => true)` deletes the table's files (#836)

    It used to return "not yet implemented". After the catalog drop, SQE
    deletes everything under the table's location with the caller's own
    storage credentials, synchronously, and writes an `AdminDdl` audit
    event. It does not use the REST catalog's purge flag, which in Polaris
    needs `DROP_WITH_PURGE_ENABLED` and deletes later with the catalog's
    credentials. Before dropping anything it refuses when `gc.enabled` is
    false, the location is a bucket root, a write path or the metadata file
    sits outside the location, or another table in the catalog shares the
    location (for example a second identifier from `register_table`). The
    result gains a `purge` column; `status` is `dropped` or `purged`. SQL
    `DROP TABLE ... PURGE` is unchanged.
