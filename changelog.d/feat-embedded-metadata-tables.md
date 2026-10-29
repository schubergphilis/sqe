### Added

- Iceberg metadata tables in embedded mode

    `sqe-cli --embedded` now answers `$snapshots`, `$history`,
    `$manifests`, `$files`, `$partitions` and `$refs` on the `--warehouse`
    catalog and on attached Iceberg catalogs, for example
    `SELECT * FROM iceberg.s."t$snapshots"`. They failed with "table
    function 'table_snapshots' not found". The catalog in front of the
    table name picks the catalog that is read; an unqualified name works
    only when one Iceberg catalog is mounted. Cluster behaviour is
    unchanged.
