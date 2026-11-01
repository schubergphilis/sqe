### Fixed

- Embedded Iceberg scans use every core

    `sqe-cli --embedded` read Iceberg tables through the vendored
    `IcebergTableProvider`. That provider scans in one partition and reloads
    the table from the catalog on every scan. Base tables now read through
    `SqeTableProvider`, the coordinator's provider. The parallel-scan
    physical rules now live in `sqe-catalog` and run in embedded mode too.
    On the 10M-row engine-bench, the Iceberg geomean drops from 0.56 s to
    0.36 s, and a filtered aggregate from 0.51 s to 0.16 s. Metadata tables
    (`t$snapshots` and others) still read through the vendored provider.

- A filter on a list, struct or map column of an Iceberg table no longer fails

    `WHERE tags IS NOT NULL` on an `ARRAY` column was pushed into the Iceberg
    scan, and Iceberg cannot bind a predicate on a nested field, so the query
    failed with `DataInvalid: Accessor for Field tags not found`. Such
    filters now stay in DataFusion. The rest of the conjunction still pushes
    down.
