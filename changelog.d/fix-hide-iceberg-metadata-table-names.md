### Fixed

- `SHOW TABLES` and `information_schema.tables` list base tables only for
  ATTACHed and embedded Iceberg catalogs

    Every table `t` was listed four times: `t`, `t$snapshots`,
    `t$manifests` and `t$history`, all as `BASE TABLE`. The metadata tables
    are no longer listed.
