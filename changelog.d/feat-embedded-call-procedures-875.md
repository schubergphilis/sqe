### Added

- `sqe-cli --embedded` runs six `CALL system.*` maintenance procedures (#875)

    `expire_snapshots`, `rewrite_manifests`, `set_current_snapshot`,
    `rollback_to_snapshot`, `register_table` and `table_health` run against
    an attached Iceberg catalog and return the cluster's result columns. A
    table they change is re-read by the next query. The other procedures say
    they are not available in embedded mode before they touch the catalog.
    Embedded mode has no role tiers: the catalog's commit authorization is
    the gate. The six procedures now share one implementation in `sqe-write`
    with the coordinator, whose behaviour, tiers and audit are unchanged.
