### Fixed

- `CAST(x AS TIMESTAMP WITH TIME ZONE)` keeps the zone (#848)

    Sessions now run in UTC (`datafusion.execution.time_zone = '+00:00'`).
    Before, the cast returned a timestamp without a zone. `to_timestamp*` now
    returns zoned values as well, typed like an Iceberg `timestamptz` column.
    View bodies and worker sessions use the same zone. An offset in the cast
    text is applied, and the value is stored in UTC. `now()` and
    `current_timestamp` still return a timestamp without a zone.

- Trino clients receive zoned timestamps as `<local> <zone>` (#848)

    `2024-01-02 01:04:05.000000 UTC` instead of Arrow's
    `2024-01-02 01:04:05.000000Z`. The Python client splits the zone off at
    the space.

- Filters on nanosecond timestamp columns no longer fail (#848)

    A filter on a V3 `timestamp_ns` / `timestamptz_ns` column failed while
    planning ("Interval data types must be compatible ... Int64"), because
    the manifest bounds came through as Int64 column statistics. After that,
    the Iceberg pushdown failed with "Can't convert datum from timestamp type
    to timestamptz_ns type". Statistics now take the column's own type, and a
    pushed-down timestamp literal is converted to its column's unit and zone.
    A literal with a sub-microsecond part on a microsecond column is no
    longer truncated: that conjunct is left to DataFusion instead of
    pruning with a rounded value.

- A filter on a lossy `CAST(col AS ...)` no longer fails (#848)

    File pruning treated `CAST(col AS t) = x` as `col = x`, so
    `CAST(f AS INT) = 3` on a REAL column, `CAST(s AS INT) = 3` on a string
    and `CAST(ts AS DATE) = DATE '...'` failed with "Can't convert datum
    from int type to float type" and similar. Only casts that keep every
    value (integer widening, string to string, the same timestamp unit and
    zone) are pruned through now; any other cast is left to DataFusion.
