### Changed

- `to_iso8601` on a timestamp without a zone no longer ends in `Z`

    `to_iso8601(TIMESTAMP '2024-01-02 03:04:05')` returned
    `2024-01-02T03:04:05.000Z` and now returns `2024-01-02T03:04:05`, as
    Trino does: the `Z` claimed UTC for a value that has no zone. The
    fraction appears only when it is non-zero (`.123`, `.123456`), where
    Trino pads to the column's declared precision, so a stored
    `timestamp(6)` column still differs from Trino by its trailing zeros.
    Queries, dbt models or views that compare these strings, or expect the
    `Z`, see different text. Zoned timestamps are unchanged.

### Fixed

- `from_iso8601_timestamp` returned NULL for any input with a zone designator

    `'2024-01-02T03:04:05Z'`, `'...+02:00'`, `'...+0200'` and `'...+02'`
    are now parsed and returned as that instant in UTC wall clock, as are
    `hh:mm` without seconds and a bare date (midnight). Input without a
    designator is read as UTC, as before.

- Clock functions in one statement read one instant

    `now()`, `current_timestamp`, `localtimestamp` and `localtime` are
    folded to the query start time, as Trino and DataFusion's own `now()`
    do. Each call used to read the clock separately, so `SELECT now(),
    current_timestamp` differed by microseconds.
