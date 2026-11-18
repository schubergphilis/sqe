### Changed

- Integer literals are `integer` when they fit, as in Trino

    `SELECT 1`, `abs(-5)`, `CASE WHEN .. THEN 1 ..`, `VALUES (1), (2)` and
    `SELECT 1 UNION ALL SELECT 2` return `integer`, not `bigint`, and
    `CREATE TABLE AS SELECT 1` writes an `int` column. A literal past
    2147483647 stays `bigint`. A literal inside `WITH RECURSIVE` stays
    `bigint`.

### Fixed

- `integer`, `smallint` and `tinyint` arithmetic fails on overflow, as in Trino

    `2147483647 + 1` fails with `integer addition overflow` instead of
    wrapping to `-2147483648`. The same holds for `-` and `*` and for column
    values. `bigint` arithmetic still wraps.
