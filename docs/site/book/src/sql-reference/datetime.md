# Date and time

The largest single category in SQE. Two layers stack:

1. **DataFusion native** functions: `date_part`, `date_trunc`, `date_bin`, `extract`, `now`, the `to_timestamp_*` family, `make_date`. Powerful but uses DataFusion-specific names.
2. **Trino aliases** registered by `sqe-trino-functions`: `year()`, `month()`, `day()`, `date_add()`, `date_diff()`, `format_datetime()`. These cover every Trino date function so dbt-trino models work unmodified.

Snowflake and Spark also map well via the Trino layer.

## Construction and current value

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `now()` | `sqe-trino-functions` | Returns `TIMESTAMP(6)`. Stable within one query. `trino_functions.rs:55` | `now` | `current_timestamp` | `now` / `current_timestamp` | `now` / `current_timestamp` |
| `current_timestamp` | `datafusion-builtin` | Same value as `now()`. SQL keyword, no parens. | `current_timestamp` | `current_timestamp` | `current_timestamp` | `current_timestamp` |
| `current_date` | `datafusion-builtin` | Today's date in session timezone. | `current_date` | `current_date` | `current_date` | `current_date` |
| `current_time` | `datafusion-builtin` | Current wall-clock time. | `current_time` | `current_time` | `current_time` | `current_time` |
| `localtime()` | `sqe-trino-functions` | Local time-of-day. Returns `TIME(6)`. `trino_functions.rs:64` | `localtime` | - | - | - |
| `localtimestamp()` | `sqe-trino-functions` | Local timestamp without offset. Returns `TIMESTAMP(6)`. `trino_functions.rs:65` | `localtimestamp` | - | - | - |
| `current_timezone()` | `sqe-trino-functions (ext)` | Returns the session timezone string. `trino_functions_ext.rs:41` | `current_timezone` | - | `current_timezone` | - |
| `make_date(year, month, day)` | `datafusion-builtin` | Construct a `DATE`. | - | `date_from_parts` | `make_date` | `make_date` |

## Extraction (year, month, day, ...)

These return integer parts. SQE registers Trino names; DataFusion's `extract` and `date_part` are also available for SQL standard syntax.

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `year(d)` | `sqe-trino-functions` | `trino_functions.rs:28` | `year` | `year` | `year` | `year` |
| `month(d)` | `sqe-trino-functions` | `trino_functions.rs:29` | `month` | `month` | `month` | `month` |
| `day(d)` | `sqe-trino-functions` | `trino_functions.rs:30` | `day` / `day_of_month` | `day` | `day` | `day` |
| `hour(d)` | `sqe-trino-functions` | `trino_functions.rs:31` | `hour` | `hour` | `hour` | `hour` |
| `minute(d)` | `sqe-trino-functions` | `trino_functions.rs:32` | `minute` | `minute` | `minute` | `minute` |
| `second(d)` | `sqe-trino-functions` | `trino_functions.rs:33` | `second` | `second` | `second` | `second` |
| `millisecond(d)` | `sqe-trino-functions (ext)` | Sub-second component. `trino_functions_ext.rs:27` | `millisecond` | - | - | - |
| `day_of_week(d)` | `sqe-trino-functions` | ISO: 1=Mon..7=Sun. `trino_functions.rs:34` | `day_of_week` / `dow` | `dayofweek` | `dayofweek` | `dayofweek` |
| `day_of_year(d)` | `sqe-trino-functions` | 1..366. `trino_functions.rs:35` | `day_of_year` / `doy` | `dayofyear` | `dayofyear` | `dayofyear` |
| `quarter(d)` | `sqe-trino-functions` | 1..4. `trino_functions.rs:36` | `quarter` | `quarter` | `quarter` | `quarter` |
| `week(d)` | `sqe-trino-functions` | ISO week number. `trino_functions.rs:37` | `week` / `week_of_year` | `weekofyear` | `weekofyear` | `weekofyear` |
| `extract(<part> from d)` | `datafusion-builtin` | SQL standard. Parts: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DOW`, `DOY`, `EPOCH`, `QUARTER`, `WEEK`. | `extract` | `extract` | `extract` | `extract` |
| `date_part('year', d)` | `datafusion-builtin` | Function form of `extract`. | `date_part` | `date_part` | - | `date_part` |

## Truncation and binning

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `date_trunc('month', d)` | `datafusion-builtin` | Round down to a calendar boundary. Parts: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`, `microsecond`. | `date_trunc` | `date_trunc` | `date_trunc` | `date_trunc` |
| `date_bin(stride, d)` | `datafusion-builtin` | Bin into fixed-width buckets, e.g. `INTERVAL '15' minutes`. SQE-relevant for time-series rollups. | - | `time_slice` | - | `time_bucket` |
| `last_day_of_month(d)` | `sqe-trino-functions (ext)` | Last calendar day of `d`'s month. `trino_functions_ext.rs:43` | `last_day_of_month` | `last_day` | `last_day` | `last_day` |

## Arithmetic

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `date_add(unit, n, d)` | `sqe-trino-functions` | `unit` is a string: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`. `trino_functions.rs:40` | `date_add` | `dateadd` | `date_add` (different shape) | `date_add` |
| `date_diff(unit, d1, d2)` | `sqe-trino-functions` | Difference `d2 - d1` in `unit`s. `trino_functions.rs:41` | `date_diff` | `datediff` | `datediff` | `date_diff` |
| `d + INTERVAL '5' DAY` | `datafusion-builtin` | SQL standard interval arithmetic. | yes | yes | yes | yes |
| `d - INTERVAL '1' MONTH` | `datafusion-builtin` | Subtraction works the same way. | yes | yes | yes | yes |

`date_add` example:

```sql
SELECT date_add('day', 7, DATE '2026-05-08');     -- 2026-05-15
SELECT date_add('month', -1, DATE '2026-05-08');  -- 2026-04-08
SELECT date_add('hour', 36, TIMESTAMP '2026-05-08 09:00:00');  -- 2026-05-09 21:00:00
```

`date_diff` example:

```sql
SELECT date_diff('day', DATE '2026-01-01', DATE '2026-01-06');   -- 5
SELECT date_diff('month', DATE '2026-01-01', DATE '2026-04-01'); -- 3
SELECT date_diff('year', DATE '2020-01-01', DATE '2026-01-01');  -- 6
```

## Formatting and parsing

Two parallel formatting families:

- **Trino / MySQL `%Y-%m-%d` style** via `date_format` / `date_parse`. Familiar to anyone who's used `strftime`.
- **Java / Joda `yyyy-MM-dd` style** via `format_datetime` / `parse_datetime`. The Trino default for new code.

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `date_format(d, fmt)` | `sqe-trino-functions` | Trino / MySQL specifiers (`%Y`, `%m`, `%d`, `%H`, `%i` for minutes, `%s` for seconds). `trino_functions.rs:51` | `date_format` | `to_char` | `date_format` | `strftime` |
| `date_parse(str, fmt)` | `sqe-trino-functions` | Inverse of `date_format`. Returns `TIMESTAMP(6)`. `trino_functions.rs:52` | `date_parse` | `to_timestamp` | `to_timestamp` | `strptime` |
| `format_datetime(d, fmt)` | `sqe-trino-functions (ext)` | Java / Joda specifiers (`yyyy-MM-dd HH:mm:ss`). `trino_functions_ext.rs:54` | `format_datetime` | - | `date_format` | - |
| `parse_datetime(str, fmt)` | `sqe-trino-functions (ext)` | Inverse of `format_datetime`. `trino_functions_ext.rs:55` | `parse_datetime` | - | `to_timestamp` | - |
| `to_iso8601(d)` | `sqe-trino-functions (ext)` | RFC 3339 / ISO 8601 string. `trino_functions_ext.rs:40` | `to_iso8601` | - | - | - |
| `from_iso8601_date(str)` | `sqe-trino-functions (ext)` | ISO 8601 -> `DATE`. `trino_functions_ext.rs:38` | `from_iso8601_date` | - | - | - |
| `from_iso8601_timestamp(str)` | `sqe-trino-functions (ext)` | ISO 8601 -> `TIMESTAMP(6)`. `trino_functions_ext.rs:39` | `from_iso8601_timestamp` | - | - | - |
| `to_char(d, fmt)` | `datafusion-builtin` | Postgres-style. Specifiers differ from Trino's `date_format`. | - | `to_char` | `date_format` | `strftime` |
| `to_date(str [, fmt])` | `datafusion-builtin` | Parse to `DATE`. | - | `to_date` | `to_date` | `strptime` |
| `to_timestamp(str [, fmt])` | `datafusion-builtin` | Parse to `TIMESTAMP`. | `parse_datetime` | `to_timestamp` | `to_timestamp` | `strptime` |
| `to_timestamp_seconds(epoch)` | `datafusion-builtin` | Cast a unix epoch to `TIMESTAMP(0)`. | - | `to_timestamp` | - | `to_timestamp` |
| `to_timestamp_millis(ms)` | `datafusion-builtin` | Cast millis to `TIMESTAMP(3)`. | - | - | - | - |
| `to_timestamp_micros(us)` | `datafusion-builtin` | Cast micros to `TIMESTAMP(6)`. | - | - | - | - |
| `to_timestamp_nanos(ns)` | `datafusion-builtin` | Cast nanos to `TIMESTAMP(9)`. V3 Iceberg ns timestamps. | - | - | - | - |

Trino-style example:

```sql
SELECT date_format(TIMESTAMP '2026-05-08 14:30:00', '%Y-%m-%d %H:%i:%s');
-- 2026-05-08 14:30:00

SELECT date_parse('2026-05-08 14:30:00', '%Y-%m-%d %H:%i:%s');
-- 2026-05-08 14:30:00 (TIMESTAMP)
```

Java-style example (preferred for new code):

```sql
SELECT format_datetime(TIMESTAMP '2026-05-08 14:30:00', 'yyyy-MM-dd HH:mm:ss');
-- 2026-05-08 14:30:00
```

## Unix epoch

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `from_unixtime(epoch)` | `sqe-trino-functions` | Seconds since epoch -> `TIMESTAMP(6)`. `trino_functions.rs:42` | `from_unixtime` | - | `from_unixtime` | `to_timestamp` |
| `to_unixtime(d)` | `sqe-trino-functions` | `TIMESTAMP` -> seconds since epoch (Double). `trino_functions.rs:43` | `to_unixtime` | - | `unix_timestamp` | `epoch` |
| `extract(epoch from d)` | `datafusion-builtin` | SQL-standard alternative for `to_unixtime`. | yes | - | yes | yes |

## Time zones

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `with_timezone(d, tz)` | `sqe-trino-functions (ext)` | Attach a timezone offset to a naive timestamp. `trino_functions_ext.rs:50` | `with_timezone` | `convert_timezone` | `from_utc_timestamp` | - |
| `at_timezone(d, tz)` | `sqe-trino-functions (ext)` | Convert a `TIMESTAMP WITH TIME ZONE` to another zone. `trino_functions_ext.rs:51` | `at_timezone` | `convert_timezone` | - | - |
| `timezone_hour(d)` | `sqe-trino-functions (ext)` | Hour component of the offset. `trino_functions_ext.rs:70` | `timezone_hour` | - | - | - |
| `timezone_minute(d)` | `sqe-trino-functions (ext)` | Minute component of the offset. `trino_functions_ext.rs:71` | `timezone_minute` | - | - | - |
| `expr AT TIME ZONE 'Europe/Amsterdam'` | `datafusion-builtin` | SQL-standard syntax. | yes | yes | yes | yes |

## Misc

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `human_readable_seconds(n)` | `sqe-trino-functions (ext)` | Format seconds as `"1d 4h 30m"`. Useful in monitoring queries. `trino_functions_ext.rs:42` | `human_readable_seconds` | - | - | - |

## Worked example

A daily revenue rollup, time-zone aware, formatted for human reports:

```sql
SELECT
    date_format(
        with_timezone(date_trunc('day', order_ts), 'Europe/Amsterdam'),
        '%Y-%m-%d'
    ) AS day_local,
    quarter(order_ts) AS qtr,
    day_of_week(order_ts) AS dow,
    SUM(amount) AS revenue
FROM orders
WHERE order_ts >= TIMESTAMP '2026-01-01 00:00:00' AT TIME ZONE 'UTC'
GROUP BY 1, 2, 3
ORDER BY 1;
```

## Iceberg V3 nanosecond timestamps

`TIMESTAMP_NS` and `TIMESTAMP_NS WITH TIME ZONE` only exist in Iceberg format-version 3. Adding such a column to a CREATE TABLE auto-bumps the table format version. All datetime functions operate on these the same way they operate on `TIMESTAMP(6)`; the underlying Arrow type is `Timestamp(Nanosecond, ...)` instead of `Timestamp(Microsecond, ...)`.

When you query a V3 ns column from a Trino client, Trino downscales to microseconds. SQE keeps the full precision when serving Arrow Flight SQL clients.
