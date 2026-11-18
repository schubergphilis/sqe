# Date and time

The largest single category in SQE. Two layers stack:

1. **DataFusion native** functions: `date_part`, `date_trunc`, `date_bin`, `extract`, `now`, the `to_timestamp_*` family, `make_date`. Powerful but uses DataFusion-specific names.
2. **Trino aliases** registered by `sqe-trino-functions`: `year()`, `month()`, `day()`, `date_add()`, `date_diff()`, `format_datetime()`. These cover every Trino date function so dbt-trino models work unmodified.

Snowflake and Spark also map well via the Trino layer.

## Construction and current value

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `now()` | `sqe-trino-functions` | Returns `TIMESTAMP(6)`. Folded to the query start time, so every clock call in one statement returns the same instant. `trino_functions.rs:55` | `now` | `current_timestamp` | `now` / `current_timestamp` | `now` / `current_timestamp` |
| `current_timestamp` | `sqe-trino-functions` | Alias of `now()`: same function, same `TIMESTAMP(6)` type. SQL keyword, no parens. | `current_timestamp` | `current_timestamp` | `current_timestamp` | `current_timestamp` |
| `current_date` | `datafusion-builtin` | Today's date in session timezone. | `current_date` | `current_date` | `current_date` | `current_date` |
| `current_time` | `datafusion-builtin` | Current wall-clock time. | `current_time` | `current_time` | `current_time` | `current_time` |
| `localtime()` | `sqe-trino-functions` | Local time-of-day. Returns `TIME(6)`. `trino_functions.rs:64` | `localtime` | - | - | - |
| `localtimestamp()` | `sqe-trino-functions` | Local timestamp without offset. Returns `TIMESTAMP(6)`, the query start time like `now()`. `trino_functions.rs:65` | `localtimestamp` | - | - | - |
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
| `millisecond(d)` | `sqe-trino-functions` | Sub-second component in milliseconds, returns `BIGINT`. | `millisecond` | - | - | - |
| `day_of_week(d)` | `sqe-trino-functions` | ISO: 1=Mon..7=Sun, returns `BIGINT`. Alias `dow`. `trino_functions.rs:34` | `day_of_week` / `dow` | `dayofweek` | `dayofweek` | `dayofweek` |
| `day_of_year(d)` | `sqe-trino-functions` | 1..366, returns `BIGINT`. Alias `doy`. `trino_functions.rs:35` | `day_of_year` / `doy` | `dayofyear` | `dayofyear` | `dayofyear` |
| `quarter(d)` | `sqe-trino-functions` | 1..4. `trino_functions.rs:36` | `quarter` | `quarter` | `quarter` | `quarter` |
| `week(d)` | `sqe-trino-functions` | ISO week number, returns `BIGINT`. Alias `week_of_year`. `trino_functions.rs:37` | `week` / `week_of_year` | `weekofyear` | `weekofyear` | `weekofyear` |
| `year_of_week(d)` | `sqe-trino-functions` | ISO year of the week, returns `BIGINT`. Alias `yow`. | `year_of_week` / `yow` | `yearofweek` | - | `isoyear` |

`day`, `hour`, `minute`, `second`, `year` and `month` also accept an interval, and `day_of_month` is an alias of `day`. `to_milliseconds(interval)` returns the interval as `BIGINT` milliseconds.
| `extract(<part> from d)` | `datafusion-builtin` | SQL standard. Parts: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `DOW`, `DOY`, `EPOCH`, `QUARTER`, `WEEK`. | `extract` | `extract` | `extract` | `extract` |
| `date_part('year', d)` | `datafusion-builtin` | Function form of `extract`. | `date_part` | `date_part` | - | `date_part` |

## Truncation and binning

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `date_trunc('month', d)` | `sqe-trino-functions` | Round down to a calendar boundary. A `DATE` input returns a `DATE`, a timestamp keeps its type and zone, and a varchar is read as a timestamp. Parts: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`, `microsecond`. Shadows the DataFusion built-in for Trino return types. | `date_trunc` | `date_trunc` | `date_trunc` | `date_trunc` |
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
| `date_format(d, fmt)` | `sqe-trino-functions` | Trino / MySQL specifiers (`%Y`, `%m`, `%d`, `%H`, `%i` for minutes, `%s` for seconds). Returns `VARCHAR`. An unknown specifier renders the letter itself; `%U`, `%u`, `%V`, `%X` and a trailing `%` raise an error. | `date_format` | `to_char` | `date_format` | `strftime` |
| `date_parse(str, fmt)` | `sqe-trino-functions` | Inverse of `date_format`. Returns `TIMESTAMP(3)` without a zone. Text that does not match the format raises an error. | `date_parse` | `to_timestamp` | `to_timestamp` | `strptime` |
| `format_datetime(d, fmt)` | `sqe-trino-functions` | Java / Joda specifiers (`yyyy-MM-dd HH:mm:ss`). Returns `VARCHAR`. | `format_datetime` | - | `date_format` | - |
| `parse_datetime(str, fmt)` | `sqe-trino-functions` | Inverse of `format_datetime`. Returns `TIMESTAMP(3) WITH TIME ZONE`. Text that does not match raises an error. | `parse_datetime` | - | `to_timestamp` | - |
| `to_iso8601(d)` | `sqe-trino-functions` | ISO 8601 string, returns `VARCHAR`. A `DATE` gives `YYYY-MM-DD`. A timestamp without a zone has no designator (`2024-01-02T03:04:05`, as in Trino); a zoned one ends in `Z` for UTC or an offset such as `+01:00`. The fraction has 3 digits, or 6 when the value has sub-millisecond digits. | `to_iso8601` | - | - | - |
| `from_iso8601_date(str)` | `sqe-trino-functions` | ISO 8601 text to `DATE`. | `from_iso8601_date` | - | - | - |
| `from_iso8601_timestamp(str)` | `sqe-trino-functions` | ISO 8601 text to `TIMESTAMP(3) WITH TIME ZONE`. Reads a `Z`, `+hh`, `+hhmm` or `+hh:mm` designator and returns that instant; no designator means UTC. Invalid text raises an error. | `from_iso8601_timestamp` | - | - | - |
| `from_iso8601_timestamp_nanos(str)` | `sqe-trino-functions` | Same as `from_iso8601_timestamp` but returns `TIMESTAMP(9) WITH TIME ZONE`. | `from_iso8601_timestamp_nanos` | - | - | - |
| `date(x)` | `sqe-trino-functions` | Cast a varchar, timestamp or `DATE` to `DATE`. | `date` | `date` | `date` | `date` |
| `parse_duration(str)` | `sqe-trino-functions` | Text such as `'1.5h'` to an interval of days to seconds. | `parse_duration` | - | - | - |
| `sequence(start, stop [, step])` | `sqe-trino-functions` | Array of dates or timestamps. Two `DATE` arguments use a one day step. Result size is capped. | `sequence` | - | `sequence` | `generate_series` |
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
-- 2026-05-08 14:30:00 (TIMESTAMP(3))
```

Java-style example (preferred for new code):

```sql
SELECT format_datetime(TIMESTAMP '2026-05-08 14:30:00', 'yyyy-MM-dd HH:mm:ss');
-- 2026-05-08 14:30:00
```

## Unix epoch

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `from_unixtime(epoch [, zone] [, hours, minutes])` | `sqe-trino-functions` | Seconds since epoch (double) to `TIMESTAMP(3) WITH TIME ZONE`. | `from_unixtime` | - | `from_unixtime` | `to_timestamp` |
| `from_unixtime_nanos(ns)` | `sqe-trino-functions` | Nanoseconds since epoch to `TIMESTAMP(9) WITH TIME ZONE`. | `from_unixtime_nanos` | - | - | - |
| `to_unixtime(d)` | `sqe-trino-functions` | `TIMESTAMP` to seconds since epoch (`DOUBLE`). | `to_unixtime` | - | `unix_timestamp` | `epoch` |
| `extract(epoch from d)` | `datafusion-builtin` | SQL-standard alternative for `to_unixtime`. | yes | - | yes | yes |

## Time zones

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `with_timezone(d, tz)` | `sqe-trino-functions (ext)` | Attach a timezone offset to a naive timestamp. `trino_functions_ext.rs:50` | `with_timezone` | `convert_timezone` | `from_utc_timestamp` | - |
| `at_timezone(d, tz)` | `sqe-trino-functions (ext)` | Convert a `TIMESTAMP WITH TIME ZONE` to another zone. `trino_functions_ext.rs:51` | `at_timezone` | `convert_timezone` | - | - |
| `timezone_hour(d)` | `sqe-trino-functions` | Hour component of the real offset of the value's zone, returns `BIGINT`. | `timezone_hour` | - | - | - |
| `timezone_minute(d)` | `sqe-trino-functions` | Minute component of the real offset, returns `BIGINT`. | `timezone_minute` | - | - | - |
| `timezone(d)` | `sqe-trino-functions` | Zone name of a `TIMESTAMP WITH TIME ZONE`, returns `VARCHAR`. | `timezone` | - | - | - |
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

## Session time zone

Every SQE session runs in UTC. `SET TIME ZONE '<zone>'` succeeds, so client connection setup works, but it does not change the zone.

- `CAST(x AS TIMESTAMP WITH TIME ZONE)` and `to_timestamp*` return zoned values (Arrow `Timestamp(*, "+00:00")`), the same type an Iceberg `timestamptz` column scans as.
- An offset in the text is applied, and the value is stored in UTC: `CAST('2024-01-02 03:04:05+02:00' AS TIMESTAMP WITH TIME ZONE)` is `2024-01-02 01:04:05 UTC`. Trino keeps the `+02:00` offset; Arrow has one zone per column, so SQE does not.
- `now()`, `current_timestamp` and `localtimestamp` return a timestamp without a zone, holding the query start time in UTC.
- Trino clients receive zoned values as `2024-01-02 01:04:05.000000 UTC` (or `... +02:00` for a column stored with an offset zone), as Trino formats them.
- Comparing a timestamp column with a zoned literal casts the literal, not the column, so the filter still prunes files.

## Iceberg V3 nanosecond timestamps

`TIMESTAMP_NS` and `TIMESTAMP_NS WITH TIME ZONE` only exist in Iceberg format-version 3. New tables are V3 by default, and a CREATE TABLE with such a column is V3 even when the configured default is 2. All datetime functions operate on these the same way they operate on `TIMESTAMP(6)`; the underlying Arrow type is `Timestamp(Nanosecond, ...)` instead of `Timestamp(Microsecond, ...)`.

When you query a V3 ns column from a Trino client, Trino downscales to microseconds. SQE keeps the full precision when serving Arrow Flight SQL clients.
