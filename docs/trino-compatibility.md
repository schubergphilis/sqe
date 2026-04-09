# Trino SQL Compatibility Matrix

> Living document. Last updated: 2026-04-08.
> Rating: ✅ equivalent | ⚠️ partial/different semantics | ❌ missing | 🔧 SQE-specific

SQE aims to be a drop-in replacement for Trino in Iceberg-only environments.
This document maps every Trino SQL function and feature to its SQE equivalent,
noting semantic differences and gaps.

## Summary

| Category | Total | ✅ | ⚠️ | ❌ | Coverage |
|---|---|---|---|---|---|
| Scalar: String | 27 | 19 | 2 | 6 | 77.8% |
| Scalar: Math | 29 | 19 | 4 | 6 | 79.3% |
| Scalar: Date/Time | 38 | 23 | 1 | 14 | 63.2% |
| Scalar: JSON | 12 | 6 | 0 | 6 | 50.0% |
| Scalar: URL | 8 | 8 | 0 | 0 | 100% |
| Scalar: Regex | 6 | 3 | 0 | 3 | 50% |
| Scalar: Conditional | 8 | 7 | 0 | 1 | 87.5% |
| Scalar: Conversion | 10 | 9 | 0 | 1 | 90% |
| Aggregate | 33 | 19 | 4 | 10 | 69.7% |
| Window | 14 | 11 | 1 | 2 | 85.7% |
| DDL/DML | 31 + 1🔧 | 18 | 3 | 10 | 58.1% |
| Type System | 27 | 18 | 2 | 7 | 74.1% |
| Iceberg-Specific | 18 | 6 | 0 | 12 | 33.3% |

## How to Read This Document

Each section lists Trino functions with their SQE status:

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `concat(s1, s2, ...)` | `concat(s1, s2, ...)` | ✅ | Native DataFusion |
| `json_extract(json, path)` | — | ❌ | Use `json_object()` for construction |
| `year(date)` | `year(date)` | ✅ | Trino compat UDF in sqe-coordinator |

---

## Scalar Functions: String

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `chr(n)` | `chr(n)` | ✅ | Native DataFusion |
| `codepoint(s)` | `ascii(s)` | ⚠️ | `ascii()` returns first byte, not Unicode codepoint |
| `concat(s1, s2, ...)` | `concat(s1, s2, ...)` | ✅ | Native DataFusion |
| `concat_ws(sep, s1, s2, ...)` | `concat_ws(sep, s1, s2, ...)` | ✅ | Native DataFusion |
| `format(fmt, ...)` | — | ❌ | No equivalent; use `concat()` for simple cases |
| `hamming_distance(s1, s2)` | — | ❌ | |
| `length(s)` | `length(s)` / `char_length(s)` | ✅ | Native DataFusion |
| `levenshtein_distance(s1, s2)` | `levenshtein(s1, s2)` | ✅ | Native DataFusion |
| `lower(s)` | `lower(s)` | ✅ | Native DataFusion |
| `lpad(s, size, pad)` | `lpad(s, size, pad)` | ✅ | Native DataFusion |
| `ltrim(s)` | `ltrim(s)` | ✅ | Native DataFusion |
| `normalize(s, form)` | — | ❌ | Unicode normalization not available |
| `position(sub IN s)` | `position(sub IN s)` / `strpos(s, sub)` | ✅ | Both syntaxes work |
| `replace(s, from, to)` | `replace(s, from, to)` | ✅ | Native DataFusion |
| `reverse(s)` | `reverse(s)` | ✅ | Native DataFusion |
| `rpad(s, size, pad)` | `rpad(s, size, pad)` | ✅ | Native DataFusion |
| `rtrim(s)` | `rtrim(s)` | ✅ | Native DataFusion |
| `soundex(s)` | — | ❌ | |
| `split(s, delim)` | `string_to_array(s, delim)` | ⚠️ | Different name, same semantics |
| `split_part(s, delim, idx)` | `split_part(s, delim, idx)` | ✅ | Native DataFusion |
| `strpos(s, sub)` | `strpos(s, sub)` | ✅ | Trino compat UDF |
| `substr(s, start, len)` | `substr(s, start, len)` | ✅ | Native DataFusion |
| `translate(s, from, to)` | `translate(s, from, to)` | ✅ | Native DataFusion |
| `trim(s)` | `trim(s)` | ✅ | Native DataFusion |
| `upper(s)` | `upper(s)` | ✅ | Native DataFusion |
| `word_stem(s)` | — | ❌ | NLP function |
| `word_stem(s, lang)` | — | ❌ | NLP function |

## Scalar Functions: Math

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `abs(x)` | `abs(x)` | ✅ | |
| `acos(x)` / `asin(x)` / `atan(x)` | Same | ✅ | |
| `atan2(y, x)` | `atan2(y, x)` | ✅ | |
| `cbrt(x)` | `cbrt(x)` | ✅ | |
| `ceil(x)` / `ceiling(x)` | `ceil(x)` | ✅ | |
| `cos(x)` / `sin(x)` / `tan(x)` | `cos(x)` / `sin(x)` / `tan(x)` | ✅ | |
| `cosh(x)` / `sinh(x)` / `tanh(x)` | — | ❌ | Hyperbolic functions not in DataFusion |
| `degrees(x)` | `degrees(x)` | ✅ | |
| `e()` | `exp(1)` | ⚠️ | No standalone `e()`, use `exp(1)` |
| `exp(x)` | `exp(x)` | ✅ | |
| `floor(x)` | `floor(x)` | ✅ | |
| `from_base(s, radix)` | — | ❌ | |
| `infinity()` | — | ❌ | Use `CAST('Infinity' AS DOUBLE)` |
| `ln(x)` | `ln(x)` | ✅ | |
| `log(b, x)` | `log(b, x)` | ✅ | |
| `log2(x)` | `log2(x)` | ✅ | |
| `log10(x)` | `log10(x)` | ✅ | |
| `mod(n, m)` | `n % m` | ⚠️ | Operator syntax, no `mod()` function |
| `nan()` | — | ❌ | Use `CAST('NaN' AS DOUBLE)` |
| `pi()` | `pi()` | ✅ | |
| `pow(x, p)` / `power(x, p)` | `power(x, p)` | ✅ | |
| `radians(x)` | `radians(x)` | ✅ | |
| `rand()` / `random()` | `random()` | ✅ | |
| `round(x)` / `round(x, d)` | `round(x, d)` | ✅ | |
| `sign(x)` | `signum(x)` | ⚠️ | Different name |
| `sqrt(x)` | `sqrt(x)` | ✅ | |
| `to_base(n, radix)` | — | ❌ | |
| `truncate(x)` | `trunc(x)` | ⚠️ | Different name |
| `width_bucket(x, bound1, bound2, n)` | — | ❌ | |

## Scalar Functions: Date/Time

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `current_date` | `current_date` | ✅ | SQL standard |
| `current_time` | — | ❌ | Time-only type not supported |
| `current_timestamp` | `current_timestamp` / `now()` | ✅ | |
| `current_timezone()` | — | ❌ | |
| `now()` | `now()` | ✅ | Trino compat UDF |
| `localtime` | `localtime()` | ✅ | Trino compat UDF |
| `localtimestamp` | `localtimestamp()` | ✅ | Trino compat UDF |
| `date(s)` | `trino_date(s)` | ✅ | Trino compat UDF |
| `from_iso8601_date(s)` | — | ❌ | Use `CAST(s AS DATE)` |
| `from_iso8601_timestamp(s)` | — | ❌ | Use `CAST(s AS TIMESTAMP)` |
| `from_unixtime(n)` | `from_unixtime(n)` | ✅ | Trino compat UDF |
| `to_unixtime(ts)` | `to_unixtime(ts)` | ✅ | Trino compat UDF |
| `to_iso8601(ts)` | — | ❌ | Use `date_format()` |
| `date_add(unit, n, ts)` | `date_add(ts, unit, n)` | ⚠️ | Different argument order |
| `date_diff(unit, ts1, ts2)` | `date_diff(unit, ts1, ts2)` | ✅ | Trino compat UDF |
| `date_trunc(unit, ts)` | `date_trunc(unit, ts)` | ✅ | Native DataFusion |
| `date_format(ts, fmt)` | `date_format(ts, fmt)` | ✅ | Trino compat UDF (MySQL format codes) |
| `date_parse(s, fmt)` | `date_parse(s, fmt)` | ✅ | Trino compat UDF (MySQL format codes) |
| `format_datetime(ts, fmt)` | — | ❌ | Joda format codes |
| `parse_datetime(s, fmt)` | — | ❌ | Joda format codes |
| `year(d)` | `year(d)` | ✅ | Trino compat UDF |
| `quarter(d)` | `quarter(d)` | ✅ | Trino compat UDF |
| `month(d)` | `month(d)` | ✅ | Trino compat UDF |
| `week(d)` | `week(d)` | ✅ | Trino compat UDF |
| `day(d)` / `day_of_month(d)` | `day(d)` | ✅ | Trino compat UDF |
| `day_of_week(d)` / `dow(d)` | `day_of_week(d)` | ✅ | Trino compat UDF |
| `day_of_year(d)` / `doy(d)` | `day_of_year(d)` | ✅ | Trino compat UDF |
| `hour(ts)` | `hour(ts)` | ✅ | Trino compat UDF |
| `minute(ts)` | `minute(ts)` | ✅ | Trino compat UDF |
| `second(ts)` | `second(ts)` | ✅ | Trino compat UDF |
| `millisecond(ts)` | — | ❌ | |
| `timezone_hour(ts)` | — | ❌ | |
| `timezone_minute(ts)` | — | ❌ | |
| `with_timezone(ts, tz)` | — | ❌ | |
| `at_timezone(ts, tz)` | — | ❌ | |
| `INTERVAL 'n' UNIT` | `INTERVAL 'n' UNIT` | ✅ | SQL standard |
| `human_readable_seconds(n)` | — | ❌ | |
| `last_day_of_month(d)` | — | ❌ | |

## Scalar Functions: JSON

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `json_object(k1, v1, k2, v2, ...)` | `json_object(k1, v1, ...)` | ✅ | Trino compat UDF |
| `json_format(json)` | `json_format(json)` | ✅ | Trino compat UDF |
| `json_parse(s)` | `json_parse(s)` | ✅ | Trino compat UDF |
| `json_extract(json, path)` | `json_extract(json, path)` | ✅ | Trino compat UDF (dot-path, not full JSONPath) |
| `json_extract_scalar(json, path)` | `json_extract_scalar(json, path)` | ✅ | Trino compat UDF |
| `json_size(json, path)` | — | ❌ | |
| `json_array_contains(json, val)` | — | ❌ | Different from json_contains which checks key existence |
| `json_array_get(json, idx)` | — | ❌ | |
| `json_array_length(json)` | `json_array_length(json)` | ✅ | Trino compat UDF |
| `is_json_scalar(json)` | — | ❌ | |
| `CAST(v AS JSON)` | — | ❌ | No JSON type |
| `CAST(json AS type)` | — | ❌ | No JSON type |

**Note:** Core JSON extraction is now supported via `datafusion-functions-json` (registered at startup) plus Trino-aliased UDFs (`json_extract`, `json_extract_scalar`, `json_array_length`, `json_parse`). Full JSONPath syntax and JSON-typed columns remain unsupported — most Iceberg workloads use structured columns rather than JSON blobs.

## Scalar Functions: URL

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `url_extract_host(url)` | `url_extract_host(url)` | ✅ | Trino compat UDF |
| `url_extract_path(url)` | `url_extract_path(url)` | ✅ | Trino compat UDF |
| `url_extract_port(url)` | `url_extract_port(url)` | ✅ | Trino compat UDF |
| `url_extract_protocol(url)` | `url_extract_protocol(url)` | ✅ | Trino compat UDF |
| `url_extract_query(url)` | `url_extract_query(url)` | ✅ | Trino compat UDF |
| `url_extract_parameter(url, name)` | `url_extract_parameter(url, name)` | ✅ | Trino compat UDF |
| `url_encode(s)` | `url_encode(s)` | ✅ | Trino compat UDF |
| `url_decode(s)` | `url_decode(s)` | ✅ | Trino compat UDF |

## Scalar Functions: Regex

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `regexp_count(s, pattern)` | `regexp_count(s, pattern)` | ✅ | Native DataFusion |
| `regexp_extract(s, pattern)` | — | ❌ | Use `regexp_match()` |
| `regexp_extract_all(s, pattern)` | — | ❌ | |
| `regexp_like(s, pattern)` | `regexp_like(s, pattern)` | ✅ | Native DataFusion |
| `regexp_replace(s, pattern, repl)` | `regexp_replace(s, pattern, repl)` | ✅ | |
| `regexp_split(s, pattern)` | — | ❌ | |

## Scalar Functions: Conditional

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `CASE WHEN ... THEN ... END` | Same | ✅ | SQL standard |
| `COALESCE(v1, v2, ...)` | Same | ✅ | |
| `NULLIF(v1, v2)` | Same | ✅ | |
| `GREATEST(v1, v2, ...)` | Same | ✅ | Native DataFusion |
| `LEAST(v1, v2, ...)` | Same | ✅ | Native DataFusion |
| `IF(cond, true, false)` | `trino_if(cond, true, false)` | ✅ | Trino compat UDF |
| `TRY(expr)` | — | ❌ | Error-suppressing evaluation |
| `TRY_CAST(v AS type)` | `TRY_CAST(v AS type)` | ✅ | Native DataFusion |

## Scalar Functions: Conversion / Type Cast

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `CAST(v AS type)` | Same | ✅ | |
| `TRY_CAST(v AS type)` | Same | ✅ | |
| `typeof(v)` | `typeof(v)` | ✅ | Trino compat UDF |
| `format(fmt, ...)` | — | ❌ | |
| `from_utf8(binary)` | `from_utf8(binary)` | ✅ | Trino compat UDF |
| `to_utf8(string)` | `to_utf8(string)` | ✅ | Trino compat UDF |
| `from_base64(s)` | `from_base64(s)` | ✅ | Trino compat UDF |
| `to_base64(binary)` | `to_base64(binary)` | ✅ | Trino compat UDF |
| `from_hex(s)` | `from_hex(s)` | ✅ | Trino compat UDF |
| `to_hex(binary)` | `to_hex(binary)` | ✅ | Trino compat UDF (named to_hex_binary to avoid conflict with DataFusion's integer to_hex) |

## Aggregate Functions

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `count(*)` / `count(x)` | Same | ✅ | |
| `count(DISTINCT x)` | Same | ✅ | |
| `sum(x)` | Same | ✅ | |
| `avg(x)` | Same | ✅ | |
| `min(x)` / `max(x)` | Same | ✅ | |
| `bool_and(x)` / `bool_or(x)` | `bool_and(x)` / `bool_or(x)` | ✅ | |
| `every(x)` | — | ❌ | Use `bool_and(x)` |
| `array_agg(x)` | `array_agg(x)` | ✅ | |
| `array_agg(x ORDER BY y)` | Same | ✅ | DataFusion supports ordered agg |
| `string_agg(x, sep)` | `string_agg(x, sep)` | ✅ | |
| `listagg(x, sep)` | `string_agg(x, sep)` | ⚠️ | Use `string_agg()` alias |
| `approx_distinct(x)` | `approx_distinct(x)` | ✅ | |
| `approx_percentile(x, p)` | `approx_percentile_cont(x, p)` | ⚠️ | Different name |
| `stddev(x)` / `stddev_samp(x)` | Same | ✅ | |
| `stddev_pop(x)` | Same | ✅ | |
| `variance(x)` / `var_samp(x)` | Same | ✅ | |
| `var_pop(x)` | Same | ✅ | |
| `covar_samp(y, x)` | `covar_samp(y, x)` | ✅ | |
| `covar_pop(y, x)` | `covar_pop(y, x)` | ✅ | |
| `corr(y, x)` | `corr(y, x)` | ✅ | |
| `regr_slope(y, x)` | `regr_slope(y, x)` | ✅ | |
| `bitwise_and_agg(x)` | `bit_and(x)` | ⚠️ | Different name |
| `bitwise_or_agg(x)` | `bit_or(x)` | ⚠️ | Different name |
| `arbitrary(x)` | — | ❌ | Returns any value from group |
| `max_by(x, y)` / `min_by(x, y)` | — | ❌ | |
| `histogram(x)` | — | ❌ | |
| `multimap_agg(k, v)` | — | ❌ | |
| `map_agg(k, v)` | — | ❌ | |
| `map_union(map)` | — | ❌ | |
| `checksum(x)` | — | ❌ | |
| `approx_most_frequent(n, x, cap)` | — | ❌ | |
| `merge(digest)` | — | ❌ | HyperLogLog/TDigest |
| `GROUPING SETS / CUBE / ROLLUP` | Same | ✅ | Native DataFusion |

## Window Functions

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `row_number()` | Same | ✅ | |
| `rank()` | Same | ✅ | |
| `dense_rank()` | Same | ✅ | |
| `ntile(n)` | Same | ✅ | |
| `percent_rank()` | Same | ✅ | |
| `cume_dist()` | Same | ✅ | |
| `lead(x, offset, default)` | Same | ✅ | |
| `lag(x, offset, default)` | Same | ✅ | |
| `first_value(x)` | Same | ✅ | |
| `last_value(x)` | Same | ✅ | |
| `nth_value(x, n)` | Same | ✅ | |
| Frame specs: ROWS/RANGE/GROUPS | ROWS/RANGE ✅, GROUPS ❌ | ⚠️ | GROUPS not in DataFusion |
| `QUALIFY` clause | — | ❌ | Use subquery with window |
| Lambda in window functions | — | ❌ | No lambda support |

## DDL / DML Statements

| Trino Statement | SQE Support | Status | Notes |
|---|---|---|---|
| `CREATE TABLE (cols) WITH (...)` | `CREATE TABLE (cols)` | ⚠️ | No WITH properties (Iceberg defaults) |
| `CREATE TABLE AS SELECT` | Same | ✅ | |
| `DROP TABLE` | Same | ✅ | |
| `ALTER TABLE ... RENAME TO` | Same | ✅ | |
| `ALTER TABLE ... ADD COLUMN` | Same | ✅ | |
| `ALTER TABLE ... DROP COLUMN` | Same | ✅ | |
| `ALTER TABLE ... RENAME COLUMN` | Same | ✅ | |
| `ALTER TABLE ... SET/DROP NOT NULL` | Same | ✅ | |
| `ALTER TABLE ... SET PROPERTIES` | — | ❌ | |
| `CREATE VIEW` | Same | ✅ | Iceberg views |
| `DROP VIEW` | Same | ✅ | |
| `CREATE OR REPLACE VIEW` | — | ❌ | |
| `CREATE MATERIALIZED VIEW` | — | ❌ | |
| `INSERT INTO ... VALUES` | Same | ✅ | |
| `INSERT INTO ... SELECT` | Same | ✅ | |
| `DELETE FROM ... WHERE` | Same | ✅ | CoW rewrite_files |
| `UPDATE ... SET ... WHERE` | Same | ✅ | CoW rewrite_files |
| `MERGE INTO ... USING ...` | Same | ✅ | CoW full-outer-join rewrite |
| `TRUNCATE TABLE` | — | ❌ | Use `DELETE FROM t` |
| `COMMENT ON TABLE/COLUMN` | — | ❌ | |
| `SHOW CATALOGS` | Same | ✅ | |
| `SHOW SCHEMAS` | Same | ✅ | |
| `SHOW TABLES` | Same | ✅ | |
| `SHOW COLUMNS FROM` | `DESCRIBE` | ⚠️ | Different syntax |
| `SHOW CREATE TABLE` | — | ❌ | |
| `SHOW STATS FOR` | — | ❌ | |
| `EXPLAIN` | Same | ✅ | DataFusion explain |
| `EXPLAIN ANALYZE` | `EXPLAIN FULL` | ⚠️ | Different keyword, similar output |
| `USE catalog.schema` | — | ❌ | Set via headers/session |
| `PREPARE` / `EXECUTE` | — | ❌ | No prepared statements |
| `CALL procedure(...)` | — | ❌ | No stored procedures |
| `GRANT` / `REVOKE` | Planned (Plan C) | 🔧 | SQE-specific grant system |

## Type System

| Trino Type | SQE/Arrow Type | Status | Notes |
|---|---|---|---|
| `BOOLEAN` | `Boolean` | ✅ | |
| `TINYINT` | `Int8` | ✅ | |
| `SMALLINT` | `Int16` | ✅ | |
| `INTEGER` | `Int32` | ✅ | |
| `BIGINT` | `Int64` | ✅ | |
| `REAL` | `Float32` | ✅ | |
| `DOUBLE` | `Float64` | ✅ | |
| `DECIMAL(p, s)` | `Decimal128(p, s)` | ✅ | Up to 38 digits |
| `VARCHAR` / `VARCHAR(n)` | `Utf8` / `Utf8View` | ✅ | Length limit not enforced |
| `CHAR(n)` | `Utf8` | ⚠️ | No fixed-length semantics |
| `VARBINARY` | `Binary` | ✅ | |
| `DATE` | `Date32` | ✅ | |
| `TIME` | — | ❌ | No time-only type in Arrow |
| `TIME WITH TIME ZONE` | — | ❌ | |
| `TIMESTAMP` | `Timestamp(Microsecond, None)` | ✅ | |
| `TIMESTAMP WITH TIME ZONE` | `Timestamp(Microsecond, Some(tz))` | ✅ | |
| `INTERVAL YEAR TO MONTH` | `Interval(YearMonth)` | ✅ | |
| `INTERVAL DAY TO SECOND` | `Interval(DayTime)` | ✅ | |
| `ARRAY(T)` | `List(T)` | ✅ | |
| `MAP(K, V)` | `Map(K, V)` | ✅ | |
| `ROW(fields...)` | `Struct(fields...)` | ✅ | |
| `JSON` | — | ❌ | No JSON type; use VARCHAR |
| `UUID` | `Utf8` | ⚠️ | Stored as string, no UUID type |
| `IPADDRESS` | — | ❌ | |
| `HyperLogLog` | — | ❌ | Trino-specific sketch type |
| `TDigest` | — | ❌ | Trino-specific sketch type |
| `SetDigest` | — | ❌ | Trino-specific sketch type |

**Type coercion:** DataFusion handles implicit coercion for numeric types (INT → BIGINT → DOUBLE) and string types. Trino has additional coercion rules for JSON, TIME, and sketch types that are not applicable in SQE.

## Iceberg-Specific SQL

| Feature | SQE Support | Trino Support | Status | Notes |
|---|---|---|---|---|
| Partition pruning | ✅ | ✅ | ✅ | DataFusion optimizer pass |
| Hidden partitioning | ✅ | ✅ | ✅ | Via Iceberg transforms |
| Schema evolution | ✅ | ✅ | ✅ | ADD/DROP/RENAME COLUMN |
| Type widening | ✅ | ✅ | ✅ | INT→BIGINT, FLOAT→DOUBLE |
| Time travel: `FOR VERSION AS OF` | — | ✅ | ❌ | Snapshot ID query |
| Time travel: `FOR TIMESTAMP AS OF` | — | ✅ | ❌ | Temporal query |
| `$snapshots` metadata table | — | ✅ | ❌ | |
| `$manifests` metadata table | — | ✅ | ❌ | |
| `$history` metadata table | — | ✅ | ❌ | |
| `$partitions` metadata table | — | ✅ | ❌ | |
| `$files` metadata table | — | ✅ | ❌ | |
| `$refs` metadata table | — | ✅ | ❌ | |
| Partition evolution | ✅ | ✅ | ✅ | Via ALTER TABLE |
| Sort order | — | ✅ | ❌ | |
| Write distribution mode | — | ✅ | ❌ | |
| ORC file format | — | ✅ | ❌ | Parquet only |
| Copy-on-Write (CoW) | ✅ | ✅ | ✅ | DELETE/UPDATE/MERGE |
| Merge-on-Read (MoR) | — | ✅ | ❌ | Planned (iceberg-rust Epic #2186) |

**Note:** Iceberg metadata tables (`$snapshots`, `$history`, etc.) are a significant usability gap. These are commonly used for debugging and operational monitoring. Implementation requires exposing iceberg-rust's `TableMetadata` as virtual table providers.

## Operational Comparison

> Run `scripts/operational-comparison.sh` to regenerate these numbers.

| Metric | SQE | Trino | Notes |
|---|---|---|---|
| **Language** | Rust | Java 23 | |
| **Build time** (release) | ~3–5 min | ~10–15 min | `cargo build --release` vs `mvn package -DskipTests` |
| **Build dependencies** | ~800 crates | ~2000+ Maven deps | `Cargo.lock` vs `pom.xml` tree |
| **Coordinator binary** | ~50 MB | N/A (JVM) | Single static binary vs JVM + JARs |
| **Docker image** | ~80 MB | ~700 MB | Alpine + binary vs JVM + plugins |
| **Cold start** | <1s | 10–30s | First query latency from container start |
| **Idle memory (RSS)** | ~20 MB | ~300 MB | After startup, no queries |
| **Loaded memory** | ~200–500 MB | ~1–4 GB | During TPC-H SF1 full suite |
| **Config surface** | ~30 TOML knobs | ~200+ properties | `sqe.toml` vs `config.properties` + `jvm.config` + catalog files |
| **Deployment** | Single binary + TOML | JVM + plugins + properties | |
| **Hot reload** | ❌ | ❌ | Neither supports hot config reload |
| **Plugins** | Compile-time features | Runtime JARs | Connectors are Cargo features vs JAR plugins |

**Key advantages:**
- **10x smaller footprint** — single binary, minimal memory
- **10x faster cold start** — no JVM warmup, no class loading
- **Simpler deployment** — one binary, one TOML file
- **Fewer moving parts** — no plugin system, no JVM tuning

**Trino advantages:**
- **Ecosystem** — 100+ connectors, mature JDBC drivers
- **Runtime extensibility** — add connectors without recompilation
- **Community** — larger community, more Stack Overflow answers
