# Trino SQL Compatibility Matrix

> Living document. Last updated: 2026-05-05 (DataFusion 53.1.0; Trino math + codepoint aliases).
> Rating: ✅ equivalent | ⚠️ partial/different semantics | ❌ missing | 🔧 SQE-specific

SQE aims to be a drop-in replacement for Trino in Iceberg-only environments.
This document maps every Trino SQL function and feature to its SQE equivalent,
noting semantic differences and gaps.

> **2026-05-04 update — three reds turn green.** The same MR that landed the
> matrix refresh shipped the SQL-side wiring for two of the four "honest
> technical debt" items called out in the previous sweep. The third (MoR
> writes) turned out to be already implemented end-to-end; the doc was
> describing a state that no longer matched the code.
>
> - **`JSON` logical type → ✅.** `CREATE TABLE t(payload JSON)` aliases
>   to `Utf8`, so `CAST(json_col AS BIGINT|VARCHAR|DOUBLE)` rides
>   DataFusion's built-in coercion. JSON-shaped extraction stays
>   available via `json_extract` / `json_extract_scalar` /
>   `json_array_length` / `json_parse` / `json_get_str` /
>   `json_get_int` / `json_get_float` / `json_get_bool`.
> - **`TIME` / `TIME(p)` → ✅.** Maps to Arrow `Time64(Microsecond)`
>   end-to-end. `localtime()` returns Time64 (was incorrectly
>   returning Timestamp before). `EXTRACT(HOUR|MINUTE|SECOND FROM
>   time_col)` works via the Trino-aliased `hour()` / `minute()` /
>   `second()` UDFs. `year()` / `month()` / `day()` / `day_of_week`
>   on a TIME column raise a clear plan error per Trino spec.
>   `TIME WITH TIME ZONE` and `TIME(p > 6)` reject with explicit
>   NotImplemented messages pointing at the workaround.
> - **MoR writes → ✅ in code, doc was stale.**
>   `handle_delete_dispatch` reads `write.delete.mode` from table
>   properties: `merge-on-read` routes to `handle_delete_mor` (no
>   primary key) or `handle_delete_equality` (with PK), each writing
>   the appropriate delete file via the existing worker writer and
>   committing via `FastAppendAction` (position deletes) or
>   `RowDeltaAction` (equality deletes). CoW remains the default.
>
> **DataFusion 53.1.0** brought three filter-pushdown bug fixes (#20996
> InList Dictionary, #21142 fetch fields on push_down_filter, #21492
> FilterExec projection). None of them unblock the ❌ items below.
> The remaining gaps are structural (Trino sketch types, Arrow type
> system limits, Iceberg spec gaps) or strategic (Parquet-only). See
> the [Engine Limitations & Roadmap](#engine-limitations--roadmap)
> section for the per-feature path forward.

## Summary

| Category | Total | ✅ | ⚠️ | ❌ | Coverage |
|---|---|---|---|---|---|
| Scalar: String | 27 | 25 | 2 | 0 | 100% |
| Scalar: Math | 29 | 29 | 0 | 0 | 100% |
| Scalar: Date/Time | 38 | 37 | 1 | 0 | 100% |
| Scalar: JSON | 12 | 11 | 1 | 0 | 100% |
| Scalar: URL | 8 | 8 | 0 | 0 | 100% |
| Scalar: Regex | 6 | 4 | 2 | 0 | 100% |
| Scalar: Conditional | 8 | 7 | 1 | 0 | 100% |
| Scalar: Conversion | 10 | 9 | 0 | 1 | 90% |
| Aggregate | 33 | 22 | 5 | 6 | 81.8% |
| Window | 14 | 13 | 0 | 1 | 92.9% |
| DDL/DML | 31 + 1🔧 | 22 | 6 | 3 | 87.1% |
| Type System | 27 | 20 | 2 | 5 | 81.5% |
| Iceberg-Specific | 19 | 12 | 5 | 2 | 89.5% |

### Overall Coverage

**~96% Trino SQL compatibility** for Iceberg-only workloads. The remaining gaps are:
- **Trino-specific sketch types** (HyperLogLog, TDigest, SetDigest). Not used in typical Iceberg analytics.
- **Map-producing aggregates** (histogram, map_agg, multimap_agg). Need custom UDAF with MapBuilder.
- **CREATE MATERIALIZED VIEW**. Not in Iceberg spec; use CTAS + scheduled refresh.
- **Lambda in window functions**. DataFusion engine limitation.
- **ORC format**. Strategic choice: Parquet only.
- **`TIME WITH TIME ZONE`**. No Arrow equivalent. Use `TIMESTAMP WITH TIME ZONE` instead. SQE rejects with a clear NotImplemented at CREATE TABLE.
- **Sort order enforcement** on write. Iceberg metadata is written but files are not physically sorted.
- **Write distribution mode**. Distributed write path lands in Phase 3+.

Items shipped in the 2026-05-04 SQL surface lift:
- **MoR writes** are wired today. Set `TBLPROPERTIES ('write.delete.mode' = 'merge-on-read')`; SQE writes position-delete files (no PK) or equality-delete files (with PK) and commits via `FastAppendAction` / `RowDeltaAction`.
- **`JSON` logical type** aliases to `Utf8`. `CAST(json_col AS T)` rides DataFusion's built-in coercion. Full JSON extraction works via the existing `json_*` UDFs.
- **`TIME` / `TIME(p ≤ 6)`** maps to `Time64(Microsecond)`. `localtime()`, `hour()`, `minute()`, `second()` all work on TIME columns.

## How to Read This Document

Each section lists Trino functions with their SQE status:

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `concat(s1, s2, ...)` | `concat(s1, s2, ...)` | ✅ | Native DataFusion |
| `histogram(x)` | — | ❌ | Map-producing UDAF not yet implemented |
| `year(date)` | `year(date)` | ✅ | Trino compat UDF in sqe-coordinator |

---

## Scalar Functions: String

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `chr(n)` | `chr(n)` | ✅ | Native DataFusion |
| `codepoint(s)` | `codepoint(s)` | ✅ | Trino compat UDF; full Unicode code point via proper UTF-8 decode. Errors on multi-character input per Trino spec |
| `concat(s1, s2, ...)` | `concat(s1, s2, ...)` | ✅ | Native DataFusion |
| `concat_ws(sep, s1, s2, ...)` | `concat_ws(sep, s1, s2, ...)` | ✅ | Native DataFusion |
| `format(fmt, ...)` | `format(fmt, ...)` | ✅ | Trino compat UDF (%s, %d, %f, zero-pad, precision) |
| `hamming_distance(s1, s2)` | `hamming_distance(s1, s2)` | ✅ | Trino compat UDF |
| `length(s)` | `length(s)` / `char_length(s)` | ✅ | Native DataFusion |
| `levenshtein_distance(s1, s2)` | `levenshtein(s1, s2)` | ✅ | Native DataFusion |
| `lower(s)` | `lower(s)` | ✅ | Native DataFusion |
| `lpad(s, size, pad)` | `lpad(s, size, pad)` | ✅ | Native DataFusion |
| `ltrim(s)` | `ltrim(s)` | ✅ | Native DataFusion |
| `normalize(s, form)` | `normalize(s, form)` | ✅ | Trino compat UDF (NFC/NFD/NFKC/NFKD) |
| `position(sub IN s)` | `position(sub IN s)` / `strpos(s, sub)` | ✅ | Both syntaxes work |
| `replace(s, from, to)` | `replace(s, from, to)` | ✅ | Native DataFusion |
| `reverse(s)` | `reverse(s)` | ✅ | Native DataFusion |
| `rpad(s, size, pad)` | `rpad(s, size, pad)` | ✅ | Native DataFusion |
| `rtrim(s)` | `rtrim(s)` | ✅ | Native DataFusion |
| `soundex(s)` | `soundex(s)` | ✅ | Trino compat UDF |
| `split(s, delim)` | `string_to_array(s, delim)` | ⚠️ | Different name, same semantics |
| `split_part(s, delim, idx)` | `split_part(s, delim, idx)` | ✅ | Native DataFusion |
| `strpos(s, sub)` | `strpos(s, sub)` | ✅ | Trino compat UDF |
| `substr(s, start, len)` | `substr(s, start, len)` | ✅ | Native DataFusion |
| `translate(s, from, to)` | `translate(s, from, to)` | ✅ | Native DataFusion |
| `trim(s)` | `trim(s)` | ✅ | Native DataFusion |
| `upper(s)` | `upper(s)` | ✅ | Native DataFusion |
| `word_stem(s)` | `word_stem(s)` | ✅ | Trino compat UDF (English default) |
| `word_stem(s, lang)` | `word_stem_lang(s, lang)` | ⚠️ | Different name, 17 languages |

## Scalar Functions: Math

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `abs(x)` | `abs(x)` | ✅ | |
| `acos(x)` / `asin(x)` / `atan(x)` | Same | ✅ | |
| `atan2(y, x)` | `atan2(y, x)` | ✅ | |
| `cbrt(x)` | `cbrt(x)` | ✅ | |
| `ceil(x)` / `ceiling(x)` | `ceil(x)` | ✅ | |
| `cos(x)` / `sin(x)` / `tan(x)` | `cos(x)` / `sin(x)` / `tan(x)` | ✅ | |
| `cosh(x)` / `sinh(x)` / `tanh(x)` | Same | ✅ | Native DataFusion (already built-in) |
| `degrees(x)` | `degrees(x)` | ✅ | |
| `e()` | `e()` | ✅ | Trino compat Nullary UDF returning `std::f64::consts::E` |
| `exp(x)` | `exp(x)` | ✅ | |
| `floor(x)` | `floor(x)` | ✅ | |
| `from_base(s, radix)` | `from_base(s, radix)` | ✅ | Trino compat UDF |
| `infinity()` | `infinity()` | ✅ | Trino compat UDF |
| `ln(x)` | `ln(x)` | ✅ | |
| `log(b, x)` | `log(b, x)` | ✅ | |
| `log2(x)` | `log2(x)` | ✅ | |
| `log10(x)` | `log10(x)` | ✅ | |
| `mod(n, m)` | `mod(n, m)` | ✅ | Trino compat UDF; coerces numeric args to Float64. Errors on `mod(_, 0)` per IEEE 754 |
| `nan()` | `nan()` | ✅ | Trino compat UDF |
| `pi()` | `pi()` | ✅ | |
| `pow(x, p)` / `power(x, p)` | `power(x, p)` | ✅ | |
| `radians(x)` | `radians(x)` | ✅ | |
| `rand()` / `random()` | `random()` | ✅ | |
| `round(x)` / `round(x, d)` | `round(x, d)` | ✅ | |
| `sign(x)` | `sign(x)` | ✅ | Trino compat UDF; matches Trino spec including `sign(0) = 0` (Rust's `f64::signum(0.0)` returns 1.0, so the UDF overrides the zero case) |
| `sqrt(x)` | `sqrt(x)` | ✅ | |
| `to_base(n, radix)` | `to_base(n, radix)` | ✅ | Trino compat UDF |
| `truncate(x[, n])` | `truncate(x[, n])` | ✅ | Trino compat UDF; truncates toward zero with optional decimal-precision argument |
| `width_bucket(x, bound1, bound2, n)` | Same | ✅ | Native DataFusion (built-in in DF 52) |

## Scalar Functions: Date/Time

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `current_date` | `current_date` | ✅ | SQL standard |
| `current_time` | `current_time` | ✅ | Native DataFusion (already built-in) |
| `current_timestamp` | `current_timestamp` / `now()` | ✅ | |
| `current_timezone()` | `current_timezone()` | ✅ | Trino compat UDF (returns "UTC") |
| `now()` | `now()` | ✅ | Trino compat UDF |
| `localtime` | `localtime()` | ✅ | Trino compat UDF |
| `localtimestamp` | `localtimestamp()` | ✅ | Trino compat UDF |
| `date(s)` | `trino_date(s)` | ✅ | Trino compat UDF |
| `from_iso8601_date(s)` | `from_iso8601_date(s)` | ✅ | Trino compat UDF |
| `from_iso8601_timestamp(s)` | `from_iso8601_timestamp(s)` | ✅ | Trino compat UDF |
| `from_unixtime(n)` | `from_unixtime(n)` | ✅ | Trino compat UDF |
| `to_unixtime(ts)` | `to_unixtime(ts)` | ✅ | Trino compat UDF |
| `to_iso8601(ts)` | `to_iso8601(ts)` | ✅ | Trino compat UDF |
| `date_add(unit, n, ts)` | `date_add(ts, unit, n)` | ⚠️ | Different argument order |
| `date_diff(unit, ts1, ts2)` | `date_diff(unit, ts1, ts2)` | ✅ | Trino compat UDF |
| `date_trunc(unit, ts)` | `date_trunc(unit, ts)` | ✅ | Native DataFusion |
| `date_format(ts, fmt)` | `date_format(ts, fmt)` | ✅ | Trino compat UDF (MySQL format codes) |
| `date_parse(s, fmt)` | `date_parse(s, fmt)` | ✅ | Trino compat UDF (MySQL format codes) |
| `format_datetime(ts, fmt)` | `format_datetime(ts, fmt)` | ✅ | Trino compat UDF (Joda→chrono translation) |
| `parse_datetime(s, fmt)` | `parse_datetime(s, fmt)` | ✅ | Trino compat UDF (Joda→chrono translation) |
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
| `millisecond(ts)` | `millisecond(ts)` | ✅ | Trino compat UDF |
| `timezone_hour(ts)` | `timezone_hour(ts)` | ✅ | Trino compat UDF (returns 0, UTC-only) |
| `timezone_minute(ts)` | `timezone_minute(ts)` | ✅ | Trino compat UDF (returns 0, UTC-only) |
| `with_timezone(ts, tz)` | `with_timezone(ts, tz)` | ✅ | Trino compat UDF (chrono-tz) |
| `at_timezone(ts, tz)` | `at_timezone(ts, tz)` | ✅ | Trino compat UDF (chrono-tz) |
| `INTERVAL 'n' UNIT` | `INTERVAL 'n' UNIT` | ✅ | SQL standard |
| `human_readable_seconds(n)` | `human_readable_seconds(n)` | ✅ | Trino compat UDF |
| `last_day_of_month(d)` | `last_day_of_month(d)` | ✅ | Trino compat UDF |

## Scalar Functions: JSON

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `json_object(k1, v1, k2, v2, ...)` | `json_object(k1, v1, ...)` | ✅ | Trino compat UDF |
| `json_format(json)` | `json_format(json)` | ✅ | Trino compat UDF |
| `json_parse(s)` | `json_parse(s)` | ✅ | Trino compat UDF |
| `json_extract(json, path)` | `json_extract(json, path)` | ✅ | Trino compat UDF (dot-path, not full JSONPath) |
| `json_extract_scalar(json, path)` | `json_extract_scalar(json, path)` | ✅ | Trino compat UDF |
| `json_size(json, path)` | `json_size(json, path)` | ✅ | Trino compat UDF |
| `json_array_contains(json, val)` | `json_array_contains(json, val)` | ✅ | Trino compat UDF |
| `json_array_get(json, idx)` | `json_array_get(json, idx)` | ✅ | Trino compat UDF (supports negative index) |
| `json_array_length(json)` | `json_array_length(json)` | ✅ | Trino compat UDF |
| `is_json_scalar(json)` | `is_json_scalar(json)` | ✅ | Trino compat UDF |
| `CAST(v AS JSON)` | `to_json(v)` | ⚠️ | Trino compat UDF (different syntax, same result) |
| `CAST(json AS type)` | `CAST(json_col AS type)` | ✅ | JSON aliases to `Utf8`; CAST rides DataFusion's built-in coercion. For typed extraction from JSONPath, use `json_get_int(j, '$')`, `json_get_str(j, '$')`, etc. |

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
| `regexp_extract(s, pattern)` | `regexp_extract(s, pattern)` | ✅ | Trino compat UDF |
| `regexp_extract_all(s, pattern)` | `regexp_extract_all(s, pattern)` | ⚠️ | Returns JSON array string, not ARRAY type |
| `regexp_like(s, pattern)` | `regexp_like(s, pattern)` | ✅ | Native DataFusion |
| `regexp_replace(s, pattern, repl)` | `regexp_replace(s, pattern, repl)` | ✅ | |
| `regexp_split(s, pattern)` | `regexp_split(s, pattern)` | ⚠️ | Returns JSON array string, not ARRAY type |

## Scalar Functions: Conditional

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `CASE WHEN ... THEN ... END` | Same | ✅ | SQL standard |
| `COALESCE(v1, v2, ...)` | Same | ✅ | |
| `NULLIF(v1, v2)` | Same | ✅ | |
| `GREATEST(v1, v2, ...)` | Same | ✅ | Native DataFusion |
| `LEAST(v1, v2, ...)` | Same | ✅ | Native DataFusion |
| `IF(cond, true, false)` | `trino_if(cond, true, false)` | ✅ | Trino compat UDF |
| `TRY(expr)` | `try(expr)` | ⚠️ | Passthrough UDF; does not catch runtime errors (DataFusion limitation), but query won't fail with "unknown function" |
| `TRY_CAST(v AS type)` | `TRY_CAST(v AS type)` | ✅ | Native DataFusion |

## Scalar Functions: Conversion / Type Cast

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `CAST(v AS type)` | Same | ✅ | |
| `TRY_CAST(v AS type)` | Same | ✅ | |
| `typeof(v)` | `typeof(v)` | ✅ | Trino compat UDF |
| `format(fmt, ...)` | `format(fmt, ...)` | ✅ | Trino compat UDF (%s, %d, %f, zero-pad, precision) |
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
| `every(x)` | `every(x)` | ✅ | Trino compat UDF (scalar alias for bool_and) |
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
| `arbitrary(x)` | `arbitrary(x)` | ✅ | Trino compat UDF (returns first non-null) |
| `max_by(x, y)` / `min_by(x, y)` | `max_by(x, y)` / `min_by(x, y)` | ⚠️ | Scalar stub (aggregate behavior requires UDAF) |
| `histogram(x)` | — | ❌ | |
| `multimap_agg(k, v)` | — | ❌ | |
| `map_agg(k, v)` | — | ❌ | |
| `map_union(map)` | — | ❌ | |
| `checksum(x)` | `checksum(x)` | ✅ | Trino compat UDF (hash-based) |
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
| Frame specs: ROWS/RANGE/GROUPS | All three supported | ✅ | Native DataFusion (GROUPS added in DF 19, 2022) |
| `QUALIFY` clause | Same | ✅ | Native DataFusion + sqlparser 0.53 |
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
| `ALTER TABLE ... SET PROPERTIES` | `ALTER TABLE ... SET TBLPROPERTIES` | ✅ | Iceberg TableUpdate::SetProperties |
| `CREATE VIEW` | Same | ✅ | Iceberg views |
| `DROP VIEW` | Same | ✅ | |
| `CREATE OR REPLACE VIEW` | Same | ✅ | Drop + recreate (non-atomic) |
| `CREATE MATERIALIZED VIEW` | — | ❌ | Not in Iceberg spec; use CTAS + scheduled refresh |
| `INSERT INTO ... VALUES` | Same | ✅ | |
| `INSERT INTO ... SELECT` | Same | ✅ | |
| `DELETE FROM ... WHERE` | Same | ✅ | CoW rewrite_files |
| `UPDATE ... SET ... WHERE` | Same | ✅ | CoW rewrite_files |
| `MERGE INTO ... USING ...` | Same | ✅ | CoW full-outer-join rewrite |
| `TRUNCATE TABLE` | `TRUNCATE TABLE t` | ✅ | Routes to DELETE FROM (no WHERE) |
| `COMMENT ON TABLE/COLUMN` | Same | ✅ | Stored as Iceberg table property (`comment` / `comment.<col>`) |
| `SHOW CATALOGS` | Same | ✅ | |
| `SHOW SCHEMAS` | Same | ✅ | |
| `SHOW TABLES` | Same | ✅ | |
| `SHOW COLUMNS FROM` | `DESCRIBE` | ⚠️ | Different syntax |
| `SHOW CREATE TABLE` | Same | ✅ | Reconstructs DDL from information_schema |
| `SHOW STATS FOR` | Same | ✅ | Returns row_count, data_file_count, total_size from snapshot summary |
| `EXPLAIN` | Same | ✅ | DataFusion explain |
| `EXPLAIN ANALYZE` | `EXPLAIN FULL` | ⚠️ | Different keyword, similar output |
| `USE catalog.schema` | Same | ✅ | Parsed and accepted (session-level, sets default catalog/schema) |
| `PREPARE` / `EXECUTE` | Partial | ⚠️ | DataFusion has infrastructure, SQL integration incomplete |
| `CALL procedure(...)` | — | ⚠️ | Returns informative error "SQE does not have stored procedures" |
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
| `TIME` / `TIME(p)` | `Time64(Microsecond)` | ✅ | Iceberg's `time` primitive is microsecond-only; precisions 0..=6 collapse to `Time64(Microsecond)`. `localtime()` returns Time64. `hour() / minute() / second()` work on TIME columns; `year() / month() / day()` raise a clear plan error per Trino spec |
| `TIME WITH TIME ZONE` | — | ❌ | No Arrow equivalent. CREATE TABLE rejects with NotImplemented pointing at `TIMESTAMP WITH TIME ZONE` |
| `TIMESTAMP` | `Timestamp(Microsecond, None)` | ✅ | |
| `TIMESTAMP WITH TIME ZONE` | `Timestamp(Microsecond, Some(tz))` | ✅ | |
| `INTERVAL YEAR TO MONTH` | `Interval(YearMonth)` | ✅ | |
| `INTERVAL DAY TO SECOND` | `Interval(DayTime)` | ✅ | |
| `ARRAY(T)` | `List(T)` | ✅ | |
| `MAP(K, V)` | `Map(K, V)` | ✅ | |
| `ROW(fields...)` | `Struct(fields...)` | ✅ | |
| `JSON` | `Utf8` | ✅ | `CREATE TABLE t(payload JSON)` aliases to `Utf8`. `CAST(json_col AS BIGINT|VARCHAR|DOUBLE)` rides DataFusion's built-in Utf8→target coercion. Full JSON extraction via `json_extract`, `json_extract_scalar`, `json_array_length`, `json_parse`, `json_get_str/int/float/bool` |
| `UUID` | `Utf8` | ⚠️ | Stored as string, no UUID type |
| `IPADDRESS` | `VARCHAR` | ⚠️ | Stored as VARCHAR, no IP-specific functions (subnet containment, etc.) |
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
| Time travel: `FOR VERSION AS OF` | `FOR SYSTEM_TIME AS OF` | ✅ | ✅ | Pre-processes AST, resolves snapshot_id via metadata |
| Time travel: `FOR TIMESTAMP AS OF` | Same mechanism | ✅ | ✅ | Timestamp resolved to nearest snapshot |
| `$snapshots` metadata table | `table_snapshots('ns', 'table')` | ✅ | ⚠️ | TVF instead of `$snapshots` syntax; queries Polaris REST catalog metadata |
| `$manifests` metadata table | `table_manifests('ns', 'table')` | ✅ | ⚠️ | TVF instead of `$manifests` syntax; reads manifest list from Polaris |
| `$history` metadata table | `table_history('ns', 'table')` | ✅ | ⚠️ | TVF syntax |
| `$partitions` metadata table | `table_partitions('ns', 'table')` | ✅ | ⚠️ | TVF syntax |
| `$files` metadata table | `table_files('ns', 'table')` | ✅ | ⚠️ | TVF syntax |
| `$refs` metadata table | `table_refs('ns', 'table')` | ✅ | ⚠️ | TVF syntax |
| Partition evolution | ✅ | ✅ | ✅ | Via ALTER TABLE |
| Sort order | — | ✅ | ❌ | |
| Write distribution mode | — | ✅ | ❌ | |
| ORC file format | — | ✅ | ❌ | Parquet only |
| Copy-on-Write (CoW) | ✅ | ✅ | ✅ | DELETE/UPDATE/MERGE |
| Merge-on-Read (MoR) reads | ✅ | ✅ | ✅ | Position deletes, equality deletes, and V3 deletion vectors all readable (RW fork has full read support) |
| Merge-on-Read (MoR) writes | ✅ via `write.delete.mode='merge-on-read'` | ✅ | ✅ | `handle_delete_dispatch` routes by table property: position deletes when no PK declared, equality deletes with PK. Position deletes commit via `FastAppendAction`; equality deletes via `RowDeltaAction`. CoW remains the default |

## Engine Limitations & Roadmap

The ~4% remaining gap consists of features that require engine-level changes, sketch data structures not applicable to Iceberg analytics, or strategic choices. None of these block typical dbt/BI workloads.

| Feature | Blocker | Path Forward |
|---|---|---|
| `TIME WITH TIME ZONE` | Arrow has no `TimeWithTimezone` type (unlike `Timestamp(unit, Some(tz))`). Trino itself discourages it: nearly every production Trino table uses `TIMESTAMP WITH TIME ZONE` instead | Not planned. Recommend `TIMESTAMP WITH TIME ZONE` as the substitute (already ✅). SQE rejects `TIME WITH TIME ZONE` at CREATE TABLE with a clear NotImplemented |
| `histogram(x)` / `map_agg(k,v)` / `multimap_agg(k,v)` | Map-producing aggregates require custom UDAF with Arrow `MapBuilder` output; cannot be expressed as scalar UDFs | Implement as UDAF using `MapBuilder` (~200–300 lines each) |
| `approx_most_frequent(n, x, cap)` | Count-Min Sketch algorithm requires stateful UDAF with sketch accumulator | Custom UDAF with sketch state (~400 lines) |
| `merge(digest)` / HyperLogLog / TDigest / SetDigest | Trino-specific sketch types with binary merge semantics; no Arrow equivalent | Not planned. These types are not used in Iceberg analytics |
| `CREATE MATERIALIZED VIEW` | Materialized views are not part of the Iceberg spec; no persistent refresh mechanism | Use CTAS + scheduled refresh (cron / Airflow DAG) |
| Lambda in window functions | DataFusion does not support lambda expressions inside window specs | Not planned. Use subqueries or lateral joins instead |
| ORC file format | Strategic choice: `datafusion-orc` is read-only and experimental | Parquet-only is the long-term strategy for Iceberg workloads |
| Sort order enforcement | Iceberg write-path: sort order metadata written but files not physically sorted | SQE planner + writer changes needed (~sort-on-write pass) |
| Write distribution mode | Architectural: requires shuffle/repartition layer before write | Planned for distributed write path (Phase 3+) |
| Iceberg V3 VARIANT type | Arrow `Variant` type proposal at `apache/arrow-rs#7142` not merged | Wait for upstream. SQE's JSON-as-Utf8 covers most JSON workloads in the meantime |

### Items shipped recently (2026-05-04)

| Feature | Status | What changed |
|---|---|---|
| `JSON` logical type / `CAST(json AS T)` | ✅ shipped | `SqlType::JSON → Utf8` in `sql_type_to_arrow` (write_handler.rs:3927). DataFusion's built-in coercion handles `CAST(json AS BIGINT|VARCHAR|DOUBLE)` |
| `TIME` / `TIME(p)` / `EXTRACT(HOUR\|MINUTE\|SECOND ...)` | ✅ shipped | `SqlType::Time` arm collapses 0..=6 precision to `Time64(Microsecond)`. `localtime()` actually returns Time64 now (was incorrectly returning Timestamp). `extract_component` handles Time64Microsecond + Time64Nanosecond arrays/scalars; `year()/month()/day()` raise plan errors on TIME columns per Trino spec |
| Merge-on-Read (MoR) writes | ✅ already wired | `handle_delete_dispatch` has shipped since Phase O+; the doc was stale. Set `TBLPROPERTIES ('write.delete.mode'='merge-on-read')` to opt in. CoW remains the default for backward compat |

### Items shipped 2026-05-05 (Trino aliases)

Five more amber rows flipped to ✅ via small alias UDFs in `sqe-trino-functions/src/trino_functions.rs`:

| Function | Status | What changed |
|---|---|---|
| `e()` | ✅ shipped | Nullary UDF returning `std::f64::consts::E`. Trino's `e()` is a function, not a constant; previously users had to write `exp(1)` |
| `mod(n, m)` | ✅ shipped | Scalar UDF coercing both args to `Float64`; errors on `mod(_, 0)`. Trino has `mod()` as a function; DataFusion only exposes the `%` operator |
| `truncate(x[, n])` | ✅ shipped | Scalar UDF that truncates toward zero with optional decimal-precision argument. Same shape as Trino's `truncate(x)` and `truncate(x, n)` |
| `sign(x)` | ✅ shipped | Scalar UDF over `Float64`. Matches Trino spec including `sign(0) = 0` (Rust's `f64::signum(0.0)` returns 1.0; the UDF overrides the zero case) |
| `codepoint(s)` | ✅ shipped | Scalar UDF returning the full Unicode code point of a single-character string. Errors on multi-character input per Trino spec. ASCII characters round-trip to the same value as `ascii()` for backward compat; Unicode characters now return the proper code point (`'é'` → 233) instead of the first UTF-8 byte (`195`) |

Net coverage delta: **Scalar: Math 25/29 (4 amber) → 29/29 (0 amber)**. **Scalar: String 24/27 (3 amber) → 25/27 (2 amber)**.

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
