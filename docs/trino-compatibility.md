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
| Scalar: Date/Time | — | — | — | — | —% |
| Scalar: JSON | — | — | — | — | —% |
| Scalar: URL | 8 | 0 | 0 | 8 | 0% |
| Scalar: Regex | 6 | 3 | 0 | 3 | 50% |
| Scalar: Conditional | 8 | 7 | 0 | 1 | 87.5% |
| Scalar: Conversion | 10 | 3 | 0 | 7 | 30% |
| Aggregate | — | — | — | — | —% |
| Window | — | — | — | — | —% |
| DDL/DML | — | — | — | — | —% |
| Type System | — | — | — | — | —% |
| Iceberg-Specific | — | — | — | — | —% |

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

_To be filled in Task 3_

## Scalar Functions: JSON

_To be filled in Task 3_

## Scalar Functions: URL

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `url_extract_host(url)` | — | ❌ | |
| `url_extract_path(url)` | — | ❌ | |
| `url_extract_port(url)` | — | ❌ | |
| `url_extract_protocol(url)` | — | ❌ | |
| `url_extract_query(url)` | — | ❌ | |
| `url_extract_parameter(url, name)` | — | ❌ | |
| `url_encode(s)` | — | ❌ | |
| `url_decode(s)` | — | ❌ | |

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
| `from_utf8(binary)` | — | ❌ | |
| `to_utf8(string)` | — | ❌ | |
| `from_base64(s)` | — | ❌ | |
| `to_base64(binary)` | — | ❌ | |
| `from_hex(s)` | — | ❌ | |
| `to_hex(binary)` | — | ❌ | |

## Aggregate Functions

_To be filled in Task 3_

## Window Functions

_To be filled in Task 3_

## DDL / DML Statements

_To be filled in Task 4_

## Type System

_To be filled in Task 4_

## Iceberg-Specific SQL

_To be filled in Task 4_

## Operational Comparison

_To be filled in Task 12_
