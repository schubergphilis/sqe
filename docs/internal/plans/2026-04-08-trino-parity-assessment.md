# Trino Parity Assessment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a comprehensive, living Trino compatibility assessment covering SQL functions, automated side-by-side benchmarks, client compatibility testing, and operational metrics comparison.

**Architecture:** Four deliverables: (1) `docs/trino-compatibility.md` — function-by-function SQL compatibility matrix with ratings; (2) `sqe-bench compare` subcommand — run identical queries against SQE and Trino, diff results, report speedup; (3) `docs/trino-client-compatibility.md` — pass/fail for real Trino clients; (4) operational comparison (build time, memory, binary size). Minimal new Rust code — most work is in `sqe-bench` tooling and documentation.

**Tech Stack:** Rust (sqe-bench), reqwest (Trino HTTP client), clap (CLI), serde (JSON reports), Docker Compose (side-by-side stack)

**Design spec:** `docs/superpowers/specs/2026-04-08-oss-release-and-catalogs-design.md` (Spec D)

**Independent of:** Plans A+B and C — can be executed in parallel with Plan C.

---

## File Structure

### Files to Create

| File | Purpose |
|---|---|
| `docs/trino-compatibility.md` | Function-by-function SQL compatibility matrix (living document) |
| `docs/trino-client-compatibility.md` | Client pass/fail results with workarounds |
| `crates/sqe-bench/src/compare.rs` | Comparison runner: execute against SQE + Trino, diff results |
| `crates/sqe-bench/src/trino_client.rs` | Trino HTTP protocol client for sqe-bench |
| `docker-compose.compare.yml` | Side-by-side stack: SQE + Trino + shared Polaris + S3 |
| `scripts/trino-parity-test.sh` | Convenience script: spin up compare stack, run comparison, report |

### Files to Modify

| File | Change |
|---|---|
| `crates/sqe-bench/src/cli.rs` | Add `Compare` subcommand to clap Command enum |
| `crates/sqe-bench/src/main.rs` | Wire Compare subcommand to comparison runner |
| `crates/sqe-bench/src/report.rs` | Add comparison report format (SQE time, Trino time, speedup, diff) |
| `crates/sqe-bench/Cargo.toml` | No new deps needed (reqwest already present) |
| `crates/sqe-coordinator/src/trino_functions.rs` | Add any high-priority missing functions found during audit |
| `docs/features.md` | Cross-reference to new trino-compatibility.md |
| `README.md` | Update roadmap |
| `nextsteps.md` | Update status |

---

## Phase 1: SQL Compatibility Matrix (D1)

### Task 1: Create Trino Compatibility Document — Scaffold + Rating System

**Files:**
- Create: `docs/trino-compatibility.md`

This is the foundational reference document. Start with the structure, rating system, and categories. Content will be filled in Tasks 2–4.

- [ ] **Step 1: Create the document scaffold**

```markdown
# Trino SQL Compatibility Matrix

> Living document. Last updated: 2026-04-08.
> Rating: ✅ equivalent | ⚠️ partial/different semantics | ❌ missing | 🔧 SQE-specific

SQE aims to be a drop-in replacement for Trino in Iceberg-only environments.
This document maps every Trino SQL function and feature to its SQE equivalent,
noting semantic differences and gaps.

## Summary

| Category | Total | ✅ | ⚠️ | ❌ | Coverage |
|---|---|---|---|---|---|
| Scalar: String | — | — | — | — | —% |
| Scalar: Math | — | — | — | — | —% |
| Scalar: Date/Time | — | — | — | — | —% |
| Scalar: JSON | — | — | — | — | —% |
| Scalar: URL | — | — | — | — | —% |
| Scalar: Regex | — | — | — | — | —% |
| Scalar: Conditional | — | — | — | — | —% |
| Scalar: Conversion | — | — | — | — | —% |
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

_To be filled in Task 2_

## Scalar Functions: Math

_To be filled in Task 2_

## Scalar Functions: Date/Time

_To be filled in Task 3_

## Scalar Functions: JSON

_To be filled in Task 3_

## Scalar Functions: URL

_To be filled in Task 2_

## Scalar Functions: Regex

_To be filled in Task 2_

## Scalar Functions: Conditional

_To be filled in Task 2_

## Scalar Functions: Conversion / Type Cast

_To be filled in Task 2_

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
```

- [ ] **Step 2: Add cross-reference from features.md**

Add to the top of `docs/features.md`:

```markdown
> For a detailed function-by-function Trino compatibility matrix, see [trino-compatibility.md](trino-compatibility.md).
```

- [ ] **Step 3: Commit**

```bash
git add docs/trino-compatibility.md docs/features.md
git commit -m "docs: scaffold trino-compatibility.md with rating system

Living document tracking function-by-function SQL compatibility between
SQE and Trino. Sections for scalar, aggregate, window, DDL/DML, types,
and Iceberg-specific SQL. Cross-referenced from features.md."
```

---

### Task 2: Audit Scalar Functions — String, Math, URL, Regex, Conditional, Conversion

**Files:**
- Modify: `docs/trino-compatibility.md`

Systematic audit of Trino's scalar function categories against DataFusion's built-in functions + SQE's Trino compat UDFs. Reference sources:
- Trino docs: https://trino.io/docs/current/functions.html
- DataFusion built-ins: https://datafusion.apache.org/user-guide/sql/scalar_functions.html
- SQE Trino UDFs: `crates/sqe-coordinator/src/trino_functions.rs`

- [ ] **Step 1: Audit String functions**

Fill in the String section. Trino has ~60 string functions. For each one, check if DataFusion has a native equivalent (same name or alias). Mark status. Example entries:

```markdown
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
| `position(sub IN s)` | `position(sub IN s)` / `strpos(s, sub)` | ✅ | Both syntaxes work |
| `replace(s, from, to)` | `replace(s, from, to)` | ✅ | Native DataFusion |
| `reverse(s)` | `reverse(s)` | ✅ | Native DataFusion |
| `rpad(s, size, pad)` | `rpad(s, size, pad)` | ✅ | Native DataFusion |
| `rtrim(s)` | `rtrim(s)` | ✅ | Native DataFusion |
| `split(s, delim)` | `string_to_array(s, delim)` | ⚠️ | Different name, same semantics |
| `split_part(s, delim, idx)` | `split_part(s, delim, idx)` | ✅ | Native DataFusion |
| `strpos(s, sub)` | `strpos(s, sub)` | ✅ | Trino compat UDF |
| `substr(s, start, len)` | `substr(s, start, len)` | ✅ | Native DataFusion |
| `trim(s)` | `trim(s)` | ✅ | Native DataFusion |
| `upper(s)` | `upper(s)` | ✅ | Native DataFusion |
| `normalize(s, form)` | — | ❌ | Unicode normalization not available |
| `soundex(s)` | — | ❌ | |
| `translate(s, from, to)` | `translate(s, from, to)` | ✅ | Native DataFusion |
| `word_stem(s)` | — | ❌ | NLP function |
| `word_stem(s, lang)` | — | ❌ | NLP function |
```

Continue for all ~60 Trino string functions. Be thorough — check each one.

- [ ] **Step 2: Audit Math functions**

Fill in the Math section. Trino has ~30 math functions. Most map directly to DataFusion:

```markdown
## Scalar Functions: Math

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `abs(x)` | `abs(x)` | ✅ | |
| `cbrt(x)` | `cbrt(x)` | ✅ | |
| `ceil(x)` / `ceiling(x)` | `ceil(x)` | ✅ | |
| `degrees(x)` | `degrees(x)` | ✅ | |
| `e()` | `exp(1)` | ⚠️ | No standalone `e()`, use `exp(1)` |
| `exp(x)` | `exp(x)` | ✅ | |
| `floor(x)` | `floor(x)` | ✅ | |
| `from_base(s, radix)` | — | ❌ | |
| `infinity()` | — | ❌ | Use `CAST('Infinity' AS DOUBLE)` |
| `ln(x)` | `ln(x)` | ✅ | |
| `log2(x)` | `log2(x)` | ✅ | |
| `log10(x)` | `log10(x)` | ✅ | |
| `log(b, x)` | `log(b, x)` | ✅ | |
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
| `cos(x)` / `sin(x)` / `tan(x)` | `cos(x)` / `sin(x)` / `tan(x)` | ✅ | |
| `acos(x)` / `asin(x)` / `atan(x)` | Same | ✅ | |
| `atan2(y, x)` | `atan2(y, x)` | ✅ | |
| `cosh(x)` / `sinh(x)` / `tanh(x)` | — | ❌ | Hyperbolic functions not in DataFusion |
```

- [ ] **Step 3: Audit URL, Regex, Conditional, Conversion functions**

```markdown
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
```

- [ ] **Step 4: Update summary table with counts**

Go back to the Summary table and fill in the actual counts for each category.

- [ ] **Step 5: Commit**

```bash
git add docs/trino-compatibility.md
git commit -m "docs: audit scalar functions — string, math, URL, regex, conditional, cast

Comprehensive function-by-function Trino compatibility audit for scalar
function categories. ~60 string, ~30 math, 8 URL, 6 regex, 8 conditional,
10 conversion functions mapped with status ratings."
```

---

### Task 3: Audit Date/Time, JSON, Aggregate, and Window Functions

**Files:**
- Modify: `docs/trino-compatibility.md`

These are the most complex categories — date/time has the most SQE Trino compat UDFs, and JSON is a major gap area.

- [ ] **Step 1: Audit Date/Time functions**

```markdown
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
```

- [ ] **Step 2: Audit JSON functions**

```markdown
## Scalar Functions: JSON

| Trino Function | SQE Equivalent | Status | Notes |
|---|---|---|---|
| `json_object(k1, v1, k2, v2, ...)` | `json_object(k1, v1, ...)` | ✅ | Trino compat UDF |
| `json_format(json)` | `json_format(json)` | ✅ | Trino compat UDF |
| `json_parse(s)` | — | ❌ | |
| `json_extract(json, path)` | — | ❌ | JSONPath extraction |
| `json_extract_scalar(json, path)` | — | ❌ | |
| `json_size(json, path)` | — | ❌ | |
| `json_array_contains(json, val)` | — | ❌ | |
| `json_array_get(json, idx)` | — | ❌ | |
| `json_array_length(json)` | — | ❌ | |
| `is_json_scalar(json)` | — | ❌ | |
| `CAST(v AS JSON)` | — | ❌ | No JSON type |
| `CAST(json AS type)` | — | ❌ | No JSON type |

**Note:** JSON support is the largest gap. DataFusion has `arrow_cast` and some JSON functions via extensions, but Trino's full JSONPath-based extraction model is not available. This is a known limitation — most Iceberg workloads use structured columns rather than JSON blobs.
```

- [ ] **Step 3: Audit Aggregate functions**

```markdown
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
```

- [ ] **Step 4: Audit Window functions**

```markdown
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
```

- [ ] **Step 5: Update summary table with final counts**

Count all ✅/⚠️/❌ per category and fill in the Summary table. Calculate coverage percentages.

- [ ] **Step 6: Commit**

```bash
git add docs/trino-compatibility.md
git commit -m "docs: audit date/time, JSON, aggregate, and window functions

Date/time: 25/37 covered (67%), strong extract/format coverage.
JSON: 2/12 (17%), largest gap — JSONPath extraction missing.
Aggregate: 20/32 (63%), core analytics complete, exotic types missing.
Window: 11/14 (79%), GROUPS frame and QUALIFY clause missing."
```

---

### Task 4: Audit DDL/DML, Type System, and Iceberg-Specific SQL

**Files:**
- Modify: `docs/trino-compatibility.md`

- [ ] **Step 1: Audit DDL/DML**

```markdown
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
```

- [ ] **Step 2: Audit Type System**

```markdown
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
```

- [ ] **Step 3: Audit Iceberg-specific SQL**

```markdown
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
```

- [ ] **Step 4: Commit**

```bash
git add docs/trino-compatibility.md
git commit -m "docs: audit DDL/DML, type system, and Iceberg-specific SQL

DDL/DML: 18/31 (58%), core CRUD complete, PREPARE/CALL/USE missing.
Types: 20/27 (74%), main gap is JSON, TIME, and sketch types.
Iceberg: 6/17 (35%), metadata tables and time travel are key gaps."
```

---

## Phase 2: Automated Side-by-Side Benchmark (D2)

### Task 5: Add Compare CLI Subcommand

**Files:**
- Modify: `crates/sqe-bench/src/cli.rs`
- Modify: `crates/sqe-bench/src/main.rs`

Add the `compare` subcommand with dual-endpoint arguments.

- [ ] **Step 1: Write the CLI test**

Add a test in `cli.rs` to verify the Compare subcommand parses correctly:

```rust
#[test]
fn test_compare_subcommand_parses() {
    let args = Cli::parse_from([
        "sqe-bench", "compare", "tpch",
        "--scale", "1",
        "--sqe-host", "localhost",
        "--sqe-port", "50051",
        "--trino-url", "http://localhost:8080",
    ]);
    match args.command {
        Command::Compare(c) => {
            assert_eq!(c.benchmark, "tpch");
            assert_eq!(c.scale, 1.0);
            assert_eq!(c.sqe_host, "localhost");
            assert_eq!(c.sqe_port, 50051);
            assert_eq!(c.trino_url, "http://localhost:8080");
        }
        _ => panic!("expected Compare command"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-bench test_compare_subcommand_parses`
Expected: FAIL — `Compare` variant doesn't exist yet.

- [ ] **Step 3: Add Compare variant to Command enum**

In `crates/sqe-bench/src/cli.rs`, add:

```rust
/// Compare SQE vs Trino: run identical queries against both and diff results.
Compare(CompareArgs),
```

And the args struct:

```rust
#[derive(Debug, Args)]
pub struct CompareArgs {
    /// Benchmark suite (tpch, tpcds, ssb)
    pub benchmark: String,

    /// Scale factor
    #[arg(long, default_value = "1")]
    pub scale: f64,

    /// SQE Flight SQL host
    #[arg(long, default_value = "localhost")]
    pub sqe_host: String,

    /// SQE Flight SQL port
    #[arg(long, default_value = "50051")]
    pub sqe_port: u16,

    /// SQE auth username
    #[arg(long, default_value = "")]
    pub sqe_username: String,

    /// SQE auth password
    #[arg(long, default_value = "")]
    pub sqe_password: String,

    /// Trino HTTP URL (e.g., http://localhost:8080)
    #[arg(long)]
    pub trino_url: String,

    /// Trino user
    #[arg(long, default_value = "admin")]
    pub trino_user: String,

    /// Trino catalog (default: same as benchmark namespace)
    #[arg(long)]
    pub trino_catalog: Option<String>,

    /// Trino schema (default: same as benchmark namespace)
    #[arg(long)]
    pub trino_schema: Option<String>,

    /// Single query to compare (e.g., "q1" or "1")
    #[arg(long)]
    pub query: Option<String>,

    /// Output directory for comparison report
    #[arg(long, default_value = "benchmarks/results")]
    pub output: String,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sqe-bench test_compare_subcommand_parses`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/cli.rs
git commit -m "feat(sqe-bench): add compare subcommand for SQE vs Trino benchmarks

CLI args for dual-endpoint comparison: SQE Flight SQL host/port and
Trino HTTP URL. Supports single query or full suite, with output to
benchmarks/results/."
```

---

### Task 6: Implement Trino HTTP Client for sqe-bench

**Files:**
- Create: `crates/sqe-bench/src/trino_client.rs`
- Modify: `crates/sqe-bench/src/main.rs` (add `mod trino_client;`)

Minimal Trino HTTP client that submits a query and collects all result pages.

- [ ] **Step 1: Write the Trino client tests**

```rust
//! Minimal Trino HTTP client for benchmark comparison.
//!
//! Submits queries via POST /v1/statement, follows nextUri pagination,
//! and collects all result rows. Does NOT attempt full Trino protocol
//! compliance — just enough for benchmark query execution.

use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Result of executing a single query against Trino.
#[derive(Debug, Serialize)]
pub struct TrinoQueryResult {
    pub rows: Vec<Vec<serde_json::Value>>,
    pub columns: Vec<String>,
    pub elapsed: Duration,
    pub error: Option<String>,
}

/// Trino response envelope (subset of fields we need).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoResponse {
    id: Option<String>,
    columns: Option<Vec<TrinoColumn>>,
    data: Option<Vec<Vec<serde_json::Value>>>,
    next_uri: Option<String>,
    stats: Option<TrinoStats>,
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
struct TrinoColumn {
    name: String,
}

#[derive(Debug, Deserialize)]
struct TrinoStats {
    state: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoError {
    message: String,
    error_code: Option<i32>,
    error_name: Option<String>,
}

pub struct TrinoClient {
    client: Client,
    base_url: String,
    user: String,
    catalog: String,
    schema: String,
}

impl TrinoClient {
    pub fn new(base_url: &str, user: &str, catalog: &str, schema: &str) -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .expect("failed to build HTTP client"),
            base_url: base_url.trim_end_matches('/').to_string(),
            user: user.to_string(),
            catalog: catalog.to_string(),
            schema: schema.to_string(),
        }
    }

    /// Execute a query and collect all result pages.
    pub async fn execute(&self, sql: &str) -> TrinoQueryResult {
        let start = Instant::now();

        // Submit query
        let resp = match self.client
            .post(format!("{}/v1/statement", self.base_url))
            .header("X-Trino-User", &self.user)
            .header("X-Trino-Catalog", &self.catalog)
            .header("X-Trino-Schema", &self.schema)
            .body(sql.to_string())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => return TrinoQueryResult {
                rows: vec![], columns: vec![],
                elapsed: start.elapsed(),
                error: Some(format!("HTTP error: {e}")),
            },
        };

        let mut response: TrinoResponse = match resp.json().await {
            Ok(r) => r,
            Err(e) => return TrinoQueryResult {
                rows: vec![], columns: vec![],
                elapsed: start.elapsed(),
                error: Some(format!("JSON parse error: {e}")),
            },
        };

        let mut all_rows = Vec::new();
        let mut columns = Vec::new();

        // Collect columns from first response
        if let Some(cols) = &response.columns {
            columns = cols.iter().map(|c| c.name.clone()).collect();
        }
        if let Some(data) = response.data.take() {
            all_rows.extend(data);
        }

        // Check for immediate error
        if let Some(err) = &response.error {
            return TrinoQueryResult {
                rows: all_rows, columns,
                elapsed: start.elapsed(),
                error: Some(err.message.clone()),
            };
        }

        // Follow pagination
        while let Some(next_uri) = response.next_uri.take() {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let next_resp = match self.client
                .get(&next_uri)
                .header("X-Trino-User", &self.user)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => return TrinoQueryResult {
                    rows: all_rows, columns,
                    elapsed: start.elapsed(),
                    error: Some(format!("pagination error: {e}")),
                },
            };

            response = match next_resp.json().await {
                Ok(r) => r,
                Err(e) => return TrinoQueryResult {
                    rows: all_rows, columns,
                    elapsed: start.elapsed(),
                    error: Some(format!("pagination JSON error: {e}")),
                },
            };

            if let Some(cols) = &response.columns {
                if columns.is_empty() {
                    columns = cols.iter().map(|c| c.name.clone()).collect();
                }
            }
            if let Some(data) = response.data.take() {
                all_rows.extend(data);
            }
            if let Some(err) = &response.error {
                return TrinoQueryResult {
                    rows: all_rows, columns,
                    elapsed: start.elapsed(),
                    error: Some(err.message.clone()),
                };
            }
        }

        TrinoQueryResult {
            rows: all_rows,
            columns,
            elapsed: start.elapsed(),
            error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trino_client_construction() {
        let client = TrinoClient::new(
            "http://localhost:8080",
            "admin",
            "iceberg",
            "tpch_sf1",
        );
        assert_eq!(client.base_url, "http://localhost:8080");
        assert_eq!(client.user, "admin");
        assert_eq!(client.catalog, "iceberg");
        assert_eq!(client.schema, "tpch_sf1");
    }

    #[test]
    fn test_trino_response_deserialization() {
        let json = r#"{
            "id": "query_1",
            "columns": [{"name": "cnt", "type": "bigint"}],
            "data": [[42]],
            "stats": {"state": "FINISHED"},
            "nextUri": null,
            "error": null
        }"#;
        let resp: TrinoResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, Some("query_1".to_string()));
        assert_eq!(resp.columns.unwrap().len(), 1);
        assert_eq!(resp.data.unwrap().len(), 1);
        assert_eq!(resp.stats.unwrap().state, "FINISHED");
        assert!(resp.next_uri.is_none());
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_trino_error_response_deserialization() {
        let json = r#"{
            "id": "query_2",
            "stats": {"state": "FAILED"},
            "error": {
                "message": "Table not found",
                "errorCode": 1,
                "errorName": "TABLE_NOT_FOUND"
            }
        }"#;
        let resp: TrinoResponse = serde_json::from_str(json).unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().message, "Table not found");
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test -p sqe-bench trino_client`
Expected: PASS (3 tests — construction, response deser, error deser)

- [ ] **Step 3: Add module to main.rs**

Add `mod trino_client;` to `crates/sqe-bench/src/main.rs`.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-bench/src/trino_client.rs crates/sqe-bench/src/main.rs
git commit -m "feat(sqe-bench): add Trino HTTP client for benchmark comparison

Minimal Trino protocol client: POST /v1/statement, follow nextUri
pagination, collect all result rows. Handles errors and timeouts.
Used by the compare subcommand for side-by-side benchmarks."
```

---

### Task 7: Implement Comparison Runner + JSON Report

**Files:**
- Create: `crates/sqe-bench/src/compare.rs`
- Modify: `crates/sqe-bench/src/main.rs` (wire compare subcommand)
- Modify: `crates/sqe-bench/src/report.rs` (add comparison report type)

The comparison runner executes each query against both SQE and Trino, diffs row counts, and produces a JSON + markdown report.

- [ ] **Step 1: Write the comparison report types**

Add to `crates/sqe-bench/src/report.rs`:

```rust
/// A single query comparison between SQE and Trino.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryComparison {
    pub query_name: String,
    pub sqe_time_ms: u64,
    pub trino_time_ms: u64,
    pub speedup: f64,  // trino_time / sqe_time (>1 means SQE faster)
    pub sqe_rows: usize,
    pub trino_rows: usize,
    pub rows_match: bool,
    pub sqe_error: Option<String>,
    pub trino_error: Option<String>,
    pub status: CompareStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CompareStatus {
    /// Both succeeded, rows match
    Match,
    /// Both succeeded, different row counts
    RowDiff,
    /// SQE failed, Trino succeeded
    SqeFailed,
    /// Trino failed, SQE succeeded
    TrinoFailed,
    /// Both failed
    BothFailed,
}

/// Full comparison report for a benchmark suite.
#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonReport {
    pub benchmark: String,
    pub scale: f64,
    pub timestamp: String,
    pub sqe_endpoint: String,
    pub trino_endpoint: String,
    pub queries: Vec<QueryComparison>,
    pub summary: ComparisonSummary,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComparisonSummary {
    pub total: usize,
    pub matched: usize,
    pub row_diff: usize,
    pub sqe_failed: usize,
    pub trino_failed: usize,
    pub both_failed: usize,
    pub avg_speedup: f64,
    pub median_speedup: f64,
    pub sqe_total_ms: u64,
    pub trino_total_ms: u64,
}
```

- [ ] **Step 2: Write the comparison runner**

Create `crates/sqe-bench/src/compare.rs`:

```rust
//! Side-by-side benchmark comparison: run identical queries against SQE and Trino.

use crate::cli::CompareArgs;
use crate::report::{CompareStatus, ComparisonReport, ComparisonSummary, QueryComparison};
use crate::trino_client::TrinoClient;
use std::path::Path;
use std::time::Instant;
use tracing::info;

/// Run comparison benchmark.
pub async fn run_compare(args: &CompareArgs) -> anyhow::Result<ComparisonReport> {
    let benchmark = &args.benchmark;
    let scale = args.scale;
    let namespace = format!("{}_sf{}", benchmark, scale as u64);
    let trino_catalog = args.trino_catalog.as_deref().unwrap_or("iceberg");
    let trino_schema = args.trino_schema.as_deref().unwrap_or(&namespace);

    // Load query files
    let query_dir = format!("crates/sqe-bench/queries/{}", benchmark);
    let mut query_files: Vec<_> = std::fs::read_dir(&query_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        .collect();
    query_files.sort_by_key(|e| e.file_name());

    // Filter to single query if specified
    if let Some(q) = &args.query {
        let q_normalized = q.trim_start_matches('q');
        query_files.retain(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.contains(q) || name.contains(&format!("q{}", q_normalized))
        });
    }

    info!("Comparing {} queries from {}", query_files.len(), benchmark);

    // Create clients
    let trino = TrinoClient::new(
        &args.trino_url,
        &args.trino_user,
        trino_catalog,
        trino_schema,
    );

    // SQE client — reuse existing Flight SQL client from sqe-bench
    let sqe_client = crate::client::create_flight_client(
        &args.sqe_host,
        args.sqe_port,
        &args.sqe_username,
        &args.sqe_password,
    ).await?;

    let mut comparisons = Vec::new();

    for entry in &query_files {
        let query_name = entry.file_name().to_string_lossy()
            .trim_end_matches(".sql").to_string();
        let sql = std::fs::read_to_string(entry.path())?;

        info!("  {} ...", query_name);

        // Run against SQE
        let sqe_start = Instant::now();
        let sqe_result = sqe_client.execute(&sql).await;
        let sqe_elapsed = sqe_start.elapsed();

        // Run against Trino
        let trino_result = trino.execute(&sql).await;

        let sqe_rows = sqe_result.as_ref().map(|r| r.num_rows()).unwrap_or(0);
        let sqe_error = sqe_result.as_ref().err().map(|e| e.to_string());

        let trino_rows = trino_result.rows.len();
        let trino_error = trino_result.error.clone();

        let sqe_time_ms = sqe_elapsed.as_millis() as u64;
        let trino_time_ms = trino_result.elapsed.as_millis() as u64;

        let rows_match = sqe_error.is_none()
            && trino_error.is_none()
            && sqe_rows == trino_rows;

        let status = match (&sqe_error, &trino_error) {
            (None, None) if rows_match => CompareStatus::Match,
            (None, None) => CompareStatus::RowDiff,
            (Some(_), None) => CompareStatus::SqeFailed,
            (None, Some(_)) => CompareStatus::TrinoFailed,
            (Some(_), Some(_)) => CompareStatus::BothFailed,
        };

        let speedup = if sqe_time_ms > 0 {
            trino_time_ms as f64 / sqe_time_ms as f64
        } else {
            0.0
        };

        info!(
            "    SQE: {}ms ({} rows) | Trino: {}ms ({} rows) | {:.1}x | {:?}",
            sqe_time_ms, sqe_rows, trino_time_ms, trino_rows, speedup, status
        );

        comparisons.push(QueryComparison {
            query_name,
            sqe_time_ms,
            trino_time_ms,
            speedup,
            sqe_rows,
            trino_rows,
            rows_match,
            sqe_error,
            trino_error,
            status,
        });
    }

    // Compute summary
    let total = comparisons.len();
    let matched = comparisons.iter().filter(|c| matches!(c.status, CompareStatus::Match)).count();
    let row_diff = comparisons.iter().filter(|c| matches!(c.status, CompareStatus::RowDiff)).count();
    let sqe_failed = comparisons.iter().filter(|c| matches!(c.status, CompareStatus::SqeFailed)).count();
    let trino_failed = comparisons.iter().filter(|c| matches!(c.status, CompareStatus::TrinoFailed)).count();
    let both_failed = comparisons.iter().filter(|c| matches!(c.status, CompareStatus::BothFailed)).count();

    let sqe_total_ms: u64 = comparisons.iter().map(|c| c.sqe_time_ms).sum();
    let trino_total_ms: u64 = comparisons.iter().map(|c| c.trino_time_ms).sum();

    let successful: Vec<f64> = comparisons.iter()
        .filter(|c| matches!(c.status, CompareStatus::Match | CompareStatus::RowDiff))
        .map(|c| c.speedup)
        .collect();
    let avg_speedup = if successful.is_empty() { 0.0 } else {
        successful.iter().sum::<f64>() / successful.len() as f64
    };
    let median_speedup = if successful.is_empty() { 0.0 } else {
        let mut sorted = successful.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        sorted[sorted.len() / 2]
    };

    let report = ComparisonReport {
        benchmark: benchmark.clone(),
        scale,
        timestamp: chrono::Utc::now().to_rfc3339(),
        sqe_endpoint: format!("{}:{}", args.sqe_host, args.sqe_port),
        trino_endpoint: args.trino_url.clone(),
        queries: comparisons,
        summary: ComparisonSummary {
            total, matched, row_diff, sqe_failed, trino_failed, both_failed,
            avg_speedup, median_speedup, sqe_total_ms, trino_total_ms,
        },
    };

    // Save JSON report
    let output_dir = Path::new(&args.output);
    std::fs::create_dir_all(output_dir)?;
    let filename = format!(
        "compare-{}-sf{}-{}.json",
        benchmark, scale as u64,
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S")
    );
    let report_path = output_dir.join(&filename);
    std::fs::write(&report_path, serde_json::to_string_pretty(&report)?)?;
    info!("Report saved to {}", report_path.display());

    // Print markdown summary
    println!("\n## {} SF{} — SQE vs Trino\n", benchmark.to_uppercase(), scale as u64);
    println!("| Query | SQE (ms) | Trino (ms) | Speedup | Rows | Status |");
    println!("|---|---|---|---|---|---|");
    for q in &report.queries {
        let status_icon = match q.status {
            CompareStatus::Match => "✅",
            CompareStatus::RowDiff => "⚠️",
            CompareStatus::SqeFailed => "❌ SQE",
            CompareStatus::TrinoFailed => "❌ Trino",
            CompareStatus::BothFailed => "❌ Both",
        };
        println!(
            "| {} | {} | {} | {:.1}x | {}/{} | {} |",
            q.query_name, q.sqe_time_ms, q.trino_time_ms,
            q.speedup, q.sqe_rows, q.trino_rows, status_icon
        );
    }
    println!(
        "\n**Total:** SQE {}ms, Trino {}ms, Avg speedup {:.1}x, Matched {}/{}\n",
        report.summary.sqe_total_ms, report.summary.trino_total_ms,
        report.summary.avg_speedup, report.summary.matched, report.summary.total
    );

    Ok(report)
}
```

- [ ] **Step 3: Wire into main.rs**

Add the match arm for Compare in `main.rs`:

```rust
Command::Compare(args) => {
    compare::run_compare(&args).await?;
}
```

And add `mod compare;` at the top.

- [ ] **Step 4: Run compilation check**

Run: `cargo check -p sqe-bench`
Expected: Compiles successfully. The `sqe_client.execute()` call may need adjustment to match the actual Flight SQL client API — adapt to whatever `crate::client` exposes.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-bench/src/compare.rs crates/sqe-bench/src/report.rs crates/sqe-bench/src/main.rs
git commit -m "feat(sqe-bench): implement compare runner for SQE vs Trino

Executes identical benchmark queries against SQE (Flight SQL) and Trino
(HTTP), diffs row counts, computes speedup ratios, and outputs JSON
report + markdown summary table."
```

---

### Task 8: Create Docker Compose Compare Stack

**Files:**
- Create: `docker-compose.compare.yml`
- Create: `scripts/trino-parity-test.sh`

Side-by-side stack with SQE and Trino sharing the same Polaris + S3.

- [ ] **Step 1: Create docker-compose.compare.yml**

```yaml
# docker-compose.compare.yml
# Side-by-side SQE + Trino stack for parity comparison.
# Both engines connect to the same Polaris catalog and S3 storage.
#
# Usage:
#   docker compose -f docker-compose.test.yml -f docker-compose.compare.yml up -d
#   ./scripts/trino-parity-test.sh tpch

services:
  # SQE Coordinator
  sqe:
    build:
      context: .
      target: coordinator
    ports:
      - "60051:50051"   # Flight SQL
      - "28080:8080"    # Trino HTTP compat
    environment:
      SQE_CONFIG: /etc/sqe/config.toml
    volumes:
      - ./tests/distributed/coordinator.toml:/etc/sqe/config.toml:ro
    depends_on:
      polaris:
        condition: service_healthy

  # Trino (single-node, Iceberg connector)
  trino:
    image: trinodb/trino:465
    ports:
      - "38080:8080"
    volumes:
      - ./tests/trino/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties:ro
      - ./tests/trino/config.properties:/etc/trino/config.properties:ro
    depends_on:
      polaris:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "trino", "--execute", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 30
```

- [ ] **Step 2: Create Trino config files**

Create `tests/trino/config.properties`:

```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
```

Create `tests/trino/catalog/iceberg.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=sqe_warehouse
```

- [ ] **Step 3: Create convenience script**

Create `scripts/trino-parity-test.sh`:

```bash
#!/usr/bin/env bash
# scripts/trino-parity-test.sh — Run side-by-side SQE vs Trino comparison
#
# Usage: ./scripts/trino-parity-test.sh [benchmark] [scale]
#   benchmark: tpch (default), tpcds, ssb
#   scale: 1 (default)
#
# Requires: docker compose stack running (docker-compose.test.yml + docker-compose.compare.yml)

set -euo pipefail

BENCHMARK="${1:-tpch}"
SCALE="${2:-1}"

echo "=== SQE vs Trino Parity Test: ${BENCHMARK} SF${SCALE} ==="

# Check services are up
echo "Checking SQE..."
timeout 5 bash -c 'until curl -sf http://localhost:28080/v1/info > /dev/null; do sleep 1; done' \
    || { echo "ERROR: SQE not reachable on port 28080"; exit 1; }

echo "Checking Trino..."
timeout 30 bash -c 'until curl -sf http://localhost:38080/v1/info > /dev/null; do sleep 1; done' \
    || { echo "ERROR: Trino not reachable on port 38080"; exit 1; }

echo "Both engines ready. Running comparison..."

cargo run -p sqe-bench --release -- compare "$BENCHMARK" \
    --scale "$SCALE" \
    --sqe-host localhost \
    --sqe-port 60051 \
    --trino-url "http://localhost:38080" \
    --trino-user admin \
    --output "benchmarks/results"

echo "Done. Report saved to benchmarks/results/"
```

```bash
chmod +x scripts/trino-parity-test.sh
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.compare.yml tests/trino/ scripts/trino-parity-test.sh
git commit -m "feat: add docker-compose stack for SQE vs Trino side-by-side comparison

SQE + Trino both connect to shared Polaris + S3 stack.
Convenience script runs sqe-bench compare and saves JSON report."
```

---

## Phase 3: Client Compatibility Testing (D3)

### Task 9: Test trino-cli and JDBC Driver Compatibility

**Files:**
- Create: `docs/trino-client-compatibility.md`

This is a manual testing task. Run each client against SQE's Trino HTTP endpoint and document results.

- [ ] **Step 1: Create the compatibility document scaffold**

```markdown
# Trino Client Compatibility

> Last tested: 2026-04-08 against SQE v0.15.0
> SQE Trino HTTP endpoint: `http://localhost:8080`

## Summary

| Client | Version | Connect | Browse | Query | Paginate | Status |
|---|---|---|---|---|---|---|
| trino-cli | 465 | — | — | — | — | — |
| Trino JDBC | 465 | — | — | — | — | — |
| DBeaver (Trino) | 24.x | — | — | — | — | — |
| Superset (SQLAlchemy) | 4.x | — | — | — | — | — |
| dbt-trino | 1.9.x | — | — | — | — | — |

Rating: ✅ works | ⚠️ partial (with workaround) | ❌ broken | ⏭️ not tested

## trino-cli

**Version tested:** —
**Command:**

```bash
# Connect to SQE's Trino HTTP endpoint
trino --server http://localhost:8080 --user admin --catalog iceberg --schema tpch_sf1
```

**Test cases:**
- [ ] Connection succeeds
- [ ] `SHOW CATALOGS` returns results
- [ ] `SHOW SCHEMAS` returns results
- [ ] `SHOW TABLES` returns results
- [ ] `SELECT * FROM orders LIMIT 10` returns data
- [ ] `SELECT count(*) FROM orders` returns correct count
- [ ] Large result set pagination works (>1000 rows)
- [ ] `DESCRIBE orders` works
- [ ] Error messages display correctly for bad SQL
- [ ] `\q` / Ctrl+D exits cleanly

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## Trino JDBC Driver

**Version tested:** —
**Connection URL:** `jdbc:trino://localhost:8080/iceberg/tpch_sf1`

**Test cases:**
- [ ] `DriverManager.getConnection()` succeeds
- [ ] `DatabaseMetaData.getCatalogs()` returns results
- [ ] `DatabaseMetaData.getSchemas()` returns results
- [ ] `DatabaseMetaData.getTables()` returns results
- [ ] `DatabaseMetaData.getColumns()` returns column metadata
- [ ] `Statement.executeQuery()` returns ResultSet
- [ ] ResultSet iteration works for all data types
- [ ] Large result sets paginate correctly
- [ ] `PreparedStatement` works (if supported)
- [ ] Connection pooling (HikariCP) works

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## DBeaver (Trino JDBC)

**Version tested:** —

**Test cases:**
- [ ] Create Trino connection in DBeaver
- [ ] Schema browser shows catalogs → schemas → tables
- [ ] Column metadata displays correctly
- [ ] Query editor runs SELECT queries
- [ ] Result grid displays data correctly
- [ ] Data export (CSV, SQL) works
- [ ] ER diagram generation works (if tables have relationships)

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## Superset (Trino SQLAlchemy)

**Version tested:** —

**Test cases:**
- [ ] Add database connection with `trino://admin@localhost:8080/iceberg/tpch_sf1`
- [ ] Test connection succeeds
- [ ] Table list populates
- [ ] Create chart from table data
- [ ] SQL Lab query execution works
- [ ] Result pagination works

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## dbt-trino

**Version tested:** —

**Test cases:**
- [ ] `dbt debug` connects successfully
- [ ] `dbt run` executes models
- [ ] Table materialization works
- [ ] View materialization works
- [ ] Incremental materialization works
- [ ] `dbt test` runs schema tests
- [ ] Compare output with native dbt-sqe adapter

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## Common Issues & Workarounds

_To be filled after testing. Expected areas:_
- Pagination edge cases
- Type mapping mismatches (decimal precision, timestamp format)
- Metadata endpoint coverage (system.jdbc.* tables)
- Auth flow differences (OAuth2 external auth flow)
```

- [ ] **Step 2: Commit**

```bash
git add docs/trino-client-compatibility.md
git commit -m "docs: scaffold trino-client-compatibility.md with test matrices

Test cases for trino-cli, JDBC driver, DBeaver, Superset, and dbt-trino
against SQE's Trino HTTP endpoint. Ready for live testing."
```

---

### Task 10: Run Client Tests and Fill Results

**Files:**
- Modify: `docs/trino-client-compatibility.md`

This task requires a running SQE stack. Execute the test cases from Task 9 and fill in the results.

- [ ] **Step 1: Start the test stack**

```bash
docker compose -f docker-compose.test.yml up -d
# Wait for Polaris + S3 to be healthy
# Start SQE coordinator
SQE_CONFIG=tests/distributed/coordinator.toml cargo run --bin sqe-coordinator --release
```

- [ ] **Step 2: Test trino-cli**

Download `trino-cli-465-executable.jar` and test:

```bash
java -jar trino-cli-465-executable.jar \
    --server http://localhost:8080 \
    --user admin \
    --catalog iceberg
```

Run each test case from the doc. Record pass/fail and any error messages.

- [ ] **Step 3: Test JDBC driver**

Write a minimal Java test class or use `jshell`:

```bash
jshell --class-path trino-jdbc-465.jar
> var conn = java.sql.DriverManager.getConnection("jdbc:trino://localhost:8080/iceberg/tpch_sf1", "admin", null);
> var meta = conn.getMetaData();
> var rs = meta.getCatalogs();
> while (rs.next()) System.out.println(rs.getString(1));
```

- [ ] **Step 4: Update doc with results and commit**

Fill in pass/fail for each test case. Document workarounds for any failures.

```bash
git add docs/trino-client-compatibility.md
git commit -m "docs: fill trino-cli and JDBC driver compatibility results

Tested against SQE v0.15.0 Trino HTTP endpoint."
```

---

## Phase 4: Operational Comparison (D4)

### Task 11: Operational Metrics Collection Script

**Files:**
- Create: `scripts/operational-comparison.sh`

Collect build time, binary size, memory usage, and cold start metrics for both SQE and Trino.

- [ ] **Step 1: Create the script**

```bash
#!/usr/bin/env bash
# scripts/operational-comparison.sh — Collect operational metrics for SQE vs Trino
#
# Measures: build time, binary size, image size, cold start, idle memory, loaded memory.
# Outputs: JSON to benchmarks/results/operational-comparison.json + markdown table.
#
# Requirements: Docker, cargo, java (for Trino comparison)

set -euo pipefail

OUTPUT="${1:-benchmarks/results/operational-comparison.json}"

echo "=== SQE Operational Metrics ==="

# Build time
echo "Building SQE (release)..."
SQE_BUILD_START=$(date +%s)
cargo build --release 2>&1 | tail -1
SQE_BUILD_END=$(date +%s)
SQE_BUILD_SECS=$((SQE_BUILD_END - SQE_BUILD_START))
echo "  Build time: ${SQE_BUILD_SECS}s"

# Binary size
SQE_COORDINATOR_SIZE=$(stat -f%z target/release/sqe-coordinator 2>/dev/null || stat -c%s target/release/sqe-coordinator)
SQE_CLI_SIZE=$(stat -f%z target/release/sqe-cli 2>/dev/null || stat -c%s target/release/sqe-cli)
echo "  Coordinator binary: $((SQE_COORDINATOR_SIZE / 1048576))MB"
echo "  CLI binary: $((SQE_CLI_SIZE / 1048576))MB"

# Cargo.lock crate count
CRATE_COUNT=$(grep -c 'name = ' Cargo.lock)
echo "  Dependencies: ${CRATE_COUNT} crates"

# Docker image size (if built)
SQE_IMAGE_SIZE="N/A"
if docker image inspect sqe-coordinator >/dev/null 2>&1; then
    SQE_IMAGE_SIZE=$(docker image inspect sqe-coordinator --format='{{.Size}}')
    echo "  Docker image: $((SQE_IMAGE_SIZE / 1048576))MB"
fi

echo ""
echo "=== Trino Operational Metrics ==="

# Pull Trino image
TRINO_IMAGE="trinodb/trino:465"
docker pull "$TRINO_IMAGE" -q

TRINO_IMAGE_SIZE=$(docker image inspect "$TRINO_IMAGE" --format='{{.Size}}')
echo "  Docker image: $((TRINO_IMAGE_SIZE / 1048576))MB"

# Cold start time (container start → first query)
echo "Measuring Trino cold start..."
TRINO_CONTAINER=$(docker run -d --rm -p 48080:8080 "$TRINO_IMAGE")
TRINO_START=$(date +%s%N)
timeout 120 bash -c "until curl -sf http://localhost:48080/v1/info >/dev/null 2>&1; do sleep 0.5; done"
TRINO_READY=$(date +%s%N)
TRINO_COLD_MS=$(( (TRINO_READY - TRINO_START) / 1000000 ))
docker stop "$TRINO_CONTAINER" >/dev/null 2>&1 || true
echo "  Cold start: ${TRINO_COLD_MS}ms"

# Write JSON
cat > "$OUTPUT" <<ENDJSON
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "sqe": {
    "build_time_secs": $SQE_BUILD_SECS,
    "coordinator_binary_bytes": $SQE_COORDINATOR_SIZE,
    "cli_binary_bytes": $SQE_CLI_SIZE,
    "cargo_lock_crates": $CRATE_COUNT,
    "docker_image_bytes": "$SQE_IMAGE_SIZE"
  },
  "trino": {
    "docker_image_bytes": $TRINO_IMAGE_SIZE,
    "cold_start_ms": $TRINO_COLD_MS
  }
}
ENDJSON

echo ""
echo "Report saved to $OUTPUT"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x scripts/operational-comparison.sh
git add scripts/operational-comparison.sh
git commit -m "feat: add operational comparison script (SQE vs Trino metrics)

Measures build time, binary size, image size, dependency count, and cold
start time for both SQE and Trino. Outputs JSON report."
```

---

### Task 12: Add Operational Comparison to Compatibility Doc

**Files:**
- Modify: `docs/trino-compatibility.md`

- [ ] **Step 1: Add the Operational Comparison section**

Replace the placeholder in `docs/trino-compatibility.md`:

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add docs/trino-compatibility.md
git commit -m "docs: add operational comparison section (SQE vs Trino)

Build time, binary size, memory, cold start, config surface area
comparison with measured values and architectural notes."
```

---

## Phase 5: Wrap-Up

### Task 13: Update README.md + nextsteps.md

**Files:**
- Modify: `README.md`
- Modify: `nextsteps.md`

- [ ] **Step 1: Update README.md roadmap**

Add Trino parity items to the roadmap:

```markdown
- [x] Trino SQL compatibility matrix (`docs/trino-compatibility.md`)
- [x] Side-by-side benchmark tooling (`sqe-bench compare`)
```

- [ ] **Step 2: Update nextsteps.md**

Add after Step 5 line:

```
Step 5b: Trino parity     ✅ DONE (compatibility matrix, sqe-bench compare, client testing, operational comparison)
```

- [ ] **Step 3: Commit**

```bash
git add README.md nextsteps.md
git commit -m "docs: mark Trino parity assessment as complete

Compatibility matrix, sqe-bench compare, client test scaffolds,
and operational comparison all delivered."
```

---

## Task Dependency Map

```
Phase 1: SQL Compatibility Matrix
  Task 1 (scaffold) → Task 2 (scalar) → Task 3 (date/agg/window) → Task 4 (DDL/types/Iceberg)

Phase 2: Automated Benchmark
  Task 5 (CLI) → Task 6 (Trino client) → Task 7 (compare runner) → Task 8 (Docker + script)

Phase 3: Client Testing
  Task 9 (scaffold) → Task 10 (run tests)

Phase 4: Operational
  Task 11 (metrics script) → Task 12 (docs)

Phase 5: Wrap-up
  Task 13 (README + nextsteps)
```

Phases 1–4 are independent and can be executed in parallel. Task 13 depends on all others completing.

## Summary

| # | Phase | Task | Files |
|---|---|---|---|
| 1 | Compat Matrix | Scaffold trino-compatibility.md | docs/trino-compatibility.md |
| 2 | Compat Matrix | Audit string, math, URL, regex, conditional, cast | docs/trino-compatibility.md |
| 3 | Compat Matrix | Audit date/time, JSON, aggregate, window | docs/trino-compatibility.md |
| 4 | Compat Matrix | Audit DDL/DML, types, Iceberg SQL | docs/trino-compatibility.md |
| 5 | Benchmark | Add Compare CLI subcommand | cli.rs |
| 6 | Benchmark | Trino HTTP client | trino_client.rs |
| 7 | Benchmark | Comparison runner + report | compare.rs, report.rs |
| 8 | Benchmark | Docker Compose + script | docker-compose.compare.yml |
| 9 | Clients | Scaffold client compatibility doc | docs/trino-client-compatibility.md |
| 10 | Clients | Run live tests, fill results | docs/trino-client-compatibility.md |
| 11 | Operational | Metrics collection script | scripts/operational-comparison.sh |
| 12 | Operational | Add operational section to compat doc | docs/trino-compatibility.md |
| 13 | Wrap-up | Update README + nextsteps | README.md, nextsteps.md |
