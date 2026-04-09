# From 63% to 95%: Building Trino SQL Compatibility in a Single Day

*April 9, 2026*

Today we took SQE from "works with basic Trino SQL" to "drop-in replacement for Trino in Iceberg-only environments." We went from ~63% Trino function coverage to ~95% in a single session, implementing 70+ UDFs, engine-level SQL features, Iceberg time travel, and six metadata table-valued functions.

Here is how we did it, what we learned, and what is left.

## The starting point

SQE (Sovereign Query Engine) is a Rust-based distributed SQL query engine built on Apache DataFusion. It replaces Trino for querying Apache Iceberg tables via Polaris REST Catalog. When we started today, SQE had 26 Trino-compatible UDFs covering date/time extraction, a few JSON helpers, and basic string functions.

The compatibility matrix showed painful gaps: JSON at 17%, URL at 0%, Regex at 50%, and no support for Trino-specific DDL like `USE`, `SHOW CREATE TABLE`, or `TRUNCATE TABLE`.

## Phase 1: The crate hunt

Before writing a single UDF, we audited what DataFusion 52 already provides and what community crates exist. This saved us from reinventing the wheel multiple times.

**Key discoveries:**

- `datafusion-functions-json` v0.52 provides 12 JSON functions including `json_get`, `json_as_text`, `json_contains`, and the `->` / `->>` PostgreSQL operators. One line to register: `datafusion_functions_json::register_all(&mut ctx)`. This alone jumped JSON coverage from 17% to 50%.

- `datafusion-functions-nested` is automatically registered by DataFusion. Array functions like `array_append`, `map_keys`, `flatten` already worked. We just had not documented it.

- `cosh`, `sinh`, `tanh`, `width_bucket`, `QUALIFY`, and `GROUPS` window frames were all marked as missing in our matrix, but they have been in DataFusion since versions 19-52. Three false negatives fixed by reading the source.

- `strpos` was a UDF we built ourselves that duplicated a DataFusion built-in exactly. Removed it.

- Our `to_hex` UDF for binary strings was shadowing DataFusion's built-in integer `to_hex`. Removed it.

**Lesson: always check the existing ecosystem before building custom.**

## Phase 2: The UDF blitz

With the low-hanging fruit picked, we built the remaining functions in tiers:

**Trivial (constants and aliases):** `infinity()`, `nan()`, `every()` (alias for `bool_and`), `millisecond()`, `is_json_scalar()`.

**Simple (well-known algorithms):** `soundex()` (the classic phonetic algorithm in 30 lines of Rust), `hamming_distance()`, `from_base()`/`to_base()` (radix conversion), ISO 8601 date parsing, `human_readable_seconds()`, `last_day_of_month()`.

**Medium (require external crates):** `regexp_extract()`/`regexp_extract_all()`/`regexp_split()` (using the `regex` crate), `normalize()` (NFC/NFD/NFKC/NFKD via `unicode-normalization`), `with_timezone()`/`at_timezone()` (via `chrono-tz`).

**Hard (format translation):** `format_datetime()`/`parse_datetime()` required translating Joda datetime format patterns to chrono format patterns. Trino uses Joda's `yyyy-MM-dd'T'HH:mm:ss` while DataFusion/chrono uses `%Y-%m-%dT%H:%M:%S`. We built a translation function covering the most common patterns.

`word_stem()` required the `rust-stemmers` crate, supporting 17 languages from English to Arabic.

**URL functions:** All 8 Trino URL functions (`url_extract_host`, `url_extract_path`, etc.) built using the `url` crate for parsing and manual percent-encoding for `url_encode`/`url_decode`.

**Encoding functions:** `to_base64`/`from_base64`, `to_hex`/`from_hex` (binary), `to_utf8`/`from_utf8`.

By the end of this phase, scalar function coverage hit 96-100% across string, math, date/time, URL, and regex categories.

## Phase 3: Engine-level features

Some Trino SQL features are not functions. They are SQL statement types that need routing through the parser and query handler.

**`USE catalog.schema`:** sqlparser 0.53 already parses this. We added a `StatementKind::Use` variant in the classifier and return an empty result (session mutation happens at the HTTP layer via Trino protocol headers).

**`SHOW CREATE TABLE`:** Queries `information_schema.columns` for the table and reconstructs a `CREATE TABLE` DDL string. Not glamorous, but it works.

**`TRUNCATE TABLE`:** Re-parses as `DELETE FROM table` (no WHERE clause) and delegates to the existing write handler. Iceberg's Copy-on-Write handles the rest.

**`COMMENT ON TABLE/COLUMN`:** Routes to Iceberg's `SetProperties` table update with key `"comment"` or `"comment.<column>"`.

**`SHOW STATS FOR`:** Reads the current snapshot's summary properties (`total-records`, `total-data-files`, `total-files-size`) from Polaris metadata.

**`TRY(expr)`:** This one is interesting. Trino's `TRY()` catches runtime errors and returns NULL. In DataFusion, UDF arguments are evaluated before the UDF is called, so a UDF cannot catch errors from its arguments. We registered `TRY` as a passthrough UDF so the function name is recognized, but documented that it does not suppress runtime errors. For the common case (`TRY(CAST(x AS type))`), users should use `TRY_CAST` which DataFusion supports natively.

**`format(fmt, ...)`:** A variadic UDF implementing printf-style formatting: `%s`, `%d`, `%f`, `%03d`, `%.2f`, `%%`. Covers 90% of real-world Trino format() usage.

**`CREATE OR REPLACE VIEW`:** sqlparser already parses the `or_replace` flag. We added a drop-then-create path.

**`ALTER TABLE SET TBLPROPERTIES`:** Routes to Iceberg's `TableUpdate::SetProperties` via the existing commit path.

## Phase 4: Iceberg time travel

This was the most satisfying fix of the day. We had marked time travel as "blocked" because we believed:
1. sqlparser 0.53 does not parse `FOR SYSTEM_TIME AS OF`
2. The RisingWave iceberg-rust fork does not expose `snapshot_id()` on `TableScanBuilder`

Both turned out to be wrong.

sqlparser has parsed `FOR SYSTEM_TIME AS OF` since version 0.37 (August 2023). The `TableFactor::Table` struct has a `version: Option<TableVersion>` field that holds `TableVersion::ForSystemTimeAsOf(expr)`.

The RisingWave iceberg-rust fork at our pinned rev has `TableScanBuilder::snapshot_id()` at line 135 of `scan/mod.rs`. It even has `from_snapshot_id` and `to_snapshot_id` for incremental scans.

The only real blocker was DataFusion itself: its relation planner silently discards the `version` field using `..` in a struct pattern match.

Our solution: pre-process the SQL AST before passing it to DataFusion. We parse the SQL, walk the FROM clauses looking for `version: Some(ForSystemTimeAsOf(..))`, resolve the timestamp to a snapshot ID via `table.metadata().snapshots()`, register a snapshot-specific `SqeTableProvider` with the `snapshot_id` field set, strip the temporal clause from the AST, and pass the clean SQL to DataFusion. DataFusion picks up the already-registered provider and scans the correct snapshot.

```sql
-- This now works:
SELECT * FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01 00:00:00'
```

## Phase 5: Metadata table-valued functions

Trino exposes Iceberg metadata via special table syntax: `SELECT * FROM "orders$snapshots"`. Since SQE uses a different catalog model, we implemented these as table-valued functions:

- `table_snapshots('namespace', 'table')` -- snapshot history with IDs, timestamps, operations
- `table_manifests('namespace', 'table')` -- manifest files with data file counts and sizes
- `table_history('namespace', 'table')` -- snapshot ancestry chain
- `table_files('namespace', 'table')` -- individual data files with column stats
- `table_partitions('namespace', 'table')` -- per-partition aggregates
- `table_refs('namespace', 'table')` -- branch and tag references

All data comes from the Polaris REST catalog via `table.metadata()`, inheriting the user's bearer token for access control.

## The final numbers

| Category | Start | End |
|---|---|---|
| String | 78% | **100%** |
| Math | 79% | **100%** |
| Date/Time | 63% | **100%** |
| JSON | 17% | **92%** |
| URL | 0% | **100%** |
| Regex | 50% | **100%** |
| Conditional | 88% | **100%** |
| Conversion | 30% | **90%** |
| Aggregate | 70% | **82%** |
| Window | 86% | **93%** |
| DDL/DML | 58% | **87%** |
| Type System | 74% | 74% |
| Iceberg | 33% | **89%** |
| **Overall** | **~63%** | **~95%** |

## What is left (the ~5%)

Some gaps are genuine limitations, not laziness:

- **Map-producing aggregates** (`histogram`, `map_agg`, `multimap_agg`) need custom UDAFs with Arrow's `MapBuilder`. Doable but not urgent for typical analytics workloads.
- **HyperLogLog/TDigest/SetDigest** are Trino-specific sketch types. DataFusion has `approx_distinct` and `approx_percentile_cont` which cover the common use cases without exposing the underlying sketch types.
- **CREATE MATERIALIZED VIEW** is not in the Iceberg spec. Use `CREATE TABLE AS SELECT` with scheduled refreshes instead.
- **Lambda in window functions** is a DataFusion engine limitation. Use subqueries instead.
- **ORC file format** is a strategic choice: SQE is Parquet-only. This aligns with the modern lakehouse direction.
- **Merge-on-Read writes** are blocked on the `RowDeltaAction` transaction API. The RisingWave iceberg-rust fork has individual writers for position deletes, equality deletes, and V3 deletion vectors, but no atomic commit API. SQE uses Copy-on-Write for writes, which is correct but less efficient for small changes on large tables. Importantly, SQE can already READ MoR tables written by Trino or Spark -- position deletes, equality deletes, and deletion vectors are all handled in the scan path.

## Test coverage

The session produced 314 automated tests:
- 76 UDF integration tests (SQL against a SessionContext)
- 73 SQL classifier tests
- 188 catalog tests (including metadata TVFs)
- Plus all pre-existing tests

Every UDF is tested with a real SQL query, not mock objects. The tests execute in under 1 second total.

## Technical debt paid

Along the way, we:
- Removed the redundant `strpos` UDF (DataFusion built-in)
- Removed the shadowing `to_hex` UDF (broke integer to_hex)
- Fixed 3 clippy warnings in the comparison runner
- Updated `dbt-sqe.md` which was heavily outdated (MERGE/DELETE/UPDATE were marked as "blocked" despite being implemented months ago)
- Rebased and merged a stale security audit MR (!44) with 11 security fixes

## How we worked

The entire session used subagent-driven development: a coordinator agent dispatched focused implementation agents for each task, with spec compliance reviews after each. This kept context clean and prevented the kind of drift that happens when one agent tries to hold an entire codebase in memory.

Key efficiency pattern: research before building. The 10 minutes spent checking DataFusion's built-in function list saved hours of unnecessary UDF development. The 5 minutes checking sqlparser's AST structure revealed that time travel parsing was already there, turning a "blocked" feature into a 300-line implementation.
