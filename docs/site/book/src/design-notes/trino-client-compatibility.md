# Trino Client Compatibility

> BI-tool compatibility (Metabase, Superset, JDBC) fixed 2026-06 (issues #1, #4, #5, #6, #327, #345 and the catalog-enumeration blocker) and verified live against a Polaris + Keycloak + Ranger stack on 2026-07-02 (see Live verification below).
> Original curl protocol matrix: 2026-04-10 against SQE v0.15.0.
> SQE Trino HTTP endpoint: `http://localhost:8080`

## Benchmark Comparison: SQE vs Trino 465 (SF0.01, same Polaris + S3)

> Historical snapshot from v0.15.0 (2026-04) at SF0.01. Superseded by the current SF1/SF10 baselines in the project benchmark results; kept here for the original client-compat context, not as a current performance claim.

| Benchmark | Matched | SQE | Trino | Winner | Notes |
|---|---|---|---|---|---|
| **TPC-H (22)** | **22/22** | **7.6s** | 8.7s | **SQE** | SQE faster on analytical queries |
| **SSB (13)** | **13/13** | **4.2s** | 5.1s | **SQE** | SQE faster on star schema |
| **TPC-DS (99)** | **92/99** | **40.1s** | 41.4s | **SQE** | Near-parity, 6 row diffs from ORDER BY tiebreaking |
| **TPC-C (17)** | 15/17 | 4.0s | 3.8s | Trino | 2 DML row count diffs |
| **TPC-E (18)** | **17/18** | 5.0s | 3.7s | Trino | 1 BothFailed (correlated subquery in SET) |
| **TPC-BB (10)** | 0/10 | 1.4s | 0.2s | N/A | Both fail (Trino catalog namespace mismatch) |
| **ClickBench (43)** | **41/43** | 12.7s | 3.9s | Trino | Simple scans favor JVM JIT |
| **Total** | **200/222** | **74.9s** | **67.0s** | Mixed | SQE wins analytical, Trino wins simple scans |

**Key finding:** SQE beats Trino on complex analytical queries (TPC-H, SSB, TPC-DS) thanks to:
- No JVM startup overhead (Rust AOT compilation)
- Efficient Iceberg scan planning with predicate pushdown
- DECIMAL precision (matching SQL standard, fixed Apr 10)

Trino wins on simple single-table scans (ClickBench) due to JVM JIT compilation advantage on hot paths.

**Correctness:** DECIMAL literal fix ensures `0.06 - 0.01 = 0.05` (exact), not `0.049999999999999996` (IEEE 754 rounding). This was a critical correctness fix that changed TPC-H q06 results from wrong (40.7M) to correct (68.2M).

## Summary

| Client | Version | Connect | Browse | Query | Paginate | Status |
|---|---|---|---|---|---|---|
| curl (Trino HTTP) | n/a | ✅ | ✅ | ✅ | ⏭️ | ✅ 26/28 tests pass |
| Trino wire protocol (JDBC/SQLAlchemy) | 465 | ✅ | ✅ | ✅ | ✅ | ✅ full handshake driven live 2026-07-02 (see below) |
| Metabase | recent | ✅ | ✅ | ✅ | ✅ | ✅ JDBC protocol path verified live; real client drove the original bug discovery |
| dbt-trino | 1.9.x | ✅ | ✅ | ✅ | ✅ | ✅ exercised end-to-end in the remote test setup (native dbt-sqe adapter is the primary path) |
| Superset (SQLAlchemy) | 4.x | ✅ | ✅ | ✅ | ⏭️ | ✅ SQLAlchemy reflection + query paths verified live via the shared wire protocol; not run inside a Superset GUI |
| DBeaver (Trino) | 24.x | ✅ | ✅ | ✅ | ⏭️ | ✅ same JDBC metadata paths verified live; not run inside the DBeaver GUI |
| trino-cli | 476 | ✅ | ✅ | ✅ | ⏭️ | ✅ official Trino CLI 476 ran SHOW/DESCRIBE/typed queries live 2026-07-02 over the TLS route |

Rating: ✅ works | ⚠️ partial (with workaround) | ❌ broken | ⏭️ not tested

The BI-tool fixes landed in 2026-06 after pointing a real Metabase at the endpoint and watching the metadata handshake fail: the `PREPARE` the parser rejected (JDBC could not connect), the two-column `SHOW TABLES` that collapsed every table into its namespace, the catalogs never enumerated, the quoted identifiers `DESCRIBE`/`SHOW COLUMNS` never matched, and the `timestamp(6)` type signature the JDBC driver refused to parse. Each surfaced as a silent zero (0 tables, 0 columns) or an "invalid response," never a server error.

The summary matrix above is the current status. The per-client checklists further down are the original v0.15.0 (2026-04) test templates, kept for the case-by-case detail; their unchecked boxes predate the 2026-06 fixes and are not a current record of what works.

## Live verification (2026-07-02)

Drove the Trino client protocol (what the JDBC driver and the SQLAlchemy dialect issue on the wire) against a live SQE stack: the data-platform quickstart with Polaris, Keycloak (`iceberg` realm), and Ranger, catalog `main_warehouse`, authenticated with Basic auth exchanged for an OIDC token. Every step of the BI handshake passed:

| Step | Query | Result |
|---|---|---|
| Connect gate | `PREPARE st FROM SELECT 1` | `FINISHED`, `X-Trino-Added-Prepare` header set, no parse error |
| Parameterized query | `PREPARE p FROM ... WHERE order_id = ?` then `EXECUTE p USING 'o-01'` | `FINISHED` with data (a type-mismatched literal is correctly rejected) |
| `SHOW CATALOGS` | | single `Catalog` column |
| `SHOW SCHEMAS` | | single `Schema` column |
| `SHOW TABLES FROM "s"` | | single `Table` column, bare names |
| Quoted `DESCRIBE` | `DESCRIBE "cat"."schema"."table"` | column rows returned, no error |
| Quoted `SHOW COLUMNS` | | column rows returned |
| SQLAlchemy reflection | `SELECT column_name, data_type FROM information_schema.columns WHERE ...` | Trino type names (`varchar`), unqualified `information_schema` resolves to the session catalog |
| Typed timestamp | `date_trunc('month', CAST(now() AS timestamp))` | `timestamp(6)` with `rawType: "timestamp"` and precision in `arguments`; value normalized to 6 fractional digits |
| Computed aggregate | `count(*)` | type `bigint`, value rendered as a JSON number |
| Time-series chart | `date_trunc('quarter', ...) GROUP BY 1` | `timestamp(6)` + `bigint`, correct rows |
| Pagination | 83,521-row result, follow `nextUri` | 84 pages of 1000 rows (521 on the last), exact row count, `RUNNING` -> `FINISHED`; the `max_result_rows` guard rejects oversized results cleanly |

A type-mismatched predicate (for example `WHERE varchar_col = 1`) used to return `errorName: EXECUTION_FAILED`, `errorType: INTERNAL_ERROR`, and the raw `DataInvalid => Can't convert datum ...` string, which told a BI client the engine was broken rather than the query. That now classifies as `TYPE_MISMATCH` (`USER_ERROR`) with the `DataInvalid => ` wrapper stripped. Genuinely bad SQL already surfaced well (`Invalid function 'frobnicate'. Did you mean 'truncate'?`). The remaining error-detail work is the generic-message cases noted below.

Cross-checked with the official Trino CLI 476 (the same client protocol the JDBC driver uses) pointed at the TLS route `https://localhost/v1/` (nginx proxies it to `sqe:8080`; password auth over the wire requires TLS): `SHOW SCHEMAS`, `SHOW TABLES`, quoted three-part `DESCRIBE` (rendered as Trino's `Column | Type | Extra | Comment`), and a `date_trunc('month', ...) , count(*)` query all returned correctly, with timestamps normalized to 6 fractional digits.

## Trino HTTP v1/statement Protocol (curl)

**Tested: 2026-04-08** via `curl -X POST http://localhost:28080/v1/statement` with Bearer token auth.

**Connection & Metadata:**
- [x] `SHOW CATALOGS` returns results (1 row)
- [x] `SHOW SCHEMAS IN <catalog>` returns results (2 rows: default, information_schema)
- [x] `SELECT 1` succeeds
- [x] `SELECT 1+1 AS result` succeeds with column alias

**Trino Date/Time Functions (compat UDFs):**
- [x] `now()`: returns current timestamp
- [x] `year(CAST('2024-01-15' AS DATE))`: returns 2024
- [x] `month(CAST('2024-03-15' AS DATE))`: returns 3
- [x] `day_of_week(CAST('2024-01-15' AS DATE))`: returns day number
- [x] `date_format(now(), '%Y-%m-%d')`: MySQL format codes work
- [x] `date_trunc('month', ...)`: native DataFusion

**String Functions:**
- [x] `upper(concat('hello', ' ', 'world'))`: HELLO WORLD
- [x] `length('hello')`: 5
- [x] `substr('hello world', 1, 5)`: hello
- [x] `replace('hello', 'l', 'r')`: herro
- [x] `trim('  hello  ')`: hello

**Conditional / Type:**
- [x] `CASE WHEN 1=1 THEN 'yes' ELSE 'no' END`: yes
- [x] `COALESCE(NULL, 42)`: 42
- [x] `NULLIF(1, 1)`: NULL
- [x] `GREATEST(1,2,3), LEAST(1,2,3)`: 3, 1
- [x] `typeof(42)`: Int64
- [x] `TRY_CAST('abc' AS INTEGER)`: NULL

**Math:**
- [x] `abs(-5), sqrt(16.0)`: 5, 4.0
- [x] `round(3.14159, 2)`: 3.14
- [x] `pi()`: 3.14159...
- [x] `random()`: random float

**JSON:**
- [x] `json_format('{"a":1}')`: formatted JSON string

**Known failures:**
- [x] `VALUES` clause: `SELECT count(*) FROM (VALUES 1,2,3) AS t(x)` works (a bare-VALUES pre-parse rewrite landed in #315; inline VALUES sources are covered by tests)
- [ ] Error detail: bad SQL returns generic `Query execution failed` instead of the underlying parse error message
- [x] Missing table: correctly returns error with table name: `table 'test_warehouse.default.nonexistent_table' not found`

**Results:** 26/28 pass. Core SQL functions, metadata, and Trino compat UDFs all work correctly over the Trino HTTP protocol.

## trino-cli

**Version tested:** ⏭️ not yet tested
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

**Results:** _Requires trino-cli binary_
**Known issues:** _To be tested_

## Trino JDBC Driver

**Version tested:** not recorded
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

**Version tested:** not recorded

**Test cases:**
- [ ] Create Trino connection in DBeaver
- [ ] Schema browser shows catalogs, then schemas, then tables
- [ ] Column metadata displays correctly
- [ ] Query editor runs SELECT queries
- [ ] Result grid displays data correctly
- [ ] Data export (CSV, SQL) works
- [ ] ER diagram generation works (if tables have relationships)

**Results:** _To be filled after testing_
**Known issues:** _To be filled after testing_

## Superset (Trino SQLAlchemy)

**Version tested:** not recorded

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

**Version tested:** not recorded

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

**Authentication:** SQE's Trino HTTP endpoint requires a Bearer token (OAuth2). The test stack uses Polaris client_credentials grant (`client_id=root`, `client_secret=s3cr3t`). The live stack uses Keycloak OIDC with OPA-enforced authorization.

**Catalog context:** Set the session catalog with the `X-Trino-Catalog` header (or use fully-qualified `catalog.schema.table` names). Both the `SELECT` and `SHOW` paths honor the session catalog and auto-discover it, so a BI tool syncing against a session catalog sees the same tables the query editor does.

**Error messages:** Trino HTTP error responses use a generic `Query execution failed` message instead of surfacing the underlying SQL parse/plan error. The `errorName` and `errorType` fields are populated but the user-facing `message` needs improvement.

**DESCRIBE:** `DESCRIBE <table>` and `SHOW COLUMNS FROM <table>` both work and resolve double-quoted identifiers (`"catalog"."schema"."table"`). `DESCRIBE OUTPUT` / `DESCRIBE INPUT` on a prepared statement are handled for JDBC `PreparedStatement` metadata calls.
