# Trino Client Compatibility

> Last tested: 2026-04-10 against SQE v0.15.0
> SQE Trino HTTP endpoint: `http://localhost:8080`
> Test stack: Polaris 1.3.0 (in-memory) + RustFS (S3-compatible)

## Benchmark Comparison: SQE vs Trino 465 (SF0.01, same Polaris + S3)

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
| curl (Trino HTTP) | — | ✅ | ✅ | ✅ | ⏭️ | ✅ 26/28 tests pass |
| trino-cli | 465 | ⏭️ | ⏭️ | ⏭️ | ⏭️ | ⏭️ not tested |
| Trino JDBC | 465 | ⏭️ | ⏭️ | ⏭️ | ⏭️ | ⏭️ not tested |
| DBeaver (Trino) | 24.x | ⏭️ | ⏭️ | ⏭️ | ⏭️ | ⏭️ not tested |
| Superset (SQLAlchemy) | 4.x | ⏭️ | ⏭️ | ⏭️ | ⏭️ | ⏭️ not tested |
| dbt-trino | 1.9.x | ⏭️ | ⏭️ | ⏭️ | ⏭️ | ⏭️ not tested |

Rating: ✅ works | ⚠️ partial (with workaround) | ❌ broken | ⏭️ not tested

## Trino HTTP v1/statement Protocol (curl)

**Tested: 2026-04-08** via `curl -X POST http://localhost:28080/v1/statement` with Bearer token auth.

**Connection & Metadata:**
- [x] `SHOW CATALOGS` returns results (1 row)
- [x] `SHOW SCHEMAS IN <catalog>` returns results (2 rows: default, information_schema)
- [x] `SELECT 1` succeeds
- [x] `SELECT 1+1 AS result` succeeds with column alias

**Trino Date/Time Functions (compat UDFs):**
- [x] `now()` — returns current timestamp
- [x] `year(CAST('2024-01-15' AS DATE))` — returns 2024
- [x] `month(CAST('2024-03-15' AS DATE))` — returns 3
- [x] `day_of_week(CAST('2024-01-15' AS DATE))` — returns day number
- [x] `date_format(now(), '%Y-%m-%d')` — MySQL format codes work
- [x] `date_trunc('month', ...)` — native DataFusion

**String Functions:**
- [x] `upper(concat('hello', ' ', 'world'))` — HELLO WORLD
- [x] `length('hello')` — 5
- [x] `substr('hello world', 1, 5)` — hello
- [x] `replace('hello', 'l', 'r')` — herro
- [x] `trim('  hello  ')` — hello

**Conditional / Type:**
- [x] `CASE WHEN 1=1 THEN 'yes' ELSE 'no' END` — yes
- [x] `COALESCE(NULL, 42)` — 42
- [x] `NULLIF(1, 1)` — NULL
- [x] `GREATEST(1,2,3), LEAST(1,2,3)` — 3, 1
- [x] `typeof(42)` — Int64
- [x] `TRY_CAST('abc' AS INTEGER)` — NULL

**Math:**
- [x] `abs(-5), sqrt(16.0)` — 5, 4.0
- [x] `round(3.14159, 2)` — 3.14
- [x] `pi()` — 3.14159...
- [x] `random()` — random float

**JSON:**
- [x] `json_format('{"a":1}')` — formatted JSON string

**Known failures:**
- [ ] `VALUES` clause — `SELECT count(*) FROM (VALUES 1,2,3) AS t(x)` fails (DataFusion parses but Trino endpoint doesn't handle inline VALUES)
- [ ] Error detail — bad SQL returns generic `Query execution failed` instead of the underlying parse error message
- [x] Missing table — correctly returns error with table name: `table 'test_warehouse.default.nonexistent_table' not found`

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

**Authentication:** SQE's Trino HTTP endpoint requires a Bearer token (OAuth2). The test stack uses Polaris client_credentials grant (`client_id=root`, `client_secret=s3cr3t`). The live stack uses Keycloak OIDC with OPA-enforced authorization.

**Catalog context:** Unlike Trino, SQE requires explicit catalog context for most queries. Use `X-Trino-Catalog` header or fully-qualified table names (`catalog.schema.table`).

**VALUES clause:** `SELECT ... FROM (VALUES ...) AS t(x)` fails. Use CTEs or temporary tables instead.

**Error messages:** Trino HTTP error responses use a generic `Query execution failed` message instead of surfacing the underlying SQL parse/plan error. The `errorName` and `errorType` fields are populated but the user-facing `message` needs improvement.

**DESCRIBE:** The `DESCRIBE <table>` statement is not supported via the Trino HTTP endpoint. Use `SHOW COLUMNS FROM <table>` or query `information_schema.columns` instead.
