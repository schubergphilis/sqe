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
