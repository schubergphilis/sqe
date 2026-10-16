**STATUS 2026-10-16: GetFlightInfo advertises the schema DoGet streams (`fix/flightinfo-schema-match`).**

Found while measuring #810. The ADBC Flight SQL driver (Go, used by `adbc_driver_flightsql` and dbt-sqe) compares the FlightInfo schema with each endpoint's stream and refuses a mismatch with `endpoint 0 returned inconsistent schema`. Over ADBC, `SELECT 1+1`, `SELECT count(*) ...`, `EXPLAIN`, `SHOW TABLES|CATALOGS|SCHEMAS` all failed; only queries whose columns were all aliased or bare column references worked.

Two causes:

1. `QueryHandler::get_schema` planned the raw SQL, while execution plans the front door's normalized text, which aliases unaliased expressions to `_colN`. It now plans through `plan_sql_for_stream`, the path DoGet and prepared statements already use, and returns `analyzed_schema` so coerced types (decimal `UNION`, #839) match too.
2. Every non-query, non-DML statement advertised an encoded zero-field schema, but SHOW / DESCRIBE / EXPLAIN stream columns. `flight_sql.rs` `advertised_schema_bytes` now sends no schema bytes for an empty schema (statement FlightInfo, prepared FlightInfo, and `ActionCreatePreparedStatementResult.dataset_schema`). The client then takes the stream's schema. `SET TIME ZONE` (zero columns) still works.

Verified with Python ADBC 1.11 against a debug `sqe-server` on the AC stack, using a passthrough policy:
- `SELECT 1+1`, `count(*)`, `EXPLAIN`, `DESCRIBE SELECT 1`, `SHOW TABLES|CATALOGS|SCHEMAS|COLUMNS` and `SET TIME ZONE` all succeed, as plain statements and prepared.
- New wire test: `flight_sql_smoke::advertised_schema_matches_the_streamed_one`.

**NEXT:** check the Arrow Flight SQL JDBC driver against an unset FlightInfo schema for SHOW/EXPLAIN. It was not tested here. Java's `FlightInfo` decodes empty bytes as an empty schema.
