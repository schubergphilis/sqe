# Design: Flight SQL for the stock BI drivers

## Context

Measured with the Arrow Flight SQL JDBC driver 19.0.0 (JDK 21) against a local coordinator, password and bearer-token sign-in, 73 checks per run. The `getTypeInfo` gap is driver-side: `ArrowDatabaseMetadata` does not override Avatica's `getTypeInfo`, so `GetXdbcTypeInfo` is never called (checked with `javap`).

## Decisions

### D1. Compression is chosen per request, in a tower layer

`flight_ipc_options` reads a tokio `task_local` that a tower layer (`ClientCompressionLayer`, innermost on the Flight server) sets from the request headers before tonic dispatches. Order of precedence:

1. `x-sqe-flight-compression: none|lz4|zstd` (the JDBC driver forwards unknown connection properties as headers, so `jdbc:arrow-flight-sql://host:port?x-sqe-flight-compression=lz4` re-enables a codec for a driver that has one). An unparsable value is ignored, not an error.
2. `user-agent` starting `grpc-java`: `none`. The Java Flight client has no LZ4/ZSTD codec unless the application adds `arrow-compression`; uncompressed is always readable.
3. `[coordinator] flight_compression`.

A task-local keeps the 15 `flight_ipc_options` call sites unchanged. Every call site computes the options in the handler body, before the response stream is returned, so the value is read while the layer's scope is active.

Rejected: changing the default to `none` (costs ADBC and dbt-sqe their compression); a config list of user agents (nothing else needs it yet).

### D2. Catalog names come from `SHOW CATALOGS`

`GetCatalogs` runs the same `handle_show_catalogs` the SQL statement runs, so the names agree with what SQL resolves (`iceberg`, the `[catalogs.*]` keys, Hive roots, the session's own default). `GetDbSchemas` and `GetTables` take the catalogs to walk from the request filter: a named catalog alone, otherwise every name `SHOW CATALOGS` lists. A catalog that fails to list (not authorized, a Hive root `list_metadata_tables` cannot read) contributes no rows and is logged; a filter that names an unknown catalog returns an empty result, as the Flight SQL spec asks.

`GetDbSchemas` runs `SHOW SCHEMAS FROM "<catalog>"` with the name quoted as an identifier (embedded quotes doubled); `GetTables` calls `list_metadata_tables(session, Some(catalog))`, which resolves the catalog with the same rule as `SHOW SCHEMAS FROM` (`show_catalog`).

### D3. Session options map onto the session's default scope

The messages are declared with prost in `flight_session_options.rs` from `Flight.proto` (arrow-flight 59 has none). Action bodies are the bare messages, not `Any`-wrapped, as the Java client sends them.

- `SetSessionOptions`: `catalog` and `schema`, string values only. A catalog not in `SHOW CATALOGS` is `INVALID_VALUE`; any other name is `INVALID_NAME`; a non-string value is `INVALID_VALUE`. Accepted values are applied together through `SessionManager::set_session_scope`.
- `GetSessionOptions`: the session's `catalog` and `schema` when set.
- `CloseSession`: `NOT_CLOSEABLE`. A bearer-token session is shared by every connection presenting that token; removing it would drop another connection's transaction. The driver ignores the result.

### D4. Table kind travels with the name

`list_metadata_tables` returns `(namespace, name, kind)` with `kind` `TABLE` or `VIEW`. `GetTables` filters on it; `GetTableTypes` returns both kinds.

### D5. SqlInfo values follow `FlightSql.proto`

- `SQL_IDENTIFIER_CASE` = `SQL_CASE_SENSITIVITY_LOWERCASE` (3): unquoted identifiers fold to lower case.
- `SQL_QUOTED_IDENTIFIER_CASE` = `SQL_CASE_SENSITIVITY_UNKNOWN` (0): quoted identifiers keep their case, and the enum (unknown, case-insensitive, upper, lower) has no value for that.
- `SQL_DEFAULT_TRANSACTION_ISOLATION` is sent as int64 (the Java driver casts to `Long`); the value `2` is READ_COMMITTED in both the Flight enum and `java.sql.Connection`.
- `SQL_SUPPORTED_GRAMMAR` = minimum | core (bits 0, 1); `SQL_ANSI92_SUPPORTED_LEVEL` = entry | intermediate (bits 0, 1). These are bitmasks; the old values set at most one bit.
- Added: `SQL_SUPPORTED_GROUP_BY` (unrelated | beyond select), `SQL_SUPPORTED_UNIONS` (union | union all), `SQL_OUTER_JOINS_SUPPORT_LEVEL` = full, `SQL_SUPPORTED_SUBQUERIES` (comparisons | exists | in | quantified).

### D6. Column metadata on the client schema

`client_schema` (already used by both the advertised FlightInfo schema and the DoGet stream, so the two stay equal) adds `ARROW:FLIGHT:SQL:TYPE_NAME` and, where the type has them, `PRECISION` and `SCALE` to every top-level field. Type names are Trino's (`bigint`, `varchar`, `decimal(10,2)`, `timestamp(6)`). `decimal(p,s)` gives `p`/`s`; integers give their decimal digit count (3, 5, 10, 19); `real` and `double` give 7 and 15, the decimal digits `GetXdbcTypeInfo` already reports.

## Risks

- A client that compares FlightInfo and stream schemas including field metadata still sees equal schemas: both come from `client_schema`.
- A Java client that does ship `arrow-compression` now receives uncompressed data unless it sends the header.
