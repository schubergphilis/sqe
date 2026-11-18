# Flight SQL works with the stock JDBC and ODBC drivers

## Why

The Arrow Flight SQL JDBC driver 19.0.0, run against SQE's default configuration, cannot read a single result: SQE compresses every client-facing IPC body with LZ4 (`[coordinator] flight_compression = "lz4"`), and the driver does not ship the `arrow-compression` codec ("Please add arrow-compression module to use CommonsCompressionFactory for LZ4_FRAME"). Worse, the JVM then hangs on non-daemon threads. With compression off, a driver matrix (password and token sign-in, 73 checks) still found these problems:

- `GetCatalogs` / `GetDbSchemas` / `GetTables` report the warehouse name (`test_warehouse`), a name SQL does not know (`unknown catalog 'test_warehouse'`): a BI tool that qualifies what it browsed gets an error.
- The `catalog=` connection property fails the connection: the driver sends the `SetSessionOptions` action and SQE answers "Unknown action type".
- Views are listed as `TABLE`; `GetTableTypes` lists only `TABLE`; `getTables(types=VIEW)` is empty.
- `SqlInfo`: the default isolation level is an int32 where the driver reads an int64 (ClassCastException in `getDefaultTransactionIsolation`); the identifier case says UPPERCASE; GROUP BY, UNION, outer joins and subqueries are reported unsupported; the grammar bitmask is 0.
- Result-set metadata has precision and scale 0 for every column (`DECIMAL(10,2)` reports `p=0 s=0`).

## What Changes

- **Compression per client.** A request from the Arrow Java Flight client (`user-agent` containing `grpc-java`; the JDBC driver sends `JDBC Flight SQL Driver 19.0.0 grpc-java-netty/...`) is answered uncompressed, and so is one from the Arrow C++ client under the ODBC driver (`grpc-c++`; Arrow's ODBC installer is built without LZ4 and ZSTD). A request header `x-sqe-flight-compression: none|lz4|zstd` picks the codec for that request and wins over both. Every other client keeps `[coordinator] flight_compression`.
- **SQL catalog names.** The catalog metadata RPCs use the names SQL resolves: `GetCatalogs` returns what `SHOW CATALOGS` returns; `GetDbSchemas` and `GetTables` walk each catalog the request's filter selects.
- **Session options.** `SetSessionOptions` accepts `catalog` and `schema` (string values) and sets the session's default scope, as `USE` does; an unknown catalog is `INVALID_VALUE`, an unknown option `INVALID_NAME`. `GetSessionOptions` returns them. `CloseSession` answers `NOT_CLOSEABLE`: a session lives as long as its token.
- **Views.** `GetTables` types each row `TABLE` or `VIEW`; `GetTableTypes` returns both.
- **SqlInfo.** Values corrected against `FlightSql.proto`; the missing GROUP BY, UNION, outer-join and subquery keys added.
- **Column metadata.** Every result field carries `ARROW:FLIGHT:SQL:TYPE_NAME`, `PRECISION` and `SCALE` where the type has them.

## Capabilities

### New Capabilities

- `flight-sql-bi-drivers`: what the stock Flight SQL JDBC and ODBC drivers can rely on from SQE.

### Modified Capabilities

None.

## Impact

- `sqe-coordinator`: `flight_sql.rs` (metadata handlers, action dispatch, `flight_ipc_options`, `client_schema`), new `flight_client_compat.rs` (compression layer, column metadata) and `flight_session_options.rs` (session-option messages), `flight_sql_metadata.rs` (SqlInfo, moved out of `flight_sql.rs` first), `query_handler.rs` (`list_metadata_tables` takes a catalog and returns the kind), `session_manager.rs`.
- `sqe-server`: one more tower layer on the Flight server.
- Performance: Java clients receive uncompressed IPC. On a LAN this is the faster choice for the client; over a slow link it costs bandwidth. ADBC, dbt-sqe and every non-Java client keep the configured codec.
- Out of scope: the driver-side gaps (`getTypeInfo` is Avatica's empty default and never calls `GetXdbcTypeInfo`; `setSchema` is never sent to the server; TIME values are shifted by the JVM time zone).
