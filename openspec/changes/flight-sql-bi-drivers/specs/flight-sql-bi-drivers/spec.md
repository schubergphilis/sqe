## ADDED Requirements

### Requirement: Java Flight clients receive results they can decode

The coordinator SHALL answer a Flight request whose `user-agent` contains `grpc-java` with uncompressed IPC bodies, whatever `[coordinator] flight_compression` says. A request header `x-sqe-flight-compression` with value `none`, `lz4` or `zstd` SHALL choose the codec for that request and take precedence over the user agent and the configuration. Any other client SHALL keep the configured codec.

#### Scenario: The Flight SQL JDBC driver reads a result under the default configuration
- **WHEN** a request with `user-agent: JDBC Flight SQL Driver 19.0.0 grpc-java-netty/1.73.0` runs `SELECT 1` and `flight_compression = "lz4"`
- **THEN** the IPC options for its result carry no compression

#### Scenario: The header wins
- **WHEN** a `grpc-java` request sends `x-sqe-flight-compression: zstd`
- **THEN** its result is ZSTD-compressed

#### Scenario: Other clients are unchanged
- **WHEN** an ADBC Go client sends no header and `flight_compression = "lz4"`
- **THEN** its result is LZ4-compressed

### Requirement: Catalog metadata uses names SQL resolves

`GetCatalogs` SHALL return the catalog names `SHOW CATALOGS` returns for the session. `GetDbSchemas` and `GetTables` SHALL report each row under such a name, and every `catalog.schema.table` they report SHALL resolve in a query on the same session.

#### Scenario: A browsed table can be queried
- **WHEN** a client lists tables through `GetTables` and runs `SELECT * FROM "<catalog>"."<schema>"."<table>"` with a reported row
- **THEN** the query resolves the table

#### Scenario: A catalog filter selects one catalog
- **WHEN** `GetDbSchemas` is called with `catalog = "iceberg"`
- **THEN** every row's catalog is `iceberg`

#### Scenario: An unknown catalog filter is empty
- **WHEN** `GetTables` is called with a catalog no `SHOW CATALOGS` row names
- **THEN** the result is empty and not an error

### Requirement: Session options set the default catalog and schema

The `SetSessionOptions` action SHALL accept the string options `catalog` and `schema` and make them the session's default scope, as `USE` does. An unknown catalog SHALL be reported as `INVALID_VALUE`, an unknown option name as `INVALID_NAME`, a non-string value as `INVALID_VALUE`; an option with an error SHALL NOT be applied. `GetSessionOptions` SHALL return the session's `catalog` and `schema` when set. `CloseSession` SHALL answer `NOT_CLOSEABLE` and leave the session in place.

#### Scenario: The JDBC catalog property connects
- **WHEN** a client sends `SetSessionOptions {catalog: "iceberg"}` and then runs `SELECT * FROM s.t`
- **THEN** the result has no errors and the query reads `iceberg.s.t`

#### Scenario: An unknown catalog is refused
- **WHEN** a client sends `SetSessionOptions {catalog: "nope"}`
- **THEN** the result maps `catalog` to `INVALID_VALUE` and the session's catalog is unchanged

#### Scenario: Unknown option names are reported
- **WHEN** a client sends `SetSessionOptions {timezone: "UTC"}`
- **THEN** the result maps `timezone` to `INVALID_NAME`

### Requirement: Views are reported as views

`GetTables` SHALL type each row `TABLE` or `VIEW` and SHALL honour a `table_types` filter on that type. `GetTableTypes` SHALL return `TABLE` and `VIEW`.

#### Scenario: A view filter lists views
- **WHEN** a schema holds table `t` and view `v` and `GetTables` is called with `table_types = ["VIEW"]`
- **THEN** the result holds `v` and not `t`

### Requirement: SqlInfo values match the Flight SQL definitions

`SqlInfo` SHALL report `SQL_IDENTIFIER_CASE` as lower case, `SQL_DEFAULT_TRANSACTION_ISOLATION` as an int64, `SQL_SUPPORTED_GRAMMAR` and `SQL_ANSI92_SUPPORTED_LEVEL` as bitmasks, and SHALL report GROUP BY, UNION, full outer join and subquery support.

#### Scenario: The JDBC driver reads the isolation level
- **WHEN** a JDBC client calls `getDefaultTransactionIsolation()`
- **THEN** it returns `TRANSACTION_READ_COMMITTED` without a ClassCastException

### Requirement: Result columns carry type metadata

Every top-level field of a result schema SHALL carry `ARROW:FLIGHT:SQL:TYPE_NAME`, and `ARROW:FLIGHT:SQL:PRECISION` and `ARROW:FLIGHT:SQL:SCALE` where the type has them. The FlightInfo schema and the stream schema SHALL stay equal, metadata included.

#### Scenario: Decimal precision reaches the client
- **WHEN** a client runs `SELECT CAST(1.5 AS DECIMAL(10,2)) AS d`
- **THEN** the `d` field carries precision `10`, scale `2` and type name `decimal(10,2)`
