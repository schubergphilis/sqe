**STATUS 2026-11-17: Flight SQL works with the stock JDBC and ODBC drivers (`fix/flight-sql-bi-drivers`).**

OpenSpec change `openspec/changes/flight-sql-bi-drivers/`. A driver matrix against the Arrow Flight SQL JDBC driver 19.0.0 and the Arrow Flight SQL ODBC driver found the gaps listed in the proposal; this branch fixes the server-side ones.

- **Compression per client** (`flight_client_compat.rs`). The Arrow Java Flight client ships no LZ4/ZSTD codec, so the default `flight_compression = "lz4"` made every JDBC result unreadable. A request whose `user-agent` contains `grpc-java` (the JDBC driver sends `JDBC Flight SQL Driver 19.0.0 grpc-java-netty/...`, so a prefix match missed it; found by the real-driver rerun) is now answered uncompressed. The same goes for `grpc-c++`: Arrow's ODBC installer is built without LZ4 and ZSTD, and the driver failed every result with "Support for codec 'lz4' not built". pyarrow shares that stack and has the codecs, so it loses compression unless it sends `x-sqe-flight-compression: lz4`. The header `x-sqe-flight-compression: none|lz4|zstd` picks the codec for one request and wins. Other clients keep the configured codec.
- **Catalog names SQL knows.** `GetCatalogs` returns `SHOW CATALOGS`. `GetDbSchemas` and `GetTables` walk each selected catalog. A catalog that cannot be listed (not authorised, a Hive root) is skipped with a warning.
- **Session options** (`flight_session_options.rs`). `SetSessionOptions` takes `catalog` and `schema` and sets the session's default scope, as `USE` does. An unknown catalog gives `INVALID_VALUE` and an unknown option `INVALID_NAME`. `GetSessionOptions` returns them. `CloseSession` answers `NOT_CLOSEABLE`, because a bearer session is shared by every connection with that token. arrow-flight 59 declares none of these messages, so they are declared by hand from `Flight.proto`.
- **Views.** `GetTables` types each row `TABLE` or `VIEW`, and `GetTableTypes` returns both.
- **SqlInfo.**
  - The identifier case is LOWERCASE and the quoted case is case-sensitive.
  - The grammar and ANSI-92 bitmasks are set.
  - The GROUP BY, UNION, outer-join and subquery keys are added.
  - The default isolation level is an int64, which the JDBC driver casts to `Long`.
- **Column metadata.** Every result field carries `ARROW:FLIGHT:SQL:TYPE_NAME` (Trino-style names), `PRECISION` and `SCALE`.

The SqlInfo builder and the LIKE helpers moved to `flight_sql_metadata.rs` first, as a pure move in its own commit.

Budget: the `sqe-coordinator` ceiling goes from 68700 to 69200 lines. The two new modules and their unit tests are about 440 lines.

Gates:

- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`: 0.
- `cargo test -p sqe-coordinator --lib -- flight_client_compat flight_session_options table_types sql_info`: 19 passed.
- `cargo test -p sqe-coordinator --test it flight_sql_smoke`: 8 passed. The new tests are `session_options_set_the_scope_jdbc_asks_for`, `table_types_and_tables_answer_bi_browsers` and `result_columns_carry_flight_sql_type_metadata`.
- `make test-integration FILTER=test_metadata_listing_types_views`: 1 passed.
- `cargo xtask budgets --check`: 0.

Left open, driver-side:

- The JDBC driver's `getTypeInfo` is Avatica's empty default.
- `setSchema` is never sent to the server.
- The ODBC driver implements only SQLTables, SQLColumns and SQLGetTypeInfo. PrimaryKeys, ForeignKeys, Statistics, SpecialColumns, Procedures and Cancel fail with IM001 in the driver.

Driver matrix, 2026-11-17. The build combined this branch, the Flight bearer handshake (!1825) and the Trino `system.jdbc` fixes (!1826). It ran with the default `flight_compression = "lz4"`, client-credentials and bearer pass-through auth, and the local Polaris + RustFS stack.

| Driver | Password | Token | Failures left |
|---|---|---|---|
| Flight SQL JDBC 19.0.0 | 71 / 72 | 71 / 72 | `setSchema` + unqualified name: the driver never sends `setSchema`. `catalog=iceberg` also 71 / 72 |
| Flight SQL ODBC (Arrow 26.0.1) | 105 / 114 | 105 / 114 | All 9 are IM001 in the driver: primary and foreign keys, statistics, special columns, procedures, `SQLNumParams` |
| Trino JDBC 480 | 63 / 74 | 63 / 74 | 9 are driver-side (`SQLFeatureNotSupported`, `getFunctions` not implemented). 2 are server-side and go to !1826: `getUserName` (bare `current_user`) and prepared `getMetaData` before execute (null table name in `DESCRIBE OUTPUT`) |

Before this branch, every Flight JDBC result failed with "Please add arrow-compression module ... LZ4_FRAME" and the JVM hung. Every ODBC result failed with "Support for codec 'lz4' not built".

**NEXT:** none for this branch. The two Trino JDBC server-side gaps are handled on !1826.
