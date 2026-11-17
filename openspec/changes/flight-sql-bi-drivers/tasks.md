## 1. Groundwork

- [x] 1.1 Move `build_sql_info_data` and the LIKE helpers out of `flight_sql.rs` into `flight_sql_metadata.rs` (pure move)

## 2. Compression per client

- [x] 2.1 `FlightCompression: FromStr`; `ClientCompressionLayer` sets a task-local from `x-sqe-flight-compression` / `user-agent`
- [x] 2.2 `flight_ipc_options` uses the task-local override; layer installed on the Flight server
- [x] 2.3 Unit tests: header parsing, user-agent rule, precedence

## 3. Catalog metadata

- [x] 3.1 `GetCatalogs` returns `SHOW CATALOGS`
- [x] 3.2 `GetDbSchemas` walks the selected catalogs with `SHOW SCHEMAS FROM`
- [x] 3.3 `GetTables` walks the selected catalogs; `list_metadata_tables` takes a catalog and returns the kind
- [x] 3.4 `GetTableTypes` returns `TABLE` and `VIEW`; the type filter applies per row
- [x] 3.5 Integration test: browsed names resolve; views typed `VIEW`

## 4. Session options

- [x] 4.1 Prost messages for `SetSessionOptions` / `GetSessionOptions` / `CloseSession`
- [x] 4.2 Action handlers; `SessionManager::set_session_scope`
- [x] 4.3 Integration test: set catalog, unknown catalog, unknown name, get, close

## 5. SqlInfo and column metadata

- [x] 5.1 SqlInfo values corrected; GROUP BY / UNION / outer join / subquery keys added
- [x] 5.2 Column metadata in `client_schema`; unit test for decimal, integer, string
- [ ] 5.3 Driver matrix rerun (JDBC and ODBC, password and token)
