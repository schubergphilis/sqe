### Fixed

- Flight SQL works with the stock Arrow Flight SQL JDBC and ODBC drivers

    The Arrow Java Flight client (`user-agent` `grpc-java...`) now gets uncompressed results, because it has no LZ4/ZSTD codec. The header `x-sqe-flight-compression: none|lz4|zstd` picks the codec for one request.

    `GetCatalogs`, `GetDbSchemas` and `GetTables` report the catalog names SQL resolves, not the warehouse name. Views are typed `VIEW`, and `GetTableTypes` lists `TABLE` and `VIEW`.

    The `SetSessionOptions`, `GetSessionOptions` and `CloseSession` actions are answered. This makes the JDBC `catalog=` property and `setCatalog` work.

    SqlInfo values are corrected: the identifier case, the grammar, GROUP BY, UNION, outer joins, subqueries, and an int64 default isolation level.

    Result columns carry `ARROW:FLIGHT:SQL:TYPE_NAME`, `PRECISION` and `SCALE`.
