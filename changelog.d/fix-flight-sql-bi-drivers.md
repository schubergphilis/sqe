### Fixed

- Flight SQL works with the stock Arrow Flight SQL JDBC and ODBC drivers

    The Arrow Java and C++ Flight clients (a `user-agent` containing `grpc-java` or `grpc-c++`) now get uncompressed results, because the JDBC driver and Arrow's ODBC installer have no LZ4/ZSTD codec. The header `x-sqe-flight-compression: none|lz4|zstd` picks the codec for one request; pyarrow, which has the codecs, can send `lz4` to get compression back.

    `GetCatalogs`, `GetDbSchemas` and `GetTables` report the catalog names SQL resolves, not the warehouse name. Views are typed `VIEW`, and `GetTableTypes` lists `TABLE` and `VIEW`.

    The `SetSessionOptions`, `GetSessionOptions` and `CloseSession` actions are answered. This makes the JDBC `catalog=` property and `setCatalog` work.

    SqlInfo values are corrected: the identifier case, the grammar, GROUP BY, UNION, outer joins, subqueries, and an int64 default isolation level.

    Result columns carry `ARROW:FLIGHT:SQL:TYPE_NAME`, `PRECISION` and `SCALE`.
