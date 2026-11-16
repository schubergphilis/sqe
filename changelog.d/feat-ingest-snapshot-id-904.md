### Added

- Flight SQL bulk ingest returns the committed snapshot id as the `x-sqe-snapshot-id` gRPC trailer (#904)

    `CommandStatementIngest` over `DoPut` can only answer with a row count, so the id of the snapshot a create or append committed now travels in the trailing metadata, next to `grpc-status`. No trailer is sent inside a transaction, for a replace, or when the ingest fails. ADBC drivers do not expose trailers; those clients still read `table_snapshots`.
