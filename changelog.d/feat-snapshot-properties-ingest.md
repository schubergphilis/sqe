### Added

- Flight SQL bulk ingest takes per-request snapshot properties as `snapshot-property.<key>` options

    The prefix is Spark's. They are entries in the request's `CommandStatementIngest.options` and apply to that request's commit only, laid over the transaction's or session's properties, with the request winning for a repeated key. A refused key or value fails the request with `InvalidArgument` before any data file is written.
