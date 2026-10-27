### Fixed

- A distributed scan no longer returns rows that delete files removed

    A worker reads the data files in its scan ticket with a plain Parquet
    reader, and the ticket carries no delete files, so position deletes,
    deletion vectors and equality deletes were not applied. A merge-on-read
    table read through workers returned its deleted rows. The coordinator now
    scans a snapshot that has delete manifests itself, where iceberg-rust
    applies them (`has_delete_files` in the distribution decision). Snapshots
    without delete files still go to the workers. Found by the Databricks
    Unity Catalog interop run: a deletion vector Databricks wrote into an
    SQE table was ignored.
