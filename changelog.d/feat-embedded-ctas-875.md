### Changed

- Embedded CTAS runs the cluster's CTAS handler (#875)

    `CREATE TABLE ... AS` into an Iceberg catalog in `sqe-cli --embedded` now runs the same
    streaming CTAS a coordinator runs, instead of an empty DataFusion CREATE followed by a
    separate INSERT. `PARTITIONED BY` and `TBLPROPERTIES` / `WITH (...)` are kept instead of
    refused, and `CREATE OR REPLACE TABLE ... AS` works: in place with one commit when the
    query's schema and partitioning match the table, otherwise a new table replaces it.
    The statement reports the written row count.
