### Fixed

- `CREATE TABLE ... PARTITIONED BY (...) TBLPROPERTIES (...)` parses

    SQE rewrites `PARTITIONED BY` to `PARTITION BY` for the transform list,
    and sqlparser only takes `LOCATION`, `WITH (...)` and `TBLPROPERTIES`
    before `PARTITION BY`. The Spark and Databricks order failed with
    "found: TBLPROPERTIES". The table clauses of a `CREATE TABLE` are now put
    in the order the parser takes, and `USING iceberg` is dropped (every
    table SQE creates is an Iceberg table). Another provider, such as
    `USING delta`, still fails.
- `SHOW CREATE TABLE` shows the partition spec of an Iceberg table

    The DDL had no `PARTITIONED BY` clause, so replaying it created an
    unpartitioned table. It now renders the default partition spec with the
    transforms `CREATE TABLE` takes: identity, `year`, `month`, `day`,
    `hour`, `bucket(N, col)`, `truncate(W, col)` and `void`.
