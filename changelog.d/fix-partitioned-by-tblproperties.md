### Fixed

- `CREATE TABLE ... PARTITIONED BY (...) TBLPROPERTIES (...)` parses

    SQE rewrites `PARTITIONED BY` to `PARTITION BY` for the transform list,
    and sqlparser only takes `LOCATION`, `WITH (...)` and `TBLPROPERTIES`
    before `PARTITION BY`. The Spark and Databricks order failed with
    "found: TBLPROPERTIES". The table clauses of a `CREATE TABLE` are now put
    in the order the parser takes, and `USING iceberg` is dropped (every
    table SQE creates is an Iceberg table). Another provider, such as
    `USING delta`, still fails.
