### Fixed

- `PARTITIONED BY (col, ...)` with bare column names was ignored in embedded mode and on the Trino HTTP path

    `CREATE TABLE ... PARTITIONED BY (region, id)` created an unpartitioned
    table with no error. The clause parsed as a Hive partition column list,
    which the DDL handler does not read, so only transforms such as
    `bucket(4, id)` or `day(ts)` took effect. The front door now rewrites
    the clause to `PARTITION BY` for every `CREATE TABLE`, as the Flight
    path already did. `CREATE EXTERNAL TABLE` keeps the Hive form.
