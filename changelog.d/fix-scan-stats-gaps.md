### Fixed

- A join against a `LIMIT`ed Hive scan can broadcast the limited side

    A Hive scan under `LIMIT` with more than one partition reads lazily
    (`LazyPartitionScanExec`) and lists nothing at plan time, so it reported
    no row count and DataFusion never broadcast it in a join. It never returns
    more than `limit` rows, and now reports that as an inexact row count. An
    integration test also pins that a join against a small `read_parquet()`
    file broadcasts the file through the coordinator's planning rules, on
    whichever side of the join the query names it.
