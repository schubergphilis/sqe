### Fixed

- A small file or Hive table in a join is broadcast again instead of hash-partitioned

    The scan wrappers around `read_parquet()` and the other file readers
    (`ScopedFileScan`) and around Hive scans (`HiveScanExec`,
    `HiveLocalScanExec`) reported no statistics under DataFusion 55, which
    reads them through `statistics_from_inputs` and no longer calls
    `partition_statistics`. With no row count, DataFusion never picks a
    `CollectLeft` join, so every row of the large side was hashed across
    partitions. They now pass their input's statistics through. On the
    engine-bench `q2_join_agg` (150M sales rows joined to a 50,000-row
    products file, embedded) the query went from 4.9 s and 4.0 GiB peak to
    1.5 s and 0.14 GiB, same answer. A Hive scan under a `LIMIT` still reads
    lazily and reports no row count.
