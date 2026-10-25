### Added

- SQE writes variant manifest bounds for shredded files (#870)

    With `write.parquet.shred-variants`, each shredded data file SQE writes,
    compaction included, now carries, per shredded path of a top-level
    variant column, the minimum and maximum value in the manifest's lower
    and upper bounds, as Spark 4.1 / Iceberg 1.12 write them. The bounds are
    taken from the values themselves, so a row written unshredded still
    counts, and a path that holds a value of another type gets no bound.
    SQE's own files are now skipped by a `variant_get` / `try_variant_get`
    comparison the bounds exclude, before and after `rewrite_data_files`.
    Integer, string (up to 64 bytes), boolean and date paths get bounds;
    double paths do not.
