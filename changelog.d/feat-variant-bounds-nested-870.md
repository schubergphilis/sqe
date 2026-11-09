### Added

- Shredded double fields in a variant get manifest bounds and prune files (#870)

    SQE writes variant manifest bounds for shredded double paths, as Iceberg
    Java does: a path that holds a NaN gets no bound, the lower bound of a
    zero is -0.0 and the upper +0.0. A `variant_get(v, '$.d', 'double')`
    comparison with a number now skips the files whose bounds exclude it,
    for SQE-written and Spark-written tables alike. The
    reader widens a zero bound to cover both zeros, because Iceberg Java
    writes +0.0 over a -0.0 row. Row groups are not pruned on a double
    field: Parquet leaves NaN out of the statistics.
