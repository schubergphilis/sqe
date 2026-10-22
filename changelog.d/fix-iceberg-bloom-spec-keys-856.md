### Fixed

- Iceberg spec bloom keys are honoured on write (#856)

    `write.parquet.bloom-filter-enabled.column.<col>` and
    `write.parquet.bloom-filter-fpp.column.<col>`, the names Spark and other
    Iceberg writers set, now drive SQE's Parquet bloom filters next to SQE's
    own `write.parquet.bloom-filter-columns` / `-fpp` / `-auto`. `true` adds a
    column, `false` removes it even when auto-detection picked it, and a
    per-column fpp outside (0, 1) falls back to the table default instead of
    panicking the writer. Struct members (`s.x`) get a bloom on their nested
    Parquet path; list and map members are skipped, and
    `write.parquet.bloom-filter-max-bytes` is ignored. Before, a table
    configured by Spark silently got no blooms from SQE writes.
