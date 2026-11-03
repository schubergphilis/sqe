### Changed

- Iceberg scans on the coordinator and in embedded mode no longer evaluate their own `WHERE` predicate as a per-row parquet `RowFilter`

    The predicate still prunes files, row groups and pages, and the filter
    above the scan drops the remaining rows, as DataFusion does with
    `pushdown_filters = false`. With the `RowFilter`, peak memory rose by
    55-110% on a 150M-row Polaris table, whether the predicate kept 92% or
    8% of the rows (engine-bench q2 2.0 -> 1.26 GiB, q1 1.4 -> 0.66 GiB), and
    no query got faster. Equality deletes and join runtime filters stay in
    the `RowFilter`, since deletes depend on it. Every pushed filter is
    `Inexact`, so a `FilterExec` always re-applies the predicate. Set
    `scan_static_row_filter = true` in a catalog's section
    (`[catalogs.<name>]`) to bring back the old behaviour. Distributed
    worker scans read parquet through their own path and are unchanged.
