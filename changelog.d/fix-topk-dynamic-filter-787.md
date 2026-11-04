### Fixed

- `ORDER BY ... LIMIT` and join dynamic filters reach the Iceberg scan again (#787)

    The parallel-scan and parallel-probe rules re-run `EnsureRequirements`,
    which rebuilds the TopK sort with a new dynamic filter. The scan kept the
    old one: nothing updated it, so it never pruned, and every scan partition
    waited 100 ms for it to seal before reading. `StarSchemaReorderRule` and
    the self-join strip orphaned join filters the same way.

    A new physical rule, `SettleDynamicFiltersRule`, runs after the
    parallel rules (coordinator and embedded). It drops scan filters that no
    plan node produces any more and re-runs DataFusion's post-optimization
    filter pushdown, so the rebuilt sort or join hands the scan a live
    filter. TopK and aggregate filters are marked progressive: the scan does
    not wait for them, and the Parquet row filter reads the current bound on
    every batch, so rows past it skip decoding the payload columns. File
    pruning at stream open does not use them yet (no bound exists then). A
    scan also no longer keeps two copies of a filter pushed twice.

    The Iceberg predicate built from a dynamic filter now converts
    `col IS [NOT] NULL`. The default descending TopK bound is
    `col IS NULL OR col > t`, and without the null arm none of it reached
    row-group pruning. Range comparisons on `FLOAT` / `DOUBLE` no longer
    convert at all: Parquet min/max statistics leave NaN out, so a row
    group holding NaN could be pruned for `x > t` while NaN sorts above
    every number. This also covers join bounds on float keys, which had
    the same hole before.

    On a 150M-row Polaris table, an integer-key `ORDER BY customer_id
    DESC|ASC LIMIT 10` decodes about 60 M rows instead of 150 M and runs in
    2.5-2.7 s instead of 3.8 s (embedded, same binary, pushdown off vs on).
    A `DOUBLE` key (`ORDER BY price LIMIT 10`) still decodes every row; it
    gets the removed 840 ms filter wait and a TopK fed about 90 K rows
    instead of 150 M.

    Joins that `StarSchemaReorderRule` reorders now get dynamic filters,
    which they lost before. `DistributedScanExec` (worker scans) is not
    covered yet.
