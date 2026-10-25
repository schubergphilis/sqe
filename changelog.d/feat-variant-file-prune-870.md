### Added

- Variant manifest bounds prune data files (#870)

    Iceberg 1.12 writers (Spark 4.1) store, per shredded variant path, the
    minimum and maximum over the data file in the manifest's lower and upper
    bounds. The vendored iceberg-rust used to drop those bounds on read; it
    now keeps them on `DataFile::variant_lower_bounds` /
    `variant_upper_bounds` and writes them back when a manifest is
    rewritten. A `variant_get` / `try_variant_get` comparison with a literal
    (int, bigint, string, date, boolean) now skips the data files whose
    bounds exclude it, on the local scan and in distributed scan planning.
    A file without a bound for the path, or with a bound of another type, is
    read. The skipped files are counted as `pruned_variant` in
    `sqe_scan_files_total` and as `sqe.scan.files_pruned_variant` on the
    query span, not as min/max pruned. SQE does not write variant bounds
    yet, so only files from another writer are skipped.
