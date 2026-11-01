### Fixed

- Vendored iceberg-rust: third round of upstream correctness fixes

    Seven fixes from apache/iceberg-rust and RisingWave's fork:
    - The Parquet writer skips NaN when it tracks column min/max.
    - Nested struct fields of files written without field ids read as NULL.
    - A DELETE or rewrite of files whose partition source column was later
      dropped no longer panics while it writes the snapshot summary.
    - Partition specs whose source column was dropped pass the name check.
    - `IN` lists prune files and row groups when every value falls outside
      the bounds, on either side.
    - Manifest bounds on nested fields are ignored instead of failing the
      manifest read.
    - Writers stamp the unpartitioned spec id on the files they write.

    The IO-runtime and scan/delete perf patches follow in a separate change.
    `vendor/iceberg-rust/README.md` lists every pick.
