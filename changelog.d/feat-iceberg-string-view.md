### Changed

- Iceberg scans read string columns as `Utf8View`

    The Iceberg reader decoded every string column into `Utf8` (one offset
    buffer, every value copied), then DataFusion's string kernels worked on
    that copy. Scans now decode top-level strings as `Utf8View`, the layout
    DataFusion already uses for plain Parquet, and the reader, its predicate
    pushdown and the missing-column defaults all produce views. On a 150M-row
    group-by over `status IN (...)` (3 GiB memory limit) wall time drops from
    1.08 s to 0.95 s, CPU from 9.3 s to 7.5 s, peak memory from 1.22 GiB to
    1.15 GiB. Flight SQL still reports `Utf8`/`Binary` in its schemas.
    `[catalog] string_view = false` restores the old layout.

### Fixed

- Paths that only handled `Utf8` strings now accept `Utf8View`, found while
  switching the scan default:
    - a partial mask (`MASK_SHOW_LAST_4` and friends) on a `Utf8View` column
      returned NULL instead of the masked value
    - manifest min/max file pruning skipped every `Utf8View` and `LargeUtf8`
      string column
    - z-order compaction refused a `Utf8View` or `BinaryView` sort column
    - `date_parse`, `json_format`, `codepoint` and string `contains` failed on
      a `Utf8View` column; array `contains` never matched a string literal
      against `Utf8View` items
