**STATUS 2026-11-01: embedded Iceberg scans go through `SqeTableProvider` and the parallel-scan rules (`fix/embedded-iceberg-parallel-scan`).**

## What changed

- **The parallel-scan rules moved.** `parallel_scan.rs` and `parallel_probe_scan.rs` moved from `sqe-coordinator` to `sqe-catalog`, unchanged apart from the import path. The coordinator re-exports them under their old paths.
- **Embedded mode runs the rules.** `sqe-cli`'s embedded context adds `ParallelProbeScanRule` and `ParallelScanRule::new(0)` after DataFusion's physical rules. There is no byte threshold in-process.
- **Base tables read through `SqeTableProvider`.** `WritableIcebergCatalog`'s `BaseTablesOnly` wrapper now resolves non-metadata tables there. Metadata tables (`$snapshots` and others) stay on the vendored provider.
  - This affects catalogs `ATTACH`ed on the coordinator as well (`runtime_catalog.rs`), since they use the same wrapper. That is intended: the coordinator's own catalogs already read this way.

## Bug found on the way

`SqeTableProvider` pushed `IS [NOT] NULL` on a list column into the Iceberg scan, and the query failed with `DataInvalid: Accessor for Field tags not found`. `expr_to_predicate.rs` now keeps filters on nested columns (list, struct, map) in DataFusion.

The coordinator has the same bug today, since it uses the same converter. The embedded smoke test `embedded_warehouse_insert_values_and_filter_on_array_column` caught it once embedded mode switched providers.

## Measured

Engine-bench at 10M rows, query_s median of 3 runs, release build. The machine was busy (Defender and Homebrew in the background, CPU gate at 50%), so treat the numbers as indicative.

| query | iceberg before | after | parquet now |
|---|---|---|---|
| q1 | 0.507 | 0.164 | 0.075 |
| q2 | 0.546 | 0.381 | 0.152 |
| q3 | 0.970 | 0.815 | 0.742 |
| q4 | 0.366 | 0.316 | 0.312 |
| geomean | 0.56 | 0.36 | 0.23 |

The q1 peak RSS rose from 0.12 to 0.37 GiB: N partitions decode at once.

## Verified

- `cargo test -p sqe-catalog --lib`: the 34 tests of the moved rules plus `base_tables_read_through_sqe_table_provider`, and the converter tests including the new `a_nested_column_is_not_pushed_down`.
- `cargo test -p sqe-cli`: all pass.
- The full integration gate passed earlier today (340 tests) with these changes in the tree, but before the converter fix.

**NEXT:** a coordinator integration test for a list-column `IS NOT NULL`; refresh the quickstart engine-bench numbers once this merges; then the iceberg-rust read-correctness cherry-picks.
