**STATUS 2026-10-24: shredded variant statistics prune row groups on the coordinator (`feat/variant-rowgroup-pruning-870`, part of #870).**

- **Vendor hook** `RowGroupPruner` (`arrow/reader/mod.rs`, `pipeline.rs`, `scan/mod.rs`, `SQE PATCH (sqe#870)`): one call per file, after the byte-range selection and before the predicate, with the candidate row groups. `TableScanBuilder::with_row_group_pruner` sets it. Vendor unit test `test_row_group_pruner_skips_row_groups` covers a whole-file read and a byte-range split.
- **`sqe-catalog/src/variant_prune.rs`**: `VariantPrune::from_expr` takes `variant_get` / `try_variant_get(col, '$.a.b', 'int|bigint|string|date|boolean') <op> literal` (either side, a cast to Int64 around integer targets). `excludes` finds `col.typed_value.<path>.typed_value` by field id. A row group is dropped only when min/max exclude the literal **and** no row can hold the value elsewhere: the leaf has no non-null values (all null), or the definition-level histogram shows no variant without a top-level `typed_value` (level 1 under an optional column, level 0 under a NOT NULL one; the review caught a version that read level 1 for both and pruned a NOT NULL column's misfit row, now a unit case) and every field residual `value` along the path is all null, or `col.value` itself is all null with all-null residuals. Type checks: signed plain ints, `int` from an INT64 leaf only if both bounds fit i32, DATE logical type, BOOLEAN, UTF8 with unsigned column order, deprecated min/max refused.
- **Wiring**: `table_provider.rs` reports such filters as Inexact and moves them off `df_filters` into `IcebergScanExec::with_variant_prunes` (workers decode `df_filters` without UDFs). Direct path: `with_row_groups(kept)` on the builder; vendor path: `with_row_group_pruner`. Metric `row_groups_pruned_variant`.
- **Writer fix (found by the gate)**: `build_writer_props` ignored `write.parquet.row-group-size-bytes`, so SQE files had one row group per ~1M rows whatever the table said. Now set when the property is a positive integer (`row_group_size_bytes_is_honoured_only_when_set`).

Gates:
- `make test-integration FILTER=prunes_shredded_row_groups` -> 1 passed (`variant_e2e::prunes_shredded_row_groups`: 20k rows read whole (direct path) and 300k rows in a >3 MiB file (vendor path), several row groups each; pruned and `+ 0` baseline return the same `100|cut`; baseline scan emits every row, the pruned scan less than half; the flipped `try_variant_get` form prunes too).
- Unit: `cargo test -p sqe-catalog --lib variant_prune` (8: shapes, refusals, range pruning, missing fields and NULL variants, residual values kept, leaf type mismatch, nested path, unshredded file), `cargo test -p sqe-compaction --lib parquet_writer_config`, vendor `test_row_group_pruner_skips_row_groups`. Clippy clean on sqe-catalog and sqe-compaction.

Open:
- Workers do not prune: a worker scan reads every row group (the UDF would have to travel in `predicate_proto`).
- Data files are not pruned: variant manifest bounds are still skipped.
- Reads still unshred `typed_value`; no typed field access from the shredded column.
- `row_groups_pruned_variant` is not in Prometheus (`sqe_scan_row_groups_pruned_total` is documented as bloom only).

**NEXT:** worker-side pruning (#870 follow-up): carry the variant prune specs in the scan task, not as a UDF expression.
