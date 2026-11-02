**STATUS 2026-11-01: Iceberg scans read strings as `Utf8View` (`feat/iceberg-string-view`).**

The vendored reader (`arrow/reader`, `record_batch_transformer`, `schema.rs`
`string_view_schema`, `value.rs`) decodes top-level string columns as
`Utf8View` when `ScanTuning.string_view` is set (default on,
`[catalog] string_view`). Predicate pushdown handles view literals
(`expr_to_predicate`, `physical_to_predicate`, the reader's predicate
visitor). Flight SQL maps `Utf8View -> Utf8` and `BinaryView -> Binary` in
reported schemas.

q1 of the engine-bench (150M rows, dev-release, 3g): 0.95 s wall, 7.5 s CPU,
1.11-1.20 GiB RSS, against 1.08 s / 9.3 s / 1.22 GiB with views off.

Audit of `Utf8`-only code reached by scan output fixed: PartialMask (returned
NULL), `build_bounds_array` (string file pruning was silently off), z-order
key, and four `Signature::any` Trino UDFs (`helpers::utf8_string_args`).
Checked and fine: Trino/quack encoders, puffin sketches, json_cast,
maintenance ledger readers (raw Parquet), `Signature::exact(Utf8)` UDFs
(DataFusion coerces views to Utf8).

The in-process `QueryHandler::execute` returns whatever layout the plan
produced, so integration tests that downcast a scanned string column to
`StringArray` now go through `common::utf8` (casts any string layout).
Only the Flight wire (`conform_batch`) promises `Utf8`.

Stacks on `fix/iceberg-backports-round3` (the transformer rewrite); commit on
`origin/main` once that has merged.

**NEXT:** wire `ScanTuning::from_config` into the embedded CLI so
`string_view` can be turned off there; the scan still evaluates a pushed
predicate twice (reader RowFilter + FilterExec, see the q1 profile).
