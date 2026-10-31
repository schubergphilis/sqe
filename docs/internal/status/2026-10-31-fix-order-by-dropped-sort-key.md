**STATUS 2026-10-31: `ORDER BY` a non-selected column (`fix/order-by-dropped-sort-key`).**

Found while testing MERGE `UPDATE SET *` (!1601): on a table that scans as
several partitions, `SELECT id + 0 AS r FROM t ORDER BY id` returned rows as
interleaved sorted runs. Wrong results, no error.

Cause: `ParallelScanRule` / `ParallelProbeScanRule` bump the scan, re-run
`EnsureRequirements`, then `restore_single_partition_root` gives the rest of
SQE a single-partition root. It read only the root's `output_ordering()`.
The root was `ProjectionExec[r]` over a `preserve_partitioning` `SortExec` on
`id`; the projection drops `id`, so the root reports no ordering and got a
`CoalescePartitionsExec`.

Fix (`parallel_probe_scan.rs` `merge_below_order_hiding_root`, shared by both
rules): when the root has no ordering, walk down through fetchless
`ProjectionExec` / `FilterExec` / `CooperativeExec` with an unchanged partition
count. On reaching a `preserve_partitioning` `SortExec`, put a
`SortPreservingMergeExec` (with the sort's fetch) on it and rebuild the
operators above. Otherwise coalesce as before.

Verified: 3 new unit tests + 838 `sqe-coordinator` lib tests (with
`RUST_MIN_STACK=32MiB`); new IT
`order_by_hidden_sort_key_e2e::order_by_a_column_that_is_not_selected` fails
without the fix (interleaved runs on the `ORDER BY id DESC` case) and passes
with it; full `make test-integration` green (339 passed).

**NEXT:** playground4 handover asks still open: `USE` does not hold across
statements on the portal statement API; about 30 statements a minute.
