**STATUS 2026-11-02: the scan predicate is no longer a per-row parquet `RowFilter` (`fix/scan-static-row-filter`).**

This came out of tracing the engine-bench q2 gap on Polaris. The iceberg
reader ANDed the task predicate, the equality-delete predicate and the
dynamic (join) predicate into one `RowFilter`. That cost decode and memory
for rows the `FilterExec` above the scan filters anyway, since every SQE
`supports_filters_pushdown` answers `Inexact`.

The vendored reader now takes `with_static_row_filter(bool)` on
`TableScanBuilder` / `ArrowReaderBuilder`, with `SQE PATCH (static row
filter)`. Its default is `true`, which is upstream behaviour, so raw
`TableScan` users (writes, compaction) are unchanged. Only `IcebergScanExec`
passes the new `[catalogs.<name>] scan_static_row_filter`, default `false`.

With the flag off:

- the task predicate still drives row-group stats, bloom and page-index
  pruning;
- the `RowFilter` keeps only equality deletes and the dynamic predicate,
  with its own decode mask;
- the worker scan path (`sqe-worker` executor) is untouched.

A/B on bench_150m (Polaris + RustFS, one coordinator, a fresh server per run,
4 runs per query, 2 reps):

| query | RowFilter on: peak RSS | off: peak RSS | time |
|---|---|---|---|
| q2 `status <> 'cancelled'` (92% kept) | 1.98 / 2.04 GiB | 1.26 / 1.26 GiB | off ~0.5 s faster |
| q2 `status = 'cancelled'` (8% kept) | 1.92 / 1.90 GiB | 1.25 / 1.20 GiB | off equal or faster |
| q1 `ts >= ... AND status IN (...)` | 1.35 / 1.44 GiB | 0.64 / 0.69 GiB | within noise |

EXPLAIN ANALYZE for the 8% query confirms the mechanism:

- **On:** the scan emits 12.0M rows and FilterExec takes 22 ms.
- **Off:** the scan emits 150M rows and FilterExec takes 868 ms.

The extra filter work costs less than what the `RowFilter` held in memory.

The setting is read from the per-catalog section (`[catalogs.polaris]`), not
from the global `[catalog]` table. The first A/B put it in `[catalog]` and
measured off against off.

Verified 2026-11-02:

- vendor `cargo test -p iceberg --lib`: 2170 passed, including
  `test_static_row_filter_off_returns_superset`;
- `sqe-catalog` lib: 751 passed;
- `sqe-core` config tests: 314 passed;
- clippy on both crates with `--tests -D warnings`: clean;
- `make test-integration`: 341 of 341. Two of them,
  `rewrite_data_files_deletes::rewrite_applies_equality_deletes` and
  `v3_e2e::equality_delete_update_on_v3_table`, read through an equality
  delete with the new default;
- `make bench-smoke`: 60 of 60 runs, every answer matches;
- `make benchmark_sf1_trino`: 230 of 230 pass and every suite matches Trino
  (tpcds q64 empty on both engines and clickbench 41/43, as on 10-18). SQE
  totals against 10-18: tpch -8%, ssb -13%, tpcds +4%, tpcc -7%, tpcbb -15%,
  clickbench -19%, bank -52%, tpce +98%.

The tpce number is contention, not this change: the compare sweep runs
Trino's 8 GB container next to SQE, and Trino's own tpce times rose 30-60%
too. tpce alone, alternating `SQE_CATALOG__SCAN_STATIC_ROW_FILTER`
false/true/false/true (the bench config has only the legacy `[catalog]`
block, so the env overlay reaches the scan): 1537 / 1488 / 1558 / 1521 ms
total, against 1613 ms on 10-18. Point lookups are equal either way; the
RowFilter-on runs are ~2% faster, mostly on the CoW update paths, within
run-to-run spread. tpce SF1 tables are one small file each, so this run says
little about selective scans on large files; that is the SF10 q6/q19 A/B
below.

**NEXT:** measure the worker scan path the same way. The `sqe-worker`
executor does not use the iceberg reader; it reads parquet directly and
installs its own two-phase `RowFilter` when its late-materialization check
says it pays (`executor.rs`, around line 1613), so this change does not reach
it, and the A/B above ran a single coordinator. Run q1/q2 on
`make test-distributed`'s topology with that `RowFilter` forced off and on.
Then TPC-H SF10 q6/q19 A/B (selective predicates on wide rows), where
late materialization should matter most. If either regresses, gate the
`RowFilter` on the predicate's estimated selectivity from row-group stats
instead of a flag.
