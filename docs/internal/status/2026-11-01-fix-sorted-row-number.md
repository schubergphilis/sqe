**STATUS 2026-11-01: `ROW_NUMBER()` over sorted input computed in one vectorised pass (`fix/sorted-row-number`).**

The engine-bench "latest row per customer" query (q3, 150M rows, 15M
customers, 3 GB) spent 38.7 s of CPU in DataFusion's `BoundedWindowAggExec`,
which keeps one state per window partition in an `IndexMap` keyed by a
`Vec<ScalarValue>`. `SortedRowNumberRule` (physical rule, registered in
`sqe_catalog::engine_context`, so both the coordinator and `sqe-cli
--embedded`) replaces a `BoundedWindowAggExec` whose every expression is
`row_number()` over one non-empty `PARTITION BY`, in `InputOrderMode::Sorted`,
with `SortedRowNumberExec`: partition boundaries from
`arrow::compute::partition`, a counter carried across batches. Planning
answers (properties, input ordering and distribution, statistics) come from
the wrapped window node. Measured with one release binary, kill switch on vs
off, two runs each: window CPU 38-40 s -> 0.4-0.5 s, process CPU 80 s -> 50 s,
wall 11.4-11.8 s -> 7.6-8.9 s (host under load; DuckDB 8.4 s on the same
data). The remaining q3 cost is the sort (`SortExec` ~42-50 s CPU, 66
spills).
`SQE_SORTED_ROW_NUMBER=off` restores DataFusion's exec.

**NEXT:** iceberg scan admissions starve sorts under a per-query quota (Polaris
q3 `ResourcesExhausted` at 3 GB); sort spill share diluted by RepartitionExec
consumers in `FairSpillPool`.
