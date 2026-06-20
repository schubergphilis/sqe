# Star Schema Benchmark

13 queries derived from TPC-H, restructured around a single fact (`lineorder`) joined against four dimensions (`customer`, `supplier`, `date`, `part`). The shape that data warehouses are usually optimised for.

SSB was the first benchmark to break when SQE's runtime-filter work shipped because of an ordering bug between SortPreservingMerge and SortExec; the chart shows the dip + recovery. It was also the first to confirm the Path B+B-2 work landed.

## Cross-scale

![SSB cross-scale](./charts/ssb-cross-scale.png)

## SF0.1

![SSB SF0.1 total](./charts/ssb-sf0.1-total.png)

![SSB SF0.1 per-query](./charts/ssb-sf0.1-per-query.png)

![SSB SF0.1 pass](./charts/ssb-sf0.1-pass.png)

## SF1

13/13 pass since late March. The total duration line shows the runtime-filter improvement landing in mid-April.

![SSB SF1 total](./charts/ssb-sf1-total.png)

![SSB SF1 per-query](./charts/ssb-sf1-per-query.png)

![SSB SF1 pass](./charts/ssb-sf1-pass.png)

## SF10

Four runs. The headline 1.4x speedup vs Trino on this scale is the result of dynamic-filter pushdown plus star-schema join reordering.

![SSB SF10 total](./charts/ssb-sf10-total.png)

![SSB SF10 per-query](./charts/ssb-sf10-per-query.png)

![SSB SF10 pass](./charts/ssb-sf10-pass.png)

## Implementation references

- Queries: `crates/sqe-bench/queries/ssb/`
- Star-schema reorder: `crates/sqe-planner/src/star_schema_reorder.rs`
- Runtime filter: `docs/features/runtime-filter-pushdown.md`
- Runner: `scripts/benchmark-test.sh ssb`
