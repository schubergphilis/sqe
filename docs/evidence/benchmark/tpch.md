# TPC-H

22-query decision-support benchmark. The most widely benchmarked SQL workload; SQE's primary comparison target against Trino.

The canonical SF1 headline is 16.8s vs Trino 465's 26.7s, a 1.6x result (see the [book Results table](../../site/book/src/features/benchmarks.md#results-sf1-vs-trino-465)). An earlier May 2026 run sat at 19.3s vs 26.6s on the same machine and storage; the path here was not linear. q01 / q15 swung up and down as the planner learned and unlearned bits of statistics, and the runtime-filter pushdown work in mid-April moved q06 / q07 / q14 from the warm orange zone to the pale yellow you see in the late-April part of the heatmap.

## Cross-scale

![TPC-H cross-scale](./charts/tpch-cross-scale.png)

## SF0.1

Small. Useful as a smoke test before the real benchmarks. Variance dominates the trendline.

![TPC-H SF0.1 total](./charts/tpch-sf0.1-total.png)

![TPC-H SF0.1 per-query](./charts/tpch-sf0.1-per-query.png)

![TPC-H SF0.1 pass](./charts/tpch-sf0.1-pass.png)

## SF1

The headline scale. Same data the README table uses. The big April 9-10 rise is a known regression that the runtime-filter work shipped a week later resolved.

![TPC-H SF1 total](./charts/tpch-sf1-total.png)

![TPC-H SF1 per-query](./charts/tpch-sf1-per-query.png)

![TPC-H SF1 pass](./charts/tpch-sf1-pass.png)

## SF10

The stress scale. Five runs on this scale to date. Q06 / q07 / q14 dominate the per-query view.

![TPC-H SF10 total](./charts/tpch-sf10-total.png)

![TPC-H SF10 per-query](./charts/tpch-sf10-per-query.png)

![TPC-H SF10 pass](./charts/tpch-sf10-pass.png)

## Implementation references

- Queries: `crates/sqe-bench/queries/tpch/`
- Loader: `scripts/benchmark-load.sh`
- Runner: `scripts/benchmark-test.sh tpch`
- Trino comparison: `scripts/benchmark-test.sh --compare-trino tpch` writes `compare-tpch-sf*.json` alongside the SQE run.
