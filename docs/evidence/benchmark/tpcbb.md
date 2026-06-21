# TPC-BB (BigBench)

10 queries from the BigBench mixed-workload benchmark. A blend of structured analytics, semi-structured (JSON), and unstructured (text) operations against a retail data warehouse schema.

The latest SF1 run (2026-06-12) is 28.0s vs Trino 465's 255.7s, a 9.1x speedup, the second-largest gap after TPC-E. The structured-query subset drives most of it; the JSON queries widen the gap further because Trino's JSON connector materialises strings that DataFusion can scan as columnar.

## Cross-scale

![TPC-BB cross-scale](./charts/tpcbb-cross-scale.png)

## SF0.1

![TPC-BB SF0.1 total](./charts/tpcbb-sf0.1-total.png)

![TPC-BB SF0.1 per-query](./charts/tpcbb-sf0.1-per-query.png)

![TPC-BB SF0.1 pass](./charts/tpcbb-sf0.1-pass.png)

## SF1

10/10 pass since late March.

![TPC-BB SF1 total](./charts/tpcbb-sf1-total.png)

![TPC-BB SF1 per-query](./charts/tpcbb-sf1-per-query.png)

![TPC-BB SF1 pass](./charts/tpcbb-sf1-pass.png)

## SF10

No clean runs at SF10 yet. The two attempted runs both errored and were dropped from the timeline.

## Implementation references

- Queries: `crates/sqe-bench/queries/tpcbb/`
- JSON functions: `crates/sqe-trino-functions/src/trino_functions.rs` and `datafusion-functions-json`
- Runner: `scripts/benchmark-test.sh tpcbb`
