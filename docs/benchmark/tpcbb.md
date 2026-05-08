# TPC-BB (BigBench)

10 queries from the BigBench mixed-workload benchmark. A blend of structured analytics, semi-structured (JSON), and unstructured (text) operations against a retail data warehouse schema.

The 5.5x speedup vs Trino at SF1 reflects the structured-query subset; the JSON queries are where the gap is largest because Trino's JSON connector materialises strings that DataFusion can scan as columnar.

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

Two runs.

![TPC-BB SF10 total](./charts/tpcbb-sf10-total.png)

![TPC-BB SF10 per-query](./charts/tpcbb-sf10-per-query.png)

![TPC-BB SF10 pass](./charts/tpcbb-sf10-pass.png)

## Implementation references

- Queries: `crates/sqe-bench/queries/tpcbb/`
- JSON functions: `crates/sqe-trino-functions/src/trino_functions.rs` and `datafusion-functions-json`
- Runner: `scripts/benchmark-test.sh tpcbb`
