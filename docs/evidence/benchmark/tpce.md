# TPC-E

11 queries from the financial-trading workload. Heavy on date-range filters, point lookups against trade-history tables, and aggregations across many small dimension tables.

The latest SF1 run (2026-06-12) is 9.3s vs Trino 465's 172.0s, an 18.5x speedup. It is the largest gap of any of the seven suites. Most of the gap is Trino's planning overhead per query: TPC-E's per-query latency targets are short, and Trino spends a non-trivial chunk of the run wall-time on plan compilation. TPC-E is also the suite that historically needed the most work, joining across 33 tables with IN-subquery patterns DataFusion cannot always decorrelate.

## Cross-scale

![TPC-E cross-scale](./charts/tpce-cross-scale.png)

## SF0.1

The 166-run history at SF0.1 is the result of TPC-E being the smoke-test suite during the auth chain rewrite in early April. Each commit on the auth branch ran the full SF0.1 sweep before merging. The chart shows the resulting density.

![TPC-E SF0.1 total](./charts/tpce-sf0.1-total.png)

![TPC-E SF0.1 per-query](./charts/tpce-sf0.1-per-query.png)

![TPC-E SF0.1 pass](./charts/tpce-sf0.1-pass.png)

## SF1

The headline scale. 11/11 since late March.

![TPC-E SF1 total](./charts/tpce-sf1-total.png)

![TPC-E SF1 per-query](./charts/tpce-sf1-per-query.png)

![TPC-E SF1 pass](./charts/tpce-sf1-pass.png)

## SF10

Four runs.

![TPC-E SF10 total](./charts/tpce-sf10-total.png)

![TPC-E SF10 per-query](./charts/tpce-sf10-per-query.png)

![TPC-E SF10 pass](./charts/tpce-sf10-pass.png)

## Implementation references

- Queries: `crates/sqe-bench/queries/tpce/`
- Loader: `scripts/benchmark-load.sh`
- Runner: `scripts/benchmark-test.sh tpce`
