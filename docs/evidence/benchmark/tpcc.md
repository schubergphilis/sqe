# TPC-C (read-only subset)

Eight read queries derived from the TPC-C OLTP transactions. SQE does not run the full OLTP benchmark (no row-level transactions, no order-line latency targets); we run the SELECTs that the transactions perform after their writes commit.

The latest SF1 run (2026-06-12) is 0.41s vs Trino 465's 2.65s, a 6.5x speedup. The gap reflects how well DataFusion's vectorised scan outperforms Trino's Hive connector on point lookups against small Iceberg tables.

## Cross-scale

![TPC-C cross-scale](./charts/tpcc-cross-scale.png)

## SF0.1

![TPC-C SF0.1 total](./charts/tpcc-sf0.1-total.png)

![TPC-C SF0.1 per-query](./charts/tpcc-sf0.1-per-query.png)

![TPC-C SF0.1 pass](./charts/tpcc-sf0.1-pass.png)

## SF1

8/8 pass throughout. Total run is sub-second on most days.

![TPC-C SF1 total](./charts/tpcc-sf1-total.png)

![TPC-C SF1 per-query](./charts/tpcc-sf1-per-query.png)

![TPC-C SF1 pass](./charts/tpcc-sf1-pass.png)

## SF10

Three runs to date.

![TPC-C SF10 total](./charts/tpcc-sf10-total.png)

![TPC-C SF10 per-query](./charts/tpcc-sf10-per-query.png)

![TPC-C SF10 pass](./charts/tpcc-sf10-pass.png)

## Implementation references

- Queries: `crates/sqe-bench/queries/tpcc/`
- Loader: `scripts/benchmark-load.sh`
- Runner: `scripts/benchmark-test.sh tpcc`
