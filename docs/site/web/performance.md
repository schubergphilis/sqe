---
title: Performance - SQE vs Trino 481, DuckDB, Polars and Spark
description: SQE is faster than Trino 481 in all eight benchmark suites at SF1 and in three of four at SF10, every answer checked against Trino. On 150M rows under a 4.5 GiB cap it finishes every run with a lower geomean than DuckDB and Spark, where Polars is killed on the window query.
---

# Eight of eight suites at SF1. Faster than Trino.

Every answer is checked against Trino 481. Since 4 November the check compares every value, not only the row count, on data generated to the TPC specs and checked against their official row counts.

## Where we stand against Trino

Faster than Trino at SF1 across all eight industry suites (2 and 4 November 2026). At SF10, rerun on 4 November on the fixed data generator, SQE leads on three of four suites: TPC-H 1.3x, TPC-DS 1.8x and TPCx-BB 4.3x, by total time.

SSB is the exception, shown rather than hidden. At SF10 it takes SQE 27.9 s against Trino's 11.8 s. It was at parity in July, so SSB lost ground, and its trace is next. TPC-DS matches Trino on 91 of 99 queries at SF10. Six of the eight differences are decimal digits: Trino keeps the input's scale for `avg` and carries fewer digits in some quotients.

## Against DuckDB, Polars and Spark

On the public engine-bench workload, 150M rows under a 4.5 GiB kill threshold, SQE on Parquet has the lowest geomean: 1.79 s against DuckDB's 2.77 s, Polars killed on the window query and Spark at 16.55 s. Every engine's answer matches DuckDB's. All three SQE modes finish every run under the cap; on Parquet SQE peaks at 2.1 GiB. With every engine reading from S3, SQE over Polaris is no slower than DuckDB on any query.

## Benchmark note

The SF10 runs are from one laptop, with SQE native and Trino in Docker under a 10 GB cap. Treat them as one data point, not a final result. Bank, ClickBench, TPC-C and TPC-E have not been rerun at SF10 since July, when they ran on a dedicated 8-core host. Full per-query method plus the cross-scale charts: docs, or run it yourself via the benchmark quickstart.
