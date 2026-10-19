---
title: Performance - SQE vs Trino 481
description: SQE is faster than Trino 481 in all eight benchmark suites at SF1 and six of seven at SF10, every query differentially validated against Trino with DuckDB's dbgen/dsdgen as an independent data oracle.
---

# Eight of eight suites. Faster than Trino.

Differentially validated against Trino 481, with DuckDB's official dbgen/dsdgen as an independent data oracle.

## Where we stand: ahead at SF1, and at SF10 too

Faster than Trino at SF1 across all eight industry suites (10 October 2026 sweep), and on six of seven at SF10 (9 July 2026, the latest SF10 run).

The SF10 crossover is closed. SSB, the suite we trailed longest, is now at parity at SF10 after shipping build-side key sets to workers. TPC-H and TPC-DS lead at SF10.

## Benchmark note

One exception, shown rather than hidden: the bank suite at SF10, where spill degraded under an 8 GB pool in the July run; the October spill fixes have not been measured at SF10 yet. TPC-C's sub-second reads, which trailed at SF1 in July, are now 4.9x faster than Trino. Full per-query method plus the cross-scale charts: docs, or run it yourself via the benchmark quickstart.
