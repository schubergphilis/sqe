---
title: Performance - SQE vs Trino 481
description: SQE is faster than Trino 481 in seven of eight benchmark suites at SF1 and six of seven at SF10, every query differentially validated against Trino with DuckDB's dbgen/dsdgen as an independent data oracle.
---

# Seven of eight suites. Faster than Trino.

Differentially validated against Trino 481, with DuckDB's official dbgen/dsdgen as an independent data oracle.

## Where we stand: ahead at SF1, and at SF10 too

Faster than Trino at SF1 across seven of eight industry suites, and ahead at SF10 as of the latest run.

The SF10 crossover is closed. SSB, the suite we trailed longest, is now at parity at SF10 after shipping build-side key sets to workers. TPC-H and TPC-DS lead at SF10.

## Benchmark note

Two exceptions, shown rather than hidden: TPC-C's sub-second OLTP reads at SF1, where fixed dispatch overhead dominates, and the bank suite at SF10, where spill still degrades under an 8 GB pool. Full per-query method plus the cross-scale charts: docs, or run it yourself via the benchmark quickstart.
