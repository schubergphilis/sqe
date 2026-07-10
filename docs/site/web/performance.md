---
title: Performance - SQE vs Trino 465
description: SQE wins six of seven benchmark suites against Trino 465 at SF1, with 222/222 queries differentially validated. Decisive on smaller scales, closing fast at SF10+.
---

# Six of seven suites. Faster than Trino.

Differentially validated against Trino, with DuckDB's official dsdgen as an independent data oracle.

## Where we stand: Decisive up to SF1, closing fast at SF10+

Decisive wins up to SF1, across six of seven industry suites.

SF10+ (distributed) is closing fast: on a forced-distribution rig, TPC-DS SF1 went from 4.4x slower than Trino to 1.7x faster after the dynamic-filter pushdown fix. SSB is next, shipping build-side key sets (bloom filters) to workers.

## Benchmark note

SSB is the one suite we still trail. Lineorder's uniform foreign-key distribution defeats row-group pruning, so the runtime filter only helps at row level. We show it honestly. Full per-query method plus the cross-scale charts: docs, or run it yourself via the benchmark quickstart.
