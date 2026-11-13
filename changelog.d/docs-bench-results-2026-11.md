### Added

- Benchmark reports from 2026-11-05 and 2026-11-10 committed under `benchmarks/results/`

    Eight run reports that had stayed uncommitted: TPC-DS SF0.1, SF1 and SF10 over Flight SQL (99/99 each) with the Trino comparisons at SF0.1 (90 matched, 9 expected empty) and SF1 (99/99), ClickBench SF100 (43/43), and two early TPC-H SF100 runs that have errors (q18 and q21 hit the 60 s timeout; the second run also lost q07 and q09 to "Too many open files"). The TPC-H SF100 runs from the same day that are already committed passed 22/22. No code change.
