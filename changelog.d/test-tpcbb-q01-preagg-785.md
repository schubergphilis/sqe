### Changed

- The TPCBB q01 pre-aggregation prototype matches the original query exactly, including its output types (#785)

    The candidate in `benchmarks/experiments/` now casts its measures to the original's types (`DECIMAL(17,2)` sum, `DECIMAL(11,6)` average). Its fixture has the edge cases the rewrite must keep: `DECIMAL(7,2)` amounts with NULLs and refunds, an item whose amounts are all NULL, NULL and dangling item keys, items with clicks but no sales, and item revisions that share an `i_item_id`. The harness compares every group exactly, with explicit NULL flags and the output types, instead of within a float tolerance.
