**STATUS 2026-11-04: the Trino comparison checks values (`fix/bench-trino-value-compare`, #785).**

`classify_status` decided on row counts only, so equal counts with different values were `Match`. New `crates/sqe-bench/src/comparison/values.rs`:

- `value_diff(sqe, trino)` sorts both sides' rows by a normalised key and compares cell by cell. The rule per column comes from both Arrow types: integer/decimal on both sides is exact after rounding half-up to the smaller scale; a float on either side is relative 1e-6 (abs floor 1e-9); anything else is the display string. NULL only equals NULL.
- `refine_status` turns `Match` into the new `CompareStatusReport::ValueDiff` when the values differ; every other status stands. `classify_status` keeps its signature.
- `QueryComparison.value_diff` (first differing cell, differing-row count) and `ComparisonSummary.value_diff`, both `serde(default)`. Wired into `comparison::run_comparison` and `suite.rs` (the `sqe-bench run --compare-trino` path), the markdown table ("VALUE DIFF"), the Total line and the `run.rs` summary. `ValueDiff` does not fail the run, like `RowDiff`.

Tests: 8 unit tests in `values::tests` (order independence, decimal 6-vs-2 scale, a wrong cent, int vs decimal, float tolerance, NULL, column count, rounding). `cargo test -p sqe-bench` 245 + 41 pass; clippy `-D warnings` clean.

Gate: `BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpcds tpcbb` exit 0 (on main's generator, so q64 is still the known `VacuousBug`; !1640 fixes it). Results: `compare-tpcds-sf1-2026-11-04T10:23:50.json`, `compare-tpcbb-sf1-2026-11-04T10:26:02.json`.

- TPC-DS SF1: matched 86/99, **12 ValueDiff**, 1 VacuousBug (q64). SQE 21.2 s vs Trino 80.4 s.
- TPCx-BB SF1: matched 9/10, **1 ValueDiff** (q09). SQE 18.4 s vs Trino 70.6 s.

The flagged queries, by cause (from the first differing cell; not yet traced one by one):

- Last digit, DataFusion truncates decimal division where Trino rounds: q31 (1.285127 vs 1.285128), q36, q59, q83 (20.6666 vs 20.6667), q90, TPCx-BB q09 (`ROUND(.., 2)` of a truncated quotient: 3.92 vs 3.93).
- `CAST(decimal AS INTEGER)` truncates in DataFusion, rounds in Trino: q54 (`segment` 4602 vs 4603).
- Different intermediate scale of a decimal ratio: q58 and q78 (`ss_dev` 11.112967 vs Trino 11.113000: Trino's quotient carries fewer digits), q61 (88.77240300 vs 88.77240368: SQE's carries fewer).
- A different value that changes which rows qualify or rank: q06 (`cnt` 213 vs 214, the `1.2 * avg(i_current_price)` threshold: Trino's `avg(decimal(7,2))` is rounded to cents), q44 (all 10 item names differ; ranks over `avg(ss_net_profit)`), q35 (`avg1` 1 vs 2 on 3 rows).

**NEXT:** decide SQE's decimal rounding (Trino semantics: round half-up on division, `avg` and casts) and trace q06/q35/q44 under it; then SSB/TPC-E money columns.
