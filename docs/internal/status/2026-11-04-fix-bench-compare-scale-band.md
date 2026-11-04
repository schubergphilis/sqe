**STATUS 2026-11-04: the Trino value comparator stops rounding twice (`fix/bench-compare-scale-band`, #785).**

Found by the !1645 gate (decimal rounding). With SQE rounding half-up, TPC-DS q12, q20 and q98 became `ValueDiff` on `revenueratio` (`sum * 100.0 / sum(sum) OVER`). SQE plans it as `Decimal128(28,7)`; Trino plans `decimal(38,6)`.

I checked it against Trino by hand on SF1, with exact inputs in `decimal(38,2)`:

- Trino's value is exact half-up at scale 6 on all 2,429 q12 rows.
- SQE's is exact half-up at scale 7. For example, 1493.54 * 100 / 123791.39 = 1.20649747934...: SQE gives 1.2064975 and Trino 1.206497.
- `values.rs` re-rounded SQE's 1.2064975 to scale 6 and got 1.206498.

The fix is in `crates/sqe-bench/src/comparison/values.rs`:

- `Cell::Exact` keeps the engine's value and scale next to the sort key, which is still rounded to the column's smaller scale.
- `same_rounded_value` matches two exact cells when `|a - b| < (ua + ub) / 2`. That is the overlap of the two half-open rounding intervals, and at equal scales it is equality.
- On overflow it falls back to the old key equality.
- A diff now shows each engine's own digits: "SQE 1.2064976 vs Trino 1.206497".

Tests: the new `rounded_values_at_different_scales_are_not_rounded_twice` covers:

- q12's 1.2064975 vs 1.206497 matches, and so does its negative mirror.
- 1.2064976 is a diff, and the lower edge sits exactly between 1.2064965 and 1.2064964.
- Equal-scale cents and q58 `ss_dev` are still diffs.

`cargo test -p sqe-bench`: 249 + 41 pass. `cargo clippy -p sqe-bench --all-targets -D warnings` is clean.

Limits:

- A truncating engine at the wider scale still passes against the narrower one (1.2064974 vs 1.206497), just as before. The narrower side cannot tell truncation from rounding.
- Band-matched cells sort by keys one unit apart. Rows tied on every earlier column could pair up out of order, but only when a third row falls between them.
- Not rerun on the bench: this branch is cut from main, which still truncates, so q12 matches either way here. The combined check is the next SF1 `--compare-trino` run once !1645 and this branch are both merged. Expected: q12, q20 and q98 clear, leaving q06, q35, q44, q58, q61 and q78.

**NEXT:** after both merge, run SF1 `--compare-trino tpcds tpcbb` to confirm 93/99. Then decide whether SQE follows Trino's result scale (q06/q35/q44/q58/q61/q78).
