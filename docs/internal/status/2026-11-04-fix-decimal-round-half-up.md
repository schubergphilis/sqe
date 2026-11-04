**STATUS 2026-11-04: decimal division, `avg` and decimal casts round half away from zero (`fix/decimal-round-half-up`, !1645, #785).**

DataFusion truncated all three (`2.00/3.00` = 0.666666, `avg(0.01,0.01,0.00)` = 0.006666, `CAST(7.5 AS INT)` = 7). Trino and PostgreSQL round. New `crates/sqe-catalog/src/session_tuning/decimal_rounding.rs`, wired into `engine_context`:

- `DecimalRoundingRule` (analyzer, after type coercion):
  - Decimal `/` gets one guard digit on the dividend (`Decimal256` when the `Decimal128` guard could overflow) and rounds back to the planned type.
  - Scale-reducing decimal casts and decimal-to-integer `CAST`/`TRY_CAST` round.
  - Names and types are preserved, and the rule is idempotent.
- `__sqe_rescale` UDF: an arrow cast (half away from zero) that the optimizer keeps. It is also registered on the worker and in `worker_task_ctx`.
- `RoundedAvg` replaces `avg`/`mean`. It keeps DataFusion's count+sum state and rounds the final division in i256.
- Also fixed: `unwrap_cast_in_comparison` dropped a scale-reducing cast in a filter (`CAST(x AS DECIMAL(10,2)) = 1.23` missed 1.225).
- Float-to-integer casts still truncate.

Tests:

- 7 unit tests in `decimal_rounding::tests`.
- `cargo test`: sqe-catalog 765, sqe-coordinator 795 (1 ignored), sqe-worker 153, all passing.
- Clippy `-D warnings` is clean on the three crates and in the workspace pre-push run.

Gate: `BENCH_SCALE=1 ./scripts/benchmark-test.sh --compare-trino tpcds tpcbb` (release), exit 0.

- TPC-DS 99/99 pass, matched 90/99 against Trino (87/99 before). q31, q36, q54, q59, q83 and q90 cleared. SQE 17.2 s vs Trino 61.1 s (19.9 s before). `tpcds-sf1-flight-2026-11-04T11:56:40.json`, `compare-tpcds-sf1-2026-11-04T11:57:59.json`.
- TPCx-BB 10/10 pass, matched 10/10 (q09 cleared). SQE 12.8 s vs Trino 60.2 s (15.2 s before). `tpcbb-sf1-flight-2026-11-04T11:58:39.json`, `compare-tpcbb-sf1-2026-11-04T11:59:52.json`.

The 9 remaining TPC-DS value diffs:

- **Result scale, not rounding (q06, q35, q44, q58, q61, q78).** Trino's `avg(decimal(p,s))` keeps scale s, and its quotients carry a different number of digits. This needs a decision on whether SQE should follow Trino's result types.
- **Comparator double rounding (q12, q20, q98).** `revenueratio` (`sum * 100.0 / sum(sum) OVER`) is planned at `Decimal128(28,7)` in SQE and `decimal(38,6)` in Trino.
  - Checked against Trino (exact inputs in `decimal(38,2)`): Trino's value is exact half-up on all 2,429 q12 rows. SQE's is exact half-up at scale 7 (1493.54 * 100 / 123791.39 = 1.20649747934..., SQE 1.2064975, Trino 1.206497).
  - `sqe-bench`'s `values.rs` rounds the scale-7 value half-up to scale 6, which gives 1.206498. Before this change SQE truncated to 1.2064974, which happened to match.
  - Fix: treat two rounded values as equal when `|a - b| <= 0.5*10^-sa + 0.5*10^-sb`.

**NEXT:** the comparator fix on its own branch (`crates/sqe-bench/src/comparison/values.rs`, a rounding band instead of re-rounding). Then decide whether SQE follows Trino's result scale (q06/q35/q44/q58/q61/q78), the TPCx-BB q02 generator at SF10 and the q01 timeout header, and trace TPC-DS q65 and q27/q75 at SF10.
