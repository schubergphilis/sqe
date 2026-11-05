**STATUS 2026-11-05: TPC-DS compare against Trino sorts deterministically and dumps its diffs (`fix/bench-tpcds-tiebreaks`).**

- q35 sorts on all six group keys, q75 on `sales_cnt_diff, sales_amt_diff` (both as in the TPC-DS spec). q73 adds `ss_ticket_number` after the spec's keys: one customer holds two tickets with the same `cnt` (1068926 and 1443550 at SF10), and the LIMIT cut between them.
- `SQE_BENCH_DUMP_DIFFS=<dir>` (`crates/sqe-bench/src/comparison/dump.rs`) writes `<suite>-sf<scale>-<query>.{sql,sqe.txt,trino.txt}` for every row or value diff.
- Commits the TPC-H, TPC-DS, SSB and TPC-BB result JSONs from 2026-11-04/05.

At SF10 after the q27 rollup fix and these ORDER BYs: 94/99 match; q06 and q44 waited on the `avg` type (!1656), q58, q61 and q78 on the decimal arithmetic types (!1657). Both are merged.

**NEXT:** rerun the TPC-DS compare at SF0.1 and SF10 on main to confirm q06, q44, q58, q61 and q78.
