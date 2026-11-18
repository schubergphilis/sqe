**STATUS 2026-11-18: `bigint` arithmetic and unary minus fail on overflow, as in Trino (`fix/trino-checked-arithmetic`, openspec trino-sql-parity 7.3, 7.4).**

`CheckedIntegerArithmeticRule` (sqe-catalog `session_tuning/integer_literals.rs`) now covers `bigint` `+ - *` and unary minus on `tinyint` to `bigint`. Both go through arrow's checked kernels (`add`, `sub`, `mul`, `neg`) behind `__sqe_checked_*` UDFs. Errors use Trino's wording: `bigint addition overflow: 9223372036854775807 + 1`, `integer negation overflow: -2147483648`.

A negative literal is folded by the SQL planner, so `-2147483648` stays a `bigint` literal and is never negated at run time. Trino types it `bigint` too: its parser reads a unary minus on the literal `2147483648`, which is outside the `integer` range (`AstBuilder.visitArithmeticUnary`, `ExpressionAnalyzer.visitLongLiteral`).

Cost: the rewrite turns every integer `+ - *` into a scalar UDF, so DataFusion's `BinaryExpr` fast paths, ordering properties and interval analysis no longer see them. TPC-H arithmetic is mostly `decimal`, which the rule leaves alone.

TPC-H compare (7.3), SF10 single node, `PROFILE=release`, all 22 queries passing in every run, run in the order AFTER, BEFORE, AFTER:

| Run | Commit | Total |
|---|---|---|
| AFTER#1 | this branch | 94.2 s |
| BEFORE | main 3673c0359 | 65.6 s |
| AFTER#2 | this branch | 63.4 s |

AFTER#1 was the first run on a cold machine and is about 40% slower than AFTER#2 on the same binary, so it is left out. AFTER#2 against BEFORE is 0.97 on the total; q03 (1.10) and q19 (1.17) are the only queries over 1.10, within the run-to-run spread. No measurable cost. Reports: `benchmarks/results/tpch-sf10-flight-2026-11-18T{17:29:35,17:55:15,18:05:35}.json`.

Gate: `cargo test -p sqe-catalog --lib` 848 passed; `cargo test -p sqe-coordinator --lib` 851 passed; `make test-integration` 419 passed, 0 failed (on 8cd6f7583, before the rebase onto main).

**NEXT:** openspec trino-sql-parity: probe the overflow cases this branch does not cover (`sum(bigint)`, `abs` of the smallest value, `/` and `%` by `-1`).
