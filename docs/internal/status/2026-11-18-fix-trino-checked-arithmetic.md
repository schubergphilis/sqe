**STATUS 2026-11-18: `bigint` arithmetic and unary minus fail on overflow, as in Trino (`fix/trino-checked-arithmetic`, openspec trino-sql-parity 7.4; stacked on `fix/trino-integer-literals`).**

`CheckedIntegerArithmeticRule` (sqe-catalog `session_tuning/integer_literals.rs`) now covers `bigint` `+ - *` and unary minus on `tinyint` to `bigint`. Both go through arrow's checked kernels (`add`, `sub`, `mul`, `neg`) behind `__sqe_checked_*` UDFs. Errors use Trino's wording: `bigint addition overflow: 9223372036854775807 + 1`, `integer negation overflow: -2147483648`.

A negative literal is folded by the SQL planner, so `-2147483648` stays a `bigint` literal and is never negated at run time. Trino types it `bigint` too: its parser reads a unary minus on the literal `2147483648`, which is outside the `integer` range (`AstBuilder.visitArithmeticUnary`, `ExpressionAnalyzer.visitLongLiteral`).

Cost: the rewrite turns every integer `+ - *` into a scalar UDF, so DataFusion's `BinaryExpr` fast paths, ordering properties and interval analysis no longer see them. TPC-H arithmetic is mostly `decimal`, which the rule leaves alone. The TPC-H compare (7.3) measures it.

Gate: `cargo test -p sqe-catalog --lib` 848 passed; `cargo test -p sqe-coordinator --lib` 851 passed.

**NEXT:** openspec trino-sql-parity 7.3: TPC-H compare of main against this branch.
