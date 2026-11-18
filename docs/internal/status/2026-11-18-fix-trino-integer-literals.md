**STATUS 2026-11-18: integer literals are `integer`, and narrow integer arithmetic fails on overflow, as in Trino (`fix/trino-integer-literals`, openspec trino-sql-parity 7.1, 7.2).**

`IntegerLiteralRule` (sqe-catalog `session_tuning/integer_literals.rs`) runs before type coercion. It narrows `Int64` literals that fit in `-2147483647..=2147483647` to `Int32` in expressions, VALUES rows and UNION inputs, and retypes a Union to its coerced input types. LIMIT/OFFSET, ordinals and extension nodes are left alone. A plan that holds a `RecursiveQuery` keeps `bigint` literals, because the work table is typed when the SQL is planned.

DataFusion integer operators wrap, and there is no setting to make them check. `CheckedIntegerArithmeticRule` runs after coercion and routes `tinyint`, `smallint` and `integer` `+ - *` through arrow's checked kernels (`__sqe_checked_add` and friends). `2147483647 + 1` now fails with `integer addition overflow`. Before the change it was a correct `bigint`; with narrowing and no check it would have wrapped.

A lambda variable keeps the field it was resolved to at SQL planning, so `array_filter(make_array(1, 2, 3), x -> x > 1)` failed physical planning (`Int64 != Int32`) once the array narrowed. When a narrowed plan holds a lambda, the rule recomputes each node's schema bottom up and resolves its lambda variables again, as DataFusion's `resolve_lambda_variables` doc asks.

Side fixes: `top_row_number` matched the window expression only bare, and the analyzer now aliases it, so the rewrite looks through `Expr::Alias`. The decimal pin in `decimal_arithmetic.rs` moved to `decimal(22,10)`, which is Trino's answer.

Limits:
- `bigint` arithmetic still wraps, as before. Checking it touches every TPC-H plan and is a separate change.
- Unary minus is not checked (`-x` on `integer` -2147483648).
- `-2147483648` as a literal stays `bigint`, which matches Trino: its parser reads the minus as a unary negation of the literal `2147483648`, and that literal is outside the `integer` range.
- 7.3 (access-control suite and a TPC-H compare before and after) is open.

**NEXT:** openspec trino-sql-parity 7.3: `make test-access-control` and a TPC-H compare on this branch against main; then checked `bigint` arithmetic.
