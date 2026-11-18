**STATUS 2026-11-18: JSON arrow on a bare column no longer parses as a lambda (#913, `fix/json-arrow-lambda-misparse`).**

A dbt model failed with "Unsupported ast node in sqltorel: Lambda" on `payload -> 'session' -> 'device' ->> 'type'`. Since !649 the planner parses with the DuckDB dialect, and sqlparser 0.62 reads a bare word followed by `->` as a lambda in any position.

`parse_and_plan` (`sqe-catalog/src/file_tvf_prepare.rs`, the one planner parse entry, which views also use) now parses with `ArrowExecutionDialect` (the execution dialect with lambdas off, from the same macro in `sqe-sql/src/front_door.rs`), then `sqe_sql::promote_lambda_arguments` turns `x -> R` back into a lambda in the second and later arguments of a function in `SessionState::higher_order_functions()`. A function argument is a whole expression, so the promotion is exact: the `x -> R` at the bottom of the argument's left spine is replaced by `R`, and the argument becomes the body. If the lambda-off parse fails (`lambda x:` form, typed parameter), the old lambda-on parse runs.

Unit tests compare the promoted AST with the GenericDialect parse (JSON arrows, incl. `AND ... OR`, `NOT`, `IS NULL`, `CASE`, first argument of a HOF) and with the DuckDB-dialect parse (lambdas, incl. constant, two-parameter, nested, JSON inside the body).

Not covered: policy expressions go through `SessionContext::parse_sql_expr`, which uses the session dialect name and still reads `col -> ...` as a lambda.

**NEXT:** route policy expression parsing (`sqe-policy/src/policy_expr.rs`) through the same lambda-off parse + promotion.
