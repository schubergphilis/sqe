**STATUS 2026-11-07: SQL pipe syntax (`|>`) parses and plans (`feat/sql-pipe-syntax-851`, #851).**

`FROM orders |> WHERE ... |> AGGREGATE ... GROUP BY ... |> WHERE ... |> ORDER BY ...` failed at
the front door: the #683 tokenization check compares the Generic token stream with the execution
dialect's, and DuckDB (the session's named dialect) reads `|>` as `|` then `>`. DataFusion 55
already plans pipe operators; only the tokenizer was missing.

- `sqe_sql::ExecutionDialect` (`front_door.rs`): DuckDB's sqlparser overrides plus
  `supports_pipe_operator`, reporting DuckDB's `TypeId` so `dialect_of!` checks are unchanged.
  The delegation list mirrors sqlparser 0.62's `DuckDbDialect`; recheck it on a sqlparser bump.
- `token_scan::reject_dialect_divergence` compares against it.
- `file_tvf_prepare`: `plan_sql`, `plan_sql_without_policy`, `plan_parsed` and
  `create_logical_plan` parse with it (`parse_and_plan`, same recursion limit and
  single-statement rule as DataFusion's `sql_to_statement`) instead of `ctx.sql` /
  `state.create_logical_plan`.
- Docs: `dml.md` "Pipe syntax" section; the FROM-first notes in `sql-reference/index.md`,
  `limitations.md` and `sql-cheatsheet.md` point at it.

Tests: `sqe-sql` `front_door::tests::pipe_syntax_passes_the_front_door`,
`execution_dialect_parses_like_duckdb` (DuckDB syntax SQE relies on gives the same AST, `|`
stays bitwise OR); `sqe-catalog` `file_tvf_prepare::tests::pipe_query_matches_its_standard_sql_form`
(the issue's query and its standard form return the same rows through `plan_sql` and
`create_logical_plan`; `|> DROP` says "not supported"; two statements stay refused).
Integration: `sql_compat_test::test_sql_compat_07_pipe_syntax` (`tests/sql/07_pipe_syntax.sql`,
through `QueryHandler::execute`). Gate: `make test-integration FILTER=test_sql_compat_07` -> 1 passed.

Left open: `SET`, `DROP`, `RENAME`, `TABLESAMPLE`, `CALL`, `PIVOT`, `UNPIVOT` steps are not
planned by DataFusion 55 (clear error). Direct `ctx.sql` calls in tests and the coordinator's
`create_logical_plan` test helpers still use the session's DuckDB parser; they never see client
text.

**NEXT:** the remaining P3 code issues (#878 lateral column aliases first) and the
refactor work packages #637, #636, #589 (#659 umbrella); #576 after those merge.
