**STATUS 2026-11-13: lateral column aliases resolve in pipe `SELECT` and `EXTEND` steps (`feat/lateral-alias-pipe-steps`, follow-up to #878).**

!1681 added lateral column aliases to the `SELECT` list. Pipe steps were not covered: `FROM t |> SELECT a + 1 AS b, b * 2 AS c` failed with an unknown column `b`.

Changed:

- `crates/sqe-sql/src/lateral_alias.rs`: the rewrite now visits every `PipeOperator::Select` and `PipeOperator::Extend` step of a query as well as its body. `inline_in_select` became `inline_in_items` over a `SelectItem` slice so both paths share it. New unit test `pipe_select_and_extend_steps_are_rewritten`.
- `crates/sqe-catalog/src/file_tvf_prepare.rs`: test `lateral_alias_in_a_pipe_step_plans` plans a pipe step with a lateral alias end to end.
- `docs/site/book/src/sql-reference/dml.md`: one line noting that pipe steps resolve lateral aliases too.

Gate: `cargo test -p sqe-sql --lib lateral_alias` (8 passed), `cargo test -p sqe-catalog --lib lateral_alias` (1 passed), `cargo clippy -p sqe-sql -p sqe-catalog --all-targets -D warnings`.

Not run: `make test-integration`.

**NEXT:** none planned. Only `SELECT` and `EXTEND` steps are rewritten; a lateral alias inside a pipe `AGGREGATE` list is not covered and has no test.
