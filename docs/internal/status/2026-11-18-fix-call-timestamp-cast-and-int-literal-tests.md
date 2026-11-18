**STATUS 2026-11-18: last `aikido-test-rust` failures on main fixed (`fix/call-timestamp-cast-and-int-literal-tests`).**

!1851 fixed the two sqe-catalog failures, but CI stops at the first failing test binary. A local `cargo test --workspace --no-fail-fast` found three more on main:

- `embedded_call_expire_snapshots_keeps_the_last` (sqe-cli): the Trino rewrite turns `TIMESTAMP '...'` into `CAST('...' AS TIMESTAMP(0))`, and the CALL parser (`sqe-sql/src/procedures.rs` `expect_timestamp`) refused it. A real bug for `older_than => TIMESTAMP '...'`; the parser now reads a timestamp cast of a literal like the literal, and still refuses a cast of anything else. New unit test `parses_older_than_as_a_rewritten_timestamp_cast`.
- `flight_sql_smoke::handshake_then_statement_returns_rows_over_grpc` and `trino_describe_test::describe_output_lists_columns_and_types` (sqe-coordinator it): still expected `1` to be `bigint`; they now expect `integer` (Int32), as Trino.

Every other test binary in the workspace was green.

Gate: `make test-integration` was cut off by a session end before it finished and was not rerun (no failure in the partial log); the five tests above pass; clippy (sqe-sql, sqe-coordinator, all targets) 0.

**NEXT:** split `crates/sqe-sql/src/trino_compat.rs` back under the 3000-line default instead of keeping its 3500 budget.
