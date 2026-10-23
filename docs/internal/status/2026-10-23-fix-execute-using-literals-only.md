**STATUS 2026-10-23: `EXECUTE ... USING` binds literals only; negative values no longer splice into `--` (`fix/execute-using-literals-only`, AI Portal request §1 / portal #224, part).**

What changed:

- `sqe_core::substitute_placeholders` parenthesizes a literal that starts
  with `-`. `1-?` bound to `-5` read `1--5`, a line comment: on the Flight SQL
  prepared-DML path (`render_bound_sql`) that could drop the predicate after
  the placeholder on the same line. Both text binders share the function.
- `sqe_trino_compat::prepared::bind_execute` refuses any `USING` argument
  that is not a literal (number, single-quoted string, boolean, NULL, hex
  string, typed string, interval over a literal, signed number), naming the
  parameter position. Before, any expression (function call, column,
  subquery) was rendered back to text and ran as part of the statement.
- Flight SQL `arrow_value_to_sql_literal` binds NaN / inf / -inf as
  `CAST('...' AS DOUBLE|REAL)`; the bare word read as a column name.

Tests:

- `cargo test -p sqe-core --lib sql_params` -> 14 passed
  (`substitute_placeholders_parenthesizes_negative_literals`)
- `cargo test -p sqe-trino-compat --lib prepared` -> 15 passed
  (`bind_keeps_an_injection_string_a_single_literal`,
  `bind_negative_number_does_not_open_a_comment`, `bind_long_string_round_trips`,
  `bind_accepts_typed_and_signed_literals`, `bind_refuses_non_literal_expressions`)
- `cargo test -p sqe-trino-compat --test wire` -> 15 passed
  (`execute_using_binds_values_not_sql`: `x'; DROP TABLE t; --`, `-5`, a
  64 KiB string each come back as one row; `1-?` with `-5` is 6;
  `lower('A')` and `(SELECT 1)` refused as parameter 1)
- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite --lib -- bound_float_literals`
  -> 1 passed (renders and runs through DataFusion)
- clippy `-p sqe-core -p sqe-trino-compat -p sqe-coordinator --all-targets -D warnings` clean

Left open (the rest of portal §1): binding as `ScalarValue`s through
`LogicalPlan::with_param_values` for `EXECUTE ... USING` and for Flight SQL
prepared DML / the expired-credentials re-plan, so values never reach a
tokenizer and stay out of the query log and lineage. That reverses the #656
decision to keep `EXECUTE ... USING` as text substitution and needs the
owner's go. Named markers (`:name`) are not supported.

**NEXT:** unchanged
