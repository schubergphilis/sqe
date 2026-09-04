# 406: Flight SQL prepared statements are text substitution over a SQL-string handle; every query is planned twice

- Scope: sqe-coordinator, read side (flight_sql.rs, query_handler.rs get_schema) with sqe-core sql_params.rs
- Severity: medium
- Category: architecture
- Effort: M (days)
- Status: open

## Problem

**Handle = SQL text.** `do_action_create_prepared_statement` plans the query for its schema,
then returns `FetchResults { handle: sql }` as the prepared handle and an empty
`parameter_schema` (`crates/sqe-coordinator/src/flight_sql.rs:2144-2183`, `:2168`,
`:2180`). There is no server-side prepared object: the "handle" is the statement.

**Parameters are spliced as literals.** `do_put_prepared_statement_query` decodes the Arrow
parameter batch into `Vec<String>` SQL literals via `arrow_value_to_sql_literal`
(`flight_sql.rs:2082-2118`, `:2439-2536`), parks them in a moka cache keyed by
`(session_id, handle)`, and `do_get_prepared_statement` splices them into the text with
`substitute_placeholders` (`flight_sql.rs:1743-1752`, `crates/sqe-core/src/sql_params.rs:15-58`).
The substitution scans for `?` outside single/double quotes only; it does not skip `--` or
`/* */` comments, `E'...'` strings, or dollar-quoted strings, so a `?` in a comment consumes a
parameter and shifts every later bind.

**Bind is consumed on first use.** `self.prepared_params.invalidate(&key)` runs before
execution (`flight_sql.rs:1746`). A JDBC client that calls `executeQuery()` twice on the same
`PreparedStatement` without re-binding (legal, and what connection pools do) executes the
second time with bare `?` and gets a planner error.

**Every statement is planned twice.** `get_flight_info_statement` calls `get_schema`
(`flight_sql.rs:1231-1235`), which builds the session context, parses, plans and runs the
policy rewriter (`query_handler.rs:3299-3367`). `do_get` then repeats all of it inside
`execute_stream`. Cache hits on the `SessionContext` make this cheaper than it sounds, but the
Polaris `load_table` and the policy store round trips are paid twice per query, and the
policy summary from the first pass is thrown away.

## Why it matters

- DataFusion already supports typed placeholders (`$1`, `with_param_values`), and
  `number_placeholders` in the same file exists for DESCRIBE INPUT. Text splicing forgoes type
  checking, quoting guarantees and plan reuse for no gain, and its correctness rests on
  `sql_quote_string` and the quote scanner staying perfect.
- Connection-pool double-execute is a real client pattern (Tableau, DBeaver "re-run").
- Planning twice doubles catalog and policy-store load per query, which is the same
  metadata pressure the 2026-07-25 review called out under P1-6.

## Proposed change

1. Introduce a `PreparedStatementStore` (moka, keyed by `(session_id, statement_id: Uuid)`)
   holding the SQL, the planned `LogicalPlan` with `$n` placeholders (via
   `number_placeholders` then `ctx.state().create_logical_plan`), the inferred parameter
   types and the output schema. Return the UUID as the handle and the real
   `parameter_schema`.
2. Bind with `LogicalPlan::with_param_values(ScalarValue...)` decoded straight from the Arrow
   parameter batch; never render literals. Keep the bound values until `close` or TTL so
   re-execution works.
3. Reuse the plan from step 1 in `do_get` so GetFlightInfo/DoGet plan once. The same store
   gives 403 its UUID ticket.
4. Keep `substitute_placeholders` only for the Trino `EXECUTE ... USING` path until that path
   moves too, and make it comment-aware in the meantime.

## Acceptance criteria

- ADBC test: prepare `SELECT * FROM t WHERE id = ?`, bind, execute, execute again without
  re-binding; both return rows.
- `parameter_schema` in `ActionCreatePreparedStatementResult` is non-empty for a query with
  placeholders, and a bind with the wrong Arrow type is rejected before execution.
- A counter of `load_table` calls per prepared execution drops from 2 to 1.
- Unit test: `SELECT 1 -- what?` with zero params does not error.

## Related

- 2026-06-26 review item 10 (Trino `PREPARE`/`EXECUTE` `todo!()`) was fixed by routing
  through `substitute_placeholders`; this issue is the follow-on design debt.
- 403 (query id in ticket).
