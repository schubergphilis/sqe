**STATUS 2026-10-24: prepared-statement values bind into the plan on every path (`feat/bound-parameters-in-plan`, finding 406 / #586, portal request §1).**

Reverses the #656 decision to keep Trino `EXECUTE ... USING` as text substitution. Every prepared path now plans the statement with `$1..$N` markers and binds typed `ScalarValue`s; no value becomes SQL text.

- **Trino `EXECUTE ... USING`**: `bind_execute` (`sqe-trino-compat/src/prepared.rs`) turns each `USING` literal into a typed value (Trino typing: integer -> BIGINT, `1.50` -> DECIMAL(3,2), `1e0` -> DOUBLE, `DATE`/`TIMESTAMP`/`DECIMAL`/`INTERVAL` typed literals, hex -> VARBINARY) and numbers the template. `ExecPlan::Bound` dispatches to the new `TrinoQueryExecutor::execute_bound_with_query_id`, implemented in sqe-server through `QueryHandler::execute_bound_with_id` / `execute_bound_stream_with_id`.
- **Flight SQL prepared DML/DDL and the expired-credentials re-plan** go through the same bound entry points. `render_bound_sql` and `arrow_value_to_sql_literal` are gone; `required_bind` returns typed values.
- **Mechanism**: the values ride in the task-local `sqe_catalog::txn::StatementScope` (`with_params`); every planner entry in `file_tvf_prepare.rs` binds them via `sqe_core::bind_params`, so a write's derived plans (MERGE source, UPDATE/DELETE filter) bind too, before policy enforcement. DELETE partition pruning reads a `$n` constraint from the bound value. Bound statements skip the result cache.
- **CASE placeholders**: DataFusion does not type a placeholder in a CASE branch, and UPDATE / MERGE assign through `CASE WHEN ... THEN <value> ELSE <column> END`. The gate showed a wrong-type `SET value = $1` failing in an execution-time cast that quoted the value into the error and query log. `bind_params` now types such a placeholder from the ELSE column, so the value is parsed or refused up front ("parameter $1: cannot convert Utf8 to Int64").
- `substitute_placeholders` is removed; its comment/quote tests now cover `number_placeholders`.

Gates:
- `make test-integration FILTER=bound_param` -> 2 passed (`bound_params_e2e::bound_params_drive_every_write_path`: VALUES with a JDBC-style string for BIGINT, INSERT...SELECT, UPDATE, DELETE with partition pruning, MERGE with `$1` in ON, CTAS, SELECT, an injection string as a value, query log holds markers; `bound_param_of_the_wrong_type_is_refused_without_echoing_it`).
- `scripts/access-control-test.sh bound_params` -> 1 passed (`access_control_e2e::bound_params_keep_masks_and_write_denials`: Ranger mask applies to a bound SELECT, a reader's bound UPDATE/INSERT is denied).
- Unit: `cargo test -p sqe-trino-compat` (incl. `wire::execute_using_binds_values_not_sql`: injection, 64 KiB, `1--5`, wrong type), `cargo test -p sqe-core --lib -- sql_params`, coordinator `prepared bound_float flight_sql`. Clippy clean on the six crates.

Open, known limits:
- A placeholder DataFusion does not type (outside comparisons, BETWEEN, IN, LIKE, INSERT VALUES and now CASE branches; e.g. a bare `SELECT $1 + col` with a string) is bound as sent; a failing cast at execution can quote the value in the error, audit and `QueryRecord.error_message`.
- `render_query_profile` prints FilterExec literals, which hold bound values when a profile is recorded.
- DDL markers (`DEFAULT`, table properties) are not bound: Trino `EXECUTE` of such a template now fails where text substitution ran it.
- Flight SQL prepared DML re-plans on every execution (no stored plan for writes).
- Flight DML written with `$n` (not `?`) reports `numbered_count` 0 (pre-existing).
- Trino interval ranges (`INTERVAL '1-2' YEAR TO MONTH`) and JSON / other typed literals in `USING` are refused.
- The sqe-server Trino adapter (`execute_as` with params) has no direct test; the wire test covers the trait with a DataFusion executor.

**NEXT:** portal AI tells the data service to switch `SqeEngine` to `X-Trino-Prepared-Statement` + `EXECUTE ... USING`; then portal request §2 follow-ups.
