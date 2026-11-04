**STATUS 2026-11-04: Flight SQL prepared DML advertises parameter types (`fix/flight-prepared-dml-param-types`, portal request §1 remainder).**

`do_action_create_prepared_statement` stores no plan for DML (#821; the write handler plans it per execution), so it advertised `vec![None; n]`, which `parameter_schema` sends as `Utf8`. A JDBC/ADBC driver therefore bound every INSERT parameter as a string.

- `QueryHandler::dml_parameter_types`: for INSERT / UPDATE / DELETE, plans the numbered statement with `plan_sql` (DDL refused, nothing executed) and reads `get_parameter_types()`. DataFusion 55's `insert_to_plan` types `VALUES` placeholders from the target columns. Any other statement, or a plan failure (MERGE, Hive targets), returns untyped, so no prepare that worked before fails now.
- `PreparedStatement::bind` checks values against the types only when a plan is stored. For plan-less statements the advertised types are advisory and execution coerces (`bind_params` -> `coerce_untyped_param`), so `"10"` for BIGINT still binds. Without this, typing the schema would have turned a working string bind into a bind-time error.
- `parameter_schema` advertises `Utf8View` / `BinaryView` as `Utf8` / `Binary`. This applies to every prepared statement, including planned SELECTs. The bind casts back because `coerce_param` puts both in one family.

Gates:
- `cargo test -p sqe-coordinator --lib -- prepared flight_sql`: 49 passed. New tests: `insert_placeholders_are_advertised_but_bind_stays_lenient`, `view_types_are_advertised_in_their_plain_form`.
- `make test-integration FILTER=bound_param`: 4 passed. New test: `prepared_dml_advertises_target_column_types` (INSERT, INSERT with column list, DELETE, MERGE stays untyped).
- `cargo test --test it -- flight`: 11 passed.
- clippy `--tests -D warnings`: clean.

**NEXT:** UPDATE's `SET col = ?` placeholders: check what DataFusion infers for assignments (not covered by the IT), and whether the portal's driver now sends typed vectors.
