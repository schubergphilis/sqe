**STATUS 2026-10-23: DELETE / UPDATE with a correlated `EXISTS` subquery (`fix/dml-exists-subquery`).**

Found by the dbt-sqe live probe (portal #42), logged open in `2026-10-23-fix-dml-in-subquery-row-alignment.md`.

Cause: `filter_batch_match` and `apply_update` in `crates/sqe-write/src/write_handler.rs` spliced the WHERE into the SELECT list (`SELECT CAST((<where>) AS BOOLEAN) FROM scratch`, `CASE WHEN <where> THEN new ELSE old END`). DataFusion's `DecorrelatePredicateSubquery` only rewrites subqueries in a Filter, so a correlated EXISTS reached the physical planner: "Physical plan does not support logical expression Exists". `IN (subquery)` worked only because `lift_in_subqueries` turns it into a join first. MERGE was not affected.

Fix:
- `filter_batch_match` runs `SELECT "<t>"."__pol_pos_<nonce>" FROM <scratch + joins> WHERE COALESCE(CAST((<where AND row filter>) AS BOOLEAN), FALSE)` and builds the mask from the returned positions (`mask_from_positions`; a duplicate or out-of-range position fails the statement).
- `apply_update_masked` takes that mask, adds it to the scratch batch as a `__pol_match_<nonce>` column and uses the column in the CASE expressions, so the WHERE is evaluated once. The CoW (`handle_update`) and equality-delete (`handle_update_equality`) paths call it with the mask they already had. `apply_update` (WHERE in, mask computed) is now test-only.
- `policy_projection_sql` passes `__pol_match_` through unguarded like `__pol_pos_`; `__pol_` aliases are refused in user SQL and assignments.

Gate:
- `make test-integration FILTER=dml_exists_subquery`: 2 passed (`dml_exists_subquery_delete_correlated`, `dml_exists_subquery_update_correlated`; CoW and MoR each, bare and fully qualified correlation names, NOT EXISTS, scalar subquery in UPDATE).
- Regression: `FILTER=dml_in_subquery` 2 passed, `access_control_e2e` 63 passed, `write_shape_e2e` 2, `in_subquery_view_rewrite` 1, `test_update` 4, `test_delete` 4, `test_merge` 2; broad `FILTER=update` 15 passed, `policy` 9, `merge` 10, `delete` 32 passed + 3 `rewrite_data_files_distributed_parity` failing for want of a worker on :50052 (environmental, same as on main).
- `RUST_MIN_STACK=33554432 cargo test -p sqe-write --features test-sqlite --lib`: 249 passed. `cargo clippy -p sqe-write --all-targets -- -D warnings` clean.

Open: none from this change. MERGE `rows_affected` (CoW reports surviving rows) is on `fix/merge-rows-affected`.

**NEXT:** unchanged
