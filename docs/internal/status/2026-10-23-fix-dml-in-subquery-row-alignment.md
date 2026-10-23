**STATUS 2026-10-23: DELETE / UPDATE with `IN (subquery)` change the matched rows again (`fix/dml-in-subquery-row-alignment`).**

Found by the dbt-sqe live probe (portal #42): a single-key `delete+insert` deleted id 1 instead of id 2.

Cause: `lift_in_subqueries` (dd53a9ab, first in v0.30.0) turns `IN (subquery)` into `LEFT JOIN` on a scratch keyset. `filter_batch_match`, `filter_batch_negate` and `apply_update` in `crates/sqe-write/src/write_handler.rs` ran `SELECT ... FROM scratch LEFT JOIN ...` and applied the result to the batch by position (and read only the first result batch). The hash join emits matched rows first, so the verdicts and the updated rows landed on the wrong rows. CoW DELETE, MoR DELETE, CoW UPDATE and MoR UPDATE were all affected; MERGE and literal `IN` lists were not.

Fix: the scratch table gets a `__pol_pos_<nonce>` UInt64 row-position column; the evaluation orders by it, concatenates every result batch, and fails if the row count differs from the input. `filter_batch_negate` now negates `filter_batch_match`. `policy_projection_sql` passes the position column through unguarded (it is internal and not user-nameable: `__pol_` aliases are refused in user SQL).

Gate:
- `make test-integration FILTER=dml_in_subquery`: 2 passed (`dml_in_subquery_delete_removes_the_matched_row`, `dml_in_subquery_update_changes_the_matched_row`; CoW and MoR each, middle row, ids asserted). Both fail on origin/main: DELETE left `[2,3,4]`, CoW UPDATE left `[(2,21),(2,20),(3,30),(4,40)]`.
- `FILTER=update` 14 passed, `FILTER=merge` 10 passed, `FILTER=in_subquery` 4 passed, `FILTER=policy` 9 passed, `FILTER=delete` 31 passed + 3 `rewrite_data_files_distributed_parity` failing for want of a worker on :50052 (environmental).
- `cargo test -p sqe-write --lib`: 215 passed.
- `scripts/access-control-test.sh` (Ranger): 50 passed, 1 failed. `unknown_tag_state_denies` failed with `FOR INCREMENTAL BETWEEN SNAPSHOT 5121633547561628585 AND SNAPSHOT 2296073137140798942: start must be older than end`. Unrelated to this change: the incremental range check compares snapshot ids numerically, but Iceberg snapshot ids are random, so the test fails whenever the older snapshot draws the larger id (fixed on `fix/incremental-snapshot-id-order`).

Open, found by the same probe:
- MERGE `WHEN MATCHED THEN DELETE` reports `rows_affected` 2 for 1 deleted row.
- `DELETE ... WHERE EXISTS (correlated subquery)` fails: "Physical plan does not support logical expression Exists".

**NEXT:** unchanged
