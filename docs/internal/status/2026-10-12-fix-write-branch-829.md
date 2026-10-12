**STATUS 2026-10-12: `SET WRITE_BRANCH` routes writes to the branch (`fix/write-branch-829`, #829, openspec iceberg-matrix-parity 4.13).**

Before this change, `SET WRITE_BRANCH` was stored on the session and never read, so every write outside a transaction landed on main. A data write now runs in `sqe_catalog::txn::write_scope`, which carries the branch in the same task-local that transactions use (so `txn::carried` covers it on the planning stack):

- Every table that holds the branch is read at it, so DELETE, UPDATE, MERGE, TRUNCATE and INSERT OVERWRITE rewrite the branch's files, not main's. A source table staged on the same branch is read at it too, as in Spark's `spark.wap.branch`.
- Every commit targets the branch, and a missing branch is an error rather than being created silently. An empty table holds no branch.
- The session catalog refuses any commit that would move main.

CTAS with a branch set is refused before it creates the table. Flight SQL bulk ingest follows the branch too. Plain SELECT still reads main.

Tests:
- SQLite rig, with main and `dev` diverged: CoW DELETE/UPDATE, MoR DELETE/UPDATE, MERGE, and the missing-branch error.
- Coordinator unit test: CTAS refusal.
- Polaris integration: `test_write_branch_routes_writes_to_the_branch`.

Known edge: inside such a write, `FOR VERSION AS OF 'main'` on a table that holds the branch reads the branch, because `pinned_to_ref` rewrites the main ref. Transactions have the same edge for the tables they wrote.

**NEXT:** #828 (column `DEFAULT` never applied on INSERT) on `fix/column-default-828`; then dbt-sqe per-connection transactions (openspec `single-table-transactions` 5.4), #816 and #803.
