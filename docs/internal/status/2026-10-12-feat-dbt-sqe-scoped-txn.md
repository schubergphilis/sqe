**STATUS 2026-10-12: dbt-sqe `delete+insert` commits its DELETE and INSERT together (`feat/dbt-sqe-scoped-txn`, openspec single-table-transactions 5.4).**

Decision for 5.4: models keep autocommit. A transaction refuses the DDL and CTAS that a model runs, and `append` and `merge` are single statements. `delete+insert` writes its target in two statements, so those two now run in one Flight SQL transaction:

- `adapter.begin_transaction()` / `adapter.commit_transaction()` drive the ADBC connection's autocommit. The DB-API commit and rollback are no-ops on an autocommit connection, so the adapter calls ADBC directly.
- The temp table's CTAS and DROP stay outside the transaction.
- `_Handle.rollback()` and `_Handle.close()` abort an open transaction. dbt calls them on every failure path, including a template error that reaches neither the commit nor `exception_handler`. A commit that fails leaves the transaction marked open, so `close()` rolls it back.

Tests:
- Verified live: an INSERT that fails after its DELETE (column-count mismatch) reports ERROR, the target keeps its deleted key, and dbt logs ROLLBACK. The next model runs normally on the same thread.
- 26 adapter unit tests.

Stacked on !1398, which this needs: before it, delete+insert never reached its INSERT.

Found on the way: inserting the string `'abc'` into a bigint column through `INSERT ... SELECT` stored 0 instead of failing (#834).

**NEXT:** #816 (token refresh-ahead), then #803 (CTAS commit 401 / re-resolve on 401).
