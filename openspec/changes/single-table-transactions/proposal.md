## Why

Since #820, SQE's transactions are autocommit-only, as in Trino's Iceberg connector. `START TRANSACTION` opens a transaction, reads run inside it, and any write fails with `AUTOCOMMIT_WRITE_CONFLICT`. That is protocol-correct, but it means a client cannot make several DML statements on one table land together or not at all. Typical cases: a dbt incremental model that deletes a partition and re-inserts it, a correction job that runs `DELETE` and then `INSERT`, or an ADBC / JDBC client that sets `autocommit=False` and expects `rollback()` to undo its writes.

Two facts shape the design:

1. **The statements inside a transaction must see each other's writes.** `BEGIN; INSERT ...; DELETE ... WHERE ...; COMMIT` is only correct if the `DELETE` scans the rows the `INSERT` added. A design that stages actions and applies them only at `COMMIT` has to rebuild that visibility itself: a scan over the base snapshot plus staged data files minus staged deletes. That is #822, and it would have to be built from scratch.
2. **The vendored `Transaction::do_commit` silently rebases onto the refreshed table** (`vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs:258-266`, upstream iceberg-rust #3134). Holding an `iceberg::Transaction` open across statements and committing it at the end would publish over concurrent changes with no conflict check.

## What Changes

A transaction writes to its own Iceberg **branch** of the table. `COMMIT` fast-forwards `main` to that branch in a single catalog commit, conditional on `main` not having moved.

- **Begin.** `BEGIN` records the transaction on the session, as it does today. The first write to a table creates the branch `sqe-txn-<id>` at `main`'s current snapshot, which is the base, and records `(table, base_snapshot_id, branch)` on the transaction.
- **Writes.** Every write inside the transaction commits to the branch, not to `main`. The vendored `fast_append` and `rewrite_files` already take a target branch; `row_delta` gets the same setter, a small vendored patch that matches the other two. Each statement is its own commit on the branch, so the existing per-statement write path, retry and cleanup are reused unchanged.
- **Reads.** Inside the transaction, a scan of that table resolves to the branch head through the existing time-travel provider path (`SqeTableProvider::with_snapshot_id`). That gives read-your-writes with no new scan code. Other tables read `main`, as now.
- **Commit.** One catalog commit that sets `main` to the branch head and removes the branch, with the requirement `AssertRefSnapshotId(main = base)`. If another writer moved `main` after the base was taken, the commit fails with a serialization error (SQLSTATE `40001`) and the transaction is rolled back. There is no silent rebase. The commit is built as a new vendored action (`PublishBranchAction`) so it works on every catalog backend, not only REST.
- **Rollback.** `ROLLBACK`, a transaction timeout, the session ending or an expired token remove the branch and delete the files its snapshots added (the cleanup guard).
- **Scope.** One table per transaction. A write to a second table fails with `Multi-table writes not supported in a single transaction` until #823. DDL inside a transaction stays refused.
- **Wire protocols.**
  - Flight SQL: implement `BeginTransaction` / `EndTransaction` and honour `transaction_id` on statement, update, ingest and prepared-statement commands. Advertise `SQL_SUPPORTED_TRANSACTION_TRANSACTION` and `SERIALIZABLE`. Savepoints stay unimplemented.
  - Trino: drop the autocommit-only refusal for single-table transactions.

### Accepted differences from the issue text

- `COMMIT` publishes the transaction's N snapshots (one per statement) in a single atomic ref move, not as one squashed snapshot. That is what an Iceberg Java `Transaction` with several operations produces as well. The acceptance criterion becomes "`main` moves from base to head in one catalog commit, or not at all".
- Over Trino this puts SQE ahead of Trino's own Iceberg connector, which refuses writes inside an explicit transaction. That is deliberate; the docs will say so.

### Non-goals

- Multi-table atomic commit (#823), cross-coordinator transaction routing (#824), and snapshot isolation that re-validates instead of aborting. Serializable abort-on-conflict is the only mode.
- Hiding the in-flight branch from other principals. It shows up in `$refs`, and a reader with `SELECT` on the table could time-travel to it. Policies still apply to such a read. A follow-up can refuse `FOR VERSION AS OF 'sqe-txn-*'` from any session other than the owning one.

## Impact

- **Affected code:** `sqe-write` (target branch threaded through every commit site), `sqe-coordinator` (transaction state, publish and cleanup, Flight RPCs, scan pinning), `sqe-trino-compat` (lift the refusal), `vendor/iceberg-rust` (`row_delta` target branch, `PublishBranchAction`).
- **Maintenance:** `expire_snapshots` keeps branch snapshots alive (branch retention). Orphan-file cleanup must treat files reachable from any ref as live; this needs a verification test.
- **Compatibility:** additive. Outside an explicit transaction nothing changes. Inside one, writes that are refused today start to work.

## Success criteria

- `BEGIN; INSERT; DELETE; COMMIT` on one table: the `DELETE` sees the inserted rows, and `main` moves once.
- `ROLLBACK` leaves `main` unchanged, the branch gone, and none of the transaction's files left in storage.
- A concurrent commit to `main` between the first write and `COMMIT` makes `COMMIT` fail with a serialization error, with no lost update.
- ADBC `autocommit=False` with `commit()` / `rollback()`, and Trino JDBC `setAutoCommit(false)` with a write and `commit()`, both work end to end.
- Integration tests run under `make test-integration`.

## Rollback strategy

Everything sits behind the transaction state. Reverting the Trino and Flight changes restores the autocommit-only behaviour of #820. Branches left behind by a crashed coordinator are named `sqe-txn-*` and can be removed with `ALTER TABLE ... DROP BRANCH`, or by a sweep of `sqe-txn-*` refs older than the transaction timeout.
