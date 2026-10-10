## Why

Since #820, SQE's transactions are autocommit-only, as in Trino's Iceberg connector. `START TRANSACTION` opens a transaction, reads run inside it, and any write fails with `AUTOCOMMIT_WRITE_CONFLICT`. That is protocol-correct, but it means a client cannot make several DML statements on one table land together or not at all. Typical cases: a dbt incremental model that deletes a partition and re-inserts it, a correction job that runs `DELETE` and then `INSERT`, or an ADBC / JDBC client that sets `autocommit=False` and expects `rollback()` to undo its writes.

Two facts shape the design:

1. **The statements inside a transaction must see each other's writes.** `BEGIN; INSERT ...; DELETE ... WHERE ...; COMMIT` is only correct if the `DELETE` scans the rows the `INSERT` added. A design that stages actions and applies them only at `COMMIT` has to rebuild that visibility itself: a scan over the base snapshot plus staged data files minus staged deletes. That is #822, and it would have to be built from scratch.
2. **The vendored `Transaction::do_commit` silently rebases onto the refreshed table** (`vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs:258-266`, upstream iceberg-rust #3134). Holding an `iceberg::Transaction` open across statements and committing it at the end would publish over concurrent changes with no conflict check.

## What Changes

A transaction writes to its own Iceberg **branch** of the table. `COMMIT` fast-forwards `main` to that branch in a single catalog commit, conditional on `main` not having moved.

- **Begin.** `BEGIN` records the transaction on the session, as it does today. The first write to a table creates the branch `sqe-txn-<id>` at `main`'s current snapshot, which is the base, and records `(table, base_snapshot_id, branch)` on the transaction.
- **Writes.** Every write inside the transaction commits to the branch, not to `main`. The vendored `fast_append` and `rewrite_files` already take a target branch; `row_delta` gets the same setter, a small vendored patch that matches the other two. Each statement is its own commit on the branch, so the existing per-statement write path, retry and cleanup are reused unchanged.
- **Reads.** Inside the transaction, every load of that table through the session catalog is pinned to the branch head (`Table::pinned_to_ref`), so reads and the scans inside DELETE / UPDATE / MERGE see the transaction's own writes with no new scan code. Other tables, and every other session, read `main`.
- **Commit.** One catalog commit that sets `main` to the branch head and removes the branch, with the requirement `AssertRefSnapshotId(main = base)`. If another writer moved `main` after the base was taken, `COMMIT` fails with `TRANSACTION_CONFLICT` and the transaction is rolled back. There is no silent rebase. The commit is built as a new vendored action (`PublishBranchAction`) so it works on every catalog backend, not only REST. Any other publish failure (catalog unreachable) keeps the transaction open, so the client can retry `COMMIT` or `ROLLBACK`.
- **Rollback.** `ROLLBACK`, and a transaction idle for 30 minutes, drop the branch. The files its snapshots added become unreferenced and are removed by the orphan-file procedure.
- **In-flight branches are hidden.** A `sqe-txn-*` branch holds uncommitted data, so SQE refuses it everywhere a user could name it: `FOR VERSION AS OF 'sqe-txn-…'`, `FOR VERSION AS OF <snapshot id>` and incremental reads of a snapshot only a transaction branch reaches, `CREATE BRANCH / TAG` from such a snapshot or with a `sqe-txn-*` name, and `SET WRITE_BRANCH`. `DROP BRANCH` stays allowed as the manual cleanup. `$snapshots` leaves those snapshots out and `$refs` lists only well-known refs. Engines that read the catalog directly (Spark, Trino) still see the branch.
- **Scope.** One table per transaction. A write to a second table fails with `Multi-table writes not supported in a single transaction` until #823, before the statement writes any file. CTAS, DDL, grants and procedures inside a transaction stay refused (`AUTOCOMMIT_WRITE_CONFLICT`), as do writes to Hive external tables, which have no branches.
- **Wire protocols.**
  - Flight SQL: SQL `BEGIN` / `COMMIT` / `ROLLBACK` on the stored session, as since #820. The `BeginTransaction` / `EndTransaction` actions and the `transaction_id` field on commands stay unimplemented (follow-up); bulk ingest (`CommandStatementIngest`) is refused inside a transaction.
  - Trino: DML runs inside `START TRANSACTION`; `COMMIT` / `ROLLBACK` go to the executor, which publishes or drops the branch. The client-supplied transaction id must be a UUID, and only the user who opened a transaction can use it.

### Accepted differences from the issue text

- `COMMIT` publishes the transaction's N snapshots (one per statement) in a single atomic ref move, not as one squashed snapshot. That is what an Iceberg Java `Transaction` with several operations produces as well. The acceptance criterion becomes "`main` moves from base to head in one catalog commit, or not at all".
- Over Trino this puts SQE ahead of Trino's own Iceberg connector, which refuses writes inside an explicit transaction. That is deliberate; the docs will say so.

### Non-goals

- Multi-table atomic commit (#823), cross-coordinator transaction routing (#824), and snapshot isolation that re-validates instead of aborting. Serializable abort-on-conflict is the only mode.
- Hiding the in-flight branch from engines that read the Iceberg catalog directly. SQE cannot; the catalog's own grants decide what they see.

## Impact

- **Affected code:** `sqe-write` (target branch threaded through every commit site), `sqe-catalog` (transaction scope, branch pinning, exposure block), `sqe-coordinator` (transaction registry, publish and rollback), `sqe-trino-compat` (route COMMIT / ROLLBACK to the executor), `vendor/iceberg-rust` (`row_delta` target branch, `PublishBranchAction`).
- **Maintenance:** `expire_snapshots` keeps branch snapshots alive (branch retention). Orphan-file cleanup must treat files reachable from any ref as live; this needs a verification test.
- **Compatibility:** additive. Outside an explicit transaction nothing changes. Inside one, writes that are refused today start to work.

## Success criteria

- `BEGIN; INSERT; DELETE; COMMIT` on one table: the `DELETE` sees the inserted rows, and `main` moves once.
- `ROLLBACK` leaves `main` unchanged and the branch gone.
- A concurrent commit to `main` between the first write and `COMMIT` makes `COMMIT` fail with `TRANSACTION_CONFLICT`, with no lost update.
- No SQE statement can read a transaction's branch from outside the transaction.
- Trino JDBC `setAutoCommit(false)` with a write and `commit()` works end to end.

## Rollback strategy

Everything sits behind the transaction state. Reverting the Trino and Flight changes restores the autocommit-only behaviour of #820. Branches left behind by a crashed coordinator are named `sqe-txn-*` and can be removed with `ALTER TABLE ... DROP BRANCH`, or by a sweep of `sqe-txn-*` refs older than the transaction timeout.
