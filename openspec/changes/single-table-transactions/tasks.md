## 1. Vendored iceberg-rust

- [x] 1.1 `RowDeltaAction::set_target_branch`, mirroring `FastAppendAction` (field, setter, `snapshot_producer.set_target_branch`)
- [x] 1.2 `PublishBranchAction { branch, expected_main }`: `SetSnapshotRef(main)` + `RemoveSnapshotRef(branch)`, with requirements `AssertRefSnapshotId(main, expected_main)` and `AssertRefSnapshotId(branch, head)`
- [x] 1.3 Vendored unit tests: a row delta lands on the branch; publishing fails when `main` moved; publishing an empty branch is a fast-forward to the base

## 2. Write path

- [x] 2.1 Target branch applied at every `fast_append` / `row_delta` / `rewrite_files` site (`commit::txn_target_branch`, from the task-local scope rather than a write-context field)
- [x] 2.2 Create the branch at the base on the first write in a transaction (empty table: the first write commit creates it)
- [x] 2.3 Refuse a write to a second table: `Multi-table writes not supported in a single transaction`, before any file is written (`txn::check_target`; replaced by `txn::admit` in #823)
- [ ] 2.4 Session-cache invalidation after a branch write covers the owning session only

## 3. Read path

- [x] 3.1 Pin loads of the transaction's table to the branch head (`Table::pinned_to_ref` in the session catalog, matched by table UUID)
- [x] 3.2 A pinned table is never served outside the transaction: the pin lives in the task-local scope, the result cache is bypassed and prepared plans are not cached inside a transaction
- [x] 3.3 Test: `INSERT` then `DELETE ... WHERE` inside one transaction removes the inserted rows
- [x] 3.4 Exposure block: refuse `sqe-txn-*` refs and branch-private snapshot ids in time travel, incremental reads, ref DDL and `SET WRITE_BRANCH`; hide them from `$snapshots` / `$refs`

## 4. Commit and cleanup

- [x] 4.1 `TxnScope` (task-local) and `TxnRegistry` in the coordinator: UUID ids, owner check, 30-minute idle sweep, 24-hour tombstones for ended ids
- [x] 4.2 `COMMIT` publishes; a requirement failure becomes `TransactionConflict` (Trino `TRANSACTION_CONFLICT`, gRPC `Aborted`); any other failure keeps the transaction open
- [x] 4.3 `ROLLBACK` and the idle timeout drop the branch; added files are left to orphan-file cleanup. Session end and token expiry roll back through the `SessionManager::on_session_end` hook (#824)
- [x] 4.4 Sweep stale `sqe-txn-*` branches left by a crashed coordinator: a 24-hour transaction lifetime cap in the registry makes a branch whose private head is older than 25 hours dead (`txn::stale_txn_refs`); `CALL system.expire_snapshots` drops them first, the maintenance scheduler in Active mode drops them on opted-in tables (`txn::drop_stale_branches`)
- [x] 4.5 Verify that orphan-file cleanup treats files reachable from any ref as live: `RemoveOrphanFilesAction::collect_reachable_files` walks `metadata.snapshots()`, every snapshot including branch ones, and skips files younger than its 3-day default `older_than`
- [x] 4.6 Routing (#824): ids carry the coordinator's incarnation (`sqe_core::txn_id`); a foreign id is `TransactionConflict` "not found on this coordinator", `ROLLBACK` of it succeeds
- [x] 4.7 Graceful shutdown rolls back every open transaction (`shutdown_with_drain` `before_close`, 10 s bound)
- [x] 4.8 Metrics: `sqe_transactions_open`, `sqe_transactions_ended_total{outcome}`, `sqe_transaction_duration_seconds`, `sqe_transaction_not_found_total`, `sqe_transaction_branch_drop_failures_total`

## 5. Wire protocols

- [x] 5.1 Flight SQL `BeginTransaction` / `EndTransaction`; `transaction_id` honoured on statement, update, ingest and prepared commands. The client holds the id; DDL and CTAS inside it autocommit and leave it open; a prepared statement is bound to the transaction it was created in
- [x] 5.2 Flight SqlInfo 8 (Transaction) / 11 / 562 (READ_COMMITTED) / 563 / 564 / 565 / 566 updated (with 5.1)
- [x] 5.3 Trino: allow DML inside a transaction (DDL, CTAS and Hive writes stay refused); `COMMIT` / `ROLLBACK` publish or drop the branch, clear header only on success
- [ ] 5.4 dbt-sqe: decide whether models should run inside a real transaction (separate change)

## 6. Audit, docs, tests

- [ ] 6.1 OCSF events for begin / commit / rollback; lineage at `COMMIT`
- [~] 6.2 Book: transactions section in `sql-reference/dml.md` (#824); a dedicated page is still open. Scope: transactions page (branch model, serializable abort, visibility of in-flight branches, SQE vs Trino connector difference)
- [~] 6.3 sqlite-rig tests in `sqe-write`: commit, read-your-writes, concurrent-commit abort, rollback, empty table, second-table refusal. Integration tests under `make test-integration`: multi-table commit and conflict (#823), client-held RPC transaction with DDL autocommit, rollback and commit. Still open: an automated ADBC `autocommit=False` test, Trino JDBC `setAutoCommit(false)`
