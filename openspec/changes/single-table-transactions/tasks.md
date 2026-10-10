## 1. Vendored iceberg-rust

- [ ] 1.1 `RowDeltaAction::set_target_branch`, mirroring `FastAppendAction` (field, setter, `snapshot_producer.set_target_branch`)
- [ ] 1.2 `PublishBranchAction { branch, expected_main }`: `SetSnapshotRef(main)` + `RemoveSnapshotRef(branch)`, with requirements `AssertRefSnapshotId(main, expected_main)` and `AssertRefSnapshotId(branch, head)`
- [ ] 1.3 Vendored unit tests: a row delta lands on the branch; publishing fails when `main` moved; publishing an empty branch is a fast-forward to the base

## 2. Write path

- [ ] 2.1 `target_branch: Option<String>` on the write context, applied at every `fast_append` / `row_delta` / `rewrite_files` site
- [ ] 2.2 Create the branch at the base on the first write in a transaction (empty table included)
- [ ] 2.3 Refuse a write to a second table: `Multi-table writes not supported in a single transaction`
- [ ] 2.4 Session-cache invalidation after a branch write covers the owning session only

## 3. Read path

- [ ] 3.1 Pin scans of the transaction's table to the branch head through the time-travel provider path
- [ ] 3.2 Make sure a pinned provider is never served to a read outside the transaction (cache key includes the txn id)
- [ ] 3.3 Test: `INSERT` then `DELETE ... WHERE` inside one transaction removes the inserted rows

## 4. Commit and cleanup

- [ ] 4.1 `TxnState` / `TxnRegistry` in the coordinator; deadline = min(timeout, token expiry)
- [ ] 4.2 `COMMIT` publishes; a requirement failure becomes `SerializationFailure` (SQLSTATE 40001, Trino `TRANSACTION_CONFLICT`)
- [ ] 4.3 `ROLLBACK`, timeout, session end and token expiry remove the branch and delete its added files
- [ ] 4.4 Sweep stale `sqe-txn-*` branches older than the timeout
- [ ] 4.5 Verify that orphan-file cleanup treats files reachable from any ref as live

## 5. Wire protocols

- [ ] 5.1 Flight SQL `BeginTransaction` / `EndTransaction`; `transaction_id` honoured on statement, update, ingest and prepared commands
- [ ] 5.2 Flight SqlInfo 8 / 563 / 564 / 11 updated
- [ ] 5.3 Trino: allow DML inside a transaction (DDL stays refused); `COMMIT` / `ROLLBACK` publish or clean up
- [ ] 5.4 dbt-sqe: decide whether models should run inside a real transaction (separate change)

## 6. Audit, docs, tests

- [ ] 6.1 OCSF events for begin / commit / rollback; lineage at `COMMIT`
- [ ] 6.2 Book: transactions page (branch model, serializable abort, visibility of in-flight branches, SQE vs Trino connector difference)
- [ ] 6.3 Integration: one-table commit, rollback with no orphan files, concurrent-commit abort, ADBC `autocommit=False`, Trino JDBC `setAutoCommit(false)`
