# Design: single-table transactions on a per-transaction branch

## Options considered

| | Staged actions (issue text) | Branch per transaction (chosen) |
|---|---|---|
| Read-your-writes | New scan: base snapshot + staged data files - staged deletes (#822 from scratch) | Load the table pinned to the branch head; existing scans |
| Per-statement write path | Must stop before commit and hand back an action | Unchanged; each statement commits to the branch |
| Conflict detection at COMMIT | Must patch `do_commit` to stop rebasing | Requirement fixed in the publish action; a rebase cannot drop it |
| Catalog backends | Any | Any (vendored action, not a REST-only POST) |
| Crash leftovers | Orphan data files only | A `sqe-txn-*` branch plus its files; dropped by name |
| Uncommitted data visible to others | No | Not through SQE (refused, see Exposure block); visible to engines reading the catalog directly |

The staged design keeps uncommitted data out of the table metadata, but it costs a second scan path and a change to every write path, where the branch design reuses both. The branch puts uncommitted data in the table metadata, so SQE refuses every way a user could name it.

## Vendored iceberg-rust findings

- `FastAppendAction::set_target_branch` (`transaction/append.rs:69`) and `RewriteFilesAction` / `OverwriteFilesAction::set_target_branch` (`transaction/replace_files.rs:460`) exist.
- `RowDeltaAction` (`transaction/row_delta.rs`) builds a `SnapshotProducer` with no branch setter. `SnapshotProducer::set_target_branch` (`transaction/snapshot.rs:1059`) exists, so the patch is a field and a setter, mirroring `append.rs`.
- `Transaction::create_branch` and `RemoveRefAction` exist (`transaction/branch.rs`).
- `TableCommit::builder().build()` is `pub(crate)`, which is why SQE's schema path POSTs raw REST JSON (`catalog_ops.rs:760`). Publishing through a vendored action avoids a REST-only path.
- SQE calls `fast_append` (~11 sites), `row_delta` (~7) and `rewrite_files` (~6), test code included; every one needs the branch applied.
- New: `Table::pinned_to_ref(name)` returns the table with `main` read as the named ref, and `TableCommit::updates()` exposes a commit's updates so the session catalog can inspect which refs it sets.

## Components

### Transaction scope (`sqe-catalog::txn`)

```rust
pub struct TxnTable {
    pub table_uuid: Uuid,              // the table is matched by UUID, not name
    pub ident: TableIdent,
    pub base_snapshot_id: Option<i64>, // None = table had no snapshot
    pub branch: String,                // "sqe-txn-<id>"
}

pub struct TxnScope {
    id: String,
    table: Mutex<Option<TxnTable>>,           // set by the first write
    catalog: Mutex<Option<Arc<dyn Catalog>>>, // bound by each branch commit
}
```

A statement runs inside `txn::scope(Option<Arc<TxnScope>>, fut)`, a tokio task-local. `scope(None, ..)` shadows an outer scope; COMMIT and ROLLBACK run that way, because the session catalog refuses to move `main` from inside a scope.

### Transaction registry (`sqe-coordinator::txn_registry`)

`TxnRegistry: id -> { owner, scope, last_used }` on the `QueryHandler`. Every protocol keys it by `Session::transaction_id`; over Trino that id comes from the client header on every request, so the registry is the only server-side state (and where #824 plugs in).

- The id must be a UUID (it becomes a branch name); anything else is `InvalidArguments`.
- An id belongs to the user who first used it; another user gets `unknown transaction`.
- A transaction idle for 30 minutes is swept and its branch dropped.
- An ended id is kept as a tombstone for 24 hours; using it again is `TransactionConflict: has ended`, so a client that missed the clear header cannot silently start a new transaction under the old id.

### Write path (`sqe-write`)

`commit::txn_target_branch(catalog, ident)` returns the transaction's branch when the statement runs inside a scope. Every `fast_append`, `row_delta` and `rewrite_files` site applies it. `commit_with_retry` is unchanged: it reloads the table, and the producer reads `snapshot_for_ref(branch)`. The silent rebase in `do_commit` is harmless on a branch only this transaction writes.

The first write creates the branch at `main`'s current snapshot with `create_branch(name).with_snapshot_id(base)`. A table with no snapshot gets no branch up front; the first write commit creates it, and the vendored `SnapshotProducer` handles the missing parent.

`txn::check_target(ident)` runs at the top of every DML handler, so a write to a second table fails before it writes any file.

### Read path

Inside a scope, the session catalog loads the transaction's table (matched by UUID) as `Table::pinned_to_ref(branch)`. Every reader — plain scans and the target scans inside DELETE / UPDATE / MERGE — sees the branch head with no change to the scan code. The result cache is bypassed and prepared plans are not cached inside a transaction, so a pinned plan never serves a non-transaction read.

### Session catalog bridge

- Inside a scope, a commit that sets `main` is refused (it would publish part of the transaction).
- A commit that sets the scope's branch binds the scope to that catalog, so COMMIT and ROLLBACK reach the table as the same principal, with the latest token.

### Publish (`COMMIT`)

New vendored action `PublishBranchAction { branch, expected_main: Option<i64> }`:

- updates: `SetSnapshotRef(main -> head(branch))`, `RemoveSnapshotRef(branch)`
- requirements: `AssertRefSnapshotId(main, expected_main)`, `AssertRefSnapshotId(branch, head)`

The requirements are fixed when the action is built, so the reload in `do_commit` cannot weaken them. A requirement failure (`PreconditionFailed` from the check in `commit()`, or a catalog `CommitFailed`) maps to `SqeErrorCode::TransactionConflict` (new; Trino error code 17 `TRANSACTION_CONFLICT`, gRPC `Aborted`). The branch is dropped and the transaction ends. Any other failure keeps the transaction open (`TxnRegistry::reopen`), so the client can retry `COMMIT` or send `ROLLBACK`.

On success the query cache and the owner's session cache are invalidated. If the transaction wrote nothing, `COMMIT` clears the state and makes no catalog call.

### Cleanup (`ROLLBACK`, idle timeout)

Commit `RemoveSnapshotRef(branch)`. The files the branch's snapshots added are no longer reachable from any ref; the orphan-file procedure removes them. A failed drop is logged and leaves the branch for a manual `ALTER TABLE ... DROP BRANCH`.

Deferred: a sweep of `sqe-txn-*` branches left by a crashed coordinator, and dropping the branch when the session ends or the token expires.

### Exposure block

A `sqe-txn-*` branch holds uncommitted data, so SQE refuses it everywhere it can be named:

- `FOR VERSION AS OF 'sqe-txn-…'`, and `FOR VERSION AS OF <id>` / incremental reads of a snapshot reachable only from a transaction branch (`txn_private_snapshots`).
- `CREATE BRANCH / TAG` from such a snapshot, branch DDL on a `sqe-txn-*` name, and `SET WRITE_BRANCH 'sqe-txn-…'`.
- `$snapshots` leaves those snapshots out; `$refs` leaves the branches out.

A session with a `WRITE_BRANCH` set cannot write inside a transaction (`NotSupported`): transactions publish to `main`.

### Wire protocols

- **Flight SQL.** SQL `BEGIN` / `COMMIT` / `ROLLBACK` on the stored session, as since #820. The `BeginTransaction` / `EndTransaction` actions and the `transaction_id` field on commands are deferred; `CommandStatementIngest` is refused inside a transaction (`FailedPrecondition`).
- **Trino.** The `allowed_in_transaction` guard allows DML (not DDL, CTAS, grants or procedures). `COMMIT` / `ROLLBACK` inside a transaction go to the executor, which publishes or drops the branch; the clear-transaction header is sent only when that succeeds. After a failed `COMMIT` the client sends `ROLLBACK`, which ends the transaction.
- **Hive external tables** have no branches; a write to one inside a transaction is `AUTOCOMMIT_WRITE_CONFLICT`.

### Security and audit

- Policy is evaluated per statement at execution time, as today. The publish needs no new privilege: it moves a ref the user's own writes created, on a table they could write.
- The token is the user's own. When it expires the publish fails and the transaction stays open; there is no service identity to fall back to.
- Deferred: OCSF audit events for begin, commit (with the published snapshot ids) and rollback; lineage recorded at `COMMIT`.
