# Design: single-table transactions on a per-transaction branch

## Options considered

| | Staged actions (issue text) | Branch per transaction (chosen) |
|---|---|---|
| Read-your-writes | New scan: base snapshot + staged data files - staged deletes (#822 from scratch) | Scan the branch head through the existing time-travel provider |
| Per-statement write path | Must stop before commit and hand back an action | Unchanged; each statement commits to the branch |
| Conflict detection at COMMIT | Must patch `do_commit` to stop rebasing | Requirement fixed in the publish action; a rebase cannot drop it |
| Catalog backends | Any | Any (vendored action, not a REST-only POST) |
| Crash leftovers | Orphan data files only | A `sqe-txn-*` branch plus its files; swept by name and age |
| Uncommitted data visible to others | No | Through `$refs` / time travel, under the same policies |

The staged design keeps uncommitted data out of the table metadata, but it costs a second scan path and a change to every write path, where the branch design reuses both. The visibility of the branch is a disclosure to principals who can already `SELECT` the table, and it can be closed later.

## Vendored iceberg-rust findings

- `FastAppendAction::set_target_branch` (`transaction/append.rs:69`) and `RewriteFilesAction` / `OverwriteFilesAction::set_target_branch` (`transaction/replace_files.rs:460`) exist.
- `RowDeltaAction` (`transaction/row_delta.rs`) builds a `SnapshotProducer` with no branch setter. `SnapshotProducer::set_target_branch` (`transaction/snapshot.rs:1059`) exists, so the patch is a field and a setter, mirroring `append.rs`.
- `Transaction::create_branch` and `RemoveRefAction` exist (`transaction/branch.rs`).
- `TableCommit::builder().build()` is `pub(crate)`, which is why SQE's schema path POSTs raw REST JSON (`catalog_ops.rs:760`). Publishing through a vendored action avoids a REST-only path.
- SQE calls `fast_append` (~11 sites), `row_delta` (~7) and `rewrite_files` (~6), test code included; every one needs the branch applied.

## Components

### Transaction state (`sqe-coordinator`)

```rust
pub struct TxnState {
    pub id: String,                    // == Session::transaction_id
    pub owner: String,                 // username; the token never leaves the session
    pub deadline: Instant,             // min(txn timeout, token expiry)
    pub table: Option<TxnTable>,       // set by the first write
}

pub struct TxnTable {
    pub catalog: String,
    pub ident: TableIdent,
    pub base_snapshot_id: Option<i64>, // None = table had no snapshot
    pub branch: String,                // "sqe-txn-<id>"
}
```

It is held in the `SessionManager`, next to `transaction_id`. Over Trino, where the session is rebuilt from headers on every request, the coordinator keeps a `TxnRegistry: id -> TxnState` instead, and the header id is the lookup key. That is also where #824 plugs in.

### Write path (`sqe-write`)

`WriteContext` gains `target_branch: Option<String>`. Every `tx.fast_append()`, `tx.row_delta()` and `tx.rewrite_files()` call applies it when it is set. `commit_with_retry` is unchanged: it reloads the table, and the producer reads `snapshot_for_ref(branch)`. The silent rebase in `do_commit` is harmless on a branch only this transaction writes.

On the first write the coordinator creates the branch at `main`'s current snapshot, with `create_branch(name).with_snapshot_id(base)`. A table with no snapshot gets an empty branch; the vendored `SnapshotProducer` handles a missing parent.

### Read path

During planning, a scan of `TxnTable.ident` inside the transaction resolves `snapshot_for_ref(branch)` and registers the provider with `with_snapshot_id(head)`. This is the same mechanism `FOR VERSION AS OF` uses (`query_handler.rs:6127`). Session caches are keyed so a pinned provider never serves a non-transaction read.

### Publish (`COMMIT`)

New vendored action `PublishBranchAction { branch, expected_main: Option<i64> }`:

- updates: `SetSnapshotRef(main -> head(branch))`, `RemoveSnapshotRef(branch)`
- requirements: `AssertRefSnapshotId(main, expected_main)`, `AssertRefSnapshotId(branch, head)`

The requirements are fixed when the action is built, so the reload in `do_commit` cannot weaken them. A requirement failure maps to `SqeErrorCode::SerializationFailure` (new; Trino `TRANSACTION_CONFLICT`, SQLSTATE `40001`), the branch is removed, and the transaction ends.

If the transaction wrote nothing, `COMMIT` clears the state and makes no catalog call.

### Cleanup (`ROLLBACK`, timeout, session end, token expiry)

1. Collect the data and delete files added by the snapshots on the branch that are not ancestors of the base.
2. Commit `RemoveSnapshotRef(branch)`.
3. Delete the collected files.

If step 3 fails, the files are orphans, and the existing orphan-file procedure removes them later.

A background sweep removes `sqe-txn-*` branches whose transaction is unknown to this coordinator and whose newest snapshot is older than the transaction timeout.

### Wire protocols

- **Flight SQL.** `BeginTransaction` returns the id and stores the `TxnState`. `EndTransaction{COMMIT|ROLLBACK}` publishes or cleans up. `CommandStatement*` / `CommandPreparedStatement*` with a `transaction_id` must match the session's id, or fail with `InvalidArgument`. SqlInfo: 8 = `SQL_SUPPORTED_TRANSACTION_TRANSACTION`, 563 = true, 564 = `SERIALIZABLE`. 11 (`SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT`) is false, because DDL is refused.
- **Trino.** `START TRANSACTION` creates the `TxnState` in the registry. The `allowed_in_transaction` guard is relaxed to allow DML (not DDL). `COMMIT` / `ROLLBACK` publish or clean up before answering with the clear header.

### Security and audit

- Policy is evaluated per statement at execution time, as today. The publish needs no new privilege: it moves a ref the user's own writes created, on a table they could write.
- The token is the user's own. When it expires the transaction aborts; there is no service identity to fall back to.
- OCSF audit events for begin, commit (with the published snapshot ids) and rollback. Lineage is recorded at `COMMIT`.
