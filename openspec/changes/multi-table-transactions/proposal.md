## Why

Since #821 a transaction writes to one table. `BEGIN; INSERT INTO a ...; DELETE FROM b ...; COMMIT` was refused at the second table, because publishing two per-table branches as two catalog commits could leave `a` committed and `b` not. The Iceberg REST spec has an endpoint for exactly this case: `POST /v1/{prefix}/transactions/commit` applies a list of table commits, each with its own requirements, all or nothing. Polaris implements it.

## What Changes

- **Writes.** Each table written in the transaction gets its own `sqe-txn-<id>` branch, as in #821. A transaction may now claim several tables, all in one catalog (`catalog URL | warehouse`).
- **Admission at write time.** Before a statement writes to a second table, the session catalog checks that the catalog can commit several tables atomically: a REST catalog must advertise `POST /v1/{prefix}/transactions/commit` in `/v1/config`, and an embedded catalog must implement `Catalog::commit_transaction`. Otherwise the statement fails with `Multi-table writes not supported in a single transaction on this catalog`, before any file is written. A write to a table in a second catalog fails with `A transaction cannot write tables in two catalogs`.
- **Commit.** One table: `PublishBranchAction` as in #821. Several tables: one `PublishBranchAction` per table is prepared into a `TableCommit` (new vendored `Transaction::prepare_commit`), and all of them go to the catalog in one `Catalog::commit_transaction` call (new vendored trait method, default `FeatureUnsupported`).
  - REST: `POST /v1/{prefix}/transactions/commit`. 409 is a retryable conflict; 404 whose body names `NoSuchTable` is a missing table; any other 404, 405 or 501 means the server does not support the route; 5xx means the commit state is unknown.
  - Embedded SQLite catalog: every pointer swap runs in one `BEGIN IMMEDIATE` SQLite transaction; a mismatch on any table rolls back all of them.
- **Conflict.** If `main` of any table moved since its branch was created, the commit fails with `TRANSACTION_CONFLICT` and every branch is dropped. Neither table moves.
- **Rollback.** Drops the branch on every table.

### Non-goals

- Writes spanning two catalogs (refused).
- Cross-coordinator transaction routing (#824).
- An atomic multi-table commit on catalogs that lack one (Glue, Hive, a REST server without the route). Those keep the one-table limit.

## Impact

- **Affected code:** `vendor/iceberg-rust` (`Catalog::commit_transaction`, `Transaction::prepare_commit`, REST `CommitTransactionRequest`), `sqe-catalog` (`txn::admit`, `publish_branches`, `SessionCatalog::admit_txn_write`, SQLite `swap_pointers`), `sqe-write` (admission at every write site), `sqe-coordinator` (`COMMIT` on several tables).
- **Compatibility:** additive. Single-table transactions behave as before.
