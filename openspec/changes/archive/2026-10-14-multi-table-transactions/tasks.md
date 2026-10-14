## 1. Vendored iceberg-rust

- [x] 1.1 `Catalog::supports_commit_transaction` / `Catalog::commit_transaction` (default `FeatureUnsupported`)
- [x] 1.2 `Transaction::prepare_commit` builds the `TableCommit` that `do_commit` sends
- [x] 1.3 REST `POST /v1/{prefix}/transactions/commit`, refused without a request when not advertised; 404 / 405 / 501 / 409 / 5xx mapping
- [x] 1.4 Vendored REST tests: success body, conflict retryable, unadvertised, route 404, missing table

## 2. Catalogs

- [x] 2.1 SQLite catalog: all pointer swaps in one `BEGIN IMMEDIATE`, roll back on any mismatch (`swap_pointers`)
- [x] 2.2 Session catalog bridge forwards `commit_transaction` and invalidates every committed table

## 3. Transactions

- [x] 3.1 `TxnScope` holds several tables and the bound catalog
- [x] 3.2 `txn::admit` at write time: refuse a second catalog; refuse a second table on a catalog without an atomic multi-table commit
- [x] 3.3 `COMMIT` publishes several tables in one `commit_transaction`, retrying retryable conflicts; a requirement failure is `TRANSACTION_CONFLICT` and drops every branch
- [x] 3.4 `ROLLBACK` drops every branch

## 4. Tests

- [x] 4.1 sqlite-rig: two tables publish together, a conflict on one rolls back both, rollback drops every branch
- [x] 4.2 Unit: second table admitted on an atomic catalog, refused otherwise, second catalog refused
- [x] 4.3 Integration against Polaris 1.7.0: `INSERT a; DELETE b; COMMIT`, and a conflict on `b` that leaves both tables unchanged

## 5. Found on the way

- [x] 5.1 Carry the transaction scope onto the planning stack (`txn::carried`): `on_planning_stack` polls on its own thread, so a SELECT or DML source query inside a transaction planned against `main` in a multi-thread runtime (#821)
