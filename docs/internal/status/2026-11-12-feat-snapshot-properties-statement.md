**STATUS 2026-11-12: `write-snapshot-properties` section 2 done (`feat/snapshot-properties-statement`, stacked on `feat/snapshot-properties-groundwork`).**

- 2.1 Classifier: `StatementKind::SetSnapshotProperties(Vec<(String, String)>)`; keys and values are single-quoted literals; `()` and `RESET` clear. Allowed in a transaction; `update_type` is `SetSession`.
- 2.2 `Session::snapshot_properties`, `SessionManager::set_snapshot_properties`.
- 2.3 `TxnScope` holds the transaction's set. The scope leaves the registry at COMMIT/ROLLBACK (`end`), session end (`abandon`) and shutdown (`abort_all`); a failed COMMIT reopens and keeps it.
- 2.4 `QueryHandler::handle_set_snapshot_properties`: transaction scope first, then the held session, otherwise `NotSupported` naming Flight SQL and transactions. Logs keys only; `SnapshotProperties`'s Debug prints keys only.
- 2.5 Embedded CLI stores the set on its own state (no transactions there).

Gates: classifier, session, txn, session_manager and handler unit tests; clippy on the five crates with `-D warnings`; `cargo xtask budgets --check`; `cargo check-server`; `cargo dev`. Integration tests not run: nothing reads the properties yet.

Known: a transaction whose only statement is this SET ends as open rather than unused, because the SET creates the scope.

**NEXT:** section 3 (carry the properties to every commit).
