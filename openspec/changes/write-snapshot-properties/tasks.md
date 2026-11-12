## 1. Groundwork

- [ ] 1.1 `t$snapshots`: build the `summary` JSON with `serde_json` (`sqe-catalog/src/iceberg_metadata_tvf.rs`); unit test with quotes, backslash, control characters and non-ASCII
- [ ] 1.2 Read the Trino HTTP session path: record in the design whether `SET` statements reach a session held by the session manager, and fix the spec's refusal scenario to match
- [ ] 1.3 `SnapshotProperties` type with the validation rules (keys, values, reserved keys and prefixes, pair and byte caps) in `sqe-core`; unit tests for every rule

## 2. Statement and session state

- [ ] 2.1 Classifier: `SET SNAPSHOT_PROPERTIES [=] ('k' = 'v', ...)`, `SET SNAPSHOT_PROPERTIES ()`, `RESET SNAPSHOT_PROPERTIES` -> `StatementKind::SetSnapshotProperties`, `allowed_in_transaction`; parser tests
- [ ] 2.2 `Session::snapshot_properties` and `SessionManager::set_snapshot_properties`
- [ ] 2.3 `TxnScope` holds the transaction's properties; dropped when the transaction ends
- [ ] 2.4 Coordinator dispatch: inside a transaction set them on its `TxnScope`, outside it on the session; refuse when the session is not held (no silent ignore)
- [ ] 2.5 Embedded CLI dispatch (`sqe-cli/src/embedded.rs`)

## 3. Carry to the commit

- [ ] 3.1 `StatementScope` field, `write_scope` argument, inherited by `scope()` and `with_params()`; `sqe_catalog::txn::snapshot_properties()` accessor
- [ ] 3.2 Resolve the value at the three `write_scope` callers (query handler execute, Flight ingest, embedded): transaction properties inside a transaction, session properties outside
- [ ] 3.3 `apply_snapshot_properties` helpers for `fast_append`, `overwrite_files` and `row_delta`; call them at every action site in `write_handler.rs` and `write_handler/ingest.rs`, reading the task-local once before the retry loop
- [ ] 3.4 Empty writes: when properties are set and nothing is added or removed, commit an empty `fast_append` with the properties
- [ ] 3.5 Audit: the write's audit record lists the property keys (not values)

## 4. Flight SQL ingest options

- [ ] 4.1 `do_put_statement_ingest`: parse `snapshot-property.<key>` options, validate, merge over the scope's properties for that request; InvalidArgument before any file is written

## 5. Tests

- [ ] 5.1 Table-driven integration test: every write kind and ingest mode, in a transaction and outside, asserts the key in the committed summary
- [ ] 5.2 Transaction scope: published head carries the marker; cleared after COMMIT and ROLLBACK; two parallel transactions on one session keep their own values
- [ ] 5.3 Empty write commits an empty snapshot with properties; no snapshot without them
- [ ] 5.4 Compaction snapshot carries no user keys
- [ ] 5.5 Ingest option applies to one request only; reserved ingest key fails before data files are written

## 6. Docs and close-out

- [ ] 6.1 SQL reference: `SET` / `RESET SNAPSHOT_PROPERTIES`, the reserved keys, the transaction form as the recommended one, compaction and expiry limits, and that clients sharing one bearer token share session properties and a SQL `BEGIN` (parallel loaders use Flight SQL `BeginTransaction` or ingest options)
- [ ] 6.2 Flight SQL connecting page: the `snapshot-property.` ingest option
- [ ] 6.3 README roadmap, status fragment, changelog fragment
