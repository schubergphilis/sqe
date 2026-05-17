## 1. Grammar and AST

- [ ] 1.1 Add `ProcedureStmt` variant to `crates/sqe-sql/src/ast.rs` with catalog, namespace, procedure name, and argument list
- [ ] 1.2 Add `ProcedureCallArg::{Positional, Named}` enum to capture both call styles
- [ ] 1.3 Wire post-parse transformation in `crates/sqe-sql/src/lib.rs`: detect `Statement::Call` whose `ObjectName` matches `[catalog.]namespace.procedure` and produce `SqeStatement::Procedure(...)`
- [ ] 1.4 Reject mixed positional + named arguments at parse time with a clear error
- [ ] 1.5 Unit test: parse `CALL system.register_table('ns', 't', 's3://...')` → `ProcedureStmt { namespace: "system", procedure: "register_table", positional args }`
- [ ] 1.6 Unit test: parse `CALL system.register_table(namespace => 'ns', table_name => 't', metadata_location => 's3://...')` → named-arg variant
- [ ] 1.7 Unit test: parse `CALL iceberg.system.register_table(...)` recognizes the explicit catalog prefix
- [ ] 1.8 Unit test: mixed positional+named call returns a parse error

## 2. Procedure framework

- [ ] 2.1 Create `crates/sqe-coordinator/src/procedures/mod.rs` with `Procedure` trait, `ProcedureArg`, `ArgType`, `ProcedureArgs`, `ProcedureOutput`
- [ ] 2.2 Implement `bind_args(schema, call_args) -> Result<ProcedureArgs>`: matches named or positional args against the declared schema, applies defaults, coerces literal types
- [ ] 2.3 Implement `ProcedureRegistry` with `default()` constructor that populates the four v1 procedures
- [ ] 2.4 Wire `ProcedureRegistry` onto `SessionContext` via the existing dependency injection; one shared registry across sessions (immutable after startup)
- [ ] 2.5 Unit test: bind_args accepts the matching schema, rejects too-few / too-many args, rejects wrong types
- [ ] 2.6 Unit test: unknown `system.foo` dispatch returns a typed error

## 3. `system.register_table`

- [ ] 3.1 Implement `RegisterTableProcedure` in `crates/sqe-coordinator/src/procedures/register_table.rs`
- [ ] 3.2 Arg schema: `namespace: String`, `table_name: String`, `metadata_location: String`
- [ ] 3.3 Policy hook: call `session.policy().require_create_in_namespace(&ns)` before invoking the catalog
- [ ] 3.4 Dispatch to `iceberg::catalog::Catalog::register_table(&ident, metadata_location)`
- [ ] 3.5 On success, invalidate the session's table cache for the registered identifier so subsequent SELECTs see it
- [ ] 3.6 Return a single-row `RecordBatch` with columns (table_identifier, snapshot_id, metadata_location)
- [ ] 3.7 Integration test: CTAS a managed table, capture its current metadata_location from `iceberg.<schema>.<table>$metadata`, drop the table, re-register, run SELECT and verify identical rows

## 4. `system.drop_table`

- [ ] 4.1 Implement `DropTableProcedure` in `crates/sqe-coordinator/src/procedures/drop_table.rs`
- [ ] 4.2 Arg schema: `namespace: String`, `table_name: String`, `purge: Boolean = false`
- [ ] 4.3 Policy hook: `require_drop_on_table(&ident)`; when `purge => true`, additionally `require_modify_on_namespace(&ns)`
- [ ] 4.4 Dispatch to `Catalog::drop_table` or `Catalog::purge_table` depending on the purge flag
- [ ] 4.5 Invalidate table cache on success
- [ ] 4.6 Integration test: register a table, drop with purge=false, verify SELECT fails and data files remain on the object store
- [ ] 4.7 Integration test: register, drop with purge=true, verify SELECT fails and data directory is empty

## 5. `system.set_current_snapshot` and `system.rollback_to_snapshot`

- [ ] 5.1 Implement `SetCurrentSnapshotProcedure` in `crates/sqe-coordinator/src/procedures/snapshots.rs`
- [ ] 5.2 Arg schema: `namespace`, `table_name`, `snapshot_id: Int64`
- [ ] 5.3 Use `iceberg::transaction::Transaction::new(&table).set_current_snapshot(snapshot_id)`
- [ ] 5.4 Implement `RollbackToSnapshotProcedure` using the matching iceberg-rust helper
- [ ] 5.5 Policy hook on both: `require_modify_on_table(&ident)`
- [ ] 5.6 Integration test: CTAS, INSERT, capture the post-CTAS snapshot id, INSERT again, `set_current_snapshot` back to the captured id, verify SELECT returns the post-CTAS row count
- [ ] 5.7 Integration test: rollback_to_snapshot variant — same flow, verify a new snapshot was appended (audit trail preserved)

## 6. Statement routing

- [ ] 6.1 Extend `crates/sqe-coordinator/src/query_handler.rs` to match `SqeStatement::Procedure(stmt)` and call `registry.dispatch(stmt, session)`
- [ ] 6.2 Wrap `ProcedureOutput` in `QueryResult` so existing Flight SQL and Trino HTTP serialization works unchanged
- [ ] 6.3 Audit log every procedure invocation: name, args (with metadata_location redacted at info level), principal, outcome
- [ ] 6.4 Integration test: invoke from both wire protocols; verify result row reaches the client

## 7. Cross-backend support matrix

- [ ] 7.1 REST (Polaris): smoke test against existing `docker-compose.test.yml` stack
- [ ] 7.2 JDBC (SQLite via existing `sqe-catalog-loader` feature): roundtrip test in `tests/integration/jdbc_catalog.rs`
- [ ] 7.3 HMS: feature-gated test under `sqe-catalog/hms` (requires Thrift Hive container, gated behind `--features hms`)
- [ ] 7.4 Glue: gated behind credentials, manual test runbook documented in `docs/sql/procedures.md`
- [ ] 7.5 S3 Tables: gated behind credentials, manual test runbook documented
- [ ] 7.6 Hadoop catalog: register returns "unsupported by Hadoop catalog (use filesystem discovery)"; drop with purge=true works (filesystem rm); other procedures return typed errors

## 8. Documentation

- [ ] 8.1 New file `docs/sql/procedures.md`: grammar, per-procedure reference, backend support matrix
- [ ] 8.2 Recipe section in `docs/sql/procedures.md`:
  - Golden dataset registration loop (the SF1000 workflow)
  - Disaster recovery from intact S3 + lost catalog DB
  - Catalog migration between Polaris instances
- [ ] 8.3 Update `docs/trino-compatibility.md` to note the matching `CALL` surface for Spark/Trino users porting workloads
- [ ] 8.4 Add a section to README "Why it is cool" mentioning the cross-engine catalog interop story
- [ ] 8.5 Update `nextsteps.md` to reflect completed v1 procedures and link to follow-up changes for rewrite / expire / compact

## 9. Performance and correctness

- [ ] 9.1 Verify procedure latency is dominated by the catalog backend, not SQE overhead. Target: 50 ms p99 for `register_table` against local Polaris on the test stack
- [ ] 9.2 Verify table cache invalidation works under concurrency: two sessions, one registers, the other SELECTs immediately, should not see stale "table not found"
- [ ] 9.3 Run the full TPC-H SF1 benchmark before and after this change to confirm zero overhead on non-procedure queries (the change is additive; this is a sanity check, not an expected regression)

## 10. Follow-ups (out of scope for this change)

- [ ] 10.1 `system.rewrite_data_files` — needs the write path and a job model for long-running operations
- [ ] 10.2 `system.expire_snapshots` — needs a snapshot retention policy hook
- [ ] 10.3 `system.rewrite_manifests` — needs the manifest compaction path
- [ ] 10.4 User-defined procedures (`CREATE PROCEDURE ...`)
- [ ] 10.5 Async procedure execution with a status table
