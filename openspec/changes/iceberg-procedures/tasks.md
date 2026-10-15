## 1. Grammar and AST

- [x] 1.1 Add `ProcedureStmt` variant to `crates/sqe-sql/src/ast.rs` with catalog, namespace, procedure name, and argument list -- done differently: typed `ProcedureCall` enum (one variant per procedure, args already bound) in `crates/sqe-sql/src/procedures.rs`, carried as `StatementKind::Procedure` in `crates/sqe-sql/src/classifier.rs`
- [x] 1.2 Add `ProcedureCallArg::{Positional, Named}` enum to capture both call styles -- done differently: no arg enum; `extract_args` in `crates/sqe-sql/src/procedures.rs` normalises to `name => value` pairs, mapping positional args via `positional_spec` (only `rollback_to_snapshot` has a verified Trino positional signature; the rest are named-only)
- [x] 1.3 Wire post-parse transformation in `crates/sqe-sql/src/lib.rs`: detect `Statement::Call` whose `ObjectName` matches `[catalog.]namespace.procedure` and produce `SqeStatement::Procedure(...)` -- done differently: `try_parse_call` (`procedures.rs`) called from the classifier (`classifier.rs`, `Statement::Call` arm) yields `StatementKind::Procedure`; `split_system_name` accepts `system.<proc>` and `<catalog>.system.<proc>`
- [x] 1.4 Reject mixed positional + named arguments at parse time with a clear error -- `extract_args`: "Named and positional arguments cannot be mixed" (Trino wording)
- [ ] 1.5 Unit test: parse `CALL system.register_table('ns', 't', 's3://...')` → `ProcedureStmt { namespace: "system", procedure: "register_table", positional args }` -- not done (2026-10-12 check): `register_table` is named-only; the positional test covers `rollback_to_snapshot` only (`positional_rollback_to_snapshot` in `procedures.rs`)
- [x] 1.6 Unit test: parse `CALL system.register_table(namespace => 'ns', table_name => 't', metadata_location => 's3://...')` → named-arg variant -- done differently: arg names are `table => 'ns.t', metadata_location => ...`; `parses_register_table` in `crates/sqe-sql/src/procedures.rs`
- [x] 1.7 Unit test: parse `CALL iceberg.system.register_table(...)` recognizes the explicit catalog prefix -- done differently: the shared `split_system_name` path is tested with `CALL sqe.system.reproject_column_tags(...)` (`parses_reproject_column_tags_namespace_and_catalog` in `procedures.rs`), not with `register_table`
- [x] 1.8 Unit test: mixed positional+named call returns a parse error -- `mixed_named_and_positional_rejected` in `crates/sqe-sql/src/procedures.rs`

## 2. Procedure framework

- [x] 2.1 Create `crates/sqe-coordinator/src/procedures/mod.rs` with `Procedure` trait, `ProcedureArg`, `ArgType`, `ProcedureArgs`, `ProcedureOutput` -- done differently: no trait or module; the `ProcedureCall` enum (`crates/sqe-sql/src/procedures.rs`) plus a `match` in `MaintenanceHandler::handle` (`crates/sqe-coordinator/src/maintenance.rs`), output `Vec<RecordBatch>`
- [x] 2.2 Implement `bind_args(schema, call_args) -> Result<ProcedureArgs>`: matches named or positional args against the declared schema, applies defaults, coerces literal types -- done differently: per-procedure `parse_*` functions using `take_table` / `take_option` / `expect_string|i64|bool|...` and `expect_no_remaining` in `procedures.rs`
- [x] 2.3 Implement `ProcedureRegistry` with `default()` constructor that populates the four v1 procedures -- done differently: static name dispatch in `try_parse_call` (`procedures.rs`); no registry type
- [x] 2.4 Wire `ProcedureRegistry` onto `SessionContext` via the existing dependency injection; one shared registry across sessions (immutable after startup) -- done differently: one `MaintenanceHandler` built at startup and held by `QueryHandler` (`crates/sqe-coordinator/src/query_handler.rs`, `maintenance_handler` field)
- [ ] 2.5 Unit test: bind_args accepts the matching schema, rejects too-few / too-many args, rejects wrong types -- not done (2026-10-12 check): too-few and too-many/unknown args are tested (`register_table_requires_metadata_location`, `register_table_rejects_unknown_arg`, `unknown_option_is_error` in `procedures.rs`); no wrong-type rejection test
- [x] 2.6 Unit test: unknown `system.foo` dispatch returns a typed error -- done differently: unknown procedures fall through to `StatementKind::Call` and the generic "not supported" error; `unknown_procedure_returns_none` (`procedures.rs`), `unknown_procedure_falls_through_to_call_error` (`crates/sqe-coordinator/tests/it/maintenance_procedures_test.rs`)

## 3. `system.register_table`

- [x] 3.1 Implement `RegisterTableProcedure` in `crates/sqe-coordinator/src/procedures/register_table.rs` -- done differently: `MaintenanceHandler::register_table` in `crates/sqe-coordinator/src/maintenance.rs`
- [x] 3.2 Arg schema: `namespace: String`, `table_name: String`, `metadata_location: String` -- done differently: `table => 'ns.t'` (optionally catalog-qualified) plus `metadata_location => '...'` (`parse_register_table` in `crates/sqe-sql/src/procedures.rs`)
- [x] 3.3 Policy hook: call `session.policy().require_create_in_namespace(&ns)` before invoking the catalog -- done differently: Admin tier via `procedure_tier` / `authorize_or_deny` in `maintenance.rs` (requires one of `[auth] admin_roles`; denials audited); the catalog then authorizes the user's own bearer token
- [x] 3.4 Dispatch to `iceberg::catalog::Catalog::register_table(&ident, metadata_location)`
- [x] 3.5 On success, invalidate the session's table cache for the registered identifier so subsequent SELECTs see it -- done differently: `sqe_catalog::invalidate_rest_catalog_cache_all()` in `register_table`, plus session-context and result-cache invalidation after every procedure in `query_handler.rs`
- [x] 3.6 Return a single-row `RecordBatch` with columns (table_identifier, snapshot_id, metadata_location) -- plus a `status` column
- [ ] 3.7 Integration test: CTAS a managed table, capture its current metadata_location from `iceberg.<schema>.<table>$metadata`, drop the table, re-register, run SELECT and verify identical rows -- not done (2026-10-12 check): no `register_table` integration test under `crates/*/tests`

## 4. `system.drop_table`

- [x] 4.1 Implement `DropTableProcedure` in `crates/sqe-coordinator/src/procedures/drop_table.rs` -- done differently: `MaintenanceHandler::drop_table` in `crates/sqe-coordinator/src/maintenance.rs`
- [x] 4.2 Arg schema: `namespace: String`, `table_name: String`, `purge: Boolean = false` -- done differently: `table => 'ns.t'`, `purge => bool` defaulting to false (`parse_drop_table` in `crates/sqe-sql/src/procedures.rs`)
- [x] 4.3 Policy hook: `require_drop_on_table(&ident)`; when `purge => true`, additionally `require_modify_on_namespace(&ns)` -- done differently: Admin tier via `procedure_tier` / `authorize_or_deny` in `maintenance.rs`, regardless of `purge`
- [x] 4.4 Dispatch to `Catalog::drop_table` or `Catalog::purge_table` depending on the purge flag -- done differently (#836): always `Catalog::drop_table`; with `purge => true` SQE then deletes the table prefix with the caller's `FileIO::delete_prefix` instead of the REST `purgeRequested` flag (which needs Polaris `DROP_WITH_PURGE_ENABLED` and deletes asynchronously with the catalog's credentials). Refused up front when `gc.enabled=false`, the location is a bucket root, a write path or the metadata file sits outside the location, or another table in the catalog shares the location (`purge_prefix` / `refuse_if_location_shared` in `maintenance.rs`)
- [x] 4.5 Invalidate table cache on success -- `invalidate_rest_catalog_cache_all()` in `drop_table`, plus the post-procedure invalidation in `query_handler.rs`
- [x] 4.6 Integration test: register a table, drop with purge=false, verify SELECT fails and data files remain on the object store -- done differently: `drop_table_without_purge_leaves_the_files` in `tests/it/maintenance_procedures_test.rs` creates the table rather than registering it and checks the data file survives
- [x] 4.7 Integration test: register, drop with purge=true, verify SELECT fails and data directory is empty -- done differently: `drop_table_purge_deletes_the_table_prefix` creates the table, then checks the prefix is empty and SELECT fails; `drop_table_purge_refuses_while_another_table_shares_the_location` covers the register-a-twin case

## 5. `system.set_current_snapshot` and `system.rollback_to_snapshot`

- [x] 5.1 Implement `SetCurrentSnapshotProcedure` in `crates/sqe-coordinator/src/procedures/snapshots.rs` -- done differently: `MaintenanceHandler::move_main_ref(.., require_ancestor = false, ..)` in `crates/sqe-coordinator/src/maintenance.rs`
- [x] 5.2 Arg schema: `namespace`, `table_name`, `snapshot_id: Int64` -- done differently: `table => 'ns.t', snapshot_id => <i64>` (`parse_set_current_snapshot` / `parse_rollback_to_snapshot` in `crates/sqe-sql/src/procedures.rs`; rollback also takes Trino positional `(schema, table, snapshot_id)`)
- [x] 5.3 Use `iceberg::transaction::Transaction::new(&table).set_current_snapshot(snapshot_id)` -- done differently: `Transaction::new(&table).create_branch(MAIN_BRANCH).with_snapshot_id(snapshot_id)` in `move_main_ref`
- [x] 5.4 Implement `RollbackToSnapshotProcedure` using the matching iceberg-rust helper -- done differently: same `move_main_ref` with `require_ancestor = true` (target must be an ancestor of the current snapshot)
- [x] 5.5 Policy hook on both: `require_modify_on_table(&ident)` -- done differently: Admin tier via `procedure_tier` / `authorize_or_deny` in `maintenance.rs`
- [x] 5.6 Integration test: CTAS, INSERT, capture the post-CTAS snapshot id, INSERT again, `set_current_snapshot` back to the captured id, verify SELECT returns the post-CTAS row count -- done differently: `rollback_rejects_non_ancestor_but_set_current_allows` in `crates/sqe-coordinator/tests/it/maintenance_procedures_test.rs` (CREATE + two INSERTs, rollback to S1, then `set_current_snapshot` to S2 and checks the row count)
- [x] 5.7 Integration test: rollback_to_snapshot variant — same flow, verify a new snapshot was appended (audit trail preserved) -- done differently: `rollback_to_snapshot_reverts_data` in `maintenance_procedures_test.rs` checks the reverted row count; rollback moves the `main` ref and appends no snapshot (snapshot log kept), so there is no new-snapshot assertion

## 6. Statement routing

- [x] 6.1 Extend `crates/sqe-coordinator/src/query_handler.rs` to match `SqeStatement::Procedure(stmt)` and call `registry.dispatch(stmt, session)` -- done differently: the `StatementKind::Procedure(ref call)` arm calls `self.maintenance_handler.handle(session, call)`
- [x] 6.2 Wrap `ProcedureOutput` in `QueryResult` so existing Flight SQL and Trino HTTP serialization works unchanged -- done differently: handlers return `Vec<RecordBatch>` through the normal statement result path
- [ ] 6.3 Audit log every procedure invocation: name, args (with metadata_location redacted at info level), principal, outcome -- not done as specified (2026-10-12 check): CALL goes through the generic per-statement audit, and `authorize_or_deny` audits denials, but `register_table` logs `metadata_location` unredacted at info level
- [ ] 6.4 Integration test: invoke from both wire protocols; verify result row reaches the client -- not done (2026-10-12 check): procedure ITs call `QueryHandler::execute` directly, not Flight SQL or Trino HTTP

## 7. Cross-backend support matrix

- [ ] 7.1 REST (Polaris): smoke test against existing `docker-compose.test.yml` stack -- partial (2026-10-12 check): `needs:polaris` tests cover `set_current_snapshot` / `rollback_to_snapshot` (`maintenance_procedures_test.rs`); none for `register_table` / `drop_table`
- [ ] 7.2 JDBC (SQLite via existing `sqe-catalog-loader` feature): roundtrip test in `tests/integration/jdbc_catalog.rs`
- [ ] 7.3 HMS: feature-gated test under `sqe-catalog/hms` (requires Thrift Hive container, gated behind `--features hms`)
- [ ] 7.4 Glue: gated behind credentials, manual test runbook documented in `docs/sql/procedures.md`
- [ ] 7.5 S3 Tables: gated behind credentials, manual test runbook documented
- [ ] 7.6 Hadoop catalog: register returns "unsupported by Hadoop catalog (use filesystem discovery)"; drop with purge=true works (filesystem rm); other procedures return typed errors

## 8. Documentation

- [ ] 8.1 New file `docs/sql/procedures.md`: grammar, per-procedure reference, backend support matrix -- not done (2026-10-12 check): `docs/site/book/src/sql-reference/procedures.md` exists but lists these four procedures only in its Permissions table, with no reference rows or backend matrix
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

- [x] 10.1 `system.rewrite_data_files` — needs the write path and a job model for long-running operations -- `ProcedureCall::RewriteDataFiles` (`crates/sqe-sql/src/procedures.rs`), local and distributed handlers in `crates/sqe-coordinator/src/maintenance.rs`, documented in `docs/site/book/src/sql-reference/procedures.md`
- [x] 10.2 `system.expire_snapshots` — needs a snapshot retention policy hook -- `ProcedureCall::ExpireSnapshots` (`older_than`, `retain_last`), handler in `maintenance.rs`, IT `expire_snapshots_retain_last_commits`
- [x] 10.3 `system.rewrite_manifests` — needs the manifest compaction path -- `ProcedureCall::RewriteManifests`, handler in `maintenance.rs`, IT `rewrite_manifests_commits_and_returns_summary`
- [ ] 10.4 User-defined procedures (`CREATE PROCEDURE ...`)
- [ ] 10.5 Async procedure execution with a status table
