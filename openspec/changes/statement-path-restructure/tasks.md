## 1. Safety net

- [x] 1.1 Bisect `access_control_e2e::insert_does_not_confer_storage_relocation` (red on `main` since before 5d1f41e62) and fix it (first bad 55b259a87: empty no-op probe is `UPDATE_TABLE` in Polaris; probe now drops the absent `main`)
- [x] 1.2 Add tests that count audit records: refusal after classification, result-cache hit, execution failure, timeout, cancellation (one record each); and pin what the size check, audit-closed check and admission refusal write today
- [x] 1.3 Add a test that a cancelled write still audits the tables of its plan (CTAS; the INSERT path is not covered)
- [x] 1.4 Record a baseline `scripts/compile-profile.sh sqe-coordinator` profile (3 runs on 1e80cd220: `evaluate_obligation` 14.97 s, typeck 1.05 s, `execute_inner` 4 ms)

## 2. Stage split (pilot)

- [x] 2.1 Introduce `StatementRun` with the fields the three parts share; no behaviour change
- [x] 2.2 Split `execute_inner` at the end of the result-cache lookup into `preflight`
- [x] 2.3 Split at the `select!` into `complete` (the `select!` stays in `execute_inner`; `complete` starts after it, so it is not generic over the dispatch future)
- [x] 2.4 Re-profile back to back; box a stage only if the profile bills it (14.71 s after; no stage over 50 ms; nothing boxed)
- [x] 2.5 `make test-integration` green; one merge request (356 passed, 0 failed, on b2f364dad)

## 3. Lift the inline dispatch arms

- [x] 3.1 `Drop` (79 lines), move-only, `FILTER=drop` (`handle_drop_statement`; 16 passed)
- [x] 3.2 `Merge` (79 lines), `FILTER=merge` (`handle_merge_statement`; 12 passed)
- [x] 3.3 `Insert` (95 lines), `FILTER=insert` (`handle_insert_statement`; 23 passed)
- [x] 3.4 `Utility` (140 lines) (`handle_utility_statement`; explain 4, show 11, describe 5, time_zone 1 passed)
- [x] 3.5 `Procedure` (144 lines), `FILTER=procedure` (`handle_procedure_statement`; 14 passed)
- [x] 3.6 `Ctas` (150 lines), `FILTER=ctas` (`handle_ctas_statement`; 16 passed; execute_inner now 702 lines)
- [ ] 3.7 Move the `handle_*` families into `statement/*.rs` files of `impl QueryHandler` blocks, one family per merge request; access-control family last and only after 1.1
  - Files are `query_handler/<family>.rs` child modules (`use super::*;`), following `query_handler/admission.rs`, instead of a `statement/` directory. Moved methods called from `execute_inner` become `pub(super)`.
  - [x] `write` (MERGE, INSERT, CTAS): `query_handler/write.rs`, 370 lines
  - [x] `show` (SHOW CREATE, CATALOGS, SCHEMAS, COLUMNS, TABLES, VIEWS, FUNCTIONS, ROLES; `deny_hidden_table`): `query_handler/show.rs`, 897 lines
  - [x] `time_travel` (FOR SYSTEM_TIME / VERSION AS OF, FOR INCREMENTAL; `TimeTravelCleanup`): `query_handler/time_travel.rs`, 580 lines

## 4. Completion stage

- [ ] 4.1 Introduce the outcome value (status, rows, duration, plan metrics, audit resources)
- [ ] 4.2 Extract span fields and metrics
- [ ] 4.3 Extract the OpenLineage completion
- [ ] 4.4 Extract the audit record and the slow-query log

## 5. Other long functions

- [ ] 5.1 `run_coordinator` (1,058 lines) and `async_main` in `sqe-server`: split at phase seams
- [ ] 5.2 `create_session_context` (759 lines): split at phase seams
- [ ] 5.3 `write_handler.rs`: `handle_merge_equality`, `handle_merge`, `handle_ctas_streaming_on`, `handle_delete_mor`
- [ ] 5.4 `iceberg_scan.rs` `execute` (1,123 lines): read first, decide between a stage split and a design note
- [ ] 5.5 Confirm that `sqe-metrics-core` `new`, config `validate` and `build_sql_info_data` are flat tables and record them as exceptions

## 6. Function-length ratchet

- [ ] 6.1 Try `clippy::too_many_lines` with `#[expect]`; if it cannot hold a recorded function to its length, add a `functions` section to `.config/budgets.json` measured in `xtask/src/budgets.rs`
- [ ] 6.2 Record the functions still over the limit after sections 2 to 5
- [ ] 6.3 Document in `docs/site/book/src/development/crates.md`

## 7. Upstream parity

- [ ] 7.1 Registry shadow test with an allowlist and a reason per entry
- [ ] 7.2 For each shadow (`contains`, `date_format`, `element_at`, `from_unixtime`, `now`, `to_unixtime`, and what the test adds): disable, run tests, delete if green
- [ ] 7.3 `vendor/PATCHES.md`: ledger for `iceberg-rust`, `datafusion-iceberg`, `object_store`, `zstd`, `datafusion-physical-plan`, `sqlite-rs`
- [ ] 7.4 Drop the patches the ledger marks as released upstream, one tree per merge request
