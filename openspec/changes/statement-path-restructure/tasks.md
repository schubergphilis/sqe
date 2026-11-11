## 1. Safety net

- [ ] 1.1 Bisect `access_control_e2e::insert_does_not_confer_storage_relocation` (red on `main` since before 5d1f41e62) and fix it
- [ ] 1.2 Add tests that count audit records: preflight refusal, execution failure, timeout, cancellation (one record each)
- [ ] 1.3 Add a test that a cancelled write still audits the tables of its plan
- [ ] 1.4 Record a baseline `scripts/compile-profile.sh sqe-coordinator` profile

## 2. Stage split (pilot)

- [ ] 2.1 Introduce `StatementRun` with the fields the three parts share; no behaviour change
- [ ] 2.2 Split `execute_inner` at the end of the result-cache lookup into `preflight`
- [ ] 2.3 Split at the `select!` into `complete`
- [ ] 2.4 Re-profile back to back; box a stage only if the profile bills it
- [ ] 2.5 `make test-integration` green; one merge request

## 3. Lift the inline dispatch arms

- [ ] 3.1 `Drop` (79 lines), move-only, `FILTER=drop`
- [ ] 3.2 `Merge` (79 lines), `FILTER=merge`
- [ ] 3.3 `Insert` (95 lines), `FILTER=insert`
- [ ] 3.4 `Utility` (140 lines)
- [ ] 3.5 `Procedure` (144 lines), `FILTER=procedure`
- [ ] 3.6 `Ctas` (150 lines), `FILTER=ctas`
- [ ] 3.7 Move the `handle_*` families into `statement/*.rs` files of `impl QueryHandler` blocks, one family per merge request; access-control family last and only after 1.1

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

- [ ] 6.1 `functions` section in `.config/budgets.json`; measurement in `xtask/src/budgets.rs`
- [ ] 6.2 Record the functions still over the limit after sections 2 to 5
- [ ] 6.3 Document in `docs/site/book/src/development/crates.md`

## 7. Upstream parity

- [ ] 7.1 Registry shadow test with an allowlist and a reason per entry
- [ ] 7.2 For each shadow (`contains`, `date_format`, `element_at`, `from_unixtime`, `now`, `to_unixtime`, and what the test adds): disable, run tests, delete if green
- [ ] 7.3 `vendor/PATCHES.md`: ledger for `iceberg-rust`, `datafusion-iceberg`, `object_store`, `zstd`, `datafusion-physical-plan`, `sqlite-rs`
- [ ] 7.4 Drop the patches the ledger marks as released upstream, one tree per merge request
