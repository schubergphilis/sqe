**STATUS 2026-11-16: Flight SQL ingest limits documented; snapshot-property and refusal paths proven end to end (`feat/flight-ingest-snapshot-id-904`, #904, openspec write-snapshot-properties 5.1, 5.5, 6.2).**

New tests, all in the `it` binary:

- `flight_ingest_e2e::ingest_snapshot_property_lands_in_every_mode_summary`: create, append and replace, plus append and replace inside a Flight SQL transaction. Each puts `snapshot-property.run` into its committed summary.
- `flight_ingest_e2e::ingest_snapshot_property_is_per_request_and_reserved_keys_fail`: the next ingest without the option has no `run`. `snapshot-property.operation` is refused with `InvalidArgument`, with no new snapshot and no new rows.
- `flight_ingest_e2e::sql_snapshot_property_lands_in_every_write_kind_summary`: `SET SNAPSHOT_PROPERTIES` reaches the summary of CTAS, INSERT, UPDATE, DELETE, MERGE and INSERT OVERWRITE, in a transaction and outside one.
- `flight_ingest_e2e::ingest_message_over_four_mib_is_refused`: six 1 MiB rows go through, because the Rust encoder splits them into separate messages. One 5 MiB row fails with `OutOfRange`, with no snapshot.
- `access_control_e2e::flight_ingest_without_write_access_is_refused_and_audited`: alice ingests into `sales_wh.ac.orders` under Ranger. She gets `PermissionDenied`, one failure audit record (`ACCESS_DENIED`, no `rows_written`), and the row count is unchanged.

Fixed, found by the 4 MiB test: an upload that broke in transport failed the ingest as `Internal` / `EXECUTION_FAILED`, with tonic's status buried in a "Stream error: External error: Tonic error: ..." message. `flight_ingest::UploadError` now keeps the first error the upload stream yields, and `statement_ingest_inner` answers with that status (`OutOfRange`; audit `INVALID_ARGUMENTS`). Unit test: `flight_ingest::tests::a_failed_upload_answers_with_the_transport_status`.

The test harness gained `setup_ranger_handler_audited` and `IngestServer::start_ranger_audited` / `client_as`.

Docs: the connecting page has a "Bulk ingest" section. It covers the option, its key rules, the 4 MiB per-message limit (the coordinator sets no `max_decoding_message_size`), admission, and the write-access refusal.

Finding, read-only: the ingest has no Ranger gap. SQL INSERT and ingest both call `SessionCatalog::check_write_access`, a probe commit that Polaris authorizes from Ranger. No SQE-side Ranger write check exists on either path. The probe is skipped for non-REST catalogs.

Left open on #904: the snapshot id in the `DoPut` reply. arrow-flight's `FlightSqlService::do_put` wraps `do_put_statement_ingest`'s `i64` into `DoPutUpdateResult { record_count }`, and that cannot be overridden without vendoring arrow-flight. The options are in the merge request.

Gates (2026-11-16, after the last edit):

- `make test-integration FILTER=flight_ingest_e2e`: 25 passed, 0 failed (the 4 MiB test failed before the `UploadError` fix with `Internal`).
- `scripts/access-control-test.sh flight_ingest_without_write_access`: 1 passed.
- `cargo test -p sqe-coordinator --lib flight_ingest`: 14 passed.
- `cargo clippy -p sqe-coordinator --all-targets -- -D warnings`: clean.

**NEXT:** decide how the ingest returns its snapshot id (response header, vendored arrow-flight field, or a documented lookup); then openspec write-snapshot-properties 5.2-5.4 and 6.1.
