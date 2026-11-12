## 1. Write handler

- [x] 1.1 Define `IngestMode { on_missing: OnMissing::{Fail, Create}, on_existing: OnExisting::{Append, Fail, Replace} }` in `sqe-write` with a `Default` of `(Fail, Append)`
- [x] 1.2 `handle_ingest_streaming` takes `mode: IngestMode` and `stream_schema: Option<SchemaRef>`; call sites pass the default
- [x] 1.3 Existing table + `Fail`: return `DuplicateTable` before reading past the schema peek
- [x] 1.4 Missing table + `Create`: refuse in a BEGIN transaction (`AutocommitWriteConflict`); otherwise build the Iceberg schema with the CTAS helpers, `ensure_namespace`, create with `create_table_properties` on the autocommit catalog; an "already exists" from the catalog falls through to `on_existing`
- [x] 1.5 Call `admit_txn_write` before writing
- [x] 1.6 Existing table + `Replace`: `refuse_replace_under_policy(.., "ingest replace")`, commit with `commit_written_files(.., overwrite = true)`
- [x] 1.7 Unit tests for the mode mapping and the refusal order

## 2. Coordinator

- [x] 2.1 Map `table_definition_options` to `IngestMode` (absent / `UNSPECIFIED` -> default); unit test every ADBC mode
- [x] 2.2 In create modes, read the decoded stream until its schema is known, keep the first batch, refuse a schema-less stream with `InvalidArgument`
- [x] 2.3 Keep the result-cache invalidation for the target after create and replace

## 3. Tests and docs

- [x] 3.1 Drop the `bug:#881` markers in `tests/it/flight_ingest_e2e.rs`
- [x] 3.2 Add e2e tests: create_append twice, replace (rows + history), replace with a mismatched stream, empty-stream create, create then rollback
- [x] 3.3 `make test-integration FILTER=flight_ingest_e2e` green
- [x] 3.4 Changelog fragment, status file
- [ ] 3.5 Close #881 (maintainer, after the MR merges)
