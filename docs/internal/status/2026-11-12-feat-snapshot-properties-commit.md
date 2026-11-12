**STATUS 2026-11-12: `write-snapshot-properties` section 3 done (`feat/snapshot-properties-commit`, stacked on `feat/snapshot-properties-statement`).**

- 3.1 `StatementScope::snapshot_properties`, a `write_scope` argument, inherited by `scope()`, `with_params`, `with_insert_target`; accessor `sqe_catalog::txn::snapshot_properties()`.
- 3.2 Resolved at the three `write_scope` callers: transaction set inside one, session set outside. CTAS/DDL that runs autocommit inside a client transaction (#824) takes the transaction's set (recorded in the design).
- 3.3 `sqe-write/src/write_handler/snapshot_props.rs`: `stamp_append`, `stamp_row_delta`, `stamp_overwrite`, called at every user-write action site, read once before the retry loop. Maintenance, Puffin carry, the ledger and the lease are untouched; compaction does not inherit parent properties.
- 3.4 `commit_empty_snapshot`: with properties set, every skip branch (no file added or removed) commits an empty `fast_append`, after the write-access and transaction admission checks. No vendor change: iceberg-rust accepts an empty append with properties.
- 3.5 `QueryStats::snapshot_property_keys` (serde-skipped when empty, so audit hash chains are unchanged), OCSF `unmapped.snapshot_property_keys`, filled for data writes from the statement scope. Flight SQL bulk ingest emits no audit record at all (pre-existing; separate issue).

Gates: `cargo test -p sqe-catalog txn`, `cargo test -p sqe-write --lib` (also with `test-sqlite`, 339 passed), coordinator `snapshot_properties` tests, clippy on four crates with `-D warnings`, `cargo xtask budgets --check`, `cargo check-server`, `cargo dev`. `write_handler.rs` is at 15859 of 16000 lines.

**NEXT:** section 4 (ingest options).
