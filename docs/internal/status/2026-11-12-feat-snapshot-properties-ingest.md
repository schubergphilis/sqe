**STATUS 2026-11-12: `write-snapshot-properties` section 4 done (`feat/snapshot-properties-ingest`).**

- 4.1 `flight_ingest::ingest_snapshot_properties` reads `CommandStatementIngest.options` named `snapshot-property.<key>` (prefix matched without regard to ASCII case, so a mistyped prefix is applied or refused, never dropped), validates them with `SnapshotProperties::try_new`, and lays them over the transaction's or session's set with `merged_with` (caps re-checked on the union). `do_put_statement_ingest` resolves it before admission and before the upload is decoded, so a refused option is an `InvalidArgument` with no file written. The resolved set goes to `write_scope` as before; `ingest.rs` reads it once.

Gates: `cargo test -p sqe-coordinator --lib flight_ingest` (12 passed). The ingest end-to-end assertion is task 5.5.

**NEXT:** section 5 (integration tests), then section 6 (docs); the missing ingest audit record is tracked separately.
