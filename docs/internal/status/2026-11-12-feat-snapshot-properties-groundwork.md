**STATUS 2026-11-12: `write-snapshot-properties` tasks 1.1 and 1.3 done (`feat/snapshot-properties-groundwork`).**

- 1.1: `t$snapshots` builds `summary` with `serde_json` (`summary_json` in `sqe-catalog/src/iceberg_metadata_tvf.rs`). Five unit tests: empty, quotes and backslashes, control characters, non-ASCII, key order.
- 1.3: `sqe-core/src/snapshot_properties.rs`, a validated `BTreeMap` newtype with `try_new` and `merged_with` (re-checks the caps). 17 unit tests. Choices within the spec: `added-dvs`/`removed-dvs` count as computed keys; duplicate keys match exactly, as in an Iceberg summary; empty values are allowed; any Unicode control character (C0, DEL, C1) is refused.

Gates: unit tests for both modules, `cargo clippy -p sqe-core -p sqe-catalog --all-targets -D warnings`, `cargo xtask budgets --check`, `cargo check-server`. Integration tests not run (no write path uses the type yet).

**NEXT:** section 2 (classifier, session state, `TxnScope`, dispatch).
