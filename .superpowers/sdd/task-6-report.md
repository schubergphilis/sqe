# Task 6 Report: LogShipExporter trait + OCSF-to-ShipRecord mapping

## Files changed
- `crates/sqe-metrics/Cargo.toml` - added `async-trait = { workspace = true }`
- `crates/sqe-metrics/src/audit/export/record.rs` - new file (trait, types, mapping, tests)
- `crates/sqe-metrics/src/audit/export/mod.rs` - wired `record` module + re-exports

## Types and trait

`Severity { Info, Warn, Error }` - derived from OCSF `status_id`.

`ShipRecord` - nine public fields: `seq: u64`, `time_unix_ms: i64`, `severity: Severity`,
`body: serde_json::Value`, `class_uid: i64`, `category_uid: i64`, `kind: String`,
`status_id: i64`, `user_name: String`.

`LogShipExporter` - `#[async_trait::async_trait]` on `Send + Sync` trait with one method:
`async fn export_batch(&self, records: &[ShipRecord]) -> Result<(), String>`.
Dyn-compatible. No OTLP/network code (Task 8).

## Mapping logic (`ocsf_to_ship_record`)

Parses via `serde_json::from_str::<Value>`. Returns `None` on parse failure (caller skips).
Field derivation:
- `seq` from `metadata.sequence`, default 0
- `time_unix_ms` from `time`, default 0
- `severity`: `status_id == 2` -> `Warn`, anything else -> `Info`
- `class_uid`, `category_uid`, `status_id` from top-level OCSF fields
- `kind` from `metadata.kind` then `kind`, then empty string
- `user_name` from `actor.user.name`, else empty string
- `body` = full parsed `Value`

## async-trait usage

`#[async_trait::async_trait]` on the trait definition makes the trait dyn-compatible.
The shipper (Task 7) can hold `Arc<dyn LogShipExporter>` and call `export_batch`.

## TDD evidence

- Wrote tests in `record.rs` before wiring the module into `export/mod.rs`.
- First `cargo test -p sqe-metrics record` run: 1 test (from another module), 0 from record
  (tests existed but module was unreachable - conceptual RED).
- After wiring `mod.rs`: all 6 tests compiled and passed immediately (GREEN, first run).
- No test failures at any point.

## Test results

`cargo test -p sqe-metrics`: 87 passed, 0 failed.
`cargo clippy -p sqe-metrics --all-targets -- -D warnings`: clean.

Tests written:
1. `success_line_maps_to_info_with_correct_fields` - seq=77, Severity::Info, class_uid=6005,
   category_uid=6, user_name="alice", body roundtrip, time_unix_ms > 0
2. `failure_line_maps_to_warn` - status_id=2 -> Severity::Warn
3. `garbage_line_returns_none` - unparseable JSON and empty string both None
4. `missing_optional_fields_default_gracefully` - `{}` -> all fields zero/empty, Severity::Info
5. `kind_field_from_audit_kind_variant` - Auth kind -> class_uid=3002
6. Plus the pre-existing chain tamper test.

## Concerns

None. The `kind` field in SQE's OCSF shape is not emitted as a top-level or metadata field
(OCSF uses `class_uid` as the discriminant instead). The `kind` field will be empty string
for all real SQE events. If Task 7 or 8 needs a human-readable kind string, derive it from
`class_uid` in a helper rather than relying on this field.
