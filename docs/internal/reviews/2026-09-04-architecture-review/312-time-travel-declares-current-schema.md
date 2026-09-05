# 312: Time-travel providers declare the current schema while the reader uses the snapshot schema

- Scope: sqe-catalog (table_provider.rs, iceberg_scan.rs), coordinator query_handler.rs time-travel call sites
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: open

## Problem

`SqeTableProvider` converts the table's *current* schema once and reuses it for every snapshot:

- `crates/sqe-catalog/src/table_provider.rs:78`: `schema_to_arrow_schema(table.metadata().current_schema())` in `try_new`.
- `table_provider.rs:175-178`: `with_snapshot_id` only records the id; it does not re-derive `schema`.
- `table_provider.rs:232-252`: `projected_columns` and `projected_schema` are built from that current-schema `self.schema`.
- Call sites: `crates/sqe-coordinator/src/query_handler.rs:5155` and `5248` build providers with `with_snapshot_id(snapshot_id)` for `FOR SYSTEM_VERSION AS OF` / `FOR SYSTEM_TIME AS OF`.

The vendored iceberg-rust scan, by contrast, validates and reads with the snapshot's own schema after issue #358 (`vendor/iceberg-rust/crates/iceberg/src/scan/mod.rs:358-380`, test `test_scan_time_travel_keeps_snapshot_schema_issue_358`). The same current-schema assumption appears in the scan node: `compute_table_statistics` aggregates against `table.metadata().current_schema()` (`iceberg_scan.rs:2504`) and `projected_field_ids` are resolved against the current schema (`iceberg_scan.rs:1506-1511`) even when `snapshot_id` is set.

After `ALTER TABLE t ADD COLUMN c`, a query `SELECT * FROM t FOR SYSTEM_VERSION AS OF <old>` declares N+1 columns to DataFusion. Depending on the path, either iceberg-rust rejects the projection ("Column c not found in table") or the direct-read path fills `c` with NULLs from a snapshot in which `c` did not exist. After `DROP COLUMN`, the old snapshot's column is not selectable at all even though its data is in the snapshot.

## Why it matters

Time travel exists to read data as it was; presenting it through the shape it has now is wrong in both directions: columns that did not exist appear (as NULL or as an error), columns that did exist are hidden. Iceberg defines the snapshot's `schema-id` precisely so readers can do this correctly, and iceberg-rust already does. The gap is in the SQE provider layer only.

## Proposed change

1. In `SqeTableProvider::with_snapshot_id`, resolve `snapshot.schema(metadata)` (as `apply_requested_puffin_ndv` already does at `table_provider.rs:378`) and rebuild `self.schema` from it; fall back to current schema only when the snapshot has no schema id.
2. Thread the snapshot schema into `IcebergScanExec` for `projected_field_ids` and `compute_table_statistics` (both take the `Table` and `snapshot_id`; they should derive the schema from the snapshot).
3. Add a `#[must_use]` note that `try_new` + `with_snapshot_id` is the only supported order, or provide `try_new_at_snapshot`.

## Acceptance criteria

- Integration test in `make test-integration`: create table with 2 columns, insert, `ADD COLUMN c`, insert; `SELECT * ... FOR SYSTEM_VERSION AS OF <first snapshot>` returns 2 columns; after `DROP COLUMN b`, the same time-travel query still returns `b`.
- Unit test on `SqeTableProvider::with_snapshot_id` asserting `schema()` equals the snapshot schema.

## Related

- Memory notes "Scan projection by field id" (!784) and "Plan-node width substitution" cover the same class of positional-schema bugs on the current snapshot.
