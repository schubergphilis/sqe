**STATUS 2026-10-12: distributed scans over files that predate `ADD COLUMN` or a rename read correctly on workers (`fix/worker-add-column-831`, #831).**

A worker projects by field id. Parquet emits the projected columns in file order and drops any the file lacks. The worker took its Flight stream schema from the first file. A fragment that mixed files from before and after an `ADD COLUMN` failed in the Flight encoder ("number of columns(2) must match number of fields(4)"), classed `worker_internal`. A renamed column could only be realigned on the coordinator by position, so a rename plus a reordered SELECT could swap values between same-typed columns.

The fix:

- **Ticket v4.** `ScanTask` gains an optional `projection_fill_b64`: a one-row Arrow IPC batch with the scan's projected schema, holding each column's `initial_default`, or NULL without one. The coordinator sends it only when a projected column was added, renamed or retyped in any schema in the table's history (`IcebergScanExec::projection_evolved`). Other tickets stay v3 and keep their old encoding, so a v3 worker still accepts every ticket for a table that never evolved. A v3 worker refuses a v4 ticket, as it should.
- **Worker conform** (`sqe-worker/src/projection_fill.rs`). With a fill, the worker declares the fill's schema as the stream schema. It reshapes every decoded batch to it: columns are matched by `PARQUET:field_id`, or by name when the file carries no ids. A narrower file type is cast up, and a missing column is filled from the default. Hive scans are unchanged.
- A worker-pushed predicate on a column its file lacks still fails to bind and is skipped. The coordinator's filter stays authoritative and sees the filled default.

Tests:
- Unit: the fill and conform in `projection_fill.rs` cover default vs NULL, int-to-bigint cast, rename plus reorder by field id, name fallback, and a fill narrower than the projection. The planner covers the v4 round trip and refusal by a v3 worker.
- Polaris + in-process worker: `distributed_schema_evolution_e2e.rs` covers two pre-ALTER files, `ADD COLUMN ... DEFAULT 'eu'`, `ADD COLUMN note`, a post-ALTER file, then `RENAME COLUMN name TO label`. It checks projection, reordered projection, `WHERE region = 'eu'`, `WHERE note IS NULL` and `WHERE region IS NULL`. With the fill disabled the test fails with the original error. `distributed_failure_e2e` still passes 6/6.

Open edges, not addressed here:
- A worker's pushed predicate binds by name against the file schema. A swap-rename (`a`->`b`, `b`->`a`) could make the worker pre-filter on the wrong column. This is pre-existing.
- The coordinator's positional fallback in `reassemble_worker_batch` stays for older workers.

Stacked on `fix/initial-default-830` (which is on `fix/column-default-828`).

**NEXT:** dbt-sqe per-connection transactions (openspec `single-table-transactions` 5.4), then #816 and #803.
