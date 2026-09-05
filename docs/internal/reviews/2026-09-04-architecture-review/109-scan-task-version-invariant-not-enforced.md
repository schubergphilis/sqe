# 109: `ScanTask.version` is a hand-set integer; nothing ties it to the fields a worker will honour

- Scope: sqe-planner (scan_task, scan_morsel), sqe-coordinator producers, sqe-worker consumer
- Severity: medium
- Category: reliability
- Effort: S (hours)
- Status: open

## Problem

`ScanTask` is the wire contract between coordinator and worker: JSON via
`to_bytes`/`from_bytes` (`crates/sqe-planner/src/scan_task.rs:269-276`), every
optional field `#[serde(default)]`, and a `version: u32` whose doc says
"Workers reject unsupported versions" (`scan_task.rs:85-89`). The worker's only
check is `validate_version` (`scan_task.rs:279-286`, called at
`sqe-worker/src/flight_service.rs:969`), which accepts any of 1, 2, 3.

The version is chosen by hand at each construction site, independent of the
payload:

- `sqe-coordinator/src/query_handler.rs:3996` sets `SCAN_TASK_VERSION_V1` for
  whole-file tasks and `:4024` sets `SCAN_TASK_VERSION_V2` for byte-range
  morsels.
- `sqe-coordinator/src/hive_scan_tasks.rs:128` sets `V3`.
- `sqe-coordinator/src/distributed_scan.rs:1525,1622` write the literal `1`.

The worker applies morsel fields whenever they are present, regardless of
version: `sqe-worker/src/executor.rs:1102` (`row_group_start`) and `:1122`
(`start_byte`/`end_byte`). Serde ignores unknown fields (no
`deny_unknown_fields` on `ScanTask`). So the guard against a mixed-version
fleet is the single integer, and it is correct only as long as every producer
remembers to bump it when it fills a newer field. `ScanTask` has 20-plus
public fields and a public constructor by struct literal, so the compiler
cannot help.

## Why it matters

The failure is silent and produces wrong data, not an error. A producer that
fills `start_byte`/`end_byte` but leaves `version: 1` (the
`distributed_scan.rs` literals are one refactor away from this) is accepted by
an older worker that predates morsels. That worker ignores the unknown fields
and scans the whole file for each morsel, so a file split into four morsels is
read four times and the query returns four copies of every row. Nothing in
the response says so. Rolling upgrades of a worker `StatefulSet` behind a
newer coordinator is exactly the window where this happens.

The same shape applies to `hive` (V3): a V2 worker given a V3 task with
`version: 2` would read the Iceberg-style fields and miss the Hive descriptor.

## Proposed change

1. Make the version derived, not set. Give `ScanTask` a private constructor
   `ScanTask::builder(fragment_id, files, storage)` with `with_byte_range`,
   `with_row_groups`, `with_hive(desc)` methods; `build()` computes `version`
   as the maximum required by the fields present. Remove the `pub version`
   field from the struct literal surface (keep it `pub` for reading).
2. Add a producer-side `validate_payload()` that returns `Err` when a field
   is present but `version` is too low, and call it in `to_bytes`.
3. Worker side: after `validate_version`, reject tasks whose version is lower
   than the fields they carry (same check, run on the consumer), and add
   `#[serde(deny_unknown_fields)]` behind a `strict` flag or log a WARN when
   unknown keys were present so an old worker at least says it is dropping
   data. Simplest robust option: have the coordinator send its `CURRENT`
   version in the task and have the worker reject anything above its own
   `CURRENT`, which today's `validate_version` already does; step 1 is what
   makes that reliable.
4. Replace the `version: 1` literals in `distributed_scan.rs` with the builder.

## Acceptance criteria

- `grep -rn 'version:' crates/sqe-coordinator/src --include=*.rs | grep -v test`
  finds no `ScanTask` literal setting `version`.
- Unit test in `scan_task.rs`: a task with `start_byte: Some(_)` and
  `version: 1` fails `validate_payload()`; a builder-built task with a byte
  range reports `version == SCAN_TASK_VERSION_V2`; with `hive: Some(_)`
  reports `V3`.
- Unit test in `sqe-worker`: decoding a task with `start_byte` and `version: 1`
  is rejected with `invalid_argument`.
- `make test-distributed` passes.

## Related

- `108-sqe-planner-dead-distributed-planning-code.md`
- Memory "Scan projection by field id" (worker path open) touches the same
  struct.
- Scope 600 (sqe-worker) may report the consumer-side handling; this file owns
  the type in `sqe-planner`.
