# fix/trino-timestamp-precision

Part of the `trino-sql-parity` OpenSpec change, tasks 2.4 (precision) and 2.5 (zone).
Design: `openspec/changes/trino-sql-parity/design.md`, section "Timestamps".

## What changed

- `sqe-trino-protocol/encode.rs`: label and fraction digits follow the Arrow unit
  (s 0, ms 3, us 6, ns 9); a zoned value renders `<local> <zone name>`, UTC as `UTC`.
- `sqe-sql/trino_compat.rs`: `TIMESTAMP '<text>'` becomes a cast at the literal's
  precision; a zone is kept through `arrow_cast(..., 'Timestamp(unit, Some("zone"))')`,
  which constant-folds. Bare `TIMESTAMP` inside CAST is precision 3; odd precisions
  round up. DDL types are untouched (pinned by a test).
- `sqe-catalog`: `scalar_value_to_datum` scales s/ms to microseconds; `cast_is_exact`
  accepts zoned to zoned at one unit; analyzer rule rounds narrowing timestamp casts
  half up.
- `sqe-write`: DDL `timestamp(0..=6)` is a microsecond column; CTAS writes any zone
  as `+00:00`.
- `sqe-trino-functions`: `now()` / `current_timestamp` are `Timestamp(ms, UTC)`,
  `localtimestamp` is `Timestamp(ms)`; `at_timezone` / `with_timezone` keep the unit.
  Rebased on the datetime function rewrite, which already reads s/ms input
  (`prepare_extract`, `TsArg`), so the branch's own widening was dropped.
- A unit test pins `to_iso8601` printing `Z` for both `UTC` and `+00:00`.
- New integration tests `tests/it/timestamp_literals_e2e.rs`.

## Gate

- unit (protocol, compat, sql, functions): exit 0
- `sqe-catalog --lib`: exit 0; `sqe-coordinator --lib`: exit 0; `sqe-write --lib`: exit 0
- clippy workspace all-targets: exit 0; `cargo test --workspace --no-run`: exit 0

- After the rebase on main: unit 664/94/197/15/235/200/41, `sqe-catalog` 847,
  `sqe-write` 341, `sqe-coordinator` 851 passed, all exit 0
- `make test-integration`: 424 passed, 1 failed. The failure is
  `a_slow_worker_is_counted_as_a_straggler_and_the_query_completes`, a straggler
  metric timing assertion, under CPU load; rerun alone it passes (1 passed, exit 0)
- `make test-distributed`: 433 passed, 0 failed (the straggler test included)
- Parity against Trino 484 (`tests/parity`, both protocols), compared with the
  2026-11-17 baseline. The branch base predates the string and datetime function
  MRs, so the totals include those:
  - functions HTTP 171 of 296 covered (was 121), signatures MATCH 431 / TYPE 42
    (was 289 / 46); Flight 167 (was 115), MATCH 425 / TYPE 43 (was 277 / 57)
  - queries HTTP 138 MATCH / 15 TYPE (was 137 / 14 TYPE / 2 DIFF), Flight
    138 / 15 (was 129 / 22 TYPE / 2 DIFF); every TYPE left is integer literal typing
  - timestamp rows still off: `to_timestamp` (DataFusion built-in, Trino returns
    `timestamp(3)`) and `timezone_hour` / `timezone_minute` on zoned TIME

## Open

- Narrowing rounds half up; Trino's result for ties (and negative ties) is not
  confirmed on the parity stack.
- `timestamp(2)` and the other odd precisions report the next of 0/3/6/9.
- `with_timezone` on nanosecond input drops sub-microsecond digits.
- Prepared-statement parameters are nanoseconds and now report `timestamp(9)`.
- `hour()`, `date_format` and `CAST(zoned AS timestamp)` use the UTC wall clock.
- The session zone stays unwired.

**NEXT:** override `to_timestamp` with Trino's `timestamp(3)` form, then integer literal typing (task 7).
