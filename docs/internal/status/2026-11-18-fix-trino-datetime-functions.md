# fix/trino-datetime-functions

Part of the `trino-sql-parity` OpenSpec change, task 6.3 and the date/time
DIFFs of 5.1.

## What changed

- New `crates/sqe-trino-functions/src/datetime_fns.rs` with `formats.rs`
  (Joda pattern walker, MySQL specifier table) and `tests.rs`.
- Extractor aliases (`dow`, `doy`, `day_of_month`, `week_of_year`, `yow`),
  `millisecond`, interval arms, `to_milliseconds`, real `timezone_hour` /
  `timezone_minute`, `timezone`.
- `from_unixtime` (1 to 3 args), `from_unixtime_nanos`, `to_unixtime`,
  `from_iso8601_timestamp[_nanos]`, `from_iso8601_date`, `to_iso8601`,
  `date(varchar|timestamp|date)`, `date_trunc` keeping a date a date (varchar
  input still coerced), `format_datetime`, `parse_datetime`, `date_parse`,
  `date_format`, `parse_duration` (Interval(MonthDayNano)), `sequence` date and
  timestamp forms with the size cap.
- Old UDFs replaced by these are deleted (`from_unixtime`, `to_unixtime`,
  `date`, `date_format`, `date_parse`, `strftime_naive`).
- Behaviour change: bad input to `parse_datetime`, `from_iso8601_timestamp` and
  `date_parse` errors instead of returning NULL or the epoch, as in Trino.
- Book `sql-reference/datetime.md` rows updated.
- Budget: `sqe-trino-functions` ceiling 13000 -> 13150.

## Gate

- `cargo test -p sqe-trino-functions`: exit 0 (147 + 197 passed)
- `cargo test -p sqe-catalog --lib`: exit 0 (835 passed);
  `athena_view_with_trino_idioms_plans`: exit 0 (1 passed)
- `cargo test -p sqe-coordinator --lib custom_udfs_plan_under_duckdb_dialect`: exit 0
- `cargo clippy -p sqe-trino-functions --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo xtask budgets --check`: exit 1 only on `sqe-catalog` (on main).

Not run: integration tests, parity run.

## Open

- `now()` / `current_timestamp` stay `timestamp(6)` without a zone; Trino gives
  `timestamp(3) with time zone`. Needs the coordinator's session time zone.
- `year()`, `month()` and friends extract in UTC for zoned values; Trino uses
  the value's zone.
- Nanosecond precision is lost in `to_iso8601` and `date_trunc`.
- Unconfirmed against Trino 484: two-digit-year pivots, `%D` / `%w` parsing,
  Joda `zzzz`, `from_unixtime` rounding, `to_iso8601` fraction digits.

**NEXT:** parity rerun for the date/time family; zoned extraction and `now()`
with the session zone.
