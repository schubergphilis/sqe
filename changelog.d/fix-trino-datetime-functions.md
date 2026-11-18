### Added

- Trino date and time functions

    `dow`, `doy`, `day_of_month`, `week_of_year`, `yow` / `year_of_week`,
    `to_milliseconds`, `timezone`, `from_unixtime_nanos`,
    `from_iso8601_timestamp_nanos`, `from_iso8601_date`, `parse_duration`,
    interval forms of `day` / `hour` / `minute` / `second` / `millisecond` /
    `year` / `month`, and `sequence(date, date)` with an implicit one-day step.

### Fixed

- Date and time functions return Trino's types and values

    `from_unixtime`, `from_iso8601_timestamp` and `parse_datetime` return
    `timestamp(3) with time zone`; `to_iso8601` writes UTC as `Z`;
    `date(varchar)` parses instead of returning NULL; `date_trunc` on a date
    returns a date; `timezone_hour` / `timezone_minute` return the zone's
    offset. `format_datetime` walks the full Joda pattern and `date_parse` /
    `date_format` the full MySQL specifier table; a value `date_parse` cannot
    parse is an error instead of the epoch. `now()` is still `timestamp(6)`
    without a zone.
