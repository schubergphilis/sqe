### Fixed

- Timestamp precision and zone follow Trino

    `TIMESTAMP '...'` is typed by its fraction digits (0, 3, 6 or 9, rounded up),
    `CAST(x AS TIMESTAMP)` is `timestamp(3)`, and `TIMESTAMP(p)` with p of 1, 2,
    4, 5, 7 or 8 works (it reports the next of 0/3/6/9). Result types report the
    precision of the Arrow unit on HTTP and Flight, with that many fraction
    digits; nanosecond values now say `timestamp(9)` instead of 6. A literal
    with a zone keeps it (`TIMESTAMP '2024-01-01 10:00:00 Europe/Amsterdam'`
    renders in that zone, UTC as ` UTC`). `now()` and `current_timestamp` return
    `timestamp(3) with time zone` in UTC, `localtimestamp` returns
    `timestamp(3)`, and `at_timezone` / `with_timezone` keep the input unit.
    Narrowing casts round half up. Coarse literals in DELETE, UPDATE and MERGE
    predicates are scaled to microseconds for Iceberg pushdown, a zoned to
    zoned cast no longer blocks pushdown, DDL `timestamp(0..3)` columns can be
    created, and CTAS writes any zoned timestamp as UTC `timestamptz`. Known
    differences: `timestamp(2)` reports `timestamp(3)`, and `with_timezone` on
    nanosecond input drops digits below the microsecond.
