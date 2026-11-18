### Fixed

- Trino type syntax in CAST

    `CAST` and `TRY_CAST` accept Trino's `array(T)`, `row(name T, ...)`
    (nested, and inside arrays) and `map(K, V)` targets, as DDL already did.
    `time(p)` and `time(p) with time zone` cast to `TIME`; precision and zone
    are dropped, since DataFusion has no zoned time.
