### Fixed

- `now()`, `date_format` and `element_at` no longer switch at random
  between SQE's Trino version and DataFusion's own

    DataFusion ships functions that claim these names too: `now` itself,
    `to_char` (which takes `date_format` as an alias) and `map_extract`
    (which takes `element_at`). A query with a memory quota rebuilds its
    session, and the rebuild could hand the name back to DataFusion's
    function. `now()` then returned nanoseconds instead of microseconds,
    `date_format` read its pattern as `to_char` syntax, and `element_at`
    on a map returned a list. Which one a query got depended on chance.
    SQE now takes the names away from those built-ins, so every query
    runs the same function.

- `current_timestamp` now returns the same type as `now()`, as in Trino

    It is the same function now. It returns `timestamp(6)` (microseconds,
    as Iceberg stores them) instead of DataFusion's nanosecond timestamp
    with a `+00:00` zone.
