### Changed

- `to_json(NULL)` returns SQL NULL instead of the text `null`

    Trino's `CAST(NULL AS JSON)` is NULL, and `CAST(x AS JSON)` is
    rewritten to `to_json(x)`, so a NULL input now stays NULL. Queries
    that compared the result with `'null'` see NULL instead. A NULL
    nested in an array, map or row is still written as `null`.

- `json_parse` and `JSON '...'` literals return Trino's canonical text

    Output is compact with object keys sorted at every level:
    `json_parse('{"b": 1, "a": 2}')` returns `{"a":2,"b":1}`, where it
    kept the input's key order. A `JSON '<text>'` literal is now read as
    `json_parse('<text>')`, so it comes back canonical too instead of
    verbatim; a malformed literal gives the text `null` (Trino raises an
    error).

### Fixed

- `CAST(x AS JSON)` of an array, map or row returned a quoted display string

    `CAST(ARRAY[1, 2] AS JSON)` returned `"[1, 2]"`. It now returns
    `[1,2]`, and maps, rows, decimals, doubles, dates and timestamps are
    written as Trino 476 writes them: maps as objects with sorted string
    keys, named rows as objects, decimals at their scale (`1.50`),
    doubles in Java notation (`1.0E300`, `"NaN"`), dates and timestamps
    as strings. A cast of a value that is already JSON (`json_parse`,
    `json_extract`, `json_array_get`, a nested cast) is the identity
    rather than re-encoded as a JSON string.
