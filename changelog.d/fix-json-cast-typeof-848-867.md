### Fixed

- `CAST(x AS JSON)` and `to_json` embed a V3 `JSON` or `VARIANT` value as JSON (#848)

    They encoded the variant's binary storage,
    `{"metadata":"1103...","value":"0202..."}`. A variant is now written as
    its JSON value at any depth, so `CAST(ARRAY[j, j] AS JSON)` gives
    `[{"a":1},{"a":1}]` as in Trino. A JSON column on a V2 table is still a
    plain string and is still quoted.

- `typeof` returns the Trino type name (#867)

    It returned the Arrow name (`Int32`, `Utf8`); it now returns the name a
    client sees in the result metadata (`integer`, `varchar`, `decimal(5,2)`,
    `array(bigint)`). `arrow_typeof` still returns the Arrow name.
