### Fixed

- `CAST(.. AS JSON)` of a JSON value is the identity through aliases and arrays (#848)

    `json_parse`, `json_extract`, `json_array_get` and `CAST(.. AS JSON)`
    mark their result as JSON, so a cast of such a value returns it
    unchanged also through a CTE, a subquery or a view, where it used to
    be quoted as a JSON string. `CAST(ARRAY[json_parse(..), ..] AS JSON)`
    embeds the elements (`[{"a":1},null]`), and `typeof` of a JSON value
    is `json`, as in Trino. The Trino protocol reports such a column as
    `json` instead of `varchar`. A stored V2 `JSON` column has no marker and is
    still cast to a JSON string.
