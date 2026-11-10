### Fixed

- `CAST(CASE .. END AS JSON)` and `CAST(coalesce(..) AS JSON)` of JSON values no longer quote them (#848)

    DataFusion types a `CASE`, `coalesce`, `nvl`, `ifnull`, `nullif` and
    `nvl2` result without field metadata, so the JSON marker of
    `json_parse` and `json_extract` was lost and the cast encoded the JSON
    text as a string: `"{\"a\":1}"` where Trino returns `{"a":1}`. When
    every result is JSON (NULL aside), the result now stays JSON, and
    `typeof` says `json`. A mix of JSON and plain strings is still a
    string. A stored V2 `JSON` column still has no marker.
