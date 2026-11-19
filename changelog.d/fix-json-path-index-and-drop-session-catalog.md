### Fixed

- `json_extract_scalar`, `json_extract` and `json_size` read array indexes and bracketed members in a JSON path

    `$.items[1].name`, `$[0]` and `$["a.b"]` used to return NULL (#914), because the path was split on dots only. The path now follows Trino's subset: `$`, `.key`, `[n]`, `["key"]` and `['key']`. Paths that worked before, such as `$.items.0.name` or a path without `$`, give the same result. A malformed path is NULL rather than an error.

- `DROP TABLE` with a one- or two-part name drops from the session catalog and schema

    `DROP TABLE t` and `DROP TABLE s.t` went to the default warehouse instead of the catalog set by `USE` or the client. With `IF EXISTS` the table was not found there, so the statement succeeded and dropped nothing (#915). The name now resolves like INSERT and CTAS do. The same applies to `DROP TABLE ... PURGE` and to a Hive root that is the session catalog.
