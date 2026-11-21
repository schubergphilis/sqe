### Changed

- `SHOW TAGS ON <table>` lists each tag's value (#923)

    The result has a third column, `value`, next to `column` and `tag`. A tag
    without a value shows an empty `value`, as `information_schema.column_tags`
    does. Clients that read the first two columns are unaffected.
