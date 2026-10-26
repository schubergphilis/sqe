### Fixed

- `table_files` returns valid JSON in `partition`, `lower_bounds` and `upper_bounds`

    Bounds on date, time, timestamp, UUID and binary columns came out
    unquoted, string bounds with a `"` or `\` were not escaped, and NaN or
    infinite float bounds were bare words, so the column did not parse as
    JSON. Those bounds are now JSON strings. The `partition` column of
    `table_files` and `table_partitions` printed Rust's internal Debug form;
    it is now a JSON array in Iceberg's single-value JSON form.
