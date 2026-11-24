### Added

- `approx_distinct` accepts `double` and `real` columns, as Trino does

    It failed with "Support for 'approx_distinct' for data type Float64 is not implemented". Every NaN counts as one value, and `-0.0` and `0.0` count as one; NULL is not counted.
