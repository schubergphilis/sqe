### Added

- Trino string and URL functions

    `luhn_check`, `format_number`, `parse_data_size`, `hamming_distance`,
    `normalize`, `url_extract_fragment`, the 2-argument `replace`, the
    3-argument `strpos`, and `index` as an alias of `strpos`.

### Changed

- `repeat(x, n)` returns an array, as in Trino

    It used to repeat a string. Use `rpad('', n, s)` or
    `array_join(repeat(s, n), '')` for a repeated string. `concat`, `reverse`
    and `concat_ws` also take arrays.

### Fixed

- String functions return Trino's types and values

    `strpos`, `length`, `levenshtein_distance` return bigint; `split_part`
    past the last field gives NULL; `human_readable_seconds` rounds to whole
    seconds; the `url_extract_*` functions return `''` for an absent component
    and `url_extract_port` returns bigint. SQL `position(x IN y)` still
    returns integer.
