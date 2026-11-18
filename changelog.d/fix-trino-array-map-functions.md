### Added

- Trino array, map and aggregate functions

    `array_last`, `zip`, `zip_with`, `array_histogram`, `contains_sequence`,
    `ngrams`, `shuffle`, `map_concat`, `map_filter`, `transform_keys`,
    `transform_values`, `split_to_map`, `split_to_multimap`,
    `map_from_entries`, `multimap_from_entries`, the map overload of
    `cosine_similarity`, `max(x, n)` / `min(x, n)` (planned as `max_n` /
    `min_n`), `max_by(x, y, n)` / `min_by(x, y, n)` and
    `approx_most_frequent` (exact counts).

### Changed

- `array_remove` removes every occurrence, `map_entries` and `zip` return
  anonymous row fields (`row(varchar, bigint)` over the Trino protocol), and
  `checksum` is an order-independent aggregate returning `varbinary` instead
  of a per-row hex string, as in Trino.
