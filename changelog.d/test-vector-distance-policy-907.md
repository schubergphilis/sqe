### Added

- Vector distance functions documented and covered end to end (#907)

    The array reference has a new "Vector distance" section for `cosine_distance`, `inner_product` and `array_distance` over an Iceberg `list<float>` column. New integration tests check:

    - the three functions rank a real Iceberg table;
    - a query vector bound as a `List<Float64>` parameter over Flight SQL ranks correctly on either side of `cosine_distance`;
    - a row filter limits the ranking to allowed rows;
    - a masked or restricted vector column gives a NULL distance for every row.

    Trino's `cosine_similarity` is not registered; the page gives `1 - cosine_distance(a, b)` instead.
