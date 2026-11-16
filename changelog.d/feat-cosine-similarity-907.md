### Added

- Trino `cosine_similarity(array, array)` (#907)

    Returns `dot(a, b) / (|a| * |b|)` as a `DOUBLE`, taking the same argument types as `cosine_distance`. Like `cosine_distance`, it returns NULL for a NULL array, an array with a NULL element (where Trino raises an error) or an all-zero array, and arrays of different length are an error. Trino's `map(varchar, double)` form is not supported.
