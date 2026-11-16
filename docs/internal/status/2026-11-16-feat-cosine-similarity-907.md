**STATUS 2026-11-16: Trino `cosine_similarity` registered (`feat/cosine-similarity-907`, #907).**

This was the open ask on #907, settled by the user's decision to add the function in `sqe-trino-functions`.

- `crates/sqe-trino-functions/src/coverage_fns.rs`: a new `CosineSimilarity` ScalarUDF, registered in `register_coverage_fns`. It wraps DataFusion's `CosineDistance` for its signature and `coerce_types`, so it takes the same column types (numeric lists, `FixedSizeList`, `LargeList`). It computes `dot / (|a| * |b|)` itself rather than `1 - cosine_distance`, which loses precision near 0.
- It returns NULL for a NULL array, an array with a NULL element, or an all-zero array, like `cosine_distance`; Trino raises an error on a NULL element. Arrays of different length are an error. Trino's map form is not supported.
- Docs: `sql-reference/array-map.md`. The function gets a row in the "Vector distance" table, and its bullet is removed from "What is NOT registered".

Gate:

- `cargo clippy -p sqe-trino-functions -p sqe-coordinator --all-targets -- -D warnings`: exit 0.
- `cargo test -p sqe-trino-functions --lib -- coverage_fns`: 8 passed, 3 of them new (`cosine_similarity_matches_trino`, `cosine_similarity_null_cases_and_length_mismatch`, `cosine_similarity_is_one_minus_cosine_distance`).
- `make test-integration FILTER=vector_distance_e2e`: 4 passed. `distance_functions_rank_an_iceberg_list_column` now also checks `cosine_similarity` over the Iceberg `array(real)` column.

Left open: nothing on #907.

**NEXT:** #898, the multi-arch release image.
