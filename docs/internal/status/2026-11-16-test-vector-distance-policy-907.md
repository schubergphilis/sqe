**STATUS 2026-11-16: vector distance functions tested end to end and documented (`test/vector-distance-policy-907`, #907).**

New file `crates/sqe-coordinator/tests/it/vector_distance_e2e.rs`. Each test gets its own table, because the tests run in parallel.

- `distance_functions_rank_an_iceberg_list_column`: `cosine_distance`, `inner_product` and `array_distance` over an `array(real)` column. Checks the order and the values for the opposite vector (2, -1, 2).
- `bound_vector_ranks_over_flight_sql_in_either_argument`: a Flight SQL prepared statement with a `List<Float64>` parameter. It checks `cosine_distance(emb, ?)`, `cosine_distance(?, emb)` and `$1`, each giving the top 3 `[1, 6, 2]`.
- `row_filter_limits_the_nearest_rows_to_allowed_ones`: an in-memory policy `region = 'EU'` gives `[1, 2, 5]`.
- `masked_or_restricted_vector_gives_null_distance`: with `MaskType::Nullify` or a restricted `emb`, all 5 rows come back with a NULL distance. There is no plan error, so the issue's worry about a wrongly typed NULL does not hold.

The policy tests go through `execute_bound_with_id`, the path Flight SQL prepared statements take. The Flight test reuses the `IngestServer` harness from !1813, so this branch is stacked on it.

Gate: `make test-integration FILTER=vector_distance_e2e`: 4 passed, 0 failed.

Docs: `sql-reference/array-map.md`, new "Vector distance" section. `cosine_similarity` is listed under "What is NOT registered".

Left open on #907: ask 3, whether to add Trino's `cosine_similarity` (the user decides; it would live in `sqe-trino-functions`, and DataFusion has no such function).

**NEXT:** user decision on `cosine_similarity`; merge after !1813.
