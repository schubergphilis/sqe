**STATUS 2026-11-18: `aikido-test-rust` green on main (`fix/catalog-it-element-at-and-304`).**

Once !1849 restored the `ci-deps` image, `aikido-test-rust` ran the sqe-catalog `it` tests again and found two failures already on main:

- `athena_views_with_a_lambda_and_a_lookup_subquery_are_queryable`: `element_at: array index must be bigint`. The integer-literal change (!1848) made `1` an `integer`. The Trino `element_at` UDF (sqe-trino-functions `coverage_fns.rs`) now widens any integer index to `bigint`, with unit cases for `integer` and `smallint`.
- `stale_entry_revalidates_with_if_none_match_and_reuses_on_304`: #903 made a 304 renew the authorization window (`mark_authorized`), so the fresh load after it pays no HEAD. The test expected the old count; it now expects `(1, 1, 1)`.

The `ci-deps` pin moves to `lock-fa9b8dfe478d` (`sha256:8909b622`), which `ci-deps-image` built on main at 2b9a287c after !1849 changed `Cargo.lock`.

Gate: `make test-integration` 425 passed, 0 failed; `cargo test -p sqe-trino-functions --lib` 247 passed; `cargo test -p sqe-catalog --test it` 249 passed; clippy (both crates, all targets) 0.

**NEXT:** split `crates/sqe-sql/src/trino_compat.rs` back under the 3000-line default instead of keeping its 3500 budget.
