# fix/trino-array-map-functions

Part of the `trino-sql-parity` OpenSpec change, tasks 6.4 and 6.5 and the
array/map/aggregate DIFFs of 5.1.

## What changed

- New `crates/sqe-trino-functions/src/array_fns.rs`, `map_fns.rs` and
  `agg_fns.rs`, registered after the built-ins.
- Arrays: `array_last`, `array_remove` (all occurrences, shadows the
  built-in for Trino semantics), `zip`, `zip_with`, `array_histogram`,
  `contains_sequence`, `ngrams`, `shuffle`, `cosine_similarity` on maps.
- Maps: `map_entries` (anonymous row fields, shadows the built-in),
  `map_concat`, `map_filter`, `transform_keys`, `transform_values`,
  `split_to_map`, `split_to_multimap`, `map_from_entries`,
  `multimap_from_entries`.
- Aggregates: `max_n` / `min_n` with an `sqe-sql` rewrite of a two-argument
  `max(x, n)` / `min(x, n)` (no DISTINCT, no OVER). DataFusion's `max` / `min`
  are not shadowed: `single_distinct_to_groupby` and
  `sqe-planner`'s `is_reaggregatable` re-aggregate by name. `max_by` /
  `min_by` n form, `approx_most_frequent` (exact counts, ties in first-seen
  order), `checksum` as an aggregate returning 8 bytes.
- `sqe-trino-protocol` `trino_type_name`: a struct field with an empty name
  renders as an anonymous row field.
- Book rows in `array-map.md`, `aggregate.md`, `encoding-url.md` updated.
- Budget: `sqe-trino-functions` ceiling 13000 -> 14250 (about 2700 lines of
  new functions plus their tests).

## Gate

- `cargo test -p sqe-trino-functions`: exit 0 (156 + 195 passed)
- `cargo test -p sqe-sql --lib`: exit 0 (651 passed)
- `cargo test -p sqe-catalog --lib`: exit 0 (835 passed)
- `cargo test -p sqe-coordinator --lib custom_udfs_plan_under_duckdb_dialect`: exit 0
- `cargo clippy -p sqe-trino-functions --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo xtask budgets --check`: exit 1 only on `sqe-catalog` (on main).

Not run: integration tests, parity run.

## Open

- `numeric_histogram` (Trino's streaming algorithm), `reduce_agg` (two
  lambdas in an accumulator), `array_sort` with a comparator lambda.
- `checksum` bytes are not confirmed against Trino.
- `max(x, n)` as a window function is not supported.

**NEXT:** parity rerun on the stacked function branches; numeric_histogram
and reduce_agg.
