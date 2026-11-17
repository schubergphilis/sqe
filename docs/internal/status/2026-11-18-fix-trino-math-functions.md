# fix/trino-math-functions

Part of the `trino-sql-parity` OpenSpec change, task 6.1 and the math DIFFs of 5.1.

## What changed

- `crates/sqe-sql/src/trino_compat.rs`: `DECIMAL '<text>'` (also `NUMERIC`,
  `DEC`) becomes `CAST('<text>' AS DECIMAL(p, s))` with p and s from the
  literal's digits; an explicit precision is kept. Exponents and non-numeric
  text are left for DataFusion.
- New `crates/sqe-trino-functions/src/math_fns.rs`: ceil/floor/sign/truncate/mod
  with Trino return types, shifts typed by the first argument, `bit_count`,
  `random(n)`/`random(m,n)` (alias `rand`), `is_nan`/`is_finite`/`is_infinite`,
  `width_bucket`, `great_circle_distance` (radius 6371.01 km), Wilson intervals,
  `euclidean_distance` (DataFusion `array_distance`), `cosine_distance` clamped
  so identical vectors give 0.
- New `stats_fns.rs`: normal, beta and Student t distribution functions
  (regularized incomplete beta by continued fraction; inverses by search).
  New workspace dep `libm` (already in Cargo.lock).
- `aggregates.rs`: `geometric_mean`.
- Removed the superseded `TrinoMod`, `TrinoTruncate`, `TrinoSign` and their tests;
  the assertions moved to `math_fns` tests.
- Budget: `sqe-trino-functions` ceiling 13000 -> 13250 (new function families;
  `trino_functions.rs` itself shrank by about 300 lines).

## Gate

- `cargo test -p sqe-trino-functions`: exit 0 (144 + 197 passed)
- `cargo test -p sqe-sql`: exit 0 (649 + 94 passed)
- `cargo clippy -p sqe-trino-functions --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo hakari generate --diff`: exit 0
- `cargo xtask budgets --check`: exit 1 only on `sqe-catalog` 64258 over 64250,
  a crate this branch does not touch.

Not run: integration tests, the parity harness against Trino 484.

## Open

- `corr`, `covar_pop`, `covar_samp`, `regr_slope`, `regr_intercept` return double
  for real input; Trino returns real.
- Error messages and the Wilson / arithmetic-shift edge values are not yet
  compared with Trino 484.
- `math_fns.rs` is about 1700 lines including tests; split when it next grows.

**NEXT:** parity rerun for the math family, then the string/url branch.
