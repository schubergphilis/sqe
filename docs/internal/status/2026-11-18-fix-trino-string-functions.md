# fix/trino-string-functions

Part of the `trino-sql-parity` OpenSpec change, task 6.2 (string and URL half)
and the string DIFFs of 5.1.

## What changed

- New `crates/sqe-trino-functions/src/string_fns.rs` (+ `string_fns/tests.rs`):
  array-aware `concat`, `concat_ws`, `reverse`; `repeat` returns an array as in
  Trino (size cap kept); 2-arg `replace`; `split_part` NULL past the end;
  bigint `strpos`/`instr`/`index`/`position(a, b)`/`length`/`char_length`,
  3-arg `strpos`; `levenshtein_distance`, `hamming_distance`, `normalize`,
  `soundex`, `human_readable_seconds` (rounded), `format_number`, `luhn_check`,
  `parse_data_size` (decimal(38,0)).
- New `url_fns.rs` (+ tests): `url_extract_*` following `java.net.URI`, `''` for
  an absent component, bigint port, new `url_extract_fragment`.
- Superseded SQE implementations removed from `trino_functions.rs` and
  `trino_functions_ext.rs`.
- `sqe-coordinator/tests/it/trino_stats_bytes_e2e.rs` builds its 64-character
  strings with `rpad` now that `repeat` is the array function.
- Book `sql-reference/string.md`: `repeat` and `reverse` rows.

## Gate

- `cargo test -p sqe-trino-functions`: exit 0 (156 + 197 passed)
- `cargo clippy -p sqe-trino-functions --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo xtask budgets --check`: exit 1 only on `sqe-catalog` (on main).

Not run: integration tests (including the edited Polaris-gated e2e), parity run.

## Open

- SQL `position(x IN y)` is planned straight to DataFusion's `strpos` and
  returns integer. Fix with an `ExprPlanner::plan_position` next to the
  `SubstringPlanner` from `fix/trino-binary-functions` once that merges.
- `normalize(s, NFD)` takes the form as a string; Trino uses a bare keyword.
- Unconfirmed against Trino 484: `format_number` rounding, URL host of an
  invalid hostname, `strpos` with an empty needle.

**NEXT:** `plan_position` after the binary branch merges; parity rerun for the
string family.
