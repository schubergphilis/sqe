# fix/trino-binary-functions

Part of the `trino-sql-parity` OpenSpec change, task 6.2 (binary and hash half).

## What changed

- New module `crates/sqe-trino-functions/src/binary_fns.rs`: Trino varbinary
  hashes (sha1, crc32, xxhash64, murmur3, hmac_*), encodings (base64, base64url,
  base32, hex, utf8), IEEE 754 and big-endian conversions.
- `md5`, `to_hex`, `lpad`, `rpad`, `substr` dispatch on varbinary and keep the
  DataFusion built-in for the other types.
- `SUBSTRING(x FROM a FOR b)` goes through a `SubstringPlanner` placed ahead of
  the built-in planners. Installing it rebuilds the session state, so it runs
  first in `register_trino_functions`, before any alias is released.
- The old string-based `encoding_udf!` functions in `trino_functions.rs`, which
  returned hex text, are removed.
- New workspace deps: md-5, sha1, crc32fast, murmur3, twox-hash (digest 0.11
  generation, same as sha2/hmac). hakari dropped `num-integer` i128.
- `column_input_test` has recipes for every new UDF and for `cosine_similarity`,
  which had none on main and failed the test.

## Gate

- `cargo test -p sqe-trino-functions`: exit 0 (lib 147 passed, it 201 passed)
- `cargo clippy -p sqe-trino-functions --all-targets -- -D warnings`: exit 0
- `cargo check-server`: exit 0
- `cargo test -p sqe-catalog --lib`: exit 0 (835 passed)
- `cargo test -p sqe-coordinator --lib custom_udfs_plan_under_duckdb_dialect`
  (no shadowed function keys after the state rebuild): exit 0 (1 passed)
- `cargo xtask budgets --check`: exit 1, `sqe-catalog` 64258 lines over 64250.
  The branch does not touch `sqe-catalog`; the overage is on main.

Not run: integration tests, the parity harness against Trino 484.

## Open

- `spooky_hash_v2_32` / `_64` not implemented.
- Byte-exact checks of xxhash64 / murmur3 against Trino wait for the parity run.

**NEXT:** parity rerun on the stack for the binary family, then the math branch
(DECIMAL typed literal, ceil/floor/mod/sign/truncate types, missing math and
statistics functions).
