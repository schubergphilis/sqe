**STATUS 2026-11-03: `sqe-cli` keeps SQL NULL apart from an empty string on every backend and output format (`fix/cli-json-null`).**

Found while building the TPCBB q01 pre-aggregation harness (#785): an
all-NULL `SUM` came back from `sqe-cli -f json` as `""`, the same as `''`,
so the harness needed `IS NULL` flag columns to see it.

What changed (`crates/sqe-cli/src/`):

- `client.rs`: `QueryResult` gains `nulls: HashSet<(row, column)>` plus
  `empty()`, `text()`, `is_null()` and `from_batches()`. `from_batches`
  formats with `ArrayFormatter` and fills `nulls` from `logical_nulls()`, so
  dictionary and run-end columns count too. It returns the formatter error.
- `flight.rs`, `embedded.rs`: both use `from_batches`. The embedded copy
  (`record_batches_to_query_result`) swallowed a formatter error into an
  empty result through `unwrap_or_default`; it is gone.
- `http.rs`: marks `Value::Null` cells; the display text is `""` like the
  Arrow backends (it was the string `NULL`, indistinguishable from `'NULL'`).
- `display.rs`: `-f json` prints `null` for NULL cells; the table prints
  `NULL`. CSV/TSV print an empty field, as Flight and embedded already did.

Tests: `embedded_client_marks_null_cells` (a NULL decimal, `''` and an int
through `UNION ALL`, asserts the null mask and the empty string) and
`json_prints_null_apart_from_an_empty_string` (display).
`cargo test -p sqe-cli` -> 109 + 21 passed;
`cargo clippy -p sqe-cli --all-targets -- -D warnings` clean.

Probe with the debug binary: `-f json` prints `{"n":null,"e":"","i":"1"}`,
the table `| NULL |   | 1 |`. `tpcbb-preaggregation.py --sqe-cli
target/debug/sqe-cli` passes (exit 0). It keeps its `IS NULL` flags so it
also works with an older binary.

Consumers checked: `access-control-parity-demo.sh` and the Databricks interop
`run.py` read TSV, which did not change for Flight or embedded, and already
treat `""`/`NULL` as NULL. `trino-compat-test.sh` calls the HTTP API with
curl, not sqe-cli. The book's CLI page (`getting-started/cli.md`) gains a
"NULL values" note.

**NEXT:** shared SF10 pass for #785 (b) and the #787 compare on regenerated
TPCBB data.
