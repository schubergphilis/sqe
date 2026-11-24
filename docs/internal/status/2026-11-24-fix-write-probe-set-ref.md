# fix/write-probe-set-ref

**NEXT:** verify on a Polaris + Ranger stack with a write grant that has
`table-snapshot-ref-set` but not `table-snapshot-ref-remove`: first CTAS into
an empty schema and first INSERT into a fresh table pass, a reader is
refused. Record whether Polaris answers the probe with 409 or 400, and the
operation it authorizes the two-update commit as.

## Done

- `crates/sqe-catalog/src/write_probe.rs`: the probe's update list.
  - With `main`: `set-snapshot-ref` of `main` to its current snapshot
    (unchanged).
  - Without `main`: `add-snapshot` (a snapshot that is never written, next
    sequence number, current schema, a row range on v3) and
    `set-snapshot-ref` of `main` to it, as `fast_append` sends. It used to
    send `remove-snapshot-ref` of `main`.
- `catalog_write_check` in `rest_catalog.rs` takes its updates from there.
  The requirement (`assert-table-uuid` that can never match) and the
  outcome handling (403 refuses; 409, 400, other errors and success pass)
  are unchanged.

## Tests run

- `cargo test -p sqe-catalog --test it write_probe` (2 passed): both update
  sets, and that the fresh-table updates apply as a valid first commit on
  v2 and v3 metadata.
- `cargo test -p sqe-catalog --lib write_check` (5 passed), including the
  wiremock test for a table without `main`, now asserting the two updates.
- `cargo clippy -p sqe-catalog --tests -- -D warnings`
- `cargo xtask budgets --check`

## Not run

- The live stack check in **NEXT**.
