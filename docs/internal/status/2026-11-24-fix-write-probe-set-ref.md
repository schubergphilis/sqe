# fix/write-probe-set-ref

**NEXT:** none for SQE. A write grant no longer needs
`table-snapshot-ref-remove`; whether a deployment keeps it is the
deployment owner's call.

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

## Live check

Run after merge on main (with the CTAS rollback change merged alongside):
native `sqe-server` built at main, against a Polaris + Ranger
stack whose write grant (CREATE TABLE + MODIFY on one schema) has
`table-snapshot-ref-set` but not `table-snapshot-ref-remove`. As a user
with only that grant:

- First CTAS into the empty schema: passed. INSERT into it: passed.
- CREATE TABLE, then INSERT into it (no `main` yet): passed; SELECT reads it.
- INSERT into a table in a schema the user only reads: refused by the
  write check ("no write access ... nothing was written").
- CTAS into that schema: refused by Polaris
  (`CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION`). DROP TABLE without a drop
  grant: refused (`DROP_TABLE_WITHOUT_PURGE`).

Direct Polaris commits with the impossible `assert-table-uuid`:

| Table | Updates | Writer | Reader |
|---|---|---|---|
| no `main` | add-snapshot + set-snapshot-ref (new) | 409 (UUID mismatch) | |
| no `main` | remove-snapshot-ref (old) | 403 `REMOVE_TABLE_SNAPSHOT_REF` | |
| with `main` | add-snapshot + set-snapshot-ref | | 403 `ADD_TABLE_SNAPSHOT` |
| with `main` | remove-snapshot-ref | | 403 `REMOVE_TABLE_SNAPSHOT_REF` |

Polaris answers the new probe with 409, not 400, and authorizes it as
`ADD_TABLE_SNAPSHOT` (the first operation it checks). The old probe was
refused for the writer, which is the failure this branch fixes.

The same run with `table-snapshot-ref-remove` added to the write grant
gives the same SQL outcomes; the old shape then answers 409 for the writer
too. The probe works with and without that right.

## Not run

- `make test-integration` and the access-control suite.
