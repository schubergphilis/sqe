# fix/ctas-rollback-orphan-visible

**NEXT:** none on this branch. Avoiding the entry altogether (a staged create
that registers the table only at its first commit) is a separate OpenSpec
proposal.

## Done

- `crates/sqe-write/src/write_handler/ctas_rollback.rs`: the CTAS rollback
  moved out of `write_handler.rs` as `roll_back`, which takes the CTAS error
  and returns the error to propagate.
- When the drop fails, `with_orphan_entry` extends the CTAS error: the
  message names the table whose entry stays registered with no data. The
  error code is kept, so the audit event's error type and code do not change.
  An internal error keeps its generic client text, reported as an execution
  failure, so the table name is added without any internal detail.
- The failed query's audit event takes its `message` from the error's client
  message, so the OCSF event (status failure) names the table with no change
  to the audit code.

## Tests

- `cargo test -p sqe-write --lib`: 395 passed, including the two new
  `ctas_rollback` tests.
- `cargo clippy -p sqe-write --lib --tests`: no warnings.
- Not run: the integration suites. No test drives a refused drop end to end.
