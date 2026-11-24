# test/tag-statement-contract

**NEXT:** none for SQE. A change to any text pinned here also needs the
portal parser (`governance_service.py`, `engines.py`) changed in the same
release.

Tests only; no engine output changes.

## Done

The data portal reads the result of SHOW TAGS and matches substrings of the
tag and policy refusals. The tests now pin both, with a comment naming the
portal parser as the consumer:

- `crates/sqe-coordinator/src/query_handler/access_control.rs`: the SHOW TAGS
  schema is `column Utf8, tag Utf8, value Utf8, applied Boolean`, in that
  order (clients read by position).
- `crates/sqe-policy/src/tag_conflict_guard.rs`: the full
  `tag change refused: it would create a mask conflict on ... under
  mask_precedence` and `... row filter conflict on ... (one row filter`
  phrases.
- `crates/sqe-policy/tests/it/rewriter_integration.rs`: `mask conflict on
  ...: more than one distinct column mask` and `row filter conflict on ...:
  more than one distinct row filter` (the AccessDenied code was already
  asserted).
- `crates/sqe-coordinator/tests/it/grant_introspection_gate_test.rs`:
  consumer comments on the existing `requires apply_tag`, `requires
  remove_tag` and `tag_removal = "admin"` asserts.

Size budget: `sqe-coordinator` goes to 72767 of 72800 lines (17 lines for
the SHOW TAGS test).

## Tests run

- `cargo test -p sqe-coordinator --lib show_tags_columns` (1 passed)
- `cargo test -p sqe-coordinator --test it grant_introspection` (49 passed)
- `cargo test -p sqe-policy --lib tag_conflict_guard` (14 passed)
- `cargo test -p sqe-policy --test it contested_` (5 passed)
- `cargo clippy -p sqe-policy -p sqe-coordinator --tests -- -D warnings`
- `cargo xtask budgets --check`

## Not run

- `make test-integration` and `make test-access-control`: no stack runs
  SHOW TAGS beyond the `#[ignore]` cases, and nothing in the engine changed.
