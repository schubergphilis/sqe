**STATUS 2026-11-07: `SET WRITE_BRANCH` in embedded mode (`feat/embedded-write-branch-875`, part of #875).**

What changed:

- `sqe-cli/src/embedded.rs`: `EmbeddedClient.write_branch`. The `SetWriteBranch` arm stores
  it (a `txn` ref name is refused, as on the cluster). `SqlClient::execute` is now a thin
  wrapper: with a branch set it classifies the statement, refuses CTAS, and runs a data
  write inside `sqe_catalog::txn::write_scope`. The old body is `execute_statement`.
  Nothing else was needed: embedded writes already go through `SessionCatalog::local`,
  which pins the target to the branch, commits there and refuses a commit that would move
  `main`, and the sqe-write commit paths read `txn::target_branch`.
- `sqe-sql` classifier: `RESET WRITE_BRANCH` = `SetWriteBranch(None)`. Finding: five
  refusal messages (coordinator, Flight SQL, session catalog, txn) and `dml.md` told users to
  `RESET WRITE_BRANCH`, which did not parse.
- Docs: `features/embedded.md` support table row and a paragraph; `sql-reference/dml.md`
  lists `RESET WRITE_BRANCH`.

Tests:

- `cargo test -p sqe-cli --test cli_smoke embedded_warehouse_write_branch`: main and `dev`
  diverge; INSERT, DELETE, UPDATE and MERGE on `dev` give `1z,3m,4d,99new` on dev, and main
  stays `1a,2b,3c,99late` plus a post-RESET `5main`. CTAS on a branch and an INSERT
  into a table without the branch are refused, and the CTAS table is not created.
- `cargo test -p sqe-sql --lib write_branch`: 6 passed, including `test_reset_write_branch_clears`.
- Gate: `make test-integration FILTER=write_branch` -> 2 passed (`test_write_branch_routes_writes_to_the_branch`, `test_iceberg_ref_write_branch_refuses_a_tag`). `cargo test -p sqe-cli --test cli_smoke embedded_warehouse_write_branch` -> 1 passed; `cargo test -p sqe-sql write_branch` -> 6 passed.

Left open on #875: one-commit CTAS in embedded mode (`handle_ctas_streaming_on` split) and
`CALL system.*` in embedded mode (needs the maintenance handler out of sqe-coordinator).

**NEXT:** #875 one-commit embedded CTAS on its own branch.
