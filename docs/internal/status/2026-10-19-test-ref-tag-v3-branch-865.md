**STATUS 2026-10-19: Iceberg refs: tags are read-only for WRITE_BRANCH, DROP ... IF EXISTS in both orders, e2e tests for tags and V3 branches (`test/ref-tag-v3-branch-865`, part of #865).**

Fixes:
- `existing_write_branch` (`crates/sqe-catalog/src/txn.rs`) accepted any ref, so `SET WRITE_BRANCH` naming a tag let an INSERT commit to the tag and rewrite it as a branch (confirmed by running before the fix). A tag is now refused with an error that points at CREATE BRANCH ... AS OF VERSION.
- `DROP BRANCH` / `DROP TAG` took IF EXISTS only after the name; Iceberg's Spark form `DROP TAG IF EXISTS v1` was rejected. `parse_drop_target` (`crates/sqe-sql/src/ddl.rs`) accepts both; a ref named `if` still parses as a name.

Tests: `test_iceberg_ref_tag_create_read_drop` (CREATE TAG pins main's current snapshot, `FOR VERSION AS OF 'rel1'` keeps reading it after main moves on, duplicate CREATE TAG fails, DROP TAG removes it, `DROP TAG rel1 IF EXISTS` is then a no-op, `DROP BRANCH main` is refused; openspec iceberg-matrix-parity 4.14), `test_iceberg_ref_v3_branch_write_read` (INSERT / DELETE / UPDATE with WRITE_BRANCH on an explicit format-version 3 merge-on-read table stay on the branch), `test_iceberg_ref_write_branch_refuses_a_tag`, and parser unit tests in `ddl.rs`. The matrix caveats for `sqe:branching-tagging:v2` and `:v3` are gone.

Left open on #865: binary / fixed / UUID defaults on INSERT, together with two related findings (every MERGE fails on a table with such a default because both MERGE handlers compute all write defaults up front; `default_to_iceberg_literal` falls back to a string literal for non-string columns). Next branch: `fix/column-defaults-865`.

**NEXT:** `fix/column-defaults-865`
