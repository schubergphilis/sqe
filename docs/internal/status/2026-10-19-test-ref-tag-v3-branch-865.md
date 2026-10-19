**STATUS 2026-10-19: e2e tests for Iceberg tags and V3 branches (`test/ref-tag-v3-branch-865`, part of #865).**

Two integration tests next to `test_write_branch_routes_writes_to_the_branch`. `test_iceberg_ref_tag_create_read_drop`: CREATE TAG pins main's current snapshot, `FOR VERSION AS OF 'rel1'` keeps reading it after main moves on, a duplicate CREATE TAG fails, DROP TAG removes it, `DROP TAG rel1 IF EXISTS` is then a no-op, and `DROP BRANCH main` is refused (openspec iceberg-matrix-parity 4.14). `test_iceberg_ref_v3_branch_write_read`: on an explicit format-version 3, merge-on-read table, INSERT / DELETE / UPDATE with WRITE_BRANCH set land on the branch and main is untouched. The matrix caveats for `sqe:branching-tagging:v2` and `:v3` are replaced by the tests; one new caveat records that IF EXISTS goes after the ref name (the Iceberg Spark form `DROP TAG IF EXISTS t` is rejected).

Gate: `make test-integration FILTER=test_iceberg_ref_` -> 2 passed.

Left open on #865: binary / fixed / UUID defaults on INSERT. Found while investigating, not fixed here: every MERGE fails on a table with such a default (both MERGE handlers compute all write defaults up front); `default_to_iceberg_literal` falls back to a string literal for non-string columns; `SET WRITE_BRANCH` to a tag name is not refused.

**NEXT:** unchanged
