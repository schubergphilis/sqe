**STATUS 2026-10-19: GRANT ... MASKED WITH / ROWS WHERE implemented (`feat/grant-masked-with`).**

The documented-but-unparsed clauses are pre-parsed in `sqe-sql/src/grant_policy.rs` (classifier kind `GrantWithPolicy`). `handle_grant` requires an admin role and calls `PolicyStore::set_grant_policies` before the backend grant. `RangerStore` keeps one item per grantee in the single mask policy (per column) or row-filter policy (per table) whose resource is exactly that table: it prepends to an existing policy with a read-modify-write PUT, or POSTs a new `sqe-grant-*` policy. An admin's REVOKE SELECT/ALL on one table to one grantee calls `clear_grant_policies` after the backend revoke, which removes the grantee's items and deletes empty policies. Other policy stores refuse with NotImplemented.

Tests: parser unit tests, five wiremock tests (create, prepend, wildcard left alone, clear, catalog-scoped), coordinator classification tests, and the live `grant_masked_with_per_role_items_and_revoke_cleanup` in `access_control_e2e` (passes against Ranger).

Limits: a GROUP grantee is written to the item's `groups` list, like CREATE POLICY (the grants backend maps groups to Ranger roles). A hand-written wildcard policy that also matches can take precedence. A re-GRANT that does not name a clause keeps that clause's existing item.

**NEXT:** unchanged
