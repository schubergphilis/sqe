**STATUS 2026-10-19: REVOKE leaves GRANT ... MASKED WITH masks in place (`fix/revoke-keeps-grant-masks`).**

!1477 merged with a REVOKE cleanup: an admin's REVOKE SELECT/ALL on one table to one grantee called `PolicyStore::clear_grant_policies`, which removed that grantee's mask and row-filter items and deleted policies left empty. That fails open. The grantee can still read the table through a schema-wide grant or another role, and the REVOKE then unmasked it. It also edited hand-written policies on the exact resource. A mask behind no SELECT is inert, so leaving it is the fail-closed choice. This branch removes `clear_grant_policies` (trait default, `RangerStore` impl, and the call in `handle_revoke`). Never released: the `feat-grant-masked-with` changelog fragment is corrected in place.

Tests: the wiremock clear test is gone; the live `access_control_e2e::grant_masked_with_per_role_items_survive_revoke` checks that bob keeps the engineer mask after engineer's REVOKE, since he still reads through analyst, and that both policies survive.

Trade-off: a revoked grantee's items stay in Ranger, so a later plain GRANT SELECT to the same grantee brings its old masks back. That is more restrictive, not a leak. Stale items are removed in Ranger by hand for now.

**NEXT:** an explicit statement that removes one grantee's mask and filter items (DROP POLICY cannot: the items of several grantees share one policy)
