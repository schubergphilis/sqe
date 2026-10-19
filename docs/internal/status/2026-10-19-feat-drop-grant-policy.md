**STATUS 2026-10-19: DROP POLICY ON TABLE ... FROM <principal> (`feat/drop-grant-policy`).**

Follow-up to !1479, where REVOKE stopped removing GRANT ... MASKED WITH items because that failed open. The masks now belong to (table, grantee): a plain re-GRANT reads through them again, which is the contract, and this statement is the one way to lift them.

`sqe-sql/src/policy_ddl.rs` parses `DROP POLICY [IF EXISTS] ON TABLE <t> FROM USER|ROLE|GROUP <name>` as `PolicyDdlStatement::DropForPrincipal`. A quoted `"ON"` is still a policy name. `handle_policy_ddl` gates it as DROP POLICY (admin-only). `PolicyStore::drop_principal_policies` defaults to NotImplemented. `RangerStore` takes the principal out of the `sqe-grant-*` type-1/type-2 policies on exactly that table and deletes one left empty. Hand-written policies are never edited, even ones GRANT added an item to (fail-closed: removing a security team's mask would leak); such an item has to be removed in Ranger. With nothing removed it errors unless IF EXISTS, and the error lists hand-written policies that still name the principal.

Tests: parser unit test; two wiremock tests (removal across mask/filter/hand-written untouched/other table/access policy; nothing-to-drop error naming the hand-written policy, and IF EXISTS). The live `access_control_e2e::grant_masked_with_per_role_items_survive_revoke` now goes on: after a plain re-GRANT engineer's mask reapplies; a non-admin DROP is refused; dropping engineer falls bob through to the analyst mask; dropping analyst deletes both policies and bob reads unmasked.

**NEXT:** unchanged
