**STATUS 2026-10-19: DROP POLICY ON TABLE ... FROM <principal> (`feat/drop-grant-policy`).**

Follow-up to !1479, where REVOKE stopped removing GRANT ... MASKED WITH items because that failed open. The masks now belong to (table, grantee): a plain re-GRANT reads through them again, which is the contract, and this statement is the one way to lift them.

`sqe-sql/src/policy_ddl.rs` parses `DROP POLICY [IF EXISTS] ON TABLE <t> FROM USER|ROLE|GROUP <name>` as `PolicyDdlStatement::DropForPrincipal`. A quoted `"ON"` is still a policy name. `handle_policy_ddl` gates it as DROP POLICY (admin-only). `PolicyStore::drop_principal_policies` defaults to NotImplemented. `RangerStore` takes the principal out of every type-1/type-2 policy on exactly that table, the set GRANT edits. It deletes a `sqe-grant-*` policy left empty and PUTs a hand-written one back empty. With nothing removed it errors unless IF EXISTS.

Tests: parser unit test; two wiremock tests (removal across mask/filter/own/hand-written/other table/access policy; nothing-to-drop error and IF EXISTS). The live `access_control_e2e::grant_masked_with_per_role_items_survive_revoke` now goes on: after a plain re-GRANT engineer's mask reapplies; a non-admin DROP is refused; dropping engineer falls bob through to the analyst mask; dropping analyst deletes both policies and bob reads unmasked.

**NEXT:** unchanged
