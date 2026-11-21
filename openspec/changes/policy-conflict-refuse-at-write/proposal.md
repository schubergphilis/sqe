# Refuse a conflicting tag when it is written (#918, item 3)

## Why

Under `policy.mask-precedence = "refuse"` a read fails with `mask conflict on ...` or `row filter conflict on ...` when two distinct masks resolve for a column, or two distinct row filters for a table, for the reading user (!1886). The tag that caused the conflict is still accepted when it is written, so a steward learns about the conflict only when a reader's query breaks. Issue #918 asks for the write to be refused as well: "SET TAGS adding a conflicting tag (mask or row filter) is refused."

## What Changes

- Under `refuse`, a tag write that adds a tag to a column checks the table's whole tag map, before and after the change, for every principal the matching mask and row-filter policies name. A conflict that exists after the change and not before refuses the statement. Nothing is written and no tag is projected to Ranger.
- The check covers the three tag write paths: `ALTER TABLE ... SET TAGS` (and the Snowflake column forms), `ALTER TABLE ... SET MASKING POLICY`, and a raw `sqe.column-tags` property write. It runs under the per-table lock those writes already take, against the live tag map.
- A write that adds no tag (UNSET, or a raw write that only removes) is never refused.
- If the policy store cannot be read, the write is refused.
- `tag` and `resource` modes are unchanged, and so is every policy backend except Ranger. A backend that cannot list the principals its policies name skips the check; read-time refusal still applies there.

## Capabilities

### New Capabilities

- `mask-precedence-refuse`: the write-time half of the `refuse` mode.

### Modified Capabilities

None.

## Impact

- `crates/sqe-policy-api/src/lib.rs`: `PolicyStore::conflict_principals`, default `Ok(None)`.
- `crates/sqe-policy/src/ranger_store.rs`: the Ranger implementation.
- `crates/sqe-policy/src/tag_conflict_guard.rs` (new): `TagConflictGuard`, the before/after comparison.
- `crates/sqe-write/src/catalog_ops.rs`: `set_column_tags` and `set_table_properties_guarded` take an optional guard and run it under the table lock.
- `crates/sqe-coordinator/src/query_handler.rs`, `query_handler/tag_authority.rs`: build the guard under `refuse` and pass it to the three paths. Policy key from the helper the tag gate already uses.
- Not covered: CREATE TABLE, CTAS and CREATE OR REPLACE carrying `sqe.column-tags`, commits made outside SQE, and later edits to Ranger policies. The read-time refusal is the backstop for those.
