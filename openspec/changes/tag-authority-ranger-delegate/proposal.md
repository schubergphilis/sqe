# Column tags by Ranger grant (#911)

## Why

`ALTER TABLE ... SET/UNSET TAGS` is gated by `require_admin`, the same gate as `CREATE/DROP POLICY`. A workspace admin, or a person the portal lets apply tags, is refused unless they also hold an engine-wide admin role. The only other route is a Polaris property commit, which skips SQE's checks (#903).

The product owner decided (16 Nov 2026) that the people allowed to tag should tag through SQE, with their own grant deciding.

## What Changes

- New setting `[access_control] tag_authority = "admin-role" | "ranger-delegate"`. The default is `admin-role`, so current deployments do not change.
- Under `ranger-delegate`, a non-admin may SET or UNSET TAGS on a column when the Ranger `hive` access policies grant them `alter` on that column. The check is column-level: a policy whose `column` resource names the column (or matches it by wildcard) counts, and so does one on `*`.
- Every column the statement touches must pass before anything is written. One refused column refuses the whole statement.
- `ALTER TABLE ... SET/UNSET MASKING POLICY` writes the same property and follows the same gate.
- An admin passes in both modes. A refusal is a `403 Forbidden` error (AccessDenied) and leaves the usual failed-statement audit record.
- A backend without a column privilege model (Polaris grants, in-memory without a registered grant, passthrough) grants nothing, so under `ranger-delegate` only admins can tag there, as before.

## Capabilities

### New Capabilities

- `tag-authority`: who may write column tags.

### Modified Capabilities

None.

## Impact

- `crates/sqe-core/src/config/mod.rs`: `TagAuthority`, `AccessControlConfig.tag_authority`.
- `crates/sqe-policy`: `PolicyStore::grants_column_privilege` (default `Ok(false)`), Ranger implementation, in-memory implementation.
- `crates/sqe-coordinator/src/query_handler.rs`: `require_tag_authority` for SET/UNSET TAGS and SET/UNSET MASKING POLICY.
- Book: access-control page, `sqe.toml.example`.

Turning the setting on turns every existing column `alter` grant into the right to change that column's tags, and so to remove its mask. Operators must review their `alter` grants first; the book says so.
