# 510: Maintenance procedure authorization is a role-name substring heuristic that allows sessions with no roles

- Scope: sqe-coordinator maintenance (maintenance.rs authorize_or_deny, session_has_write_privilege)
- Severity: medium
- Category: security
- Effort: S (hours)
- Status: open

## Problem

`CALL system.*` procedures that mutate or delete are gated by
`session_has_write_privilege`
(`crates/sqe-coordinator/src/maintenance.rs:3281-3303`):

```rust
if roles.is_empty() { return true; }
let has_write_like = roles.iter().any(|r| lower.contains("write") || lower.contains("admin") || lower.contains("owner"));
let has_read_only  = roles.iter().any(|r| lower.starts_with("read") || lower.starts_with("select") || lower.contains("readonly"));
if has_read_only && !has_write_like { return false; }
true
```

Used at `maintenance.rs:749` for `rewrite_data_files`, `expire_snapshots`
(which runs `clear_expire_files(true)` and deletes data files, `:2364`),
`remove_orphan_files` (deletes objects through `FileIO` with no catalog
commit, `:2425`), `purge_orphan_locations` with `dry_run = false`
(`delete_prefix`, `:2635`), `drop_table` with `purge`, `set_current_snapshot`,
`rollback_to_snapshot`, `register_table`.

A session with an empty role list is granted. A user whose roles are, say,
`["analyst", "reporting"]` is granted. Only a role literally starting with
`read`/`select` or containing `readonly` is denied, and only if no other role
contains the substring `write`, `admin` or `owner` (`"co-writer-viewer"`
passes).

The same file uses a real check for one procedure:
`self.config.auth.has_admin_role(&session.user.roles)` for
`reproject_column_tags` (`maintenance.rs:735`). `query_handler.rs` uses
`require_admin` for ATTACH/DETACH/secrets (`:2105-2121`).

For catalog-committing procedures Polaris is the backstop, since the commit
runs with the caller's token. For `remove_orphan_files` and
`purge_orphan_locations` there is no commit: they delete with whatever S3
credentials the table's `FileIO` was vended, and a vended read/write credential
for a table the user can INSERT into is enough to delete its files.

## Why it matters

Any authenticated user who can write to a table can run
`remove_orphan_files(older_than => now)` while another session's CTAS or
compaction is between "files uploaded" and "commit", deleting the in-flight
files (the default `older_than` of 3 days is the only protection and it is a
parameter). `expire_snapshots(retain_last => 1)` destroys time-travel history
for everyone. These are administrative operations in Spark and Trino, gated by
table-owner or admin privileges. The heuristic also gives a false sense of a
gate: an operator reading `authorize_or_deny` assumes read-only roles are
excluded, but that depends on how their IdP names roles.

## Proposed change

1. Split procedures into three tiers in `ProcedureCall`: read-only (table_health,
   suggest_bloom_filter_columns, dry-run purge), table-write (rewrite_data_files,
   rewrite_manifests), destructive/admin (expire_snapshots, remove_orphan_files,
   purge_orphan_locations, drop_table, register_table, set/rollback snapshot).
2. Gate admin-tier procedures on `config.auth.has_admin_role` (the existing
   `require_admin` path) or on `has_maintenance_authority` for the scheduler's
   minted session.
3. Gate table-write procedures on the policy store when one is configured
   (`resolve_effective_policy` + `policy_denies_all`, as `hive_ddl_handler.rs:732-747`
   does), falling back to the Polaris commit as backstop. Delete the substring
   heuristic, or keep it only behind an explicit
   `[maintenance] legacy_role_heuristic = true` with a startup warning.
4. Empty roles deny by default for the admin tier.

## Acceptance criteria

- Unit tests replacing `write_privilege_*` at `maintenance.rs:3362-3418`:
  empty roles denied for `expire_snapshots`; `analyst` denied for
  `remove_orphan_files`; admin role allowed; maintenance-authority session
  allowed for `rewrite_data_files` only.
- Audit event on denial keeps the existing `AdminGateDenied` shape.
- Docs: `docs/site/book` maintenance page lists which procedures need admin.

## Related

- `docs/internal/reviews/2026-07-25-production-readiness-review.md` P1 items on
  "keep execution separately authorized" for maintenance recommendations.
- 511 (scheduler is the legitimate non-admin caller; it uses
  `has_maintenance_authority`).
