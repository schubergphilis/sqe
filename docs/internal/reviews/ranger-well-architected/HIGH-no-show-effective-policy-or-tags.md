# No SHOW EFFECTIVE POLICY or SHOW TAGS; masking is un-introspectable

- **ID:** no-show-effective-policy-or-tags
- **Pillar:** Operational Excellence
- **Severity:** High
- **Status:** Open
- **Files:** crates/sqe-sql/src/classifier.rs:44 (StatementKind); CLAUDE.md (SHOW EFFECTIVE POLICY designed, not implemented); tag authoring crates/sqe-sql/src/tags.rs plus set_column_tags has no read-back

## Problem
There is no way to ask "what filters, masks, or restrictions apply to user U on table T". The resolved policy is computed only inside evaluate and never surfaced. After `ALTER TABLE SET TAGS` there is no `SHOW TAGS` to read back `sqe.column-tags`. The masking path is effectively un-introspectable in production, so operators cannot verify or debug a policy decision.

## Proposed fix
Add `SHOW EFFECTIVE POLICY [FOR USER u] ON <table>` that runs the resolver and returns (filter/mask/restriction, source policy id, tag), gated by `require_self_or_admin`. Add `SHOW TAGS ON <table>` returning `column -> [tags]`.

## Acceptance criteria
`SHOW EFFECTIVE POLICY ON t` returns the applied masks and filters. `SHOW TAGS ON t` round-trips a prior `SET TAGS`.
