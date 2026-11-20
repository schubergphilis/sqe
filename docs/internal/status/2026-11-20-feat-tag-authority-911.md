**STATUS 2026-11-20: column tags by Ranger grant (`feat/tag-authority-911`, #911).**

- OpenSpec change `tag-authority-ranger-delegate` (proposal, design D1-D7, spec, tasks; all tasks ticked).
- `[access_control] tag_authority = "admin-role" (default) | "ranger-delegate"` (`TagAuthority`, parse test).
- `PolicyStore::grants_column_privilege` (sqe-policy-api), default `Ok(false)`. Ranger answers per column (`access_grants_column_type`, sharing one deny-first body with the table check through `access_grants_type_with`). In-memory answers from its table grants.
- `require_tag_authority` (`query_handler/access_control.rs`) gates SET/UNSET TAGS and SET/UNSET MASKING POLICY (the latter before `masking_policy_to_tag_op`, so its errors cannot probe the policy list). The key is the same as SHOW EFFECTIVE POLICY's: session catalog, session schema for a bare name, `catalog_scoped`.
- Found while building: `set_column_tags` and `load_column_tags` wrote and read a bare name in the configured warehouse, not the session catalog, so the gate and the write could check and write different tables. Both now use `resolve_target_catalog`, like DML.
- Book: "Who may tag a column" in the access-control tutorial; `sqe.toml.example`.

Gate:
- `scripts/access-control-test.sh tag_authority`: exit 0, 3 passed (`tag_authority_steward_tags_and_untags_their_column`, `tag_authority_refused_column_writes_nothing_and_is_audited`, `tag_authority_bare_name_is_checked_against_the_session_namespace`).
- `scripts/access-control-test.sh tag`: exit 0, 7 passed.
- `cargo test -p sqe-coordinator --test it grant_introspection_gate`: exit 0, 21 passed. `sqe-core --lib tag_authority`: 1 passed. `sqe-policy --lib column_alter`: 4 passed. `sqe-write --lib catalog_ops`: 20 passed.
- `cargo clippy -p sqe-write -p sqe-core -p sqe-policy -p sqe-coordinator --all-targets -- -D warnings`: exit 0. rustfmt ratchet clean on every changed file.

Left open: the other `ALTER TABLE` paths in `catalog_ops.rs` (rename, column DDL, properties, views) still ignore the session catalog for a bare name. That is a separate fix branch.

**NEXT:** bare-name session-catalog fix for the remaining `catalog_ops` ALTER paths; then #912.
