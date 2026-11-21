**STATUS 2026-11-21: `mask_precedence = refuse` refuses the conflicting tag write (#918, write-time half; `feat/policy-conflict-refuse-at-write-918`).**

OpenSpec change: `openspec/changes/policy-conflict-refuse-at-write/`.

- `PolicyStore::conflict_principals` (default `Ok(None)` = cannot say, write allowed). Ranger lists every user, role and group named in a mask or row-filter item of the matching resource policies and of the tag policies on the table's tags, plus a public principal, one synthetic `SessionUser` each (`ranger_store/conflict_principals.rs`).
- `TagConflictGuard` (`sqe-policy/src/tag_conflict_guard.rs`) resolves each principal, merges the tags before and after under `refuse`, and refuses when the after set holds a conflict the before set does not. A change that adds no tag is not checked. A store error refuses.
- `CatalogOps::set_column_tags` and `set_table_properties_guarded` take `Option<&TagConflictGuard>` and run it under the table lock, against the live map. The raw property path now takes the lock whenever a guard is present (so an admin's raw write is checked too); under `refuse` an admin's raw write over an unparseable live map is checked against an empty map.
- The coordinator builds the guard only under `refuse`, keyed like the tag gate (`tag_policy_key`), with the table's catalog: under `catalog_scoped` (#919) tags resolve per catalog as on the read path.
- Fixed the pre-existing failure of `tag_from_out_of_band_commit_masks_the_next_query` on main: it parsed `sqe.column-tags` as `Vec<String>`, which fails since tag values are stored as `{tag: value}` objects.

Gate (live Ranger, after the last edit, on top of !1895): `scripts/access-control-test.sh refuse_at_write` -> exit 0, 4 passed (`refuse_at_write_mask_conflict`, `refuse_at_write_disjoint_roles_allowed`, `refuse_at_write_row_filter_conflict`, `refuse_at_write_catalog_scoped_tag_policy`). Control: with the guard builder forced to `None`, `refuse_at_write_mask_conflict` fails with "write went through". Regression: `scripts/access-control-test.sh tag` -> exit 0, 8 passed. Unit: `cargo test -p sqe-policy --lib tag_conflict_guard` 10 passed, plus 2 Ranger principal tests. `cargo clippy -p sqe-policy-api -p sqe-policy -p sqe-write -p sqe-coordinator --all-targets -- -D warnings` exit 0.

Base: this branch sits on `fix/tag-views-catalog-arg` (!1895), because main at ca59929b does not compile.

Left open: conflicts that need two memberships of one user (read-time refusal only, pinned by a test); CREATE TABLE / CTAS carrying `sqe.column-tags`; Ranger policy edits after the tag is written (read-time refusal is the backstop).

**NEXT:** #920 (tag entries keyed by field id) as an OpenSpec change.
