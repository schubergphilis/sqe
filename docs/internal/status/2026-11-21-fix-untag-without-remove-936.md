**STATUS 2026-11-21: tag writes no longer remove table properties (`fix/untag-without-remove-936`, #936).**

Found by `make test-access-control` on main 1669cadac: `tag_authority_steward_tags_and_untags_their_column`, `tag_authority_bare_name_is_checked_against_the_session_namespace` and `tag_names_checks_the_live_ranger_tag_definitions` failed with `Principal 'dave' is not authorized for op 'REMOVE_TABLE_PROPERTIES'`. The steward grant in the suite holds `table-properties-set`, not `table-properties-remove`.

Cause (#920): `crates/sqe-write/src/live_tags.rs` emitted `RemoveProperties` for `sqe.column-tag-ids` when a write left no ids (the last tag removed) and in `restore()` for a property that was absent before the failed write.

Fix: `record()` overwrites stale ids with `ids::empty_ids(<new tags>)` (version 1, digest of the new map, no ids), which resolves by name exactly like an absent property. `restore()` returns one `SetProperties`: the raw values read, an absent tags property as `{}`, absent ids as empty ids bound to the restored map. New unit test `no_tag_write_removes_a_property` runs every start state through every write and the rollback.

Gate: `make test-access-control` -> 72 passed, 0 failed; `make test-integration FILTER=tag` -> 47 passed; `cargo test -p sqe-write --lib` -> 372 passed; clippy on sqe-write and sqe-core clean.

Left: `json_columns_update` in `catalog_ops.rs` still removes `sqe.json-columns` when the last JSON column goes; that runs under ALTER, which already needs structure privileges.

Pass 2 of the access-control suite under `SQE_POLICY__MASK_PRECEDENCE=refuse` showed two more failures, both expected: `tag_mask_beats_resource_mask_live` asserts tag precedence, and `refuse_at_write_mask_conflict` uses the default handler as tag mode to write a real conflict.

**NEXT:** schema tag inheritance behind an off-by-default flag (#924 part 2).
