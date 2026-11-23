**STATUS 2026-11-23: several tag masks on one column join to the most restrictive (`fix/tag-mask-most-restrictive-942`, #942).**

`merge_tag_masks` under `tag` and `resource` precedence applied the first tag in stored order that had a mask, so a steward holding only `apply_tag` could add a weaker tag that sorted first and loosen a column. It now collects the mask of every tag on the column and joins them (`join_masks` in `plan_rewriter.rs`), independent of tag order: `Nullify` beats all, `Redact` beats the rest (two different redactions join to `Nullify`), partial masks join to their meet (min `show_first`, min `show_last`, replacement chars kept only when all agree, else one class-less char), and masks that cannot be joined (Hash next to a partial mask, two different CUSTOM masks, and so on) are a `PolicyConflict::Mask`: the column is restricted and the read refused. `TagConflictGuard` already re-runs the merge, so the tag write that creates such a conflict is refused; its message now names the configured precedence. Resource-versus-tag semantics, `refuse` mode, unmappable-tag handling and row filters are unchanged. A broken CUSTOM template on any tag still applies no tag mask.

Limit: a column that already carries two tag masks that cannot be joined refuses reads after upgrade until a tag is removed.

Gate: `cargo test -p sqe-policy` 406 + 109 passed; `make test-integration FILTER=tag` 60 passed; `make test-access-control` 79 passed (includes `weaker_tag_does_not_loosen_a_masked_column_live`). Spark parity not run.

**NEXT:** unchanged
