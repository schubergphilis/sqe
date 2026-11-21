**STATUS 2026-11-21: one row filter per table and user (TA-13, #931), `feat/one-row-filter-per-table-ta13`.**

- `plan_rewriter::merge_tag_row_filters`: once a tag contributes a row filter, more than one distinct filter for the table (resource or tag, bare or catalog-scoped) is a `PolicyConflict::RowFilter` under every `mask-precedence`. It used to be refused only under `refuse`; the other modes ANDed. The same filter twice counts once, the deny-all sentinel never counts, and resource-only filters still AND.
- Callers already turned any conflict into `row filter conflict on <table>` (rewriter) or a denied policy (`resolve_effective_policy`), so no caller changed. DML targets go through the rewriter and are refused alike.
- Behaviour change for tag policies: a user covered by both a resource and a tag row filter now gets an error instead of the intersection.
- OpenSpec change `one-row-filter-per-table`; book section "One row filter per table".

Gate:
- `cargo test -p sqe-policy`: 365 + 106 passed (6 new row-filter tests under `tag`, `resource`, `refuse`).
- `scripts/access-control-test.sh tag_and_resource_row_filter`: 1 passed (SELECT and DELETE refused for bob, alice unaffected).
- Full `scripts/access-control-test.sh`: 65 passed, 1 failed (exit 101). The failure is `tag_from_out_of_band_commit_masks_the_next_query`, which already fails on main and is fixed in !1897, which this branch does not contain.
- `cargo clippy -p sqe-policy -p sqe-coordinator --all-targets -- -D warnings`: exit 0.

Open: the #918 write guard (`tag_conflict_guard`) runs only under `refuse`, so in the other modes a `SET TAGS` that creates a row-filter conflict is accepted and shows up when the table is read (tasks 4.1).

Base: stacked on `fix/tag-views-catalog-arg` (!1895).

**NEXT:** extend the write guard to row-filter conflicts in every mode once !1897 lands.
