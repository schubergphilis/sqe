**STATUS 2026-11-21: `mask-precedence = "refuse"`, the read-time half of #918 (`feat/policy-conflict-refuse-918`).**

- `MaskPrecedence::Refuse` (`sqe-core` config). `merge_tag_masks` now returns the conflicts it found; under `Refuse` it hands off to `merge_tag_masks_refusing`, which collects every mask that resolves for a column (resource plus every tag, not the first tag only) and every row filter once a tag adds one. Equal masks and filters dedupe: `MaskType` gained `PartialEq`, and a CUSTOM mask compares by its expression after `{col}` substitution.
- More than one distinct mask: `PolicyConflict::Mask(column)`, and the column is restricted. More than one distinct row filter: `PolicyConflict::RowFilter`. The `lit(false)` deny-all sentinel is never counted and is always kept, including when a tag filter supplies it.
- `evaluate_plan` turns the first conflict into `ACCESS_DENIED` naming the qualified table (and column). `resolve_effective_policy` maps a conflict to the deny-all sentinel. Its callers (metadata TVFs, SHOW STATS, SHOW EFFECTIVE POLICY, the Hive visibility gate) only read the policy, so they fail closed.
- The fail-closed rules of `tag` and `resource` hold under `refuse`: a restricted column stays restricted, an unmappable tag with no resource mask restricts the column, a CUSTOM template that does not parse restricts it when nothing else masks it.

Tests: `sqe-policy` unit tests `merge_tag_masks_refuse_same_custom_mask_from_two_tags_is_one_mask` and `merge_tag_masks_refuse_keeps_the_fail_closed_rules`. The second one found that the first draft dropped a tag's `lit(false)`. Rewriter tests (`tests/it`): `contested_column_refuses_under_refuse`, `contested_column_refuses_nothing_when_the_masks_agree`, `contested_row_filter_refuses_under_refuse`. Results: `cargo test -p sqe-policy --lib` 352 passed, `--test it` 105 passed.

Not done (so the MR says Part of #918):
- No live Ranger test. `make test-access-control` runs with the default `tag` precedence, and the change is opt-in.
- Ask item 3: `SET TAGS` refusing a tag whose mask or row filter would conflict for some principal. This needs a principal-free comparison of the tag's policy items against the column's resource policies.

**NEXT:** the SET TAGS write-time conflict check (#918 item 3); live refuse case in the k3s governance suite.

Integration gate: `make test-integration FILTER=tag_mask` exited 0, but all 5 tests it selected skipped without the Ranger access-control stack (`SQE_AC_E2E=1`). It is not evidence; `make test-access-control` was not run.
