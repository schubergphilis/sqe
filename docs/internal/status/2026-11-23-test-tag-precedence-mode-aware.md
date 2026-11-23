**STATUS 2026-11-23: access-control live suite asserts each mask precedence mode (`test/tag-precedence-mode-aware`).**

`make test-access-control` used to assume the default `policy.mask_precedence = tag`, so the `refuse` mode a deployment ships had no end-to-end run. The suite's handler already took `SQE_POLICY__MASK_PRECEDENCE` from the env overlay; two tests hard-coded the `tag` outcome.

- `tag_mask_beats_resource_mask_live` reads the suite's mode (`suite_precedence()`, from the same config the handler loads) and asserts that mode's outcome: tag mask under `tag`, resource mask (`xxx-xx-`) under `resource`, a refused read under `refuse`.
- `refuse_at_write_mask_conflict` runs its `tag` step on an explicit `precedence_handler(Tag)`, so it holds under any suite mode.
- Helpers: `precedence_handler`, `read_refused`, `sees_on`.

Gates: clippy `-p sqe-coordinator --features test-sqlite --test it -D warnings` exit 0; rustfmt check rc 0. Live runs on their own compose project, each torn down with `down -v`:
- `SQE_POLICY__MASK_PRECEDENCE=refuse`: 77 passed, 0 failed (460.8 s). The log shows the plan rewriter refusing bob's read on the `ssn` mask conflict.
- `SQE_POLICY__MASK_PRECEDENCE=tag`: 77 passed, 0 failed (400.9 s).
- `SQE_POLICY__MASK_PRECEDENCE=resource`: 77 passed, 0 failed (447.9 s).

**NEXT:** the owner-blocked Track 1 items (#942 precedence decision, #943).
