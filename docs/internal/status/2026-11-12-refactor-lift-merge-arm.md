**STATUS 2026-11-12: MERGE dispatch arm lifted into `handle_merge_statement` (`refactor/lift-merge-arm`, stacked on `refactor/lift-drop-arm`, openspec statement-path-restructure 3.2).**

A pure move, checked with `--color-moved`. The only non-moved lines are the signature, the doc comment, the call, `*captured_plan` / `*policy_summary`, `plan_metrics` passed without the extra borrow (it is now `&Arc<..>`), and a rustfmt wrap of one `return Err(..)`. The function takes eight arguments, so it carries `#[allow(clippy::borrowed_box, clippy::too_many_arguments)]`. Gates: check-server, clippy -D warnings, `make test-integration FILTER=merge` (12 passed), budgets.

**NEXT:** 3.3 lift the `Insert` arm.
