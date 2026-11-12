**STATUS 2026-11-12: DROP dispatch arm lifted into `handle_drop_statement` (`refactor/lift-drop-arm`, stacked on `refactor/statement-stage-split`, openspec statement-path-restructure 3.1).**

A pure move, checked with `--color-moved`. The only non-moved lines are the signature, the call, `*captured_plan`, and a rustfmt reflow of the `routed_to_hive` match, whose tokens are unchanged. The parameter stays `&Box<Statement>` under `#[allow(clippy::borrowed_box)]` so the body is untouched; changing it to `&Statement` is a later cleanup. Gates: check-server, clippy -D warnings, `make test-integration FILTER=drop` (16 passed), budgets.

**NEXT:** 3.2 lift the `Merge` arm.
