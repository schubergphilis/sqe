**STATUS 2026-11-12: INSERT dispatch arm lifted into `handle_insert_statement` (`refactor/lift-insert-arm`, stacked on `refactor/lift-merge-arm`, openspec statement-path-restructure 3.3).**

A pure move, checked with a whitespace-insensitive `--color-moved` diff. The only non-moved lines are the signature, the doc comment, the call, `&mut *captured_plan` / `&mut *policy_summary`, and three rustfmt reflows inside the new function (one `invalidate_session_cache` call, two `SqeError::with_code` calls). The body reads neither `query_id` nor `plan_metrics`, so the method takes five arguments. Gates: check-server, clippy -D warnings, `make test-integration FILTER=insert` (23 passed), budgets.

**NEXT:** 3.4 lift the `Utility` arm.
