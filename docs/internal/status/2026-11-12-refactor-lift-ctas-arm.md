**STATUS 2026-11-12: CTAS dispatch arm lifted into `handle_ctas_statement` (`refactor/lift-ctas-arm`, stacked on `refactor/lift-procedure-arm`, openspec statement-path-restructure 3.6).**

A pure move, checked with a whitespace-insensitive `--color-moved` diff. The only non-moved lines are the signature, the doc comment, the call, `&mut *captured_plan` / `&mut *policy_summary`, and rustfmt joins inside the new function (error constructors and two `invalidate_session_cache` calls); every error message is byte-identical. Gates: check-server, clippy -D warnings, budgets, `cargo test --lib query_handler` (109 passed), `make test-integration FILTER=ctas` (16 passed). With all six arms lifted, `execute_inner` is 702 lines.

**NEXT:** full `make test-integration` on the tip of the lift stack, then 3.7 (move the `handle_*` families into `statement/*.rs`).
