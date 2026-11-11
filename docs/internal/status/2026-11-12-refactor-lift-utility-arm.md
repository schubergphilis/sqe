**STATUS 2026-11-12: Utility dispatch arm lifted into `handle_utility_statement` (`refactor/lift-utility-arm`, stacked on `refactor/lift-insert-arm`, openspec statement-path-restructure 3.4).**

A pure move, checked with a whitespace-insensitive `--color-moved` diff. The only non-moved lines are the signature, the doc comment, the call and two rustfmt reflows inside the new function. The body reads `session`, `stmt`, `query_id` and `memory` only; it writes neither the lineage hint nor the policy summary. Gates: check-server, clippy -D warnings, budgets, `cargo test --lib query_handler` (109 passed), `make test-integration` with `FILTER=explain` (4), `show` (11), `describe` (5), `time_zone` (1) and `materialized` (0 tests match).

**NEXT:** 3.5 lift the `Procedure` arm.
