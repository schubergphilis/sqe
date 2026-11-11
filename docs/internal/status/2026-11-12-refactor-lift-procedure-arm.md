**STATUS 2026-11-12: Procedure dispatch arm lifted into `handle_procedure_statement` (`refactor/lift-procedure-arm`, stacked on `refactor/lift-utility-arm`, openspec statement-path-restructure 3.5).**

A pure move, checked with a whitespace-insensitive `--color-moved` diff. The only non-moved lines are the signature, the doc comment, the call, `*captured_plan = Some(..)`, and three rustfmt joins inside the new function (one `else if let` header, two `invalidate_session_cache` calls). `call` stays `&Box<sqe_sql::ProcedureCall>` under `#[allow(clippy::borrowed_box)]` so the body's `**call` is untouched. Gates: check-server, clippy -D warnings, budgets, `cargo test --lib query_handler` (109 passed), `make test-integration FILTER=procedure` (14 passed).

**NEXT:** 3.6 lift the `Ctas` arm.
