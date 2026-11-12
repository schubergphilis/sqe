**STATUS 2026-11-12: DDL statement family moved into `query_handler/ddl.rs` (`refactor/move-ddl-family`, stacked on `refactor/move-catalog-family`, openspec statement-path-restructure 3.7, fifth family).**

Three methods moved verbatim (line-range cut, bottom-up), all `pub(super)`: `handle_drop_statement`, `handle_create_view`, `handle_comment_on`. rustfmt on the new file only re-wrapped two `SqeError::with_code(.., format!(..))` calls and one signature; the normalised hash is identical before and after. Gates: check-server, clippy -D warnings, budgets (ceiling 11,750 -> 11,500), `cargo test --lib query_handler` (109), `make test-integration FILTER=drop` (16), `view` (7), `comment` (1). Distributed and access-control suites not run for this move.

**NEXT:** access-control family (`invalidate_policy_cache` .. `grants_to_record_batch`), after !1729 merges; then a full integration + distributed + access-control sweep on the top of the stack.
