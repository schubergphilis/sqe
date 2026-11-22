**STATUS 2026-11-22: sqe-policy test modules moved out of the two largest files (`refactor/split-policy-files-932`, #932).**

- Moved, not rewritten: `plan_rewriter.rs` `mod tests` -> `plan_rewriter/tests.rs`; `ranger_store.rs` `mod tests`, `mod mask_policy_listing_tests` and `mod catalog_scope_and_precedence_tests` -> `ranger_store/*.rs`. The only content change is the `include_str!` path (`../testdata/...`). `plan_rewriter.rs` 3346 -> 1688 lines, `ranger_store.rs` 5092 -> 2579; both are under the 3000-line default, so their file budget entries are dropped.
- The new files are rustfmt-clean (the rustfmt ratchet has no HEAD baseline for a new file). The inline modules carried rustfmt drift, so formatting them adds about 360 lines; production code is untouched.
- Budget: sqe-policy crate 19400 -> 19700 (19684 with this change, all from formatting the moved tests); owner decision 22 Nov 2026, #938. Crate counts include in-crate tests, so a split inside a crate never lowers a crate budget.
- Gates: `cargo test -p sqe-policy` (383 lib + 106 it passed), `cargo clippy -p sqe-policy --all-targets -- -D warnings`, `cargo check-server`, `cargo xtask budgets --check`, all exit 0. A whitespace-insensitive comparison of each moved module against HEAD shows them identical, and the remaining source byte-identical apart from the `mod NAME;` lines.
- Not run: `make test-integration` (pure test-module move, no runtime change).

**NEXT:** #938 crate budgets back to their pre-raise ceilings needs code to leave the crate (sqe-catalog ~1860 lines, sqe-coordinator and sqe-core ~340 each, sqe-write ~70), so it gets its own OpenSpec change.
