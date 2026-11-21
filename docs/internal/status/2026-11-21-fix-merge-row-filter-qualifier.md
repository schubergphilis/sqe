**STATUS 2026-11-21: MERGE works under a row filter and without a target alias (`fix/merge-row-filter-qualifier`).**

Found while gating the table-tag inheritance MR (!1910): a MERGE into a row-filtered table failed with `No field named <table>.id`. Two bugs:

- `sqe-policy/src/write_predicates.rs`: `unparse_predicates` rendered the row filter with the probe table's qualifier. DELETE and UPDATE alias their scratch table to the table name, so it resolved there; MERGE reads the target through `__sqe_merge_target_*` (`target_policy_select`, and the WITH CHECK in `merge_select`), where it did not. The filter is now unparsed through `unnormalize_col`, like masks.
- `sqe-write/src/merge_sql.rs`: `alias_unaliased_tables`, called at the top of `dispatch_merge_on`. MERGE without a target alias defaulted the alias to `t` (source `s`), so `ON tbl.id` and `ON ns.tbl.id` failed with or without a policy. The bare table name becomes the alias and qualified references in ON / WHEN / SET are shortened to it. A source table whose bare name equals the target's alias is left alone.

Tests: `crates/sqe-coordinator/tests/it/merge_row_filter_e2e.rs` runs MERGE UPDATE, an unaliased MERGE, MERGE DELETE and MERGE INSERT (inside and outside the filter) under an in-memory row filter `id > 1`, in copy-on-write and merge-on-read, and checks the raw table after each step; three unit tests for the alias rewrite.

Gate: `make test-integration FILTER=merge_row_filter` -> 2 passed; `FILTER=merge` -> 16 passed; `cargo test -p sqe-write -p sqe-policy --lib` -> 380 + 369 passed; clippy on sqe-policy, sqe-write and sqe-coordinator clean; budgets ok.

Left open: the tag-inheritance docs and test (!1910) leave MERGE out; restore those claims once both MRs have merged.

**NEXT:** restore the MERGE claims in the tag-inheritance docs, then schema and catalog tag inheritance (#924 part 2).
