**STATUS 2026-11-21: SHOW TAGS lists tag values (#923), `feat/tag-values-in-show-and-views`.**

- `SHOW TAGS ON <table>` returns `column`, `tag`, `value`. The values were stored since !1887 but `SHOW TAGS` read keys only (`load_column_tags`). A key-only tag shows `value = ''`, the `column_tags` view's form.
- `CatalogOps::load_column_tag_entries` loads the table's tags with their values; `load_column_tags` (keys only, used by `SHOW EFFECTIVE POLICY`) now wraps it.
- The `handle_show_tags` doc comment had drifted onto `handle_show_masking_policies`; moved back.
- The object tag views (`table_tags`, `schema_tags`, `catalog_tags`) are not in this MR: !1901 does them.

Gate:
- `make test-integration FILTER=ctas_write_modes_e2e`: 16 passed, 0 failed (new `show_tags_lists_tag_values_923`; `ctas_replace_with_a_new_schema_keeps_properties_and_tags_896` uses the changed helper).
- `cargo test -p sqe-coordinator --lib tags_batch`: 2 passed.
- `cargo clippy -p sqe-write -p sqe-coordinator --all-targets -- -D warnings`: exit 0.

**NEXT:** #928 tag projection drift (SET TBLPROPERTIES, CREATE OR REPLACE, RENAME).
