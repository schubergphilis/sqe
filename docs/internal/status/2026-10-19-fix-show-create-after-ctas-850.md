**STATUS 2026-10-19: SHOW CREATE TABLE lists columns right after a commit (`fix/show-create-after-ctas-850`, #850).**

`information_schema.columns` enumerates tables through the synchronous listing hooks, which serve only the process-global `TABLE_NAMES_SNAPSHOTS`; the pre-plan `refresh_listing_snapshots` is what fills them. Every commit path calls `invalidate_table_all_tokens`, which drops the namespace's snapshot for all tokens. `handle_show_create_table` planned its column query with `plan_sql` directly and skipped the refresh, so the first SHOW CREATE TABLE after a CTAS, INSERT or ALTER saw no table names and returned an empty body; the next query of any kind re-listed and healed it. It now calls `refresh_listing_snapshots` with the column SQL first, as `columns_for_table` (SHOW COLUMNS, DESCRIBE) already did.

Tests: `ctas_write_modes_e2e::show_create_table_right_after_ctas_lists_columns_850` (CTAS then SHOW CREATE, then ALTER + INSERT then SHOW CREATE); `variant_e2e::variant_survives_ctas_and_insert_select` now checks SHOW CREATE before its SELECT instead of after it.

**NEXT:** unchanged
