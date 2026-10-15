**STATUS 2026-10-15: the MCP read-parity access-control test reads `list_tables`' real column again (`test/mcp-list-tables-column-843`, #843).**

`mcp_reads_are_denied_for_the_ungranted_user_and_valid_for_the_admin` failed deterministically on main since `list_tables` rows became `{table_name, table_type}`: the test still read `"Table"`, and `column()` indexed the JSON row with `r[name]`, which yields `null` for a missing key, so the MCP side compared as `[]` against the handler's `["orders"]`. The product was correct; the test was stale.

The test now reads `table_name`, and `column()` panics on a row without the requested field, as `col_strings` already does for record batches, so a renamed column fails loudly instead of comparing as empty. No other MCP test reads the old name; the remaining `"Table"` reads are `SHOW TABLES` results, whose column is still `Table`.

Verified: `scripts/access-control-test.sh mcp_access_control_e2e` 4/4.

**NEXT:** #844 (IncrementalTableProvider carries table properties).
