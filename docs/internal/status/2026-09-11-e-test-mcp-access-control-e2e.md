**STATUS 2026-09-11 (e): MCP access-control e2e + discovery tools (`test/mcp-access-control-e2e`, stacked on !992); third of three MCP MRs.**

`tests/it/mcp_access_control_e2e.rs` drives `sqe_coordinator::mcp_server::mcp_router`
in-process over a Ranger-wired handler sharing the fixture's warm cache, with
a test `AuthProvider` that maps a synthetic bearer (`sub` = persona, scopes
in the payload) to the persona's REAL Keycloak session, so Polaris still
sees the user's token. Four cases: denied-but-valid read (carol control,
alice `eventually` denied) plus `list_tables` parity with `SHOW TABLES`;
MASK_NULL + MASK_SHOW_LAST_4 + `region = 'EU'` row filter through
`execute_sql_readonly` AND `sample_rows` (settled through the handler first,
so a slow policy cache is a handler finding, not an MCP one); the change
tool's role gate (alice, -32003 "writer role"), catalog gate (`ops_wh`
outside `write_catalogs`) and source gate (dave) around a live Polaris
INSERT grant for bob; `search_tables` substring match, missing-catalog
error and literal safety. The module name contains `access_control_e2e`,
so `scripts/access-control-test.sh` runs it by default (the script's
`$1` filter now passes a name containing `access_control_e2e` through
unprefixed; before, `access-control-test.sh mcp_access_control_e2e` built
`access_control_e2e::mcp_access_control_e2e`, matched nothing and exited 0
with "0 passed": the green-that-never-ran the suite exists to avoid). New
tools: `search_tables(pattern, [catalog])`, a server-side LIKE
(`like_matches`, bare word = substring) over `SHOW SCHEMAS` + `SHOW TABLES`
per schema, unlistable schemas skipped, capped at `max_result_rows`; and
`sample_rows(catalog, schema, table, [limit])`, default 10, capped 100.
**Finding (file an issue):** a catalog-qualified
`sales_wh.information_schema.tables` is answered by DataFusion's BUILT-IN
information_schema, not `sqe_catalog::info_schema::InformationSchemaProvider`:
the live probe returned 40 rows, all `information_schema`/`runtime`, and no
`ac.orders` that `SHOW TABLES FROM sales_wh.ac` lists. The first
`search_tables` was written over information_schema and returned nothing
against the real stack; the unit tests could not see that. Live run: 3/4
green before the rewrite. 33 sqe-mcp tests. **NEXT:** merge !991 ->
retarget !992 -> retarget this; then the information_schema issue, Hive
`SHOW STATS` for `get_table_stats`, MCP resources/prompts, and the mode-2
quickstart arm.
