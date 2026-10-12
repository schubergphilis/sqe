**STATUS 2026-09-11 (d): MCP catalog scoping (`feat/mcp-catalog-scoping`, stacked on !991); second of three MCP MRs.**

`[mcp] write_catalogs` gates the change tool on the statement's
parser-visible target: table of INSERT/UPDATE/DELETE/MERGE/CREATE TABLE
[AS]/ALTER TABLE/DROP/TRUNCATE/COMMENT, view of CREATE VIEW, schema of
CREATE/DROP SCHEMA (`write_targets` in sqe-mcp, over the sqlparser 0.62
AST). Sources stay reads. While gated, unresolvable targets and target-less
kinds (CALL, grants, policy DDL) are refused, not guessed. `[[hive.roots]]`
catalogs need the explicit entry even with an empty list, because Hive has
no per-user storage credentials (reversal 1 of the Hive design): an agent
writing there acts as the engine. The coordinator passes the root names
(`mcp_server::hive_root_names`); ATTACHed Hive catalogs cannot exist over
MCP since a session lives one request. `default_catalog`/`default_schema`
per endpoint and per static principal are set on the session, so they drive
`information_schema` qualification, name resolution and the gate alike. MCP
classifies through `SqlFrontDoor::new(WireDialect::Mcp)` (pre-parse pipeline
first, ORIGINAL text to the executor; the door's output is classification
input with time-travel clauses stripped). Found: `SHOW STATS` resolves
through `show_catalog` + `load_table`, so `get_table_stats` errors on a Hive
table; documented, tool description says so. 29 sqe-mcp tests (7 new), 3
sqe-core config tests. **NEXT (MR 3, `test/mcp-access-control-e2e`):**
`tests/it` over `/mcp` against Polaris + Ranger for masks, row filters,
denied tables invisible in `list_tables`, Hive write refusal; then
`search_tables` and `sample_rows` tools. Later: Hive `SHOW STATS`.
