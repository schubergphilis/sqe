**STATUS 2026-09-11 (c): MCP calls are attributable in the audit trail (`feat/mcp-audit-identity`); first of three MCP MRs (audit identity -> catalog scoping -> access-control e2e).**

Before this, an OCSF event for an AI agent's query through `/mcp` was
indistinguishable from a Flight SQL client's: the Trino door stamps
`Session.source` from `X-Trino-Source`, MCP never set it, and the audit
`Actor` had no field for it anyway. Now `Actor.app_name` (OCSF
`actor.app_name`) carries the session source for every protocol, and every
MCP tool call stamps `mcp:<tool>:<client>/<version>`. The client comes from
the `clientInfo` on `initialize`; SQE answers with an `Mcp-Session-Id` and
applies the label to later calls that echo it under the same identity. The
id is advisory (unknown, pre-restart, or another identity's id falls back to
the sanitized `User-Agent`, then `mcp:<tool>`), and the registry is bounded
(4096 entries, 24 h idle). The MCP auth rate limit now spends the same
`(peer IP, bearer fingerprint)` tuple as the Trino path instead of
`(peer IP, "mcp-bearer")`, which was one bucket per proxy address.
`sqe_trino_compat::server::bearer_fingerprint` is public for that; one
definition of "the same credential". 22 sqe-mcp tests (5 new), 71
sqe-metrics audit tests, `quickstart/mcp/test-mcp.sh` gains two checks
(not run here; needs the Docker stack). **NEXT (MR 2, `feat/mcp-catalog-scoping`):**
`[mcp] write_catalogs` allowlist (Hive roots write with engine credentials,
so the change tool needs a target gate), per-principal default catalog and
schema, `WireDialect::Mcp` through `SqlFrontDoor`, Hive over MCP documented
and `get_table_stats` checked against a Hive root. **MR 3:** `tests/it`
cases over `/mcp` against Polaris + Ranger for masks, row filters, denied
tables; then `search_tables` and `sample_rows` tools.
