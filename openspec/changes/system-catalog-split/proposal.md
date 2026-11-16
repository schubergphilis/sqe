# `system` catalog: virtual schemas stay, the rest goes to a Polaris catalog named `system` (#901)

## Why

A governed Iceberg catalog named `system` (audit, query history, job runs, lineage) lets SQL written for Databricks `system.access.*`, `system.query.*` and `system.lakeflow.*` run unchanged on SQE. SQE claims the whole name `system` today: the virtual provider is registered last (`crates/sqe-coordinator/src/session_context.rs:649`) and silently replaces a Polaris catalog of that name, and policy, cache, qualification, Trino compat and MCP all treat every `system.*` reference as virtual. Nothing can be exported there.

## What Changes

- Under `system`, only the virtual schemas `runtime`, `metadata`, `jdbc` and `information_schema` are served by `SystemCatalogProvider`. Every other schema resolves through a Polaris REST catalog named `system` when one is configured or discovered.
- A composite provider registered as `system` does the split. Without a Polaris `system` catalog it is today's provider and nothing changes.
- The Polaris part gets every check any catalog gets: the caller's token, Ranger resource policies, masks, row filters, tag policies, the Polaris load check, audit and OpenLineage. Writes (Flight SQL ingest, INSERT, DELETE) work as on any catalog.
- `is_reserved_virtual_ref`, `qualify`, the result-cache rule, the known-catalog set, Trino compat and MCP match the virtual *schemas*, not the catalog name.
- `SHOW SCHEMAS FROM system` (and tables/columns) merge the virtual and Polaris schemas.
- Creating a schema named `runtime`, `metadata`, `jdbc` or `information_schema` in the Polaris `system` catalog is refused; one that already exists is ignored.

## Capabilities

### New Capabilities

- `system-catalog`: how the name `system` is split between SQE's virtual schemas and a governed Polaris catalog.

### Modified Capabilities

None.

## Impact

- `sqe-catalog`: a `SystemCompositeProvider` (virtual schemas first, Polaris for the rest), `RESERVED_SYSTEM_SCHEMAS` constant shared by every caller.
- `sqe-coordinator`: registration (`session_context.rs:545-572,649`), known-catalog set (`query_handler.rs:1292,1336`), Polaris load check (`query_handler.rs:5801`), result cache (`query_cache.rs:167`), `SHOW SCHEMAS/TABLES/COLUMNS` (`query_handler/show.rs`), `CREATE SCHEMA` refusal.
- `sqe-policy`: `is_reserved_virtual_ref` and both callers (`plan_rewriter.rs:513,1228`), `qualify` (`lib.rs:149`).
- `sqe-trino-compat` (`info_schema_compat.rs:31`) and `sqe-mcp` (`lib.rs:1338,2045,2181`).
- `sqe-sql`: `CALL system.<proc>` keeps its meaning (procedures resolve before catalog lookup).
- Security-relevant: the change moves references from "skipped by policy" to "policed". The failure to guard against is the reverse: a Polaris table reachable under a name the rewriter still treats as virtual.
- Out of scope: the export itself, renaming the catalog, new virtual tables.
