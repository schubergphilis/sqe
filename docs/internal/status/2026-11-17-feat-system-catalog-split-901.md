**STATUS 2026-11-17: the `system` catalog is split between SQE's virtual schemas and a Polaris catalog named `system` (`feat/system-catalog-split-901`, #901).**

With `catalog_discovery = "polaris-auto"`, `system.runtime`, `system.metadata`, `system.jdbc` and `system.information_schema` stay SQE's virtual schemas. Every other `system.<schema>` resolves to the Polaris catalog `system` when one exists. Before this change, SQE answered every `system.*` name itself, so a Polaris catalog named `system` could not be reached.

- `sqe-core::system_catalog` holds the reserved schema names and the match rule. The catalog name must match exactly and the schema name ignores case.
- `sqe-catalog`: `SystemCompositeProvider` serves the virtual schemas and delegates the rest to Polaris.
- Coordinator: SHOW SCHEMAS, TABLES and COLUMNS cover both parts. `ATTACH ... AS system` and `AS datafusion` are refused. `CREATE SCHEMA system.<reserved>` is refused with `INVALID_ARGUMENTS`.
- `CALL system.<procedure>` still runs the built-in procedures.
- Policy rewriting, SQL qualification, the query cache, the MCP denylist and the Trino `information_schema` compat path use the same rule.
- `scripts/bootstrap-test.sh` creates a Polaris catalog `system` (bucket `warehouse-system`, namespace `access`) for the tests.

Gate: `make test-integration FILTER=system_split`: 6 passed, 0 failed. The tests:

- `system_split_show_schemas_lists_both_parts`
- `system_split_virtual_tables_unchanged`
- `system_split_polaris_table_round_trip`
- `system_split_flight_ingest_appends_to_polaris_table`
- `system_split_create_reserved_schema_refused`
- `system_split_call_procedure_unchanged`

The full suite passed 415/415 on this tree before the ingest test was added.

Docs: `operations/catalogs.md`, new section "A Polaris catalog named `system`".

Left open on #901: task 6.2, a Ranger e2e test for a tag mask on `system.access.audit` and a refusal without a grant. It needs a Polaris `system` catalog in the access-control stack.

**NEXT:** 6.2 Ranger e2e once the access-control stack carries a `system` catalog.
