## 1. Reserved-schema helper

- [ ] 1.1 `RESERVED_SYSTEM_SCHEMAS` + `is_virtual_system_ref(catalog, schema)` in `sqe-catalog`; unit tests incl. case folding
- [ ] 1.2 Grep every `"system"` literal in `crates/` that compares a catalog name; move each to the helper or note why it stays

## 2. Composite provider

- [ ] 2.1 `SystemCompositeProvider` (D2) with unit tests: reserved -> virtual, other -> Polaris, reserved Polaris namespace hidden, register refused
- [ ] 2.2 Registration in `session_context.rs`: wrap a Polaris `system` instead of replacing it
- [ ] 2.3 Known-catalog set (`query_handler.rs:1292,1336`) and Polaris load check (:5801)

## 3. Policy and cache

- [ ] 3.1 `is_reserved_virtual_ref` and both callers (`plan_rewriter.rs:513,1228`)
- [ ] 3.2 `qualify` (`sqe-policy/src/lib.rs:149`)
- [ ] 3.3 Result cache rule (`query_cache.rs:167`)

## 4. Statements

- [ ] 4.1 `SHOW SCHEMAS/TABLES/COLUMNS FROM system...` merge (`query_handler/show.rs`)
- [ ] 4.2 `CREATE SCHEMA system.<reserved>` refused
- [ ] 4.3 `CALL system.<proc>` pinned by a test with a Polaris `system` present (D5)
- [ ] 4.4 Flight SQL ingest, INSERT, DELETE into `system.<polaris schema>`

## 5. Protocols

- [ ] 5.1 Trino compat `info_schema_compat.rs:31`
- [ ] 5.2 MCP phantom-catalog check (`sqe-mcp/src/lib.rs:1338,2045,2181`) uses the helper

## 6. Tests and docs

- [ ] 6.1 Integration: acceptance 1-6 (Polaris catalog `system` created by the test fixture)
- [ ] 6.2 Ranger e2e: tag mask on `system.access.audit`, refused without grant
- [ ] 6.3 Book: catalogs page documents the split and the reserved names
