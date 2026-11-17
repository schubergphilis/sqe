## 1. Reserved-schema helper

- [x] 1.1 `RESERVED_SYSTEM_SCHEMAS` + `is_virtual_system_ref(catalog, schema)` in `sqe-core::system_catalog` (moved from `sqe-catalog`, see design D1); unit tests incl. case folding
- [x] 1.2 Grep every `"system"` literal in `crates/` that compares a catalog name; move each to the helper or note why it stays (stay: the audit actor `"system"`, the `CALL system.` pre-parse in `procedures.rs`, the known-catalog insert in `query_handler.rs`)

## 2. Composite provider

- [x] 2.1 `SystemCompositeProvider` (D2) with unit tests: reserved -> virtual, other -> Polaris, reserved Polaris namespace hidden, register refused
- [x] 2.2 Registration in `session_context.rs`: wrap a Polaris `system` instead of replacing it
- [x] 2.3 Known-catalog set (`query_handler.rs:1292,1336`) and Polaris load check (:5801)

## 3. Policy and cache

- [x] 3.1 `is_reserved_virtual_ref` and both callers (`plan_rewriter.rs:513,1228`)
- [x] 3.2 `qualify` (`sqe-policy/src/lib.rs:149`)
- [x] 3.3 Result cache rule (`query_cache.rs:167`)

## 4. Statements

- [x] 4.1 `SHOW SCHEMAS/TABLES/COLUMNS FROM system...` merge (`query_handler/show.rs`)
- [x] 4.2 `CREATE SCHEMA system.<reserved>` refused
- [x] 4.3 `CALL system.<proc>` pinned by a test with a Polaris `system` present (D5)
- [x] 4.4 Flight SQL ingest, INSERT, DELETE into `system.<polaris schema>` (CREATE, INSERT, DELETE, SELECT, DROP in `system_split_polaris_table_round_trip`; ingest in `system_split_flight_ingest_appends_to_polaris_table`)

## 5. Protocols

- [x] 5.1 Trino compat `info_schema_compat.rs:31`
- [x] 5.2 MCP phantom-catalog check (`sqe-mcp/src/lib.rs:1338,2045,2181`) uses the helper

## 6. Tests and docs

- [x] 6.1 Integration: acceptance 1-6 (Polaris catalog `system` created by `scripts/bootstrap-test.sh`)
- [ ] 6.2 Ranger e2e: tag mask on `system.access.audit`, refused without grant
- [x] 6.3 Book: catalogs page documents the split and the reserved names
