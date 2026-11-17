# Design: system catalog split

## Context

`SystemCatalogProvider::schema` answers `jdbc`, `metadata` and `runtime` and `None` otherwise (`crates/sqe-catalog/src/system_catalog.rs:83`). `information_schema` is served by DataFusion in every catalog. The policy rewriter skips tag policies for any table whose catalog is `system` or `datafusion` (`plan_rewriter.rs:1136`).

## Decisions

### D1. One list of reserved schemas, matched by (catalog, schema)

`sqe-catalog` exports `RESERVED_SYSTEM_SCHEMAS = ["runtime", "metadata", "jdbc", "information_schema"]` and `fn is_virtual_system_ref(catalog, schema) -> bool` (ASCII case-insensitive, like DataFusion's identifier fold). Every place that today tests `catalog == "system"` calls it instead: the policy rewriter, `qualify`, the result cache, the Polaris load check, Trino compat and MCP. A grep gate in the tasks lists every `"system"` literal and each one is either moved to the helper or justified.

As built: the helper lives in `sqe-core::system_catalog`, not `sqe-catalog`, because `sqe-policy`, `sqe-sql`, `sqe-mcp` and `sqe-trino-compat` need it and must not depend on `sqe-catalog`. The schema matches ASCII case-insensitively. The catalog matches exactly (`system`), like DataFusion's catalog lookup after the unquoted-identifier fold: a quoted `"SYSTEM"` is a different catalog. The MCP denylist folds the catalog too, because a denylist may be wider than routing but never narrower. `ATTACH ... AS system` (and `AS datafusion`) is refused, so the only Polaris part of `system` is the one discovery finds. The Polaris part is looked up only with `catalog_discovery = "polaris-auto"`; in static mode `system` stays virtual-only.

`is_reserved_virtual_ref` keeps returning true for the `datafusion` catalog and for `information_schema` in any catalog; for `system` it returns true only for the reserved schemas. This is fail-closed in the direction that matters: a `system.access.audit` reference now goes through every policy step.

### D2. Composite provider, virtual first

`SystemCompositeProvider { virtual_: SystemCatalogProvider, polaris: Option<Arc<dyn CatalogProvider>> }`:

- `schema(name)`: reserved name -> virtual (a Polaris namespace of that name is never returned); otherwise `polaris.schema(name)`.
- `schema_names()`: virtual names, then Polaris names minus reserved ones.
- `register_schema` / `deregister_schema` for a reserved name: refused with a `Catalog` error naming the reserved list; otherwise delegated.

Registration: config and discovered catalogs register first; when the next step finds a Polaris catalog called `system` it wraps it instead of replacing it. Order no longer matters because the composite is built from both.

### D3. Known-catalog set and load check

`query_handler.rs:1292,1336` add `system` to the known set unconditionally, so a Polaris `system` is never probed. They add it only when no Polaris `system` exists; otherwise the normal probe path covers it. The Polaris load check skips only virtual refs (D1).

### D4. SHOW and CREATE SCHEMA

`SHOW SCHEMAS FROM system` lists the composite's `schema_names()`. `SHOW TABLES FROM system.<schema>` and `SHOW COLUMNS` route by D1. `CREATE SCHEMA system.<reserved>` is refused in the SQE handler before Polaris is called (Polaris would accept it), with the message "schema name `<x>` is reserved in catalog `system`".

### D5. Procedures keep their meaning

`CALL system.<proc>(...)` and `CALL <catalog>.system.<proc>(...)` are recognised by the pre-parser (`sqe-sql/src/procedures.rs`) before any catalog lookup, so a Polaris namespace `system` inside catalog `system` cannot capture them. A test pins this: `CALL system.rewrite_data_files` with a Polaris `system` catalog present still runs the procedure on the session catalog.

### D6. Results cache

Results reading a virtual schema are never cached (today's rule, narrowed). Results reading the Polaris part follow the normal cache rules, which already key on the caller and the snapshot.

## Risks

- A missed `"system"` comparison leaves a Polaris table treated as virtual: no masks, no tag policies. Mitigation: D1's single helper, the grep task, and the acceptance test that a tag mask on `system.access.audit` applies.
- A Polaris namespace named `runtime` that predates the change becomes unreachable. Documented; D4 refuses new ones.
