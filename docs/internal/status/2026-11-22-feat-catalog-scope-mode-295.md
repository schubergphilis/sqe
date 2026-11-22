**STATUS 2026-11-22: catalog scope mode and refusal reasons (`feat/catalog-scope-mode-295`, #295, #908 follow-up).**

`[session.catalog_scope] mode = "binding" | "union" | "off"` (OpenSpec `catalog-scope-mode`). `binding` is the default and today's behaviour. `union` still requires, verifies and audits the scope on the doors that carry one, but attaches nothing, so the session is not narrowed and the scoped-client check at context build (MCP, Quack) stays off. `off` installs no gate and ignores the header. No section means off; `binding` and `union` refuse to start without `client_id`, `issuer` and `jwks_url`. The mode is logged at startup, as a `warn` when a configured section sets `union` or `off` (enforcement relaxed by config).

Refusal reasons: `ScopeReason { NotInWorkspace, ReadOnly }` is the source of the coded error, and the audit record's `error_type` names it (`catalog_not_in_workspace`, `catalog_read_only`). The client still gets "Catalog 'x' not found"; the read-only message ends in `(catalog_read_only)`. To keep the reason on every path:
- `preflight_resolve_catalogs` refuses a hidden 3-part qualifier before resolution or discovery, so the planner's "table not found" never stands in;
- `plan_sql_for_stream` maps a planning failure to the scope refusal when the plan names a hidden catalog (`catalog_scope_gate::hidden_catalog_refusal`, covers a hidden default catalog);
- `rest_catalog::bridge_error` keeps the reason as the iceberg error's source, and the create/drop table write sites keep it through `keep_scope_reason` instead of flattening to `SqeError::Catalog(String)`.

Gate: `make test-integration FILTER=catalog_scope` -> 5 passed, 0 failed (incl. new `catalog_scope_modes_union_verifies_without_narrowing_and_off_ignores` and the reason assertions in `acceptance_2_and_3_...` for SELECT, USE, INSERT, DELETE, UPDATE, CREATE TABLE, CTAS, DROP TABLE, CALL). `cargo test -p sqe-core --lib catalog_scope` -> 10 passed; `cargo test -p sqe-coordinator --lib audit_emitter` -> 6 passed; clippy on sqe-core, sqe-catalog, sqe-write, sqe-coordinator clean.

Left: the union-mode test checks that a valid scope attaches nothing and leaves no effective scope (offline); a live read outside the scope under `union` is not exercised. An unreachable JWKS under `union` follows the same verify path as `binding` and has no own test.

**NEXT:** #897, then the test/ MR making `refuse_at_write_mask_conflict` and `tag_mask_beats_resource_mask_live` mode-aware.
