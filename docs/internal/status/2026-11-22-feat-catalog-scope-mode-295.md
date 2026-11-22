**STATUS 2026-11-22: catalog scope mode and refusal reasons (`feat/catalog-scope-mode-295`, #295, #908 follow-up).**

`[session.catalog_scope] mode = "binding" | "union" | "off"` (OpenSpec `catalog-scope-mode`). `binding` is the default and today's behaviour. `union` still requires, verifies and audits the scope on the doors that carry one, but attaches nothing, so the session is not narrowed and the scoped-client check at context build (MCP, Quack) stays off. `off` installs no gate and ignores the header. No section means off; `binding` and `union` refuse to start without `client_id`, `issuer` and `jwks_url`. The mode is logged at startup, as a `warn` when a configured section sets `union` or `off` (enforcement relaxed by config).

Refusal reasons: `ScopeReason { NotInWorkspace, ReadOnly }` is the source of the coded error, and the audit record's `error_type` names it (`catalog_not_in_workspace`, `catalog_read_only`). The client still gets "Catalog 'x' not found"; the read-only message ends in `(catalog_read_only)`. To keep the reason on every path:
- `preflight_resolve_catalogs` refuses a hidden 3-part qualifier before resolution or discovery, so the planner's "table not found" never stands in;
- `plan_sql_for_stream` maps a planning failure to the scope refusal when the plan names a hidden catalog (`catalog_scope_gate::hidden_catalog_refusal`, covers a hidden default catalog);
- `rest_catalog::bridge_error` keeps the reason as the iceberg error's source, and the create/drop table write sites keep it through `keep_scope_reason` instead of flattening to `SqeError::Catalog(String)`.

Gate: `make test-integration FILTER=catalog_scope` -> 5 passed, 0 failed (it runs `--ignored` only: `acceptance_2_and_3_...` with the reason assertions for SELECT, USE, INSERT, DELETE, UPDATE, CREATE TABLE, CTAS, DROP TABLE, CALL, plus four access_control_e2e tests that skip without `SQE_AC_E2E`). The offline tests are plain `#[tokio::test]` and run with `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it catalog_scope_e2e` -> 3 passed, 1 ignored: `acceptance_1_scoped_client_needs_a_valid_scope`, `catalog_scope_modes_union_verifies_without_narrowing_and_off_ignores`, `catalog_scope_unreachable_jwks_refuses_in_union_and_binding` (a JWKS on a closed port refuses a well-signed scope and audits it, in `union` and `binding`). `cargo test -p sqe-core --lib catalog_scope` -> 10 passed; `cargo test -p sqe-coordinator --lib audit_emitter` -> 6 passed; clippy on sqe-core, sqe-catalog, sqe-write, sqe-coordinator clean.

Left: the union-mode test checks that a valid scope attaches nothing and leaves no effective scope (offline); a live read outside the scope under `union` is not exercised.

Budgets: sqe-catalog 68641 (ceiling 68500 -> 69400), sqe-coordinator 71582 (71250 -> 72300), sqe-core 26712 (26400 -> 27000), sqe-write 34025 (34000 -> 34400). Raised about 1% by owner decision 22 Nov 2026; the crate reorganisation is #938.

**NEXT:** #897, then the test/ MR making `refuse_at_write_mask_conflict` and `tag_mask_beats_resource_mask_live` mode-aware.
