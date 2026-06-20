# Updates from Book Review

Factual corrections and code tasks identified by audience reviewers.

## Book Fixes (applied)

- [x] Ch 2: "partition evolution unique to Iceberg" → "Iceberg pioneered transparent partition evolution"
- [x] Ch 2: S3 conditional writes footnote (Aug 2024 If-None-Match)
- [x] Ch 2: PyIceberg GIL characterization — clarify scan planning is GIL-bound, I/O is not
- [x] Ch 3: Pull vs push oversimplification — add nuance about HashJoin/aggregation materialization
- [x] Ch 3: DuckDB comparison table — "no user-extensible plan rewriting" not just "No"
- [x] Ch 3: Flag `block_in_place` + `block_on` as known DataFusion design constraint
- [x] Ch 7: Fix `x-iceberg-update-sequence-number` — Iceberg REST spec uses `assert-current-snapshot-id` in body
- [x] Ch 8: Remove "designed not implemented" language — security features are being implemented now
- [x] Ch 8: Note that `sha256` requires UDF registration (not built-in DataFusion)
- [x] Ch 1: Acknowledge Polaris has role-based access control (not "no opinions" about security)

## Code Fixes (applied)

- [x] **sqe-policy: sha256 UDF** — `sha256_udf.rs` with ScalarUDFImpl, registered in coordinator SessionContext. 3 passing tests.
- [x] **sqe-policy: PolicyPlanRewriter** — `plan_rewriter.rs` with row filter injection, column masking, column restriction. Uses `transform_down` on LogicalPlan.
- [x] **sqe-policy: PolicyStore trait** — in `lib.rs` with `ResolvedPolicy`, `MaskType` enum.
- [x] **sqe-policy: InMemoryPolicyStore** — `policy_store.rs` for testing/dev. 2 passing tests.
- [x] **sqe-policy: OPA backend** — `opa.rs` with REST client, response parsing, moka cache. 5 passing tests.
- [x] **sqe-catalog: SchemaProvider safety docs** — added comment block about `block_in_place` as DataFusion design constraint.
- [x] **Cargo.toml: sha2 workspace dependency** — added `sha2 = "0.10"`.

All 12 new tests pass. Full workspace compiles clean (`cargo check --all`).

## Remaining Code Tasks (TODO)

- [ ] **sqe-policy: Cedar backend** — local Cedar evaluation with entity-based policies
- [ ] **sqe-policy: SQL extensions** — GRANT/REVOKE/SHOW GRANTS handlers (parser routing exists, handlers need implementation)
- [ ] **sqe-catalog: Evaluate S3 conditional writes** — If-None-Match for direct S3 commits without catalog
- [ ] **sqe-coordinator: DataFusion version upgrade strategy** — document trait API changes between versions
- [ ] **docs: Operational cost / maintenance section** — upgrade cadence, dependency tracking, on-call implications
- [ ] **docs: Multi-tenancy / resource isolation design** — query queuing, per-user memory limits, fair scheduling
- [x] **sqe-catalog: Filter restricted columns from information_schema.columns** — DONE. `PolicyStore` + `SessionUser` threaded into `InformationSchemaProvider` and `SqeCatalogProvider`. Restricted columns filtered in `build_columns_table()`. Backward-compatible (`None` = show all).
