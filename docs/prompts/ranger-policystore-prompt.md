# Implementation prompt: Ranger fine-grained PolicyStore (adapt OPA -> Ranger)

Hand this to an implementer (or an agent). It is self-contained. It builds the
fine-grained enforcement backend (row filters + column masks) on Apache Ranger,
modeled on the existing OPA store. This is the `RangerStore` from
`docs/fine-grained-policy.md`. It is separate from the already-shipped
`RangerGrantBackend` (which handles GRANT/REVOKE, not enforcement).

---

You are adding an Apache Ranger fine-grained policy enforcement backend to SQE,
adapting the existing OPA implementation. SQE already enforces row filters and
column masks by rewriting the LogicalPlan; today the policy SOURCE can be OPA,
Cedar, in-memory, or passthrough. Add Ranger as a source.

Working dir: /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
Branch from `main`: `feat/ranger-policy-store`. Never push to main; open an MR.

## Context: how enforcement works today

- `crates/sqe-policy/src/lib.rs` defines:
  - `PolicyStore` trait: `async fn resolve(&self, user: &SessionUser, table_name: &str, namespace: &str) -> Result<ResolvedPolicy>` + `fn invalidate_all(&self)`.
  - `ResolvedPolicy { row_filters: Vec<Expr>, column_masks: HashMap<String, MaskType>, restricted_columns: Vec<String> }`.
  - `MaskType { Nullify, Redact(String), Hash, Custom(Expr) }`.
  - `PolicyEnforcer` (the plan rewriter is the consumer).
- `crates/sqe-policy/src/opa.rs` is the reference implementation: `OpaStore`
  implements `PolicyStore`, with a moka TTL cache, a three-state circuit breaker,
  fail-closed behavior (deny on error / unparseable filter / missing policy),
  row-filter string parsing, and mask-type parsing. READ IT FIRST and mirror its
  structure, caching, breaker, and fail-closed posture.
- `crates/sqe-policy/src/plan_rewriter.rs` consumes `ResolvedPolicy` (injects
  `Filter` above the scan, swaps columns for mask expressions, drops restricted
  columns). Do not change the rewriter; just feed it.
- Config: `crates/sqe-core/src/config.rs` has
  `PolicyEngine { Passthrough, InMemory, Opa, Cedar }` and `PolicyConfig { engine, mask_key, opa: OpaConfig }`.
- `SessionUser { username, roles: Vec<String> }` (`crates/sqe-core/src/session.rs`).
  IMPORTANT: SQE's own `SessionUser.roles` come from the user's token
  (`realm_access.roles`), so for SQE-SIDE enforcement you match the user + these
  roles directly against Ranger policy items. (This is different from the Polaris
  gate, where Polaris ignores token roles and needs Ranger role membership. On
  the SQE side you do NOT need Ranger role membership.)
- Design rationale + the broader function roadmap: `docs/fine-grained-policy.md`.

## What to build

   SERVICE TYPE (decided in `docs/ranger-fine-grained-service-type.md`): read a
   **`hive`-type Ranger service** (the one Apache Spark's Kyuubi plugin reads),
   NOT the `polaris` service (which has no row-filter/data-mask defs). This is
   what lets SQE and Spark share one policy set. Resource model is
   `database/table/column` (no catalog level): flatten the Iceberg
   catalog/namespace/table into the `database`/`table` strings using the SAME
   convention Kyuubi uses (two-part `db.table`; dotted namespace) or cross-engine
   policies will not match.

1. **`RangerStore` implementing `PolicyStore`** in a new file
   `crates/sqe-policy/src/ranger_store.rs`:
   - Constructor takes the Ranger Admin base URL, the `hive` service name,
     basic-auth admin user/password (`SecretString`), timeout, cache TTL/size,
     and breaker settings (mirror `OpaConfig`/`OpaStore::new`).
   - Fetch policies with the PLUGIN DOWNLOAD endpoint (one call gets everything):
     `GET {url}/service/plugins/policies/download/{serviceName}?lastKnownVersion={v}`
     returns `ServicePolicies` = `policies[]` + `serviceDef` (mask transformer
     templates + rowFilterDef) + `tagPolicies` + `policyVersion`. Returns 304 when
     `lastKnownVersion` matches -> use it for cheap incremental refresh. Do NOT
     use `/service/public/v2/api/policy` (resource-only, no serviceDef/tags).
   - `resolve(user, table, namespace)`: flatten to hive `database/table` (above),
     select the policies whose `resources` match, then read the fine-grained
     policy TYPES (NOTE the integers: `0 = access`, `1 = DATAMASK`,
     `2 = ROWFILTER`):
        - **data-mask** policies (`policyType == 1`): `dataMaskPolicyItems[]` with
          `{users, groups, roles, dataMaskInfo:{dataMaskType, valueExpr}}`. Map
          the Ranger mask type to `MaskType`:
          `MASK_NULL` -> `Nullify`; `MASK_NONE` -> no mask (exemption, evaluate
          first); `CUSTOM` (with `valueExpr`, `{col}` placeholder) ->
          `Custom(parsed Expr)`; `MASK_HASH` -> `Hash`;
          `MASK`/`MASK_SHOW_LAST_4`/`MASK_SHOW_FIRST_4`/`MASK_DATE_SHOW_YEAR` ->
          new partial/regex/date mask types (need DataFusion UDFs; see the mask
          table in `docs/ranger-fine-grained-service-type.md`). Map the masked
          column into `column_masks`.
        - **row-filter** policies (`policyType == 2`): `rowFilterPolicyItems[]`
          with `{users, groups, roles, rowFilterInfo:{filterExpr}}`. Parse
          `filterExpr` (a Hive/Spark-dialect SQL boolean string; translate to
          DataFusion) into an `Expr` and add to `row_filters`. Reuse OPA's
          filter-string parser (`opa.rs`; factor it out and share).
        - **tag** policies (from `tagPolicies` in the bundle): apply when the
          resource carries a matching tag. Tag-to-resource associations are NOT
          in the policy bundle; fetch them from the tag store / tag REST (or defer
          tag-based masking to a follow-up if no tag source exists yet).
     Match items on the user + `SessionUser.roles` DIRECTLY (token roles).
     Evaluation order: tag policies first, deny-overrides, then resource
     access -> mask -> row-filter. Multiple matching items combine the OPA way
     (union restrictions, AND row filters) -- mirror `OpaStore` merge semantics.
   - `invalidate_all`: clear the cache (called after GRANT/REVOKE).
   - Caching (moka), circuit breaker, and FAIL-CLOSED (deny / restrict on any
     fetch or parse error) exactly like `OpaStore`. Do not fail open.
   - User/role matching helper: an item matches if
     `item.users.contains(user.username)` OR
     `item.roles` intersects `user.roles`. (groups: skip unless a groups source
     is added later.)

2. **Config**: add `PolicyEngine::Ranger` and a `RangerPolicyConfig` nested under
   `PolicyConfig` (mirror `OpaConfig`: url, `service_name` = the **hive** service
   to share with Spark, admin_user, admin_password: SecretString, timeout, cache
   TTL/size, breaker thresholds). Update the `PolicyEngine` `FromStr` + the
   unknown-value error message. (This is the fine-grained POLICY engine, separate
   from `access_control.backend = "ranger"` which is the GRANT/REVOKE write path
   against the `polaris` service.)

3. **Wiring**: where the coordinator builds the `PolicyStore` / `PolicyEnforcer`
   from `PolicyEngine` (search `sqe_server.rs` / wherever `OpaStore` is
   constructed), add the `Ranger` arm constructing `RangerStore`.

4. **Reuse, do not duplicate**: factor the OPA SQL-filter-string parser and the
   mask-type plumbing into a shared module if it currently lives inside `opa.rs`,
   so both stores use it. Keep changes minimal and follow existing patterns.

## Out of scope (explicitly)

- Session-context SQL functions (`current_user()`, `is_role_in_session()`, ...)
  and the richer `SessionUser` role model: tracked separately in
  `docs/fine-grained-policy.md`. The first version matches on the flat
  `user.username` + `user.roles` only; row-filter exprs that reference context
  functions can be deferred.
- Tag-based masking (also in `docs/fine-grained-policy.md`).
- Any change to `plan_rewriter.rs` or the `RangerGrantBackend` (grants).

## TDD + verification

- Follow TDD. Unit-test the pure pieces without a live Ranger: policy JSON ->
  `ResolvedPolicy` parsing (row-filter expr parse, mask-type mapping, user/role
  matching, multi-item merge, fail-closed on malformed JSON). Mirror the test
  layout in `opa.rs` / `tests/opa_wiremock.rs` (wiremock for the HTTP path).
- Gates before opening the MR (project bar): `cargo build --all`,
  `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`.
- Optional e2e: extend `quickstart/polaris-ranger-keycloak` with a row-filter +
  masking demo (e.g. mask `sales_wh.sales.orders.amount` for role `analyst`,
  row-filter by `region`), set `[policy] engine = "ranger"`, and assert the
  masked/filtered output through SQE.

## Report

Status (DONE/BLOCKED), files changed, test + clippy output, the new
`PolicyEngine::Ranger` config example, and any deviations.
