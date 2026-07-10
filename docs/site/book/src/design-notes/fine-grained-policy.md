# Fine-grained policy: row filters, column masking, tag-based masking (next-steps notes)

Future phase, not yet built. Design notes for row-level filtering, column
masking, and tag-based masking in SQE, driven by Apache Ranger, reaching rough
parity with Snowflake's row-access + masking + tag-masking. Pairs with the
coarse Ranger access-control backend already shipped (catalog/table allow-deny
via Polaris) and with `docs/s3vending.md`.

## Why this lives in SQE, not Polaris

Polaris + Ranger gives a COARSE allow/deny per catalog operation (LOAD_TABLE,
CREATE_NAMESPACE, ...). The Polaris service-def declares no `rowFilterDef` /
`dataMaskDef`, and the Polaris authorizer reads only the boolean decision, so
Polaris cannot enforce row filters or column masks even though the Ranger engine
can compute them. Fine-grained enforcement has to happen in the query engine.

Two enforcement models:

- **Push/sync (Snowflake, closed engine).** You cannot intercept Snowflake's
  planner, so Privacera PolicySync translates Ranger policies into Snowflake
  native objects on a schedule: `GRANT`/`REVOKE`, `CREATE ROW ACCESS POLICY`,
  `CREATE MASKING POLICY`, object `TAG` + tag-based masking. Snowflake enforces
  natively at query time; Ranger is the source of truth and the compiler.
- **Pull/rewrite (SQE, open engine).** SQE owns its `LogicalPlan` and already
  enforces by rewriting it: `PolicyEnforcer::evaluate()` runs between planning
  and optimization, injects row-filter `Filter` nodes above the scan, swaps
  columns for mask expressions, and DROPS denied columns entirely (PostgreSQL-RLS
  model, strictly more expressive than Snowflake's mask-to-NULL workaround).

SQE should keep pull/rewrite. The work is not the model (it exists) but the
policy VOCABULARY to express Snowflake-equivalent policies, plus a Ranger-backed
policy source.

## Which Ranger service-def (authoritative: `docs/ranger-fine-grained-service-type.md`)

Fine-grained policies do NOT go on the `polaris` service (it has no
`dataMaskDef`/`rowFilterDef` - coarse allow/deny only). Use the **`hive`
service-def** (the service Apache Spark's Kyuubi Ranger plugin reads) so SQE and
Spark share one policy set, plus a linked **`tag`** service for tag-based masking.
Key consequences that shape this phase:
- `hive` resources are `database -> table -> column` (NO catalog level). SQE must
  flatten Iceberg catalog + namespace into the `database` string using the SAME
  convention Kyuubi uses, or cross-engine policies silently won't match.
- policyType integers: `0 = access`, `1 = DATAMASK`, `2 = ROWFILTER`.
- mask transformers in the service-def are Hive UDFs (`mask`, `mask_show_last_n`,
  `mask_hash`); SQE reimplements them as DataFusion UDFs or rewrites them.
- pull everything in one call via the plugin DOWNLOAD endpoint (below).

## Ranger policy type -> where it is enforced

| Ranger policy type | Service | Enforcement | Status |
|---|---|---|---|
| resource access (catalog/ns/table allow-deny) | `polaris` | Polaris (embedded Ranger authorizer) | shipped |
| row-filter (policyType 2) | `hive` | SQE PlanRewriter (Filter above scan) | this phase |
| data-mask (policyType 1) | `hive` | SQE PlanRewriter (column -> mask expr) | this phase |
| tag (mask/row-filter) | `tag` linked to `hive` | SQE, via tag-resource associations | this phase |

A single Ranger policy store drives both: the coarse Polaris gate (already wired)
and SQE's fine-grained rewriter (new `RangerStore: PolicyStore`).

## What SQE has today

- `crates/sqe-policy/src/lib.rs`: `PolicyEnforcer`, `PolicyStore`,
  `ResolvedPolicy { row_filters: Vec<Expr>, column_masks, restricted_columns }`,
  `MaskType { Nullify, Redact(const), Hash, Custom(Expr) }`.
- `crates/sqe-policy/src/plan_rewriter.rs`: the pull/rewrite enforcement point.
- `crates/sqe-policy/src/sha256_udf.rs`: the only registered policy mask UDF
  (HMAC-SHA256 for the Hash mask).
- `crates/sqe-core/src/session.rs`: `SessionUser { username, roles: Vec<String> }`
  (flat role list, no hierarchy / secondary roles).
- `OpaStore` already implements `PolicyStore` (resolve row filters + masks from
  OPA) and is the template for a `RangerStore`.

## What to add (the function/primitive vocabulary)

1. **Session-context SQL functions** (the biggest gap). SQE advertises
   `CURRENT_USER` / `CURRENT_SCHEMA` in Flight SQL `getSqlInfo` metadata but does
   NOT register them as evaluable functions. Register, resolved per-session from
   `SessionUser`:
   - `current_user()`, `current_role()`, `current_database()`, `current_schema()`
   - `is_role_in_session(role)`, `current_available_roles()` /
     `current_secondary_roles()` equivalents
   - **Prerequisite:** a richer role model. `SessionUser.roles` is a flat
     `Vec<String>` with no hierarchy / secondary-role notion; `is_role_in_session`
     needs `sqe-auth` to surface the full active + inherited role set into the SQL
     eval context. This is the real work behind the context functions.

2. **Mask types** (extend `MaskType` so policies are declarative, not all
   `Custom`). Have: nullify, constant/redact, hash, custom-SQL. Add as
   first-class: **partial/substring** (e.g. show last 4) and **regexp_replace**.
   Each maps to a Snowflake masking CASE body (`THEN val ELSE <transform>`).

3. **Row-filter mapping-table idiom.** The Filter-injection mechanism exists; add
   a first-class lookup-table pattern (an `EXISTS` / semi-join against a mapping
   table keyed on the session role), the idiom Snowflake row-access policies lean
   on. Row-filter expressions reference the session-context functions from (1).

4. **Tag-based masking** (this is where tagging belongs). A tag store -> masking-
   policy binding -> auto-apply path: assign a mask to a tag once, tag a column,
   masking applies automatically and new matching columns inherit it. SQE's
   natural tag source is Iceberg / Polaris column properties plus Ranger tag
   policies. Note: Ranger tag policies need a tag source; for the Polaris gate
   there is none today (no Atlas hook / tagsync for Polaris resources), but SQE
   can read Iceberg column properties directly as the tag source for its own
   masking, independent of the Polaris path.

## Component to build

`RangerStore: PolicyStore` (new, modeled on `OpaStore`):
- Reads a **`hive`-type Ranger service** (the one Spark uses), NOT the `polaris`
  service. Pull the whole bundle in one call:
  `GET /service/plugins/policies/download/{serviceName}` returns `ServicePolicies`
  = resource `policies[]` (access=0, datamask=1, rowfilter=2) + `serviceDef`
  (mask transformer templates, rowFilterDef) + `tagPolicies` + `policyVersion`
  (304 on unchanged -> cheap polling). The public-v2 `/api/policy` array is
  resource-only and insufficient.
- `resolve(user, table, namespace) -> ResolvedPolicy`: flatten the Iceberg
  catalog/namespace/table to the hive `database/table/column` naming (match
  Kyuubi's convention), select matching row-filter + data-mask (+ tag) items for
  the user, and return row_filters + column_masks + restricted_columns. Match on
  the user + `SessionUser.roles` DIRECTLY (SQE's session roles come from the
  token, unlike the Polaris gate which needs Ranger role membership). Evaluation
  order: tag policies first, deny-overrides, then resource access -> mask ->
  row-filter. Translate `filterExpr`/`valueExpr` from Hive/Spark dialect to
  DataFusion; realize Hive mask transformers as DataFusion UDFs (see the mask
  table in `docs/ranger-fine-grained-service-type.md`).
- Wire it as a `PolicyEngine::Ranger` variant in config, feeding the existing
  `PolicyEnforcer` / `PlanRewriter` (which today runs passthrough).
- Cache + fail-closed like `OpaStore` (use `policyVersion` for incremental refresh).

See `docs/ranger-fine-grained-service-type.md` for the full service-type
rationale, the mask-type mapping, the flattening sharp edge, and cross-engine
sharing requirements.

## Snowflake context functions to mirror (reference)

`CURRENT_USER`, `CURRENT_ROLE`, `CURRENT_AVAILABLE_ROLES`,
`CURRENT_SECONDARY_ROLES`, `IS_ROLE_IN_SESSION(role)`, `INVOKER_ROLE`,
`CURRENT_ACCOUNT`, `CURRENT_DATABASE`, `CURRENT_SCHEMA`, `POLICY_CONTEXT` (test).
The role-hierarchy ones (`IS_ROLE_IN_SESSION`) are what require the richer
`SessionUser` role model.

## Phase shape

**Phase 2A (shipped, branch `feat/ranger-mask-vocabulary`).** The full Ranger hive built-in mask
vocabulary is implemented and wired. Steps 3, 4, and 6 from the original list are done:

- `MaskType` extended with `PartialMask { show_first, show_last, upper, lower, digit }` and
  `DateShowYear`. `RangerStore` maps every standard `dataMaskType` string to the corresponding
  `MaskType` variant.
- `mask_partial` DataFusion UDF realises the Hive char-level transformer (uppercase, lowercase,
  digit substitution chars; punctuation and non-ASCII pass through unchanged; Unicode scalar
  counting). The full hive set is now: MASK_NULL, MASK, MASK_SHOW_LAST_4, MASK_SHOW_FIRST_4,
  MASK_HASH, MASK_DATE_SHOW_YEAR, CUSTOM.
- Quickstart `polaris-ranger-keycloak` updated: orders table gains an `ssn VARCHAR` column;
  a `MASK_SHOW_LAST_4` policy seeds on that column for role `engineer`; test.sh section 5
  proves `111-11-1111` becomes `xxx-xx-1111` for bob (engineer) and stays raw for alice
  (analyst-only).

**Phase 3a (shipped, branch `feat/tag-based-masking`).** Tag-based masking enforcement:

- `TagSource` trait + `NoopTagSource` default + `CacheTagSource` production implementation. Tags stored as Iceberg `sqe.column-tags` table property.
- `PolicyStore::resolve_tags` default no-op; `RangerStore` overrides to fetch `tagPolicies` from the Ranger bundle and return `(tag_masks_by_tag, tag_filters, unmappable_tags)`.
- `PolicyPlanRewriter` wired: calls `tag_source.column_tags(catalog, full-namespace-vec, table)` using the FULL namespace path (not last component), then `resolve_tags`, then `merge_tag_masks`. Precedence rules: resource mask wins, restricted stays restricted, unmappable tag fails closed.
- Four executable integration tests prove: tag mask end-to-end, full-namespace identity (FakeTagSource capture), resource-mask-wins precedence, unmappable-tag fail-closed.
- NOT live-demoed (quickstart stack drift); the executable tests are the proof.
- Phase 3b (shipped): CUSTOM tag masks + cache invalidation on `SET TBLPROPERTIES`.

**Phase 3b (shipped, branch `feat/tag-ddl`).** CUSTOM tag mask support + cache invalidation:

- `TagMaskSpec` enum (`Ready(MaskType)` or `Custom(String)`) replaces the raw `MaskType` in `PolicyStore::resolve_tags` return type. CUSTOM masks carry the raw `{col}` template; the rewriter substitutes the column name at merge time.
- `resolve_tag_policies` in `ranger_store.rs`: CUSTOM tags with a `value_expr` now produce `TagMaskSpec::Custom(template)` instead of being marked unmappable. Tags with no `value_expr` remain unmappable (fail-closed, nothing to substitute).
- `merge_tag_masks` in `plan_rewriter.rs`: `TagMaskSpec::Custom(template)` branches substitute `{col}` with the real column name, call `parse_sql_predicate`, produce `MaskType::Custom(expr)` on success, restrict column on parse failure (fail-closed).
- `set_table_properties` in `catalog_ops.rs`: explicit `session_catalog.invalidate_table` call after `commit_schema_update` so a `SET TBLPROPERTIES('sqe.column-tags'=...)` is visible on the next query without waiting for TTL expiry. `invalidate_policy_cache()` also called in `QueryHandler` for defense-in-depth.
- Two new tests cover: CUSTOM with valid expression applies `MaskType::Custom` (not restriction); CUSTOM with unparseable expression restricts the column.

**Phase 2B (not yet started).** Session-context SQL functions:

1. `sqe-auth` / `SessionUser`: richer role model (active + inherited/secondary).
2. Register `current_user()`, `current_role()`, `current_available_roles()` as evaluable scalar
   UDFs resolved per-session from `SessionUser`.

**Phase 2C (not yet started).** Cross-engine dynamic transformer + tag-based masking:

3. Dynamic transformer configuration for arbitrary-N show-first/show-last masks (currently
   hard-coded to 4).
4. Tag-based masking: Iceberg column-property tag source -> mask binding, plus Ranger tag
   policies. The tag source for the Polaris gate has no Atlas hook today, but SQE can read
   Iceberg column properties directly.

## Sources

- Snowflake row access policies: https://docs.snowflake.com/en/user-guide/security-row-intro
- Snowflake masking policies: https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy
- Snowflake tag-based masking: https://docs.snowflake.com/en/user-guide/tag-based-masking-policies
- Snowflake context functions: https://docs.snowflake.com/en/sql-reference/functions-context
- Snowflake IS_ROLE_IN_SESSION: https://docs.snowflake.com/en/sql-reference/functions/is_role_in_session
- Privacera PolicySync (Ranger -> Snowflake push model): https://docs.privacera.com/resources/design/access-management/integrations/privacera_policysync.html
- Apache Ranger policy model (row-filter / data-mask / tag types): https://ranger.apache.org/blogs/policy_model.html
