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

## Ranger policy type -> where it is enforced

| Ranger policy type | Enforcement | Status |
|---|---|---|
| resource access (catalog/ns/table allow-deny) | Polaris (embedded Ranger authorizer) | shipped |
| row-filter | SQE PlanRewriter (Filter above scan) | this phase |
| data-mask (column masking) | SQE PlanRewriter (column -> mask expr) | this phase |
| tag (tag-based masking) | SQE, tag -> masking-policy binding | this phase |

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
- `resolve(user, table, namespace) -> ResolvedPolicy`: fetch the table's Ranger
  row-filter, data-mask, and tag policies (Ranger Admin REST or the embedded
  policy bundle), evaluate them for the user's roles (Ranger role membership),
  and return row_filters + column_masks + restricted_columns.
- Wire it as a `PolicyEngine::Ranger` variant in config, feeding the existing
  `PolicyEnforcer` / `PlanRewriter` (which today runs passthrough).
- Cache + fail-closed like `OpaStore`.

## Snowflake context functions to mirror (reference)

`CURRENT_USER`, `CURRENT_ROLE`, `CURRENT_AVAILABLE_ROLES`,
`CURRENT_SECONDARY_ROLES`, `IS_ROLE_IN_SESSION(role)`, `INVOKER_ROLE`,
`CURRENT_ACCOUNT`, `CURRENT_DATABASE`, `CURRENT_SCHEMA`, `POLICY_CONTEXT` (test).
The role-hierarchy ones (`IS_ROLE_IN_SESSION`) are what require the richer
`SessionUser` role model.

## Phase shape (when we build it)

1. `sqe-auth` / `SessionUser`: richer role model (active + inherited/secondary).
2. Register session-context scalar UDFs resolved from the session.
3. Extend `MaskType` with partial + regexp masks (+ matching mask UDFs).
4. `RangerStore: PolicyStore` pulling Ranger row-filter / data-mask policies;
   `PolicyEngine::Ranger` config; wire into the rewriter.
5. Tag-based masking: Iceberg column-property tag source -> mask binding.
6. Quickstart additions: row-filter + masking demo on the existing
   `polaris-ranger-keycloak` stack (e.g. mask `orders.amount` for `analyst`,
   row-filter `orders` by region per role), proving end to end.

## Sources

- Snowflake row access policies: https://docs.snowflake.com/en/user-guide/security-row-intro
- Snowflake masking policies: https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy
- Snowflake tag-based masking: https://docs.snowflake.com/en/user-guide/tag-based-masking-policies
- Snowflake context functions: https://docs.snowflake.com/en/sql-reference/functions-context
- Snowflake IS_ROLE_IN_SESSION: https://docs.snowflake.com/en/sql-reference/functions/is_role_in_session
- Privacera PolicySync (Ranger -> Snowflake push model): https://docs.privacera.com/resources/design/access-management/integrations/privacera_policysync.html
- Apache Ranger policy model (row-filter / data-mask / tag types): https://ranger.apache.org/blogs/policy_model.html
