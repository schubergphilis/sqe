# Overview: Polaris + Apache Ranger + Keycloak

## The two halves

Access control splits into a write path and an enforcement path.

**Write path (SQE).** SQE's `ranger` access-control backend turns each `GRANT` /
`REVOKE` into a call to the Ranger Admin REST API
(`POST /service/plugins/services/grant/polaris`). `SHOW GRANTS` reads Ranger
policies back. SQE never enforces anything itself on this path.

**Enforcement path (Polaris).** Polaris 1.5 runs its embedded Ranger authorizer
(`polaris.authorization.type=ranger`). When SQE asks Polaris to load a table
(carrying the user's Keycloak token), Polaris asks Ranger whether that principal
may perform the operation. An ungranted operation fails at Polaris with a 403,
which SQE surfaces as an error.

```
SQE  --GRANT/REVOKE-->  Ranger Admin        (policies stored here)
SQE  --query+token-->   Polaris  --check-->  Ranger    (enforcement)
```

## Identity model

This is the part that needs care, and the part the quickstart pins down through
live testing. The mapping has two halves: users and roles, handled differently.

**Users (principals).** Polaris federates the principal from the Keycloak token:
the principal name is `preferred_username`. But federation RESOLVES an existing
principal entity; it does not create one. So each user must be pre-created as a
Polaris principal (the data bootstrap creates `alice`, `bob`, `carol`, `dave`).
A token for a principal that does not exist is rejected with 401 "Failed to
resolve principal". Polaris sends this principal name to Ranger as the `user`.

This was tested directly with `polaris.authentication.type=external` (not just
`mixed`): the `DefaultAuthenticator` still logs "Failed to resolve principal" and
returns 401 for a Keycloak user with no Polaris principal, even though the JWT
verifies and Ranger holds the user's roles. So Keycloak + Ranger alone is NOT
enough; the Polaris principal entity is required regardless of auth.type.
External mode also disables the internal `root` token, creating a bootstrap
chicken-and-egg (nothing can authenticate to create the first principal), which
is why this stack uses `mixed` (internal token for provisioning + external OIDC
for users). Confirmed against source: `DefaultAuthenticator` (`@Identifier("default")`)
is the ONLY authenticator in Polaris 1.5.0 and current `main`; it always looks the
principal up in the metastore (`findPrincipalByName`/`findPrincipalById`) and 401s
if absent, and its javadoc states it "does not support federated principals that
are not managed by Polaris". `polaris.authentication.authenticator.type` only
accepts `default`. So eliminating per-user principal provisioning is NOT a config
option; it would require a custom `Authenticator` bean (a code change / custom
build). Provision a principal per user instead.

**Roles.** This is the surprising part. Polaris IGNORES the token's realm roles
(they lack Polaris's expected `PRINCIPAL_ROLE:` prefix, so they are dropped
during authentication). And Polaris principal-roles cannot help either: the
1.5.0 Ranger authorizer leaves all principal-role management operations
unmapped, so creating or assigning them is always denied. The mapping that
actually works is **Ranger role membership**: the user-to-role relationship
lives in Ranger's own role store. Polaris sends the `user` to Ranger, and Ranger
resolves that user's roles from membership. In production this membership comes
from Ranger usersync (LDAP/AD/SCIM); here `ranger-setup` sets it explicitly:

```
analyst  -> alice, bob, carol
engineer -> bob, carol
sqe_admin -> carol
```

**Groups** are not forwarded by Polaris at all (no usersync of groups here), so
this backend supports USER and ROLE grantees only; GROUP grants are rejected.

So the end-to-end mapping is:

1. Keycloak issues a token with `preferred_username` (and realm roles, which
   Polaris ignores).
2. Polaris resolves `preferred_username` to a pre-created principal entity and
   sends that username to Ranger.
3. Ranger resolves the user's roles from its role-membership store.
4. SQE writes Ranger policies keyed on usernames (`GRANT TO USER`) and role
   names (`GRANT TO ROLE`); Ranger matches them against the resolved user+roles.

## Grant granularity: baseline vs the LOAD gate

A single SQL operation through SQE touches several Polaris operations, each
needing a specific Ranger access type, and the embedded authorizer does NOT honor
service-def implied-grants. So SQE expands each SQL privilege to the full
explicit set (`map_sql_to_ranger_access`): `SELECT` -> the read set,
`INSERT` -> `table-data-write` plus every snapshot/schema/properties commit type.

The effective read gate is `LOAD_TABLE` (`table-properties-read`), not credential
vending. SQE reads parquet with its own configured S3 credentials, so once a user
can load a table's metadata it can read the data; Polaris's `table-data-read`
(vended-credential) check never fires for this deployment. The quickstart uses
that fact to make `GRANT` the visible gate:

- **Baseline (provisioning):** `ranger-setup` grants each role a traverse set
  (`catalog-list`, `catalog-properties-read`, `namespace-list`,
  `namespace-properties-read`, `table-list`). This is the "USAGE" level: a member
  can connect and list, but cannot load a table. It deliberately omits
  `table-properties-read`.
- **Data (SQE GRANT):** `GRANT SELECT` writes `table-properties-read` +
  `table-data-read`; `GRANT INSERT` writes the full write+commit set. Because the
  baseline omits `table-properties-read`, `GRANT SELECT` is what actually lets a
  member load and read a table, and `REVOKE` takes it away. A Ranger DENY on
  `table-properties-read` (added to the same policy) overrides the allow.

A denied table is invisible: SQE surfaces a load denial as "table not found"
rather than a permission error, matching the Polaris information-hiding model.

## Why a seed admin policy

With the Ranger authorizer enabled, Polaris delegates every decision to Ranger,
including the bootstrap's own catalog and namespace creation by the `root`
principal. Without a policy, that bootstrap is denied. `ranger/bootstrap-ranger.sh`
seeds a broad admin grant for the `root` user and the `sqe_admin` role before
Polaris starts.

## The resource-shape note

A Ranger policy SQE writes must match the resource Polaris sends at enforcement.
The Polaris service-def hierarchy is `root -> catalog -> namespace -> table`. The
`root` level carries a realm/context value. SQE controls it through
`[access_control.ranger] realm` in `sqe.toml`:

- `"*"` (this stack): every policy carries `root = *`, which matches the realm
  value Polaris sends. This is required: a `{catalog:*}` policy without `root`
  never matches Polaris's `CREATE_NAMESPACE`/table checks (verified), so a
  granted user would still be denied.
- A precise realm string can be used instead of `"*"` for tighter scoping if you
  confirm the exact value Polaris sends (Ranger Admin audit tab or
  `docker compose logs polaris`), then restart SQE.

Resolved value for this stack: `"*"` (root required, wildcard-matched).

## Fine-grained enforcement (SQE-side)

The Polaris gate tested above is coarse: it answers "may this user load this table?" SQE also
enforces row filters and column masks at the query-plan layer, reading a separate `hive`-type
Ranger service. These two paths are independent.

**How SQE reads the hive service.** On startup (and on a configurable refresh interval) SQE
calls `GET /service/plugins/policies/download/hive` to download the policy set. The
`[policy] engine = "ranger"` setting activates the `RangerStore: PolicyStore`, which caches
these policies and evaluates them against each query's catalog, namespace, and table.

**Plan rewriting.** SQE rewrites the `LogicalPlan` before DataFusion optimization. Row filters
inject as `Filter` nodes above the `TableScan`; column masks replace column references with
`CASE WHEN ... THEN NULL END` expressions. DataFusion's optimizer can push user predicates
through row-filter nodes but not through masked columns (masking a column blocks predicate
pushdown on that column's raw value, matching PostgreSQL RLS semantics).

**Resource mapping.** SQE passes the last dotted component of the namespace as the `database`
resource. For `sales_wh.sales.orders` the resource sent to Ranger is `database = "sales"`,
`table = "orders"`. Ranger policies must use `"sales"` as the database value, not the full
three-part path `"sales_wh.sales"`.

**Separation from the coarse path.** `GRANT`/`REVOKE` go to the `polaris` Ranger service via
`[access_control]`; Polaris enforces those at the catalog level. The `hive` Ranger service is
read by SQE's policy engine for row/column enforcement. A query must pass both gates: the Polaris
gate (can the user load the table?) and SQE's rewriter (what rows and columns may the user see?).
Revoking the coarse `SELECT` grant still denies the query before any fine-grained check runs.

**Shared with Apache Spark / Kyuubi.** The `hive` service is the same service those engines read,
so the same policy set is shared across tools. A mask or row filter written for SQE applies to
Spark queries through the same Ranger service and vice versa.

**Phase 2 (not yet implemented).** The current enforcement covers `MASK_NULL` (replace column
value with NULL) and row-filter expressions. Phase 2 will add mask UDFs (hash, partial, date
truncation), session-context SQL functions (`current_user()`, `current_role()`) inside filter
expressions, and tag-based masking via Ranger tag policies.

## Versions

- Apache Polaris 1.5.0 (embedded Ranger authorizer, Beta).
- Apache Ranger 2.8.0 (required by the Polaris plugin; it uses the new embedded
  authorizer API).
- Keycloak 26.5.
