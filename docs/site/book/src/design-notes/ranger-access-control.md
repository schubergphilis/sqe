# Catalog-based access control with Polaris + Apache Ranger

This is a reference for SQE's `ranger` access-control backend. On this path,
access control is catalog-based and enforced by Apache Polaris, not by SQE. SQE
only writes policies and surfaces denials. It does not filter rows or mask
columns here. That is the fine-grained path, covered separately (see the end of
this document).

## Overview

SQE supports several access-control backends for `GRANT` / `REVOKE` /
`SHOW GRANTS` dispatch. The selector is `access_control.backend` in `sqe.toml`.
The values are `none` (default), `chameleon`, `polaris`, and `ranger`, defined in
`crates/sqe-core/src/config.rs` (`AccessControlBackend`). This document covers the
`ranger` backend.

The `ranger` backend assumes Polaris running its embedded Ranger authorizer
(`polaris.authorization.type=ranger`). Two halves are at work:

- **Write path (SQE).** SQE translates each `GRANT` / `REVOKE` into a call to the
  Ranger Admin REST API. `SHOW GRANTS` reads Ranger policies back. SQE never
  enforces anything itself on this path.
- **Enforcement path (Polaris).** When SQE asks Polaris to load a table (carrying
  the user's Keycloak token), Polaris asks Ranger whether the principal may
  perform the operation. An ungranted operation fails at Polaris.

The backend code is `crates/sqe-policy/src/grants/ranger.rs`
(`RangerGrantBackend`). The doc-comment on that file states the design directly:
"Enforcement is delegated to Polaris's embedded Ranger authorizer; this
backend only writes/reads Ranger policies."

## Architecture and flow

```
SQE  --GRANT/REVOKE-->  Ranger Admin        (policies stored here)
SQE  --query+token-->   Polaris  --check-->  Ranger    (enforcement)
```

The write path is HTTP basic-auth to Ranger Admin:

- `GRANT`  -> `POST /service/plugins/services/grant/polaris`
- `REVOKE` -> `POST /service/plugins/services/revoke/polaris`
- `SHOW GRANTS` -> `GET /service/public/v2/api/policy?serviceName=polaris`

The last URL component (`polaris`) is the configured `service_name`. It is the
only URL-interpolated value, and it is operator-controlled config, not user
input.

The enforcement path runs entirely inside Polaris. SQE sends the query plus the
user's bearer token. Polaris resolves the principal, asks its embedded Ranger
authorizer for a decision, and either serves the table metadata or refuses.

A denied operation does not surface as a permission error. SQE surfaces a load
denial as "table not found", matching the Polaris information-hiding model. A
denied table is invisible, not forbidden. The quickstart test treats both
"not found" and an explicit 403 as a denial for exactly this reason.

## The `polaris` Ranger service-def

The Ranger service type used here is `polaris`, defined by
`quickstart/polaris-ranger-keycloak/ranger/servicedef-polaris.json`. It is a
coarse allow/deny service: it answers "may this user perform this operation on
this resource?" per catalog operation.

- **Resource hierarchy.** `root -> catalog -> namespace -> table` (the service-def
  also declares `principal` and `policy` resource levels). `RangerGrantBackend`
  writes the catalog, namespace, and table levels; see `build_resource_map`.
- **Access types.** 69 Polaris-native access types, named with hyphens
  (`table-data-read`, `namespace-create`, `catalog-content-manage`, and so on).
  These are the verbs Polaris checks at enforcement.
- **No fine-grained constructs.** The service-def declares no `rowFilterDef` and
  no `dataMaskDef`. Row filtering and column masking are not part of this
  service. They live on the separate frontend-query service read by SQE's policy
  engine, named `query` in the quickstarts.

## GRANT and REVOKE mapping

SQL privileges map to Ranger access types in `map_sql_to_ranger_access`
(`ranger.rs`). A single SQL privilege expands to the full explicit set of access
types the corresponding Polaris operations check. The mapping:

| SQL privilege | Ranger access types | Resource level |
|---|---|---|
| `SELECT` | `table-data-read`, `table-properties-read`, `table-list` | table |
| `INSERT` | `table-data-write` plus the full snapshot/schema/properties commit set (22 types) | table |
| `DROP` | `table-drop` | table |
| `CREATE TABLE` | `table-create` | namespace |
| `USAGE` | `namespace-list`, `namespace-properties-read` | namespace |
| `DROP SCHEMA` | `namespace-drop` | namespace |
| `CREATE SCHEMA` / `CREATE` | `namespace-create` | catalog |
| `ALL` / `ALL PRIVILEGES` | `catalog-content-manage` | catalog |
| anything else | the value, lowercased | table |

Unknown privileges pass through lowercased, so an operator can name native Ranger
access types directly in a `GRANT` statement.

### A table grant alone does not make a table readable

`GRANT SELECT ON cat.ns.tbl` needs catalog-level discovery already in place to be
readable through SQE, and the reason is on SQE's side rather than Polaris's.

Polaris will serve the table: a direct `LOAD_TABLE` carrying only the table-level
grant returns 200. But `SqeCatalogProvider::schema()` answers only for a namespace
present in its cached namespace list, and `list_visible_namespace_names` builds
that list from two calls that must both succeed. `list_namespaces` needs
`LIST_NAMESPACES`, authorized at the CATALOG level (Polaris does not use Ranger's
`SELF_OR_DESCENDANTS` matching, so a namespace-scoped `namespace-list` will not
satisfy it). The visibility filter then probes each namespace with
`get_namespace`, which is `LOAD_NAMESPACE_METADATA` and needs namespace-level
`namespace-properties-read`; a 403 hides the namespace so ungranted names do not
leak through `SHOW SCHEMAS`.

Either failure leaves an empty schema list, `schema()` returns `None`, and
planning ends at `table not found` with `LOAD_TABLE` never attempted. Nothing in
the SQE log shows a 403, because there is no denial to report.

A privilege therefore expands into a multi-level plan (`build_grant_plan`),
outermost first:

```sql
GRANT SELECT ON cat.ns.tbl TO ROLE r;   -- writes THREE policies
```

| Level | Access type |
|---|---|
| catalog | `namespace-list` |
| namespace | `namespace-properties-read` |
| table | the privilege's own set |

`GRANT USAGE ON DATABASE cat` remains the way to write catalog discovery on its
own: `USAGE` binds to the namespace level, and with no namespace named the
resource map degrades to `{root, catalog}`, which is what `LIST_NAMESPACES` is
authorized against.

Four properties of the expansion, each load-bearing:

**Both the shape and the sets come from `grant-profile.json`, now at v5.** Its
`SELECT` is `catalog:[namespace-list] | namespace:[namespace-properties-read] |
table:[table-data-read]`, and the data-platform control plane generates its
policies from the same file. SQE writing a different set for the same statement
would make "who granted this, and does it mean the same thing" unanswerable, and
there is a drift gate whose whole job is to keep the two in step.

The sets are not in the file, and that is on purpose. `privileges` ships **seeds**;
`access_types` carries the implication graph, and SQE walks it at write time to
produce what Polaris actually checks. Shipping finished sets would make the
profile's fixtures self-satisfying, this code asserting it read what it read,
where today they compare SQE's closure against one the platform computed
independently. The closure is exactly what drifted before v4, when SQE's
hand-written `WRITE_ACCESS` carried `table-properties-write`, which
`table-data-write` does not imply.

v5 folded that graph in from a second vendored file. `servicedef-polaris.json` is
still the Ranger service DEFINITION, registered with Ranger Admin by the
quickstarts, but it is no longer an input to planning.

**The catalog level is a real widening, accepted rather than hidden.** Its holder
can enumerate every namespace NAME in the catalog, unrelated ones included, and
that now happens on every table grant. Auto-adding it was initially refused here on
exactly that ground, and the refusal was overturned: diverging from the contract
both tools share is the worse failure, and the leak is names rather than data.
Separate catalogs are the boundary when namespace names are themselves sensitive.

**Outermost first.** Ranger has no transaction spanning several calls.
Outermost-first fails to "can list, nothing readable", which is inert;
innermost-first would fail to "has table access, table unreachable", the exact
symptom being removed. On a deepest-level failure the error says the outer grants
were left in place.

**Revoke releases the deepest level only.** The catalog and namespace policies are
shared with every other grant anyone holds in that catalog, so walking the plan
backwards would strip discovery out from under unrelated grants: an outage dressed
up as a narrow revoke. Traversal policies therefore accumulate and nothing cleans
them up. That is the correct trade, and it is the position the platform takes too.
Provenance labels are written at the deepest level only for the same reason:
stamping shared plumbing with one grantee's privilege would misrepresent it as
privately owned.

The quickstart's bootstrap seeds wildcard discovery for `analyst` and `engineer`,
which is why a single `GRANT SELECT` looks sufficient there. A principal outside
those roles still needs the catalog grant.

Verified on Polaris 1.7 with a clean database, one variable at a time: with
catalog discovery and a table `SELECT` grant but no namespace visibility, the read
failed `table not found`; adding ONLY `namespace-properties-read` at
`{catalog, namespace}` returned rows. Full transcript in
`docs/internal/research/2026-08-02-catalog-traversal-gate.md`.

### The named scope must match the privilege's level

The right-hand column is not advisory. It decides which keys go into the Ranger
resource map, and the keys below that level are dropped. Naming an object deeper
than the level a privilege binds to used to widen the grant silently:

```sql
GRANT ALL ON wh.sales.orders TO USER alice;
```

`ALL` binds to the catalog, so the namespace and table were dropped and the
write landed as `catalog-content-manage` on `wh`. The statement named one table,
reported success, and gave alice every table in the catalog. Nothing in the
response distinguished it from the narrow grant that was asked for, and the
operator reading `SHOW GRANTS` months later found a catalog policy nobody
remembered writing.

SQE now refuses the statement and names both the level and the scope that would
have been written:

```
Privilege 'ALL PRIVILEGES' binds to the catalog level, but the statement names
a namespace or table. The policy would apply to 'wh' and everything under it,
which is wider than the object named. Re-issue the statement against 'wh', or
name a privilege that binds to the object you meant.
```

The check is general rather than an `ALL` special case, because `USAGE` on a
table and `CREATE SCHEMA` on a namespace widen through the same path. It applies
to `GRANT`, `REVOKE` and `DENY` alike, so the three agree on what a statement's
scope means. A widened `DENY` over-restricts rather than over-grants, which is
the safer direction, but locking a grantee out of a whole catalog when one table
was named is no less surprising.

Grants written before this check exists are still catalog-wide. Revoking one
needs the statement re-issued at the level it actually landed on, which is what
the error text tells you.

### Why the full explicit set

The Polaris embedded authorizer does not honor service-def implied-grants. A
service-def can declare that `table-data-write` implies the commit verbs, but the
embedded authorizer ignores those declarations. So SQE expands each privilege to
every access type the operations will check. `SELECT` reads three types because a
read through SQE loads the table then reads files. `INSERT` lists `table-data-write`
plus every snapshot, schema, sort-order, partition-spec, and properties commit
type, because a write loads the table and commits a new snapshot, which fans out
into many fine-grained Polaris operations. The constants are `READ_ACCESS` and
`WRITE_ACCESS` in `ranger.rs`.

### Grantees: USER and ROLE only

`grantee_to_fields` splits the grantee into the Ranger request fields:

- `GRANT ... TO USER "alice"` writes to the `users` array.
- `GRANT ... TO ROLE "analyst"` writes to the `roles` array.
- `GRANT ... TO GROUP ...` is rejected with `NotImplemented`. Polaris does not
  deliver groups to Ranger unless Ranger usersync runs, so the backend will not
  write a grant whose grantee it cannot confirm exists.

The write and read paths are deliberately asymmetric here, and the asymmetry is
worth knowing before it surprises you. SQE will not WRITE a group grant, but it
does ENFORCE one: a group-bound policy authored in the Ranger console is matched
against the session's groups on the fine-grained read path (`ranger_store`,
pinned by `group_bound_items_match_the_session_groups`). Before that, such a
policy was skipped outright, which meant a mask an operator could see in the
Ranger UI quietly did nothing.

`CHECK ACCESS` does not resolve groups either, for the same reason the write path
refuses them, so a grant held only through a group is invisible to introspection
while still being enforced.

The request body is `GrantRevokeRequest`, serialized with Ranger's exact JSON
field names (`accessTypes`, `delegateAdmin`, `enableAudit`,
`replaceExistingPermissions`, `isRecursive`). Audit is on; delegate-admin,
replace-existing, and recursive are off.

### All tables and future tables in a schema

`GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.sales TO ROLE analyst` grants the
privilege across every table in the namespace. So does `ON FUTURE TABLES IN
SCHEMA`. SQE translates both to a Ranger policy with a table wildcard
(`table = "*"`). New tables created later in `sales` are covered automatically,
with no follow-up grant.

The two forms are equivalent in SQE, which is one difference from Snowflake:
Snowflake's FUTURE grant applies only to objects created after the grant, and its
ALL grant only to objects that already exist. Ranger has no future-only resource,
so the wildcard necessarily covers both. Either statement means "every table in
this schema, present and future." Use a table-specific grant when you need to
scope to a single existing table.

Do not confuse either form with `GRANT ... ON SCHEMA`, which stays a
namespace-level resource and does **not** reach the tables inside it. Namespace
`SELECT` is `namespace-list` plus `namespace-properties-read` and deliberately
carries no `table-data-read`; Ranger does not widen a namespace policy to the
tables beneath it. A namespace grant lets a role see that the schema and its
tables exist, not read their rows.

### Identifier validation

Catalog, namespace, table, and grantee names come from `GRANT` SQL and flow into
the JSON resource map. `validate_identifier` rejects empty values and any value
containing `/ ? # % \`, whitespace, or control characters. A `GRANT` that needs
no catalog is also rejected: the backend requires `catalog.namespace.table`
form.

### SHOW GRANTS and CHECK ACCESS read Ranger back

`SHOW GRANTS` calls `fetch_policies`, flattens each policy's allow and deny items
into rows (`policies_to_entries`), and filters by grantee or by resource prefix.
The resource-prefix match is dot-boundary aware: `SHOW GRANTS ON CATALOG "wh"`
matches `wh` and `wh.sales.orders` but never sibling catalogs like `wharf.ns.t`
or `wholesale` (`resource_matches_prefix`).

`CHECK ACCESS` is best-effort introspection only. `evaluate_access` applies
deny-overrides-allow against the fetched policies for a user and access type. Its
own doc-comment is explicit: "The authoritative decision is Polaris enforcement;
this is for `CHECK ACCESS` introspection only." It does not account for tag
policies, conditions, or wildcard resource matching beyond exact match and bare
`*`.

It DOES resolve the target user's Ranger roles, including nested ones. That is
worth stating because it did not, and the failure was quiet. `check_access`
passed an empty role list, under a comment claiming roles were unknown at this
layer, when Ranger serves them at `/service/public/v2/api/roles`. Since role
grants are the normal way to grant, the practical result was:

```
CHECK ACCESS SELECT ON sales_wh.acdemo.orders FOR USER "alice";
-- false | No matching grant for alice table-data-read on sales_wh.acdemo.orders
```

while `SHOW GRANTS` on the same table listed `table-data-read` for `ROLE
analyst`, alice was a member, and alice was reading the table. The answer looked
authoritative, so an auditor would conclude the table was closed while a user
read from it. It now reports:

```
-- true | Allowed via ROLE 'analyst'
```

Membership follows nested roles (a role listing another role confers it), walked
with a seen-set because Ranger does not prevent an operator creating a cycle.

**Groups are not resolved.** Ranger only knows a user's groups when usersync
runs, so a group-derived role would be a guess. A grant reachable only through a
group does not appear in `CHECK ACCESS`, which keeps the answer conservative in
the direction it already erred. A role-lookup failure says so in the `reason`
rather than degrading to a confident "no".

## Identity model

This is the part the quickstart pins down through live testing, documented in
`quickstart/polaris-ranger-keycloak/OVERVIEW.md`. The mapping has two halves,
users and roles, handled differently.

**Principals must pre-exist in Polaris.** Polaris federates the principal from
the Keycloak token: the principal name is `preferred_username`. But federation
resolves an existing principal entity; it does not create one. Each user must be
pre-created as a Polaris principal. The bootstrap creates `alice`, `bob`,
`carol`, `dave`. A token for a principal that does not exist is rejected with 401
"Failed to resolve principal". The token is a lookup key, not an identity source.
This holds in `external` mode too, confirmed against Polaris source:
`DefaultAuthenticator` is the only authenticator in Polaris 1.5, and it always
looks the principal up in the metastore. See [polaris-principal-provisioning.md](./polaris-principal-provisioning.md)
for the full investigation. Eliminating per-user provisioning is not a config
option; it would require a custom `Authenticator` bean.

**Roles come from Ranger role membership.** Polaris ignores the token's realm
roles. They lack Polaris's expected `PRINCIPAL_ROLE:` prefix, so they are dropped
during authentication. Polaris principal-roles cannot help either: the 1.5 Ranger
authorizer leaves principal-role management operations unmapped, so creating or
assigning them is always denied. The mapping that works is Ranger role
membership. Polaris sends the username to Ranger; Ranger resolves that user's
roles from its own role store. In production this comes from Ranger usersync
(LDAP/AD/SCIM). In the quickstart, `ranger-setup` sets it explicitly:

```
analyst   -> alice, bob, carol
engineer  -> bob, carol
sqe_admin -> carol
```

**Groups are not forwarded** by Polaris at all. The backend supports USER and
ROLE grantees only.

**The `root="*"` realm is required.** A policy SQE writes must match the resource
Polaris sends at enforcement. The Polaris service-def hierarchy is
`root -> catalog -> namespace -> table`, and the `root` level carries a
realm/context value. SQE controls it through `[access_control.ranger] realm` in
`sqe.toml`. For this stack the resolved value is `"*"`: every policy carries
`root = *`, which matches the realm value Polaris sends. This is required. A
`{catalog:*}` policy without `root` never matches Polaris's checks, so a granted
user would still be denied. A precise realm string can replace `"*"` for tighter
scoping if you confirm the exact value Polaris sends (Ranger Admin audit tab or
`docker compose logs polaris`) and restart SQE.

**The LOAD_TABLE read gate.** SQE reads parquet with its own configured S3
credentials. So once a user can load a table's metadata it can read the data, and
Polaris's `table-data-read` (vended-credential) check never fires for this
deployment. The effective read gate is `LOAD_TABLE` / `table-properties-read`,
not credential vending. The quickstart uses that fact to make `GRANT` the visible
gate: the baseline traverse set (`catalog-list`, `catalog-properties-read`,
`namespace-list`, `namespace-properties-read`, `table-list`) deliberately omits
`table-properties-read`, so `GRANT SELECT` is what actually lets a member load
and read a table, and `REVOKE` takes it away.

## Group bindings

Policy items bound to a GROUP are enforced, matched against the group
memberships on the session.

Enterprise Ranger deployments usually bind policies to directory groups rather
than naming users, with usersync mirroring the directory into Ranger. SQE
previously matched only the username and the token roles and skipped group-bound
items outright, so that whole class of policy silently did not apply. The session
already carried the memberships; the matcher ignored them.

Groups come from the provider's `groups_claim`, which is separate from
`roles_claim` and unset by default. When it is unset the session carries no
groups and a group-bound item still cannot match: SQE logs which knob fixes that
at debug level rather than failing silently. Users, roles and groups are OR-ed,
so a policy naming any of the three applies.

## Views

`GRANT SELECT ON VIEW cat.ns.v` works, and so do `ON ALL VIEWS IN SCHEMA` and
`ON FUTURE VIEWS IN SCHEMA`. Two facts shape how, both established against a
live Polaris 1.6 rather than inferred from the service-def.

**A view has no resource level of its own.** The `polaris` service-def declares
`root -> catalog -> namespace -> table` and no `view`. A view is addressed by
putting its NAME in the `table` slot; only the access-type set differs. Granting
`view-properties-read` + `view-list` on `{catalog, namespace, table: <view>}` is
what lets a grantee load the view, so that is what `SELECT ON VIEW` emits.

**A view is NOT a privilege boundary.** SQE expands the view's SQL and plans
against the base tables, so the reader needs its own grant on those tables too.
Granting only the view produces

```
Failed to plan view 'v' SQL: table 'cat.ns.orders' not found
```

which is the base-table denial surfacing through the view. This differs from
Snowflake secure views and Databricks views, where the definer's privileges
apply and the reader needs nothing on the base table. There is no definer's-
rights mode here: a view cannot be used to expose a subset of a table to someone
who may not read the table. Use a row filter or a column mask for that, which is
the mechanism SQE does support.

Column masks and row filters DO still apply through a view, because the rewrite
happens on the expanded scan. A view is therefore safe (it cannot launder a
masked column) but not sufficient (it cannot stand in for a grant).

## DENY

`DENY <privilege> ON <object> TO <grantee>` writes an explicit denial, which
Ranger evaluates ahead of any allow.

It does NOT go through the grant endpoint. `/services/grant` writes allow items
only and has no field for a denial, so DENY uses the policy API and merges a
`denyPolicyItems` entry into the policy covering the resource.

**It merges into an existing policy rather than creating its own, because Ranger
forbids the alternative.** Only one policy may exist per exact resource per
service; a second is rejected with

```
Validation failure: error code[3010], reason[Another policy already exists for
matching resource: policy-name=[...], service=[polaris]]
```

So a dedicated deny policy is impossible, and the deny lands on whichever policy
already covers that resource. Repeating the statement is idempotent: items are
deduplicated on grantee plus access-type set, not on JSON equality, because
Ranger echoes a stored item with every optional field populated (`users: []`,
`conditions: []`, `delegateAdmin`) and a byte comparison appended a duplicate
every time.

Two asymmetries with GRANT, both deliberate:

- **Not scoped to the caller's delegate authority.** The policy API authorizes
  the authenticated REST user and takes no `grantor`, so `[auth] admin_roles` is
  the only check. Ranger offers no grantor-scoped deny.
- **`REVOKE` clears a denial too.** There is no `UNDENY` keyword; `REVOKE` removes
  the grant whether it was an allow or a deny, which is Unity Catalog's
  behaviour. Without this DENY would be a one-way door, since the grant endpoint
  only touches allow items and undoing a denial would need console access.
  Matching is on grantee plus access-type set, so the revoke removes exactly what
  the equivalent DENY would have written.

`SHOW GRANTS` lists denials with `effect = DENY` alongside allows, so a denial is
visible to the same audit path as everything else. `DENY` is also recorded in the
audit log as a privilege change (`AuditKind::Grant`), not as an ordinary
statement.

## Who may grant: the caller, checked by Ranger

SQE sends the **authenticated caller** as the Ranger `grantor`, never its own
service identity, and Ranger decides whether that caller may grant.

This is an authority check, not an audit field. Verified against a live Ranger
2.8: a POST to `/service/plugins/services/grant/{service}` carrying
`grantor: "dave"` is refused with

```
HTTP 403 {"msgDesc":"User doesn't have necessary permission to grant access"}
```

even though the request authenticates with admin REST credentials. Ranger
authorizes the named grantor. So passing the real caller makes grant authority
**resource-scoped** (does this user hold delegate admin on THIS table?) rather
than merely role-scoped, and Ranger's audit record names the human instead of
`admin`.

`WITH GRANT OPTION` maps to Ranger's `delegateAdmin`, which is how that authority
is handed on. Without it nobody except the principals seeded at bootstrap could
ever grant.

The `[auth] admin_roles` gate on GRANT and REVOKE stays in place by default as
defence in depth. The two checks answer different questions: the role gate is
coarse and local ("may this session issue grant statements at all"), while
Ranger's is per-resource. The gate also still matters for the `polaris`
access-control backend, which swaps the caller's token for a service token
(issue #204) and so has no equivalent check of its own.

### Delegated grants: `grant_authority`

Both checks together mean a table owner holding `WITH GRANT OPTION` still cannot
use it without an engine-wide admin role. `[access_control] grant_authority`
decides which check applies:

```toml
[access_control]
backend = "ranger"
# admin-role      (default) require an [auth] admin_roles role, then Ranger
# ranger-delegate let Ranger's per-resource delegateAdmin be the only check
grant_authority = "ranger-delegate"
```

The default is `admin-role`, so an upgrade changes nobody's deployment. Read the
Ranger policies before switching: `ranger-delegate` widens who may issue grants to
everyone holding `delegateAdmin`, and a wildcard discovery policy (`catalog = *`)
written with `delegateAdmin: true` hands its roles the authority to grant those
access types anywhere in the service. The quickstart's `analyst` and `engineer`
roles are exactly that shape.

`ranger-delegate` is only honoured for a backend that authorizes the caller
(`GrantBackend::enforces_grantor_authority`). Asking for it against a backend that
acts with SQE's identity leaves the gate in place rather than removing the last
check.

`DENY` ignores the setting entirely and always requires an admin role. See the two
asymmetries above: the policy API authorizes the REST user and takes no grantor, so
there is nothing finer to hand over to.

### Delegate admin does not cascade upward

A `GRANT` on a table writes three policies, and Ranger authorizes each one
separately against the grantor. Measured on Ranger 2.8, with a grantor holding
`delegateAdmin` on `cat.ns.tbl` only:

| Request | Result |
|---|---|
| grant on `cat.ns.tbl` | 200 |
| grant on `cat.ns` | 403 |
| grant on `cat` | 403 |
| revoke on `cat.ns.tbl` | 200 |
| revoke on `cat` | 403 |
| grant `table-data-write` on `cat.ns.tbl` (outside their delegate set) | 403 |

The plan writes the catalog level FIRST, so a delegated grant would fail on its
very first call. SQE therefore **skips a traversal level the grantee already holds
at that exact resource**: Ranger merges access types, so re-POSTing a set already
present changes nothing, and skipping it removes the only call the delegated
grantor was not authorized to make. The level the statement NAMES is never skipped,
because it may still add access types or `delegateAdmin`.

What follows from that is the real shape of delegated grants: an admin onboards a
principal to a catalog and namespace once, and table owners manage their own tables
from then on. A grantee with no discovery yet cannot be served by a delegated
grant, and the error says so, naming the level that failed and the statements that
fix it.

The check is deliberately exact-resource. A wildcard policy can cover the same
target, but deciding that needs Ranger's own matcher, and a wrong "already covered"
would skip a level the grantee does not hold, leaving a grant that reports success
and confers nothing. Being too cautious costs one redundant POST. A policy disabled
in the console is treated as holding nothing: Ranger returns it with `isEnabled:
false` and its items intact while enforcement ignores it.

One cost worth knowing before a large deployment: Ranger's policy API has no
by-resource query, so each lookup fetches the whole policy list for the service. A
table `GRANT` now does that twice for the skip check plus once for the provenance
label, where it used to do it once, and the fetch is linear in total policy count.
Invisible at tens of policies, not at thousands.

Ownership, then, is `delegateAdmin`, and `WITH GRANT OPTION` is how it is handed
on. SQE does not yet grant it automatically to whoever creates a table; that is
tracked separately.

### Migration note

**This is a behaviour change for existing deployments.** A caller who holds an
SQE admin role but NOT `delegateAdmin` in Ranger could previously grant (every
grant was performed as the Ranger admin user) and will now be refused 403.

Grant delegate authority to whoever should be able to grant. The quickstart does
this at bootstrap: `post_grant` sends `"delegateAdmin": true` for the `root` user
and the `sqe_admin` role, which is why `carol` can still grant after this change.
For an existing stack, either add a Ranger policy giving the administrator
delegate admin on the resources they manage, or grant it through SQL with
`WITH GRANT OPTION`.

## Configuration

The `ranger` access-control backend is configured with two TOML blocks. The
Ranger Admin base URL is taken from `[access_control] url`, not from a field
inside `[access_control.ranger]`. This matches the Polaris backend convention.

```toml
[access_control]
backend = "ranger"
url = "http://ranger-admin:6080"

[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
# Polaris includes the `root` resource in every authorization request, so every
# Ranger policy SQE writes must carry a matching `root` value. "*" matches the
# realm Polaris sends (verified against this stack). Without it, GRANTs succeed
# but enforcement silently never matches.
realm = "*"
```

Field reference (`RangerConfig` in `crates/sqe-core/src/config.rs`):

| Key | Meaning | Default |
|---|---|---|
| `access_control.url` | Ranger Admin base URL | (none) |
| `service-name` | Ranger service instance; must match Polaris `polaris.authorization.ranger.service-name` | `polaris` |
| `admin-user` | Ranger Admin user for HTTP basic auth | `admin` |
| `admin-password` | Ranger Admin password (a secret) | (empty) |
| `realm` | the Polaris `root` resource value; empty omits the `root` level | (empty) |
| `timeout-secs` | HTTP timeout for one Ranger Admin call | `30` |
| `accept-invalid-certs` | accept self-signed TLS on Ranger Admin | `false` |

The admin password should be supplied by environment variable rather than
written into the file:

```
SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD=...
```

Two different "realm" concepts appear in the same `sqe.toml` and should not be
confused. The Keycloak realm (`iceberg-ranger`, in the `[auth]` `token_url`) is
the OIDC realm. The `[access_control.ranger] realm = "*"` is the Polaris `root`
resource value. They are unrelated.

## Quickstart

The reference deployment is `quickstart/polaris-ranger-keycloak/`: Polaris 1.7
with its embedded Ranger authorizer, Apache Ranger 2.8, and Keycloak 26.5. The
`OVERVIEW.md` there is the authoritative identity-model and enforcement
reference.

`test.sh` proves the catalog-level path end to end:

- A `GRANT SELECT` enables a query that was denied before the grant.
- A `REVOKE SELECT` disables it again.
- A Ranger `DENY` added to the same resource policy overrides the allow
  (deny-overrides-allow).
- USER grants (`GRANT SELECT ... TO USER "bob"`) and ROLE grants
  (`GRANT SELECT ... TO ROLE "analyst"`) both work, resolved through Ranger role
  membership.
- A user with no role (`dave`) is denied.
- `SHOW GRANTS ON sales_wh.sales.orders` round-trips and lists the `analyst` and
  `engineer` grants written earlier.

`GRANT` / `REVOKE` are themselves gated behind an admin allowlist
(`access_control.admin_roles = ["sqe_admin"]` in the quickstart `sqe.toml`), so
only `carol` (who holds `sqe_admin`) can run them.

## What this path does NOT do

This path is coarse. It answers one question: may this user load this table? It
does not do any of the following.

- **No row filtering.** It cannot restrict a query to a subset of rows.
- **No column masking.** It cannot redact or null a column's values.
- **No tag-based policy.** The `polaris` service-def declares no `rowFilterDef`
  and no `dataMaskDef`.

Those are the fine-grained path, enforced by SQE itself at the query-plan layer
by reading a separate Ranger service of servicedef type `hive`. SQE downloads those policies and
rewrites the `LogicalPlan` before DataFusion optimization: row filters inject as
`Filter` nodes above the `TableScan`, column masks replace column references with
masking expressions. The two paths are independent, and a query must pass both:
the Polaris gate (can the user load the table?) and SQE's rewriter (what rows and
columns may the user see?). Revoking the coarse `SELECT` grant still denies the
query before any fine-grained check runs.

The fine-grained path is configured under `[policy] engine = "ranger"` with
`[policy.ranger] service-name`, a separate setting from
`access_control.backend = "ranger"`. The default is `hive`; the quickstarts name the
instance `query`, because nothing in the picture is a Hive metastore and the old name
sent every reader looking for one. Only the instance name changed: the servicedef type
stays `hive`, since Spark's Kyuubi plugin is hardwired to the hive resource shape
(`database` / `table` / `column`). For the fine-grained model see the
"Fine-grained enforcement" section of
`quickstart/polaris-ranger-keycloak/OVERVIEW.md`, the design notes in
[fine-grained-policy.md](./fine-grained-policy.md), and the service-type decision in
[ranger-fine-grained-service-type.md](./ranger-fine-grained-service-type.md).

## Two engines, two tiers: how Spark reaches the same gates

Spark runs against the same Polaris catalog and the same Ranger instance, and it
is subject to the same object-level policies, with no engine code on SQE's side.
What makes that work is a credential choice, not an enforcement layer.

Polaris already runs its own Ranger plugin (`polaris.authorization.type: ranger`)
keyed on the federated OIDC identity. So Spark's Iceberg REST catalog is given a
per-user Keycloak token, and Polaris authorizes the end user:

```
spark.sql.catalog.<c>.token=<the user's Keycloak JWT>
spark.sql.catalog.<c>.token-refresh-enabled=false
```

The second line is load-bearing. Left at its default, Iceberg exchanges the
external JWT against Polaris's own token endpoint and the identity silently
reverts to the service account, at which point every access-control test passes
for the wrong reason. Connecting as a service principal, which is the common
Spark pattern, bypasses the object tier completely.

Identity then reaches the two tiers by different routes, and the asymmetry is the
most important property of the arrangement:

```
Keycloak token (signature verified)   ->  Polaris  ->  `polaris` service   [object]
HADOOP_USER_NAME (asserted string)    ->  Kyuubi   ->  `query` / `tag`     [fine grained]
```

### Why the frontend service carries a blanket allow

Kyuubi checks its own privilege BEFORE Polaris is consulted, and default-denies
without a matching `policyType-0` item:

```
AccessControlException: Permission denied: user [bob] does not have
  [select] privilege on [sales/orders/id]
```

SQE ignores `policyType-0` entirely, so a grant that works in SQE fails in Spark
and the failure looks like a Polaris bug. Object level belongs to Polaris, so the
`query` service carries one deliberate blanket allow that makes Kyuubi defer, and
holds nothing else beyond masks and row filters.

The item grants `select`, `update`, `create`, `drop`, `alter`, `index`, `lock`,
`read` and `write` to group `public` on `database=*`/`table=*`/`column=*`. Every
one of those access types has to be listed: Kyuubi checks `update` for `INSERT`
and `create` for DDL, and a missing one short-circuits exactly as above.

It is written with Ranger's grant API rather than as a self-documenting named
policy, because creating a hive-type service makes Ranger auto-generate
`all - database, table, column` over that exact resource signature, granted to
`admin` and `{OWNER}` only. That policy owns the signature, so a named policy is
refused:

```
Validation failure: error code[3010], reason[Another policy already exists for
matching resource: policy-name=[all - database, table, column]]
```

Every other wildcard shape is taken by a sibling auto policy. The grant API
merges an item into the existing match instead, so the defer item appears as the
group `public` item on `all - database, table, column`.

**Two consequences, both requirements rather than notes.**

Read out of context the item says "everyone may select everything". It grants no
data access, because Polaris still decides, and
`object_denial_survives_the_frontend_defer_policy` exists to prove exactly that:
with the item present and no Polaris grant, the read is still refused, by Polaris.
Do not delete it to tighten security; Spark stops working and nothing is gained.

**Any engine that reads the frontend service must also authorize through
Polaris.** An engine that trusts `query` alone would be wide open. That is a
standing constraint on adding engines, not a property of the current two.

### Testing it

`make test-access-control-spark` writes each grant through SQE's `GRANT`
statement and asserts it through Spark, so one grant path is checked against two
engines. Every denial assertion names the tier it expects: a Kyuubi denial where
Polaris was expected means the defer item went missing and the assertion never
reached the tier under test.

Two traps are worth knowing before reading a result. Kyuubi caches the policy
bundle on disk and refreshes on a 10s poll, so a `spark-sql` JVM started seconds
after a policy change can still enforce the previous bundle. And
`ranger-spark-security.xml` is a bind-mounted single file: editing it on the host
with `sed` replaces the inode, breaks the mount, and leaves the container with
`FileNotFoundException`, after which Kyuubi enforces nothing at all. Recover with
`docker compose up -d --force-recreate spark`.

## Versions

- Apache Polaris 1.7.0 (embedded Ranger authorizer, Beta).
- Apache Ranger 2.8.0 (required by the Polaris plugin; new embedded authorizer
  API).
- Keycloak 26.5.
