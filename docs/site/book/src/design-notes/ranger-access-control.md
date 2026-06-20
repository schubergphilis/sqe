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

The `ranger` backend assumes Polaris 1.5 running its embedded Ranger authorizer
(`polaris.authorization.type=ranger`). Two halves are at work:

- **Write path (SQE).** SQE translates each `GRANT` / `REVOKE` into a call to the
  Ranger Admin REST API. `SHOW GRANTS` reads Ranger policies back. SQE never
  enforces anything itself on this path.
- **Enforcement path (Polaris).** When SQE asks Polaris to load a table (carrying
  the user's Keycloak token), Polaris asks Ranger whether the principal may
  perform the operation. An ungranted operation fails at Polaris.

The backend code is `crates/sqe-policy/src/grants/ranger.rs`
(`RangerGrantBackend`). The doc-comment on that file states the design directly:
"Enforcement is delegated to Polaris 1.5's embedded Ranger authorizer; this
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
  service. They live on the separate `hive` service read by SQE's policy engine.

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
  deliver groups to Ranger unless Ranger usersync runs, so the backend does not
  support group grantees.

The request body is `GrantRevokeRequest`, serialized with Ranger's exact JSON
field names (`accessTypes`, `delegateAdmin`, `enableAudit`,
`replaceExistingPermissions`, `isRecursive`). Audit is on; delegate-admin,
replace-existing, and recursive are off.

### Future tables in a schema

`GRANT SELECT ON FUTURE TABLES IN SCHEMA sales_wh.sales TO ROLE analyst` grants
the privilege across every table in the namespace. SQE translates it to a Ranger
policy with a table wildcard (`table = "*"`). New tables created later in
`sales` are covered automatically, with no follow-up grant.

One difference from Snowflake: Snowflake's FUTURE grant applies only to objects
created after the grant. Ranger has no future-only resource, so SQE's wildcard
also covers tables that already exist in the schema. The grant means "every
table in this schema, present and future." Use a table-specific grant when you
need to scope to a single existing table.

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
policies, conditions, or wildcard resource matching beyond exact match, and it
matches on the user dimension only because roles are resolved by Ranger at
enforcement, not at this layer.

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
looks the principal up in the metastore. See `docs/polaris-principal-provisioning.md`
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

The reference deployment is `quickstart/polaris-ranger-keycloak/`: Polaris 1.5
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
by reading a separate `hive`-type Ranger service. SQE downloads those policies and
rewrites the `LogicalPlan` before DataFusion optimization: row filters inject as
`Filter` nodes above the `TableScan`, column masks replace column references with
masking expressions. The two paths are independent, and a query must pass both:
the Polaris gate (can the user load the table?) and SQE's rewriter (what rows and
columns may the user see?). Revoking the coarse `SELECT` grant still denies the
query before any fine-grained check runs.

The fine-grained path is configured under `[policy] engine = "ranger"` with
`[policy.ranger] service-name = "hive"`, a separate setting from
`access_control.backend = "ranger"`. For the fine-grained model see the
"Fine-grained enforcement" section of
`quickstart/polaris-ranger-keycloak/OVERVIEW.md`, the design notes in
`docs/fine-grained-policy.md`, and the service-type decision in
`docs/ranger-fine-grained-service-type.md`.

## Versions

- Apache Polaris 1.5.0 (embedded Ranger authorizer, Beta).
- Apache Ranger 2.8.0 (required by the Polaris plugin; new embedded authorizer
  API).
- Keycloak 26.5.
