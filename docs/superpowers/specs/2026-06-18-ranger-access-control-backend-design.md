# Apache Ranger access-control backend for SQE

Date: 2026-06-18
Status: Approved (design), pending implementation plan
Branch: `feat/ranger-access-control-backend`

## Summary

Add Apache Ranger as a second access-control backend for SQE, alongside the
existing Polaris-native and Chameleon backends. SQE translates `GRANT` /
`REVOKE` / `SHOW GRANTS` into Apache Ranger Admin REST calls. Enforcement is
delegated to Apache Polaris 1.5, which runs its embedded Ranger authorizer and
consults Ranger when SQE (carrying the user's bearer token) requests catalog
metadata or table access.

BFF is explicitly out of scope for this work.

## Motivation

Organizations already running Apache Ranger for their broader data platform want
to extend the same authorization policies to their Iceberg catalog rather than
maintaining a separate access model. Polaris 1.5 added a pluggable Authorizer
SPI with an Apache Ranger authorizer (Beta). This gives SQE users a second,
enterprise-grade option for Polaris access control.

## Verified facts (the binding contract)

Confirmed from `apache/polaris` and `apache/ranger` source (2026-06-18):

- **Polaris Ranger authorizer** lives in `extensions/auth/ranger/impl/`
  (module `polaris-extensions-auth-ranger`), key class
  `RangerPolarisAuthorizer` with `SERVICE_TYPE = "polaris"`. It uses the Ranger
  *embedded* authorizer API (`RangerEmbeddedAuthorizer`) and **requires Apache
  Ranger 2.8.0 or later**.
- **Service-def** is canonical in `apache/ranger`:
  `agents-common/src/main/resources/service-defs/ranger-servicedef-polaris.json`,
  `"name": "polaris"`. Resource hierarchy:
  `root -> catalog -> namespace -> table | policy`, plus `principal` under
  `root`. **No `column` resource** (no column-level grants on this path).
- **Access types** are Polaris-native hyphenated names (NOT `select`/`read`,
  NOT `TABLE_READ_DATA`): `table-data-read`, `table-data-write`, `table-create`,
  `table-drop`, `table-list`, `namespace-create`, `catalog-create`, etc. (69
  total). Many carry `impliedGrants`.
- **Identity passed to Ranger** (from `RangerUtils.toUserInfo`): Ranger
  `user = principal.getName()` (Polaris principal name),
  `roles = principal.getRoles()` (Polaris principal-roles), and
  **`groups = null`**. Keycloak groups are not delivered to Ranger unless Ranger
  runs its own UserStore/usersync and the plugin group flags are enabled.
- **Ranger grant/revoke REST primitive**:
  `POST /service/plugins/services/grant/{serviceName}` and
  `.../revoke/{serviceName}`, taking a `GrantRevokeRequest`
  (`grantor`, `users`, `groups`, `roles`, `accessTypes`, `resource`,
  `delegateAdmin`, `replaceExistingPermissions`, `enableAudit`). Ranger performs
  find-or-create-policy + read-modify-write server-side, avoiding races.
- **Polaris config** to enable: `polaris.authorization.type=ranger`,
  `polaris.authorization.ranger.service-name=<name>`,
  `polaris.authorization.ranger.authz.default.policy.source.impl=org.apache.ranger.admin.client.RangerAdminRESTClient`,
  `polaris.authorization.ranger.authz.default.policy.rest.url=<ranger-admin-url>`.
- **Service-def registration** is a required one-time setup step (not scripted
  by Polaris): `POST /service/public/v2/api/servicedef` (the polaris service-def)
  then `POST /service/public/v2/api/service` (a service instance whose name
  matches `service-name`).

## Decisions

- **Scope: table-level only.** SQE writes grants to Ranger; Polaris enforces at
  catalog/namespace/table granularity via its embedded Ranger authorizer. SQE
  does not enforce. No row filters, no column masks (those would require a
  separate `RangerStore: PolicyStore` feeding SQE's LogicalPlan rewriter, which
  this change excludes). Mirrors the Lake Formation precedent.
- **Identity dimensions: User + Role.** `GRANT TO USER` keys on the Polaris
  principal name (= Keycloak `preferred_username`); `GRANT TO ROLE` keys on the
  Polaris principal-role (= Keycloak realm role). Both are natively delivered by
  Polaris to Ranger. `GRANT TO GROUP` is rejected with a clear error (would
  require usersync).
- **Write primitive: Ranger server-side grant/revoke endpoint** (race-free).
- **Ranger Admin URL: reuse `access_control.url`** (consistent with the Polaris
  and Chameleon backends).
- **`check_access`: best-effort.** Reads policies for the resource and matches
  user/role + access type. Documented limitation: does not replicate Ranger's
  full deny-precedence / tag / condition evaluation. The authoritative answer is
  Polaris enforcement.

## Architecture

Two independent halves:

1. **Enforcement (no SQE code).** SQE already passes the user's Keycloak bearer
   token to Polaris on every catalog operation. Polaris maps the token to a
   principal-name + principal-roles and asks its embedded Ranger authorizer. An
   ungranted operation fails at Polaris (403), which SQE surfaces as an error.
2. **Write path (new SQE code).** `GRANT` / `REVOKE` / `SHOW GRANTS` translate
   to Ranger Admin REST via a new `RangerGrantBackend`.

The work sits on the existing seam: the `GrantBackend` trait
(`crates/sqe-policy/src/grants/mod.rs`), the `AccessControlBackend` config enum
(`crates/sqe-core/src/config.rs`), and `build_grant_backend()` in
`crates/sqe-coordinator/src/bin/sqe_server.rs`. No new trait; no changes to the
enforcement path.

## Components

### `RangerGrantBackend` (`crates/sqe-policy/src/grants/ranger.rs`)

Implements `GrantBackend`:

- `grant` -> `POST {url}/service/plugins/services/grant/{service_name}` with a
  `GrantRevokeRequest`: `grantor` (admin user), `users` or `roles` (from
  grantee), `accessTypes` (mapped privilege), `resource` map
  (`{catalog, namespace, table}`), `delegateAdmin=false`, `enableAudit=true`.
- `revoke` -> `POST .../revoke/{service_name}`, same shape.
- `show_grants` / `show_effective` -> read policies via the policy-search API and
  parse `policyItems` back into `GrantEntry`s.
- `check_access` -> best-effort policy read + match (see Decisions).
- `backend_name` -> `"ranger"`.

**Privilege -> access-type map** (centralized, like `polaris.rs:56-72`):
`SELECT -> table-data-read`, `INSERT -> table-data-write`,
`CREATE TABLE -> table-create` (namespace resource),
`DROP -> table-drop`, `CREATE NAMESPACE -> namespace-create`, etc.

**Grantee mapping:** `User(name) -> users=[name]`, `Role(name) -> roles=[name]`,
`Group(_) -> error`.

**Auth to Ranger Admin:** HTTP basic auth (admin user/password). No service-token
cache needed.

### Config (`crates/sqe-core/src/config.rs`)

- New `AccessControlBackend::Ranger` variant.
- New `RangerConfig` nested under `AccessControlConfig` (mirrors `OpaConfig`
  under `PolicyConfig`):
  - `service_name` (must match Polaris `polaris.authorization.ranger.service-name`)
  - `admin_user`, `admin_password`
    (env `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD`)
  - `timeout_secs`, `accept_invalid_certs`
  - Ranger Admin base URL reuses `access_control.url`.
- `build_grant_backend()` gets a `Ranger` match arm.

## Test environment: `quickstart/polaris-ranger-keycloak/`

New quickstart, follows the `quickstart/polaris-keycloak-*` pattern.
`docker-compose.yml` services:

- **Keycloak** (reuse `_shared/keycloak`), realm extended with roles `analyst`,
  `engineer`, `admin` and matching users.
- **Postgres** — Ranger Admin backing DB.
- **Ranger Admin 2.8.0** (version the Polaris plugin requires). Audit disabled
  (no Solr) to keep the stack light.
- **Polaris 1.5** with `polaris.authorization.type=ranger`, the `service-name`,
  `policy.rest.url` -> Ranger Admin, and OIDC mapping
  (`preferred_username -> principal name`, `realm_access.roles ->
  principal-roles`) so Ranger user/role keys line up with what SQE grants on.
- **rustfs / minio** S3 (reuse `_shared`).
- **SQE** coordinator + worker with `access_control.backend=ranger`.
- **Bootstrap step** (script): register the polaris service-def and create the
  service instance via Ranger Admin REST before Polaris starts.

## End-to-end test (`quickstart/polaris-ranger-keycloak/test.sh`)

Covers all four complexity dimensions:

1. Bootstrap multiple catalogs + nested namespaces + tables with data.
2. Create roles `analyst` / `engineer` / `admin` (escalating grants) and users.
3. As admin, via SQE: `GRANT SELECT` / `INSERT` / `CREATE` / `DROP` to
   roles/users, including a Ranger deny policy overlapping an allow.
4. Connect as each user; assert allow and deny precedence (deny overrides allow).
5. Negative tests: ungranted user/role denied on `SELECT` / `INSERT` / `DROP`
   (proves enforcement is not a silent no-op).
6. `SHOW GRANTS` round-trips what was granted.

## Top correctness risk

The resource-value format SQE writes must exactly match what Polaris sends to
Ranger at enforcement time. The plugin uses a realm/context-prefixed resource
path (the apache/polaris test fixture `dev_polaris.json` uses a `POLARIS`
segment). If catalog/namespace/table values don't match, policies are written
but enforcement silently no-ops. The negative tests are the guard: they fail
loudly if the shape is wrong. The exact resource values will be verified against
the apache/polaris test fixture and the live authorizer during implementation.

A second open item to verify during implementation: whether
`PolarisPrincipal.getName()` equals the Keycloak `preferred_username` under the
chosen federation config, or a separately-registered Polaris principal name. The
Polaris OIDC principal-mapper config in the test env must make these line up.

## Out of scope (YAGNI)

- Column-level grants (no column resource in the Polaris service-def).
- Row filters / column masks (would need a separate `RangerStore: PolicyStore`).
- Ranger tag-based policies.
- `GRANT TO GROUP` / Ranger usersync.
- BFF.

## Files touched (anticipated)

- `crates/sqe-policy/src/grants/ranger.rs` (new)
- `crates/sqe-policy/src/grants/mod.rs` (register module)
- `crates/sqe-core/src/config.rs` (`AccessControlBackend::Ranger`, `RangerConfig`)
- `crates/sqe-coordinator/src/bin/sqe_server.rs` (`build_grant_backend` arm)
- `quickstart/polaris-ranger-keycloak/**` (new test env + test script)
- `README.md`, `nextsteps.md` (roadmap updates after completion)
