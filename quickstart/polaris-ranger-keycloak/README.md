---
slug: polaris-ranger-keycloak
title: "Polaris + Apache Ranger + Keycloak"
description: "Run SQE against Apache Polaris with Apache Ranger as the access-control backend. SQE translates GRANT/REVOKE into Ranger Admin REST calls; Polaris enforces them via its embedded Ranger authorizer; column masks are enforced byte-identically to Apache Spark + Kyuubi."
---

# Polaris + Apache Ranger + Keycloak

A second access-control option for Apache Polaris. SQE translates `GRANT` /
`REVOKE` / `SHOW GRANTS` into Apache Ranger Admin REST calls (the `ranger`
access-control backend). Polaris 1.5's embedded Ranger authorizer enforces those
policies when SQE asks Polaris for table access on behalf of a user. On top of
that, SQE reads Ranger column masks directly and renders them byte-identically
to Apache Spark + Kyuubi.

This is the path for shops that already run Ranger as their authorization plane
and want SQE and Spark to share one policy set rather than maintaining separate
grants per engine. Pick it over the OPA/Cedar policy engine when Ranger is the
existing source of truth.

## What you get

A larger stack than the other quickstarts, because it proves cross-engine parity:

| Service | Role |
|---|---|
| `keycloak` | Identity provider, realm `iceberg-ranger` (users carol, bob, alice, dave). |
| `polaris` | Iceberg REST catalog with the embedded Ranger authorizer (`mixed` auth). |
| `ranger-admin` | Apache Ranger Admin: stores the policies SQE writes; Polaris and Spark both read it. |
| `ranger-setup` | One-shot: seeds Ranger services, roles, and user-to-role membership. |
| `rustfs` | S3-compatible warehouse storage. |
| `sqe` | The query engine: Keycloak ROPC, token passthrough, Ranger GRANT/REVOKE + mask enforcement. |
| `spark` (Spark 3.5 + Kyuubi Authz) | The cross-compare engine in the parity test (shares the Ranger `hive` service). |

## Prerequisites

- Docker (with Compose v2). The first run builds the SQE image and an Apache
  Spark image and downloads roughly 250 MB of Spark/Iceberg/Kyuubi jars.
- Ranger Admin first-boot takes 2-4 minutes; the compose `--wait` blocks on it.

## Run it

```bash
cd quickstart/polaris-ranger-keycloak
cp .env.example .env
docker compose up -d --build --wait   # Ranger Admin first-boot takes 2-4 min
./test.sh            # SQE GRANT/REVOKE + fine-grained mask enforcement
./parity-test.sh     # SQE <-> Spark (Kyuubi Authz) Ranger mask cross-compare
```

Or run both through `run.sh`:

```bash
./run.sh             # up -> wait -> ./test.sh
./run.sh --check     # same: test.sh IS the assertion harness
./run.sh --down      # tear everything down (-v)
```

There is no `OUTPUT.md` capture here. `test.sh` is the assertion harness: it
keeps its own PASS/FAIL count, prints a `RESULT: N passed, M failed` line, and
exits non-zero on any failure. `run.sh --check` runs that harness and propagates
its exit code.

## How it works

Access control splits into a write path and an enforcement path.

- **Write path (SQE).** SQE's `ranger` access-control backend turns each `GRANT`
  / `REVOKE` into a Ranger Admin REST call
  (`POST /service/plugins/services/grant/polaris`). `SHOW GRANTS` reads the
  policies back. SQE enforces nothing itself on this path.
- **Enforcement path (Polaris).** Polaris runs its embedded Ranger authorizer.
  When SQE asks Polaris to load a table (carrying the user's Keycloak token),
  Polaris asks Ranger whether that principal may perform the operation. An
  ungranted operation fails at Polaris with a 403, which SQE surfaces as an
  error.

```
SQE  -- GRANT/REVOKE -->  Ranger Admin        (policies stored here)
SQE  -- query+token  -->  Polaris  -- check -->  Ranger    (enforcement)
```

The identity model is the part that needs care. Polaris federates the principal
from the token's `preferred_username`, but federation resolves an *existing*
principal, so each user must be pre-created as a Polaris principal (the bootstrap
creates alice, bob, carol, dave). Polaris ignores the token's realm roles
(missing the `PRINCIPAL_ROLE:` prefix), so the user-to-role mapping that works is
**Ranger role membership**: Polaris sends the user to Ranger, and Ranger resolves
that user's roles from its own store. `OVERVIEW.md` documents why, with the
source-level findings.

Column masks are a separate, finer path: SQE reads them directly from a
`hive`-type Ranger service (the same one Spark + Kyuubi read), which is what
makes the cross-engine parity test possible.

## Configuration explained

### `sqe.toml`

The config wires three subsystems: auth, the coarse access-control backend, and
the fine-grained policy engine.

```toml
[auth]
keycloak_url = "http://keycloak:8080"
realm = "iceberg-ranger"
admin_roles = ["sqe_admin"]            # GRANT/REVOKE gated behind this role

[[auth.providers]]
type = "oidc_password"                 # ROPC: SQE mints the user's token
roles_claim = "realm_access.roles"
```

- `[auth]` + the `oidc_password` provider: SQE exchanges username + password for
  a Keycloak token (ROPC) and passes it to Polaris. `admin_roles = ["sqe_admin"]`
  gates `GRANT`/`REVOKE` behind that role (carol holds it).
- `[catalogs.sales_wh]` and `[catalogs.ops_wh]`: two Polaris warehouses, so
  3-part identifiers route to the right one.
- `[query] catalog_discovery = "polaris-auto"`: lets the write path route DML
  into a non-default catalog named in a 3-part identifier (e.g. `ops_wh.ops.x`),
  discovering it from Polaris. Without it, writes fall back to the default
  catalog.
- `[access_control] backend = "ranger"` + `[access_control.ranger]`: the coarse
  path. `service-name = "polaris"` must match Polaris's
  `polaris.authorization.ranger.service-name`. `realm = "*"` is load-bearing:
  Polaris includes a `root` resource in every authorization request, so every
  Ranger policy SQE writes must carry a matching `root` value, and `"*"` matches
  what Polaris sends. Without it, GRANTs succeed but enforcement silently never
  matches.
- `[policy] engine = "ranger"` + `[policy.ranger]`: the fine-grained path
  (row filters + column masks). It reads a `hive`-type Ranger service directly,
  the same service Spark + Kyuubi read, which is why the two engines can share
  one mask policy. This is separate from the coarse `[access_control]` path.

### `.env.example`

The realm, the offset host ports (Keycloak `38080`, Polaris `28181`, Ranger
`26080`, SQE Flight `60061`), and the Ranger admin password. Secrets like the
Ranger admin password can also be supplied via
`SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD` /
`SQE_POLICY__RANGER__ADMIN_PASSWORD`.

### `docker-compose.yml` and the bootstrap

Compose brings up Keycloak, Polaris (Ranger authorizer on), Ranger Admin, RustFS,
SQE, and the Spark container. The `ranger-setup` one-shot seeds the Ranger
`polaris` and `hive` services, the `analyst`/`engineer` roles, and the
user-to-role membership (in production that membership comes from Ranger usersync
over LDAP/AD/SCIM). The Polaris and Spark configs share the same Ranger service
names so all three engines see one policy set.

## Output

There is no `OUTPUT.md`. The evidence is the harness output. `test.sh` prints a
per-check PASS/FAIL log and ends with a `RESULT: N passed, M failed` line; a
clean bring-up is all green. `parity-test.sh` prints the masked `ssn` column from
both engines for `bob` and confirms they match:

| Engine | Identity to Ranger | ssn output |
|---|---|---|
| SQE | `bob` (Keycloak ROPC) | `xxx-xx-1111`, `xxx-xx-2222` |
| Spark 3.5 + Kyuubi Authz | `bob` (`HADOOP_USER_NAME`) | `xxx-xx-1111`, `xxx-xx-2222` |

The mask uses a CUSTOM portable-SQL transformer
(`concat('xxx-xx-', substr({col},8,4))`) so both engines render byte-identical
output. `OVERVIEW.md` explains why named Ranger mask types are not byte-portable
between SQE and Kyuubi.

## How it is tested

`test.sh` is the assertion harness (and `./run.sh --check` runs it). It runs the
full matrix from a clean bring-up, all green:

- two catalogs (`sales_wh`, `ops_wh`); 3-part names route to the right one via
  `catalog_discovery = "polaris-auto"`,
- a `GRANT SELECT` visibly enabling a read that was denied before it,
- role grants (`analyst`, `engineer`) and a user grant (`bob`),
- a Ranger DENY that overrides an allow (deny precedence),
- negative tests: an ungranted user (`dave`) and a read-only role are denied,
- a `SHOW GRANTS` round-trip, a `REVOKE` that takes effect, and `CHECK ACCESS`.

`parity-test.sh` then proves SQE and Spark agree on the same Ranger column mask
for `bob`, sharing one Polaris catalog and one Ranger `hive` service. The harness
exits non-zero on any failure, so a green run is the pass.

## Gotchas

- **Ranger Admin is slow to boot.** First boot takes 2-4 minutes; the compose
  `--wait` blocks on the healthcheck, so let it finish.
- **`realm = "*"` is required for enforcement.** Polaris sends a `root` resource
  in every authz request; without a matching value, GRANTs write fine but
  enforcement silently never matches. See the `[access_control.ranger]`
  annotation above.
- **Each user needs a Polaris principal.** Keycloak + Ranger alone is not enough.
  Polaris resolves (does not create) the principal, so a token for a missing
  principal 401s with "Failed to resolve principal". The bootstrap pre-creates
  the four users.
- **Roles come from Ranger, not the token.** Polaris drops the token's realm
  roles; the working user-to-role mapping is Ranger role membership.
- **Spark 4.0 is not used.** `kyuubi-spark-authz_2.13` is not on Maven Central,
  so the parity test pins Spark 3.5 (Scala 2.12) + the shaded Kyuubi authz jar.
  See `spark/Dockerfile`.
- See `OVERVIEW.md` for the architecture, the identity model, and the
  source-level findings behind these constraints.
