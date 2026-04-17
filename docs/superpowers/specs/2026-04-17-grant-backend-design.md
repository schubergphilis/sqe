# GrantBackend: Pluggable Access Control for GRANT/REVOKE SQL

**Date:** 2026-04-17
**Status:** Design approved
**Author:** Jacob Verhoeks

## Problem

SQE has a single `AccessControlClient` that speaks the Chameleon platform API for all GRANT/REVOKE/SHOW GRANTS operations. The `backend = "polaris"` config value is accepted but ignored. The same Chameleon-shaped HTTP calls (`POST /grant`, `POST /revoke`, `GROUP`/`USER` grantee types) go out regardless of backend.

Apache Polaris has a fundamentally different grant model. Polaris uses a three-level role hierarchy (PRINCIPAL -> PRINCIPAL_ROLE -> CATALOG_ROLE -> privilege on resource) and a Management REST API with different endpoints, request shapes, and a richer privilege set. The current architecture cannot express this.

Future backends (Unity Catalog, Gravitino) will have their own grant APIs. The system needs a pluggable abstraction.

## Architecture

Two independent enforcement layers run in sequence:

```
Layer 1: GrantBackend (Polaris / Chameleon / future: Unity)
  Determines: who can access what resource, which privileges
  Manages: GRANT, REVOKE, SHOW GRANTS, SHOW EFFECTIVE GRANTS, CHECK ACCESS

Layer 2: PolicyEnforcer (OPA / InMemory / Passthrough)
  Determines: row filters, column masks, column restrictions
  Applied on top of whatever Layer 1 allows
```

Both layers run. Polaris says "analysts can SELECT on orders". OPA says "but only rows where `region = 'EU'` and mask the `ssn` column". The `GrantBackend` handles privilege administration. The `PolicyEnforcer` handles fine-grained data-level enforcement.

## GrantBackend Trait

```rust
// sqe-policy/src/grants/mod.rs

#[async_trait]
pub trait GrantBackend: Send + Sync {
    async fn grant(&self, token: &str, stmt: &GrantStatement) -> sqe_core::Result<()>;
    async fn revoke(&self, token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()>;
    async fn show_grants(&self, token: &str, filter: &GrantFilter) -> sqe_core::Result<Vec<GrantEntry>>;
    async fn show_effective(&self, token: &str, user: &str) -> sqe_core::Result<Vec<GrantEntry>>;
    async fn check_access(&self, token: &str, check: &AccessCheck) -> sqe_core::Result<AccessCheckResult>;
    fn backend_name(&self) -> &str;
}
```

The `token` parameter is the caller's bearer token. In passthrough mode (default), this is the user's OIDC token. In service credential mode (optional), the coordinator resolves a service token before calling the backend.

### Shared Types

```rust
pub struct GrantStatement {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub grantee: Grantee,
}

pub enum Grantee {
    User(String),
    Role(String),
    Group(String),
}

pub struct RevokeStatement {
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
    pub grantee: Grantee,
}

pub enum GrantFilter {
    OnResource {
        catalog: Option<String>,
        namespace: Option<String>,
        table: Option<String>,
    },
    ToGrantee(Grantee),
}

pub struct AccessCheck {
    pub user: String,
    pub privilege: String,
    pub catalog: Option<String>,
    pub namespace: Option<String>,
    pub table: Option<String>,
}

// Defined in sqe-policy/src/grants/mod.rs (mirrors sqe-catalog's GrantEntry shape):
pub struct GrantEntry {
    pub privilege: String,
    pub resource: String,
    pub grantee_type: String,
    pub grantee_name: String,
    pub effect: String,
    pub granted_by: Option<String>,
    pub granted_at: Option<String>,
}

pub struct AccessCheckResult {
    pub allowed: bool,
    pub reason: Option<String>,
}
```

No backend-specific types leak into the trait. Each backend maps `Grantee::Role("analysts")` to its own model internally.

## ChameleonGrantBackend

Thin adapter wrapping the existing `AccessControlClient` from `sqe-catalog`. Zero behavior change for current deployments.

```rust
// sqe-policy/src/grants/chameleon.rs

pub struct ChameleonGrantBackend {
    client: Arc<AccessControlClient>,
}
```

**Grantee mapping:**
- `Grantee::User("alice")` -> `grantee_type: "USER"`
- `Grantee::Role("admins")` -> `grantee_type: "GROUP"`
- `Grantee::Group("SG-Risk")` -> `grantee_type: "GROUP"`

Each trait method delegates 1:1 to the existing `AccessControlClient` methods. The adapter translates between the trait's types (`GrantStatement`, `Grantee`) and the client's types (`GrantRequest`). The existing `AccessControlClient` stays unchanged.

## PolarisGrantBackend

New implementation that calls the Polaris Management REST API.

```rust
// sqe-policy/src/grants/polaris.rs

pub struct PolarisGrantBackend {
    client: reqwest::Client,
    management_url: String,
    service_token_source: Option<ServiceTokenSource>,
}

struct ServiceTokenSource {
    token_url: String,
    client_id: String,
    client_secret: String,
    cache: moka::future::Cache<String, String>,  // TTL = expires_in - 30s
}
```

### Three-Step Grant Chain

`GRANT SELECT ON warehouse.ns.orders TO ROLE "analysts"` executes:

**Step 1: Ensure catalog role exists.**
```
POST /catalogs/{warehouse}/catalog-roles
body: {"catalogRole": {"name": "sqe_analysts"}}
```
HTTP 409 (already exists) is expected and ignored.

**Step 2: Grant privilege to catalog role.**
```
PUT /catalogs/{warehouse}/catalog-roles/sqe_analysts/grants
body: {"grant": {"type": "table", "privilege": "TABLE_READ_DATA",
       "namespace": ["ns"], "tableName": "orders"}}
```

**Step 3: Assign catalog role to principal role.**
```
PUT /principal-roles/analysts/catalog-roles/{warehouse}
body: {"catalogRole": {"name": "sqe_analysts"}}
```

### Catalog Role Naming Convention

`sqe_{principal_role_name}` -- prefixed with `sqe_` to avoid collisions with manually created catalog roles. One catalog role per principal role per catalog.

### Privilege Mapping

SQL privileges map to Polaris privilege strings:

| SQL | Polaris | Resource type |
|---|---|---|
| `SELECT` | `TABLE_READ_DATA` | table |
| `INSERT` | `TABLE_WRITE_DATA` | table |
| `CREATE TABLE` | `TABLE_CREATE` | namespace |
| `DROP` | `TABLE_DROP` | table |
| `ALL` | `CATALOG_MANAGE_CONTENT` | catalog |
| `USAGE` (on namespace) | `NAMESPACE_LIST` | namespace |
| `CREATE SCHEMA` | `NAMESPACE_CREATE` | catalog |
| `DROP SCHEMA` | `NAMESPACE_DROP` | namespace |

**Pass-through rule:** If the privilege string is already a Polaris native name (contains underscore, all uppercase), send it verbatim. Power users can write `GRANT TABLE_WRITE_PROPERTIES ON catalog.ns.table TO ROLE "ops"` without SQE needing to know every future Polaris privilege.

### Revoke

Removes the specific privilege grant from the catalog role:
```
DELETE /catalogs/{c}/catalog-roles/{r}/grants
```
Does NOT delete the catalog role or unassign it from the principal role. Other privileges may still be attached.

### Show Grants

`SHOW GRANTS ON catalog.ns.table`:
1. `GET /catalogs/{c}/catalog-roles` to list all catalog roles
2. For each role, `GET /catalogs/{c}/catalog-roles/{r}/grants` to get privileges
3. Filter by requested resource (namespace/table match)
4. Map to `Vec<GrantEntry>`

`SHOW GRANTS TO ROLE "analysts"`:
1. Find the `sqe_analysts` catalog role
2. `GET /catalogs/{c}/catalog-roles/sqe_analysts/grants`
3. Map to `Vec<GrantEntry>`

### Show Effective Grants

`SHOW EFFECTIVE GRANTS FOR USER "alice"`:
1. Query Polaris for the user's principal-roles: `GET /principals/alice/principal-roles`
2. For each principal-role, get catalog-role assignments: `GET /principal-roles/{pr}/catalog-roles/{c}`
3. For each catalog-role, get grants: `GET /catalogs/{c}/catalog-roles/{r}/grants`
4. Flatten into `Vec<GrantEntry>`

### Check Access

Walk the same chain as show_effective but short-circuit on first matching grant. Return `AccessCheckResult { allowed, reason }`.

### Token Handling

- **Passthrough mode (default):** User's OIDC bearer token goes to the Management API. Polaris enforces whether the user has admin rights. If not, Polaris returns 403 and SQE surfaces the error. No extra config needed.
- **Service credential mode (optional):** When `client_id` and `client_secret` are configured, SQE fetches a management token via `POST /api/catalog/v1/oauth/tokens` with `grant_type=client_credentials`. The token is cached in a moka cache with TTL = `expires_in - 30s`, max capacity 1.

### Error Handling

| Polaris response | SQE behavior |
|---|---|
| HTTP 403 | `SqeError::Auth("Insufficient privileges to manage grants")` |
| HTTP 404 (role not found) | `SqeError::Execution("Catalog role not found: {name}")` |
| HTTP 409 (role already exists) | Ignored (idempotent creation) |
| Network timeout | `SqeError::Execution("Polaris management API request failed: {e}")` |

## Coordinator Integration

### Startup (main.rs)

```rust
let grant_backend: Option<Arc<dyn GrantBackend>> = match config.access_control.backend.as_str() {
    "chameleon" => Some(Arc::new(ChameleonGrantBackend::new(
        AccessControlClient::new(&config.access_control.url)?
    ))),
    "polaris" => Some(Arc::new(PolarisGrantBackend::new(
        &config.access_control.url,
        config.access_control.client_id.clone(),
        config.access_control.client_secret.clone(),
    )?)),
    _ => None,  // "none" or unrecognized
};
```

`QueryHandler` receives `grant_backend: Option<Arc<dyn GrantBackend>>` instead of `access_control_client: Option<Arc<AccessControlClient>>`.

### Handler Refactoring

`extract_grant_fields` is refactored to `extract_grant_statement`, returning a `GrantStatement` with a `Grantee` enum instead of raw strings. No backend-specific mapping in the coordinator.

The five handler methods (`handle_grant`, `handle_revoke`, `handle_show_grants`, `handle_show_effective_grants`, `handle_check_access`) simplify to:
1. Convert classifier output to trait types
2. Call `grant_backend.method(token, &stmt)`
3. Convert result to `RecordBatch`

### Config Changes

```toml
[access_control]
backend = "polaris"                              # "chameleon", "polaris", "none"
url = "http://polaris:8181/api/management/v1"
# Optional: service credentials (default: passthrough token)
client_id = "sqe-service"
client_secret = "secret"
```

Two new optional fields added to `AccessControlConfig`:
- `client_id: Option<String>` (default: None)
- `client_secret: Option<String>` (default: None)

When both are absent, passthrough token is used.

## File Layout

```
crates/sqe-policy/src/
  grants/
    mod.rs              -- GrantBackend trait, shared types, privilege mapping
    chameleon.rs        -- ChameleonGrantBackend (wraps AccessControlClient)
    polaris.rs          -- PolarisGrantBackend (Management API, 3-step chain)
  lib.rs                -- add pub mod grants;

crates/sqe-core/src/
  config.rs             -- add client_id, client_secret to AccessControlConfig

crates/sqe-coordinator/src/
  query_handler.rs      -- replace access_control_client with grant_backend,
                           refactor extract_grant_fields -> extract_grant_statement
  main.rs               -- backend selection at startup

crates/sqe-catalog/src/
  access_control.rs     -- unchanged (wrapped by ChameleonGrantBackend)
```

Unchanged: `sqe-sql/src/classifier.rs`, `plan_rewriter.rs`, `opa.rs`, `policy_store.rs`.

## Test Strategy

- **Unit tests (per backend):** Serialization/deserialization of request/response types. Privilege mapping (SQL -> Polaris). Catalog role naming. Grantee type translation.
- **PolarisGrantBackend:** Three-step grant logic, 409 handling, token fallback (passthrough vs service credential), revoke-without-delete-role behavior.
- **ChameleonGrantBackend:** Type translation from `GrantStatement`/`Grantee` to `GrantRequest`.
- **Coordinator:** `extract_grant_statement` returns `Grantee` enum correctly for all sqlparser `GranteesType` variants.
- **Integration tests:** Against real Polaris instance in docker-compose stack, covering GRANT -> SHOW GRANTS -> CHECK ACCESS -> REVOKE round-trip.

## Future Backends

Adding Unity Catalog or Gravitino means one new file in `sqe-policy/src/grants/` implementing the `GrantBackend` trait, plus a new match arm in `main.rs` startup. The trait, shared types, and coordinator routing stay unchanged.
