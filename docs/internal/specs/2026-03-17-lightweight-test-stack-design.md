# Lightweight Test Stack

**Date:** 2026-03-17
**Status:** Draft

## Problem

Integration tests require the full quickstart stack (Polaris + PostgreSQL + Keycloak + OPA + MinIO + bootstrap jobs) — 5+ containers, slow startup, fragile. All integration tests are `#[ignore]` because of this.

## Solution

A 2-container test stack: Polaris (in-memory) + RustFS (S3). No database, no Keycloak, no OPA.

### Architecture

```
sqe-server → Polaris (in-memory, bootstrap principal) → RustFS (S3-compatible)
                ↑
         OAuth2 client_credentials grant
         (root / s3cr3t)
```

### Components

**Polaris (in-memory mode)**
- `apache/polaris:1.3.0-incubating`
- `POLARIS_PERSISTENCE_TYPE=in-memory` — no PostgreSQL needed
- `POLARIS_BOOTSTRAP_CREDENTIALS=iceberg,root,s3cr3t` — auto-creates principal on startup
- `POLARIS_PRODUCTION_READINESS_CHECKS_ENABLED=false`
- No external OIDC — uses built-in token endpoint at `/api/catalog/v1/oauth/tokens`

**RustFS (S3-compatible storage)**
- `rustfs/rustfs:latest`
- Lightweight Rust-based S3-compatible object storage
- Replaces MinIO — same S3 API, much lighter
- Stores Iceberg data files (Parquet) and metadata

### Auth: client_credentials grant

SQE's auth layer gains support for OAuth2 `client_credentials` grant alongside existing Keycloak ROPC.

**Config selection logic:**
- If `auth.keycloak_url` is set → Keycloak ROPC (existing behavior)
- If `auth.token_endpoint` + `auth.client_id` + `auth.client_secret` are set → client_credentials grant against that endpoint

**Test config:**
```toml
[auth]
token_endpoint = "http://localhost:8181/api/catalog/v1/oauth/tokens"
client_id = "root"
client_secret = "s3cr3t"

[catalog]
polaris_url = "http://localhost:8181/api/catalog"
warehouse = "test_warehouse"

[storage]
s3_endpoint = "http://localhost:9000"
s3_access_key = "s3admin"
s3_secret_key = "s3admin"
s3_region = "us-east-1"
s3_path_style = true
```

### Bootstrap script

`scripts/bootstrap-test.sh` — runs after `docker compose up`, before tests:

1. **Create S3 bucket** — `PUT http://rustfs:9000/warehouse` via S3 API
2. **Get Polaris token** — `POST /api/catalog/v1/oauth/tokens` with `grant_type=client_credentials`
3. **Create warehouse catalog** — `POST /api/management/v1/catalogs` with S3 storage config pointing at RustFS
4. **Grant catalog access** — assign `catalog_admin` role to root principal
5. **Create default namespace** — `POST /api/catalog/v1/test_warehouse/namespaces`

All steps are idempotent (safe to re-run).

### Docker Compose

```yaml
# docker-compose.test.yml
services:
  polaris:
    image: apache/polaris:1.3.0-incubating
    environment:
      POLARIS_PERSISTENCE_TYPE: in-memory
      POLARIS_BOOTSTRAP_CREDENTIALS: "iceberg,root,s3cr3t"
      POLARIS_PRODUCTION_READINESS_CHECKS_ENABLED: "false"
    ports:
      - "8181:8181"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/healthcheck"]
      interval: 5s
      timeout: 3s
      retries: 10

  rustfs:
    image: rustfs/rustfs:latest
    command: server /data
    environment:
      RUSTFS_ROOT_USER: s3admin
      RUSTFS_ROOT_PASSWORD: s3admin
    ports:
      - "9000:9000"
```

### Changes required

| File | Change |
|------|--------|
| `crates/sqe-core/src/config.rs` | Add `token_endpoint`, `client_secret` to `AuthConfig` |
| `crates/sqe-auth/src/lib.rs` | Add `client_credentials` grant flow alongside ROPC |
| `docker-compose.test.yml` | New file: Polaris in-memory + RustFS |
| `scripts/bootstrap-test.sh` | New file: create bucket, warehouse, namespace |
| `tests/sqe-test.toml` | Update to point at lightweight stack |
| `crates/sqe-coordinator/tests/integration_test.rs` | Remove `#[ignore]` from tests that work with lightweight stack |
| `.gitlab-ci.yml` | Add test stage that starts lightweight stack + runs integration tests |

### What this does NOT change

- Production deployment still uses Polaris + PostgreSQL + Keycloak + OPA
- SQE's token passthrough model is unchanged (bearer token → Polaris)
- No new crate dependencies — `reqwest` already supports form-encoded POST
- `SessionCatalog` / `RestCatalog` path is identical — Polaris REST API is the same regardless of backend
