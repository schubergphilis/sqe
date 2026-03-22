# Making SQE Work Everywhere: Pluggable Auth and Catalogs

*How we're turning a single-vendor query engine into something that runs against any identity provider, any catalog, and any cloud.*

---

## The single-vendor trap

SQE was born inside a specific stack: Keycloak for identity, Polaris for the Iceberg catalog, S3 for storage. That stack works well — it's what we run in production. But the moment you open-source an engine, people show up with different stacks.

"Can I use this with AWS Glue?"
"We're on Azure. Does it work with ADLS?"
"Our service accounts authenticate with bearer tokens, not passwords."
"We have a Nessie catalog. Is that supported?"
"We don't have a catalog server at all — just Iceberg tables on S3."

The answer to all of these was "no." Not because of fundamental limitations, but because auth and catalog access were hardwired to specific implementations. The OIDC password grant was baked into the authenticator. The Polaris REST client was the only catalog path. S3 was the only storage option.

This is the classic trap: you build for your environment, then discover that environment is a deployment detail, not a design constraint.

---

## Pluggable auth: five ways to prove who you are

The core problem with hardwired auth is that different environments have fundamentally different credential flows. A BI analyst opens DBeaver and types a username and password. A dbt pipeline running in CI has a pre-obtained service account token. A microservice in a Kubernetes pod has an mTLS certificate. A data scientist running a notebook has an API key. A development laptop has nothing at all.

These aren't edge cases. They're the five most common auth patterns in modern data infrastructure. SQE needs to handle all of them.

### The AuthProvider trait

The design is a trait with a chain:

```rust
trait AuthProvider: Send + Sync {
    async fn authenticate(&self, credentials: &Credentials) -> AuthResult;
    async fn refresh_catalog_token(&self, identity: &Identity) -> Option<String>;
}
```

Providers are configured as an ordered list. Each provider inspects the credentials and returns one of three outcomes: "here's the identity" (match), "not my credential type, try the next one" (skip), or "these credentials are mine but invalid" (reject). First match wins.

This means you can configure:

```toml
# Try bearer token first (service accounts, CI),
# then fall back to OIDC password (humans with DBeaver)
[[auth.providers]]
type      = "bearer_token"
jwks_url  = "https://idp.example.com/.well-known/jwks.json"
audience  = "sqe"

[[auth.providers]]
type      = "oidc_password"
token_url = "https://idp.example.com/realms/myapp/protocol/openid-connect/token"
client_id = "sqe"
```

The bearer token provider detects JWTs by the `eyJ` prefix in the password field (Flight SQL's Basic auth is the only hook we have). If it's not a JWT, it passes to the next provider. If it is a JWT but validation fails, authentication stops with an error.

### The five providers

**OIDC Password** — what we had before, but generalised. Instead of hardwiring Keycloak's token URL format, you configure any OIDC token endpoint. The `roles_claim` is configurable too — Keycloak puts roles in `realm_access.roles`, Auth0 uses `permissions`, Okta uses `groups`. One implementation, any provider.

**Bearer Token** — for pre-authenticated clients. Validates JWT signature against a JWKS endpoint (cached, refreshed on key rotation), checks expiry and audience, extracts user identity from claims. The token is passed through to the catalog as-is — no re-authentication needed. This is the path for Kubernetes service accounts, GitHub Actions OIDC, and any system that already has a token.

**API Key** — for scripts, dbt pipelines, and simple integrations. Keys are opaque strings (`sqe_k_abc123...`) mapped to groups in an external TOML file. Groups map to roles via a global role-mapping table. The key file is hot-reloaded without restart — add a key, and it's available within seconds. Constant-time comparison prevents timing attacks.

**mTLS** — for service mesh environments where the TLS handshake is the authentication. The client certificate CN becomes the username, OU or SAN fields map to groups. No password needed. This works naturally with Istio, Linkerd, and any zero-trust network that terminates mTLS at the sidecar.

**Anonymous** — for development, testing, or intentionally open datasets. Fixed identity, no credentials required. You'd never run this in production, but it makes local development frictionless.

### Role mappings

All providers produce an `Identity` with a username and a list of groups. Groups are mapped to roles via a single config table:

```toml
[auth.role_mappings]
"data-engineering" = ["writer", "reader", "admin-tables"]
"bi-reader"        = ["reader"]
"public"           = []
```

Roles feed into the policy engine (when it arrives in Step 5). This decouples "who are you?" from "what can you do?" — the auth provider determines identity, the role mapping determines permissions, and the policy engine enforces them at query time.

---

## Pluggable catalogs: where is your data?

Auth tells us who the user is. The catalog tells us where the data is. And it turns out people keep their data in wildly different places.

### The CatalogBackend trait

```rust
trait CatalogBackend: Send + Sync {
    async fn list_namespaces(&self, auth: &CatalogCredential) -> Result<Vec<String>>;
    async fn list_tables(&self, namespace: &str, auth: &CatalogCredential) -> Result<Vec<String>>;
    async fn load_table(&self, namespace: &str, table: &str, auth: &CatalogCredential) -> Result<TableMetadata>;
}
```

Five backends, each targeting a different catalog ecosystem:

**Iceberg REST** — the generalised version of what we have today. Works with Polaris, Snowflake Open Catalog, Databricks Unity Catalog (which exposes an Iceberg REST endpoint), and any other implementation of the Iceberg REST spec. Zero behaviour change for existing deployments.

**AWS Glue** — the dominant Iceberg catalog on AWS. Glue exposes a standard Iceberg REST endpoint at `glue.{region}.amazonaws.com/iceberg`, but authenticates with SigV4, not bearer tokens. The Glue backend wraps the Iceberg REST client with AWS IAM signing — instance profile, environment variables, or explicit keys.

**Project Nessie** — Dremio's open-source catalog with git-like versioning. Nessie has its own REST API (not Iceberg REST), with branch/tag/commit references. The backend speaks Nessie's API to list namespaces and tables, then resolves content entries to Iceberg metadata locations. You can pin queries to a branch: `ref = "main"` or `ref = "feature/new-schema"`.

**Hive Metastore** — the legacy Thrift-based catalog that still dominates Hadoop and Spark shops. The backend speaks Thrift to list databases and tables, extracts the Iceberg metadata location, and hands off to iceberg-rust for actual data access. This is a migration path: existing HMS-managed Iceberg tables become queryable without re-registering them in a REST catalog.

**Storage-only** — no catalog server at all. Point SQE at an S3 path and it scans for Iceberg metadata files. Useful for ad-hoc exploration, migration, and environments where standing up a catalog is overkill:

```toml
[catalog]
type       = "storage_only"
base_path  = "s3://my-data-lake/"
scan_depth = 3
```

SQE walks the directory tree up to `scan_depth` levels, looks for `metadata/v*.metadata.json`, and derives namespace and table names from the directory structure. For known tables, you can register them explicitly:

```toml
[[catalog.tables]]
name = "sales.orders"
path = "s3://my-data-lake/sales/orders/"
```

### Multi-cloud storage

The catalog tells us which files to read. The storage layer reads them. We're extending beyond S3:

**AWS S3** (and S3-compatible: Ceph, SeaweedFS, Garage, Cloudflare R2) — via `object_store` with endpoint override and path-style access. Most S3-compatible stores work with zero code changes.

**Azure Data Lake Storage Gen2** — native Azure Blob access with access key, SAS token, or workload identity authentication.

**Google Cloud Storage** — service account key file or workload identity federation.

**Local filesystem** — for development and CI. No cloud, no credentials, just a path on disk.

All storage backends are configured in `[storage]`, independent of the catalog choice. You can run a Nessie catalog with data on Azure, or an AWS Glue catalog with data on S3-compatible Ceph. The catalog and storage are decoupled.

### Delta Lake (read-only)

One more thing: Delta Lake support via `delta-rs`, gated behind a `delta` feature flag. Unity Catalog serves both Iceberg and Delta tables, and many organisations have a mix. Read-only Delta support means SQE can query both formats without requiring table migration.

---

## What this enables

With pluggable auth and catalogs, SQE stops being "the query engine for our stack" and becomes "a query engine that fits your stack." Concretely:

**AWS shops** deploy with Glue + IAM + S3. No Keycloak, no Polaris. Bearer tokens from Cognito or IAM Identity Center flow through the auth chain.

**Azure shops** deploy with Unity Catalog's REST endpoint + Entra ID + ADLS Gen2. Same engine, different config.

**On-premise Hadoop migrations** deploy with Hive Metastore + OIDC + Ceph/MinIO-replacement. Existing Iceberg tables become queryable immediately.

**Edge deployments** deploy with storage-only + API keys + local filesystem. No servers to run except SQE itself.

**Multi-tenant SaaS** deploys with multiple auth providers (bearer for service accounts, OIDC for humans, API keys for scripts) and a single catalog.

The engine code is the same in all cases. Only the config changes.

---

## Implementation status

This is the planned work — not yet implemented. Pluggable auth is 59 tasks across 7 phases. Pluggable catalogs is 83 tasks across 11 phases. Both are designed, spec'd, and ready to build. They can run in parallel — auth and catalog are independent trait hierarchies.

The designs exist as openspec changes with full proposals, architecture docs, GIVEN/WHEN/THEN specs, and task checklists. If you're interested in contributing, start with `openspec/changes/pluggable-auth/` and `openspec/changes/pluggable-catalogs/`.

---

*SQE is open-source under Apache 2.0. The current engine (Steps 1-3) is production-functional. Pluggable auth and catalogs (Steps 4-5) are next.*
