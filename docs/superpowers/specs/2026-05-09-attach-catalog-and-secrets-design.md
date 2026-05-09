# ATTACH CATALOG and Secrets Design

**Status:** draft, awaiting review
**Date:** 2026-05-09
**Owner:** Jacob Verhoeks
**Inspiration:** DuckDB's `ATTACH` and `CREATE SECRET`

## 1. Overview

Add SQL primitives for runtime catalog attach and credential management. Today, embedded mode mounts only SQLite-backed local Iceberg, while cluster mode supports six external catalogs through a TOML-only path. After this change, both modes support every catalog backend through SQL:

```sql
ATTACH 'https://polaris.example.com:18181/api/catalog' AS polaris
  (TYPE iceberg_rest, WAREHOUSE 'my_wh');

CREATE SECRET aws_prod (TYPE aws, REGION 'us-east-1');  -- uses AWS credential chain

ATTACH 'arn:aws:glue:us-east-1:123:catalog/sales' AS glue
  (TYPE glue, SECRET aws_prod);

SELECT * FROM glue.sales.orders;
```

Identical syntax in cluster and embedded mode. Catalogs and secrets live for the process lifetime; restart wipes them. Persistence (auto-replay on boot) is a follow-up.

## 2. Goals and non-goals

**Goals**

- Support every catalog backend the cluster supports, attachable at runtime: `iceberg_rest`, `glue`, `s3tables`, `hms`, `jdbc`, `sqlite`, `hadoop`.
- AWS credential chain by default. `TYPE glue` without an explicit secret uses `aws_config::load_defaults` (env vars, shared credentials, IMDS, ECS, EKS Pod Identity).
- Per-catalog credential override via `SECRET <name>` option, resolved against an in-memory secret store.
- One handler path that works identically in embedded and cluster mode.
- Existing TOML-driven catalog config keeps working unchanged. ATTACH is additive.

**Non-goals (v1)**

- Persisting catalogs across process restart. Operators that want this set them in TOML.
- On-disk encrypted secret store. In-memory only.
- Per-user secret scoping. Secrets are process-global. Authorization gates *who can ATTACH*, not *who can read which secret*.
- Catalog backends beyond what cluster mode already supports.
- DuckDB's database-attach syntax for non-Iceberg sources (`ATTACH 'foo.duckdb'`).
- Secret rotation hooks. Drop and recreate.

## 3. SQL syntax

### 3.1 ATTACH

```
ATTACH '<location>' AS <catalog_name>
  ( TYPE <kind> [ , <option> = <value> ]* )
```

`<catalog_name>` must be a valid SQL identifier and unique among currently-attached catalogs. `<location>` is the primary identifier the backend needs (REST URL, Glue catalog ARN, JDBC URL, Hadoop warehouse path, etc.). `<kind>` is one of: `iceberg_rest`, `glue`, `s3tables`, `hms`, `jdbc`, `sqlite`, `hadoop`.

### 3.2 DETACH

```
DETACH <catalog_name>
```

Removes the named catalog from the DataFusion session. In-flight queries against it complete. Subsequent queries see "catalog not found".

### 3.3 SHOW CATALOGS (existing, behaviour preserved)

`SHOW CATALOGS` already lists catalogs registered with DataFusion. After ATTACH, the new name appears. After DETACH, it disappears. The `CatalogConfig` struct remains the source-of-truth for TOML-attached catalogs; ATTACH-attached catalogs are tracked separately in a `RuntimeCatalogRegistry` (see §5.2) but report through the same `SHOW CATALOGS` output.

### 3.4 CREATE SECRET

```
CREATE SECRET <secret_name>
  ( TYPE <kind> [ , <option> = <value> ]* )
```

Kinds:

- `aws`: options `ACCESS_KEY`, `SECRET_KEY`, `SESSION_TOKEN`, `REGION`, `PROFILE`. Any subset; missing fields fall through to the AWS credential chain.
- `bearer`: option `TOKEN`. For Iceberg REST endpoints requiring a static bearer.
- `basic`: options `USERNAME`, `PASSWORD`. For HMS Thrift over SASL/PLAIN, JDBC, etc.

### 3.5 DROP SECRET

```
DROP SECRET <secret_name>
```

Errors if any currently-attached catalog references the secret. Detach those catalogs first.

### 3.6 SHOW SECRETS

```
SHOW SECRETS
```

Returns one row per secret: `name`, `type`. Never returns the values. Output format:

```
name        | type
------------+-------
aws_prod    | aws
polaris_jwt | bearer
```

## 4. Per-backend options and resolution

### 4.1 `iceberg_rest`

| Option | Default | Description |
|---|---|---|
| `WAREHOUSE` | required | Iceberg REST warehouse identifier |
| `SECRET <name>` | none | Bearer secret; falls back to no auth |
| `TOKEN '<value>'` | none | Inline bearer; less safe than `SECRET` |
| `PREFIX '<value>'` | none | API prefix for non-standard deployments |

Maps to `iceberg-catalog-rest::RestCatalogConfig`. Polaris, Nessie, Unity OSS, and Glue's REST endpoint all flow through this code path.

### 4.2 `glue`

| Option | Default | Description |
|---|---|---|
| `REGION` | from secret/env/profile | AWS region |
| `SECRET <name>` | none | AWS secret; chain fallback |
| `WAREHOUSE` | derived | Optional warehouse path override |

`<location>` is the catalog ARN: `arn:aws:glue:<region>:<account>:catalog/<name>`. Maps to `iceberg-catalog-glue::GlueCatalogConfig`. Credential resolution per §5.3.

### 4.3 `s3tables`

| Option | Default | Description |
|---|---|---|
| `REGION` | from secret/env | AWS region |
| `SECRET <name>` | none | AWS secret; chain fallback |
| `ENDPOINT_URL` | none | Custom endpoint (LocalStack, etc.) |

`<location>` is the table-bucket ARN: `arn:aws:s3tables:<region>:<account>:bucket/<name>`. Maps to `iceberg-catalog-s3tables`.

### 4.4 `hms`

| Option | Default | Description |
|---|---|---|
| `WAREHOUSE` | required | HMS warehouse path |
| `AUTH_MODE` | `none` | `none` \| `kerberos` \| `plain` |
| `SECRET <name>` | required when `plain` | Basic-auth secret |

`<location>` is the Thrift URL: `thrift://hms.example.com:9083`. Maps to `iceberg-catalog-hms`.

### 4.5 `jdbc`

| Option | Default | Description |
|---|---|---|
| `WAREHOUSE` | required | Iceberg warehouse path |
| `SECRET <name>` | from URL | Database basic-auth secret |

`<location>` is the JDBC URL: `jdbc:postgresql://...`, `jdbc:mysql://...`, `jdbc:sqlite:///...`. Maps to `iceberg-catalog-sql::SqlCatalogBuilder` (the same builder embedded mode already uses for its SQLite default).

### 4.6 `sqlite`

| Option | Default | Description |
|---|---|---|
| `WAREHOUSE` | required | Filesystem path for warehouse data |

`<location>` is the SQLite file path or `sqlite://...` URL. Equivalent to today's embedded `--catalog NAME=PATH` flag, expressed in SQL. Multiple ATTACH `sqlite` statements give the cross-catalog joins embedded mode already supports.

### 4.7 `hadoop`

| Option | Default | Description |
|---|---|---|
| (none) | | |

`<location>` is the warehouse root. Storage-only catalog, scans metadata directly without a server. Uses SQE's native `hadoop` backend at `crates/sqe-catalog/src/backends/hadoop.rs`.

## 5. Implementation

### 5.1 Parser (`sqe-sql`)

`sqlparser-rs` does not natively recognise `ATTACH ... AS ... (TYPE ...)`. Follow the GRANT/REVOKE pattern: parse with sqlparser, post-process to detect the custom shape, emit a SQE-specific AST node.

New AST nodes in `sqe-sql/src/ast.rs`:

```rust
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}

pub struct DetachStatement { pub name: String }

pub struct CreateSecretStatement {
    pub name: String,
    pub kind: SecretKind,
    pub options: BTreeMap<String, OptionValue>,
}

pub struct DropSecretStatement { pub name: String }

pub struct ShowSecretsStatement;

pub enum CatalogKind {
    IcebergRest, Glue, S3Tables, Hms, Jdbc, Sqlite, Hadoop,
}

pub enum SecretKind { Aws, Bearer, Basic }

pub enum OptionValue { String(String), SecretRef(String) }
```

`StatementKind` gains: `Attach(AttachStatement)`, `Detach(DetachStatement)`, `CreateSecret(CreateSecretStatement)`, `DropSecret(DropSecretStatement)`, `ShowSecrets`.

Pre-classifier rewriting handles the `ATTACH '<location>' AS <name> (TYPE ...)` shape. sqlparser parses `ATTACH '<location>'` (it has a Statement::Attach variant for SQLite-style attach). We extend with the `AS name (...)` tail, populate the options dictionary with case-insensitive keys, and reject unrecognised options at parse time.

### 5.2 Runtime catalog registry (`sqe-coordinator`)

`crates/sqe-coordinator/src/runtime_catalog.rs` (new):

```rust
pub struct RuntimeCatalogRegistry {
    catalogs: Arc<RwLock<HashMap<String, AttachedCatalog>>>,
}

pub struct AttachedCatalog {
    pub name: String,
    pub kind: CatalogKind,
    pub catalog: Arc<dyn iceberg::Catalog>,
    pub secret_ref: Option<String>,    // tracked for DROP SECRET safety
}

impl RuntimeCatalogRegistry {
    pub async fn attach(&self, stmt: &AttachStatement, secrets: &SecretStore) -> Result<()>;
    pub async fn detach(&self, name: &str, ctx: &SessionContext) -> Result<()>;
    pub fn list(&self) -> Vec<String>;
}
```

The registry is a process-global singleton injected into `QueryHandler` and `EmbeddedClient`. Backed by an `Arc<RwLock<>>` so reads (every query) cheap. Writes (ATTACH/DETACH) rare.

### 5.3 Catalog mount and credential resolution (`sqe-catalog`)

`crates/sqe-catalog/src/mount.rs` (new):

```rust
pub async fn build_catalog(
    location: &str,
    kind: CatalogKind,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>>;
```

Dispatches by `kind`. Reuses the same `iceberg-catalog-*` builders the cluster's `flattened_catalogs()` already calls.

**AWS credential resolution** for `glue` and `s3tables`:

```rust
async fn build_aws_config(
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(secret_name) = options.get("SECRET") {
        let secret = secrets.get(secret_name).await?;
        if let Secret::Aws { access_key, secret_key, session_token, region, profile } = secret {
            // Explicit creds path
            if let (Some(ak), Some(sk)) = (access_key, secret_key) {
                let creds = Credentials::new(ak, sk, session_token, None, "sqe-secret");
                loader = loader.credentials_provider(creds);
            }
            if let Some(r) = region { loader = loader.region(Region::new(r)); }
            if let Some(p) = profile { loader = loader.profile_name(p); }
        }
    }
    if let Some(r) = options.get("REGION") { loader = loader.region(Region::new(r.as_str()?)); }
    loader.load().await
}
```

The resolution order from §3.4 (`SECRET` -> env -> profile -> IMDS chain) is exactly what `aws_config::defaults` gives us once we layer the secret on top.

### 5.4 Secret store (`sqe-core`)

`crates/sqe-core/src/secret.rs` (new):

```rust
pub enum Secret {
    Aws { access_key: Option<String>, secret_key: Option<String>,
          session_token: Option<String>, region: Option<String>, profile: Option<String> },
    Bearer { token: String },
    Basic  { username: String, password: String },
}

pub struct SecretStore {
    inner: Arc<RwLock<HashMap<String, Secret>>>,
}

impl SecretStore {
    pub fn create(&self, name: &str, secret: Secret) -> Result<()>;
    pub fn drop(&self, name: &str, in_use_by: &[String]) -> Result<()>;
    pub async fn get(&self, name: &str) -> Result<Secret>;
    pub fn list(&self) -> Vec<(String, &'static str)>;  // never values
}
```

Process-global. Lives in `QueryHandler` and `EmbeddedClient`. Memory-only; the `Drop` impl zeroes secret bytes via `zeroize`.

### 5.5 Coordinator integration

In `crates/sqe-coordinator/src/query_handler.rs::execute_statement`, add new arms to the dispatch:

```rust
StatementKind::Attach(stmt)        => self.handle_attach(session, stmt).await,
StatementKind::Detach(stmt)        => self.handle_detach(session, stmt).await,
StatementKind::CreateSecret(stmt)  => self.handle_create_secret(session, stmt).await,
StatementKind::DropSecret(stmt)    => self.handle_drop_secret(session, stmt).await,
StatementKind::ShowSecrets         => self.handle_show_secrets(session).await,
```

Each handler routes to `runtime_catalog` + `secret_store`. Authorisation check (admin only in cluster, anyone in embedded) sits at the top of each handler.

### 5.6 Embedded integration

`crates/sqe-cli/src/embedded.rs::EmbeddedClient` gains the same `runtime_catalog` and `secret_store` fields. The existing `--catalog NAME=PATH` flag becomes equivalent to a synthetic `ATTACH '<path>' AS <name> (TYPE sqlite)` issued at startup. CLI flag preserved for backward compatibility.

## 6. Authorisation

`config.security.attach_admin_users` (new TOML field, default `[]`) lists usernames allowed to ATTACH/DETACH/CREATE/DROP secrets in cluster mode. Empty list means only the bootstrap admin (matches existing GRANT/REVOKE behaviour). Embedded mode always allows the local user.

Future: an OPA policy hook so attach-time authorisation can defer to a central policy. Not v1.

## 7. Testing

### 7.1 Unit tests

| Crate | What |
|---|---|
| `sqe-sql` | parser tests for ATTACH/DETACH/CREATE SECRET/DROP SECRET/SHOW SECRETS, including option parsing, case-insensitive keys, error cases |
| `sqe-core::secret` | create/drop/get; in-use guard rejects DROP SECRET while attached; zeroize on Drop |
| `sqe-catalog::mount` | per-backend builders return correct catalog handle; AWS credential layering with mock environment vars |

### 7.2 Integration tests

`crates/sqe-coordinator/tests/attach_integration_test.rs`:

- ATTACH iceberg_rest against wiremock; SELECT a 3-part name; verify expected REST traffic.
- ATTACH glue with explicit SECRET; query a stub Glue API (mock via `aws-smithy-mocks` or LocalStack); verify SigV4 headers.
- ATTACH glue without SECRET; populate `AWS_ACCESS_KEY_ID` env; verify chain pickup.
- ATTACH sqlite; identical behaviour to existing `--catalog NAME=PATH`.
- DETACH and re-ATTACH; verify DataFusion sees the change.
- DROP SECRET while attached; verify error.

`crates/sqe-cli/tests/embedded_attach_test.rs`:

- Full ATTACH cycle in embedded mode.
- Cross-catalog joins between two ATTACH'd backends.

### 7.3 Documentation

| Doc | Change |
|---|---|
| `docs/book/src/operations/catalogs.md` (new chapter) | ATTACH/DETACH/SECRET reference with examples per backend |
| `docs/cli-embedded.md` | Section on SQL-driven catalog attach; deprecation notice on `--catalog` URL forms (still work, but `ATTACH` is preferred) |
| `docs/blog/2026-05-09-attach-catalog.md` | Operator-facing post |
| `docs/ebook/chapters/16f-attaching-the-world.md` | Narrative chapter (post-implementation) |
| `docs/roadmap.md` | Move ATTACH out of "Planned" |
| `README.md` | Add an ATTACH example to the lead |

## 8. File layout

### New files

```
crates/sqe-sql/src/attach.rs                 # AST + parser hooks
crates/sqe-core/src/secret.rs                # SecretStore
crates/sqe-catalog/src/mount.rs              # build_catalog dispatch
crates/sqe-catalog/src/aws_config.rs         # AWS chain layering
crates/sqe-coordinator/src/runtime_catalog.rs
crates/sqe-coordinator/src/handlers/attach.rs
crates/sqe-coordinator/src/handlers/detach.rs
crates/sqe-coordinator/src/handlers/secret.rs
crates/sqe-coordinator/tests/attach_integration_test.rs
crates/sqe-cli/tests/embedded_attach_test.rs
docs/book/src/operations/catalogs.md
```

### Modified files

```
crates/sqe-sql/src/lib.rs                    # export new statement kinds
crates/sqe-sql/src/classifier.rs             # dispatch ATTACH/DETACH/SECRET shapes
crates/sqe-core/src/lib.rs                   # re-export secret
crates/sqe-core/src/config.rs                # security.attach_admin_users
crates/sqe-coordinator/src/query_handler.rs  # new dispatch arms
crates/sqe-coordinator/src/lib.rs            # wire registry
crates/sqe-cli/src/embedded.rs               # plumb registry + secrets
crates/sqe-cli/src/main.rs                   # auto-ATTACH for legacy --catalog flag
README.md                                    # ATTACH in the lead
docs/cli-embedded.md
docs/roadmap.md
```

### Net-new dependencies

`zeroize = "1"` for the secret-store memory hygiene. Everything else (`aws-config`, `aws-sdk-glue`, `aws-sdk-s3tables`, `iceberg-catalog-rest/glue/hms/sql/s3tables`) already in the workspace.

## 9. Open questions deferred to v2

- Persistent secrets and catalogs across restart (likely `~/.sqe/catalogs.toml` + `~/.sqe/secrets.toml` with mode 600).
- Encrypted-at-rest secrets (envelope encryption with a KMS key reference).
- Per-user secret scoping (today: process-global).
- OPA policy integration for ATTACH authorisation.
- DESCRIBE CATALOG <name> for showing per-catalog config.
- ALTER SECRET for in-place rotation.

## 10. Inspirations

- [DuckDB ATTACH](https://duckdb.org/docs/stable/sql/statements/attach)
- [DuckDB Secrets Manager](https://duckdb.org/docs/stable/configuration/secrets_manager)
- [aws-sdk-rust default credential chain](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html)
- The existing SQE GRANT/REVOKE post-parse rewriting pattern (`sqe-sql/src/lib.rs`)
