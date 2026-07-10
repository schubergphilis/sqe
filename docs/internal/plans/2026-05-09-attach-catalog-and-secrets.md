# ATTACH Catalog and Secrets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add SQL `ATTACH`/`DETACH` for runtime Iceberg catalog mounting plus `CREATE`/`DROP`/`SHOW SECRETS` for credential management. Embedded and cluster modes share one handler path. AWS catalogs honour the standard AWS credential chain when no explicit secret is provided.

**Spec:** `docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md`

**Architecture:**
- Parser: post-process sqlparser-rs output to detect ATTACH/DETACH/CREATE SECRET shapes (mirrors GRANT/REVOKE pattern).
- Runtime registry: process-global `Arc<RwLock<HashMap<String, AttachedCatalog>>>` lives in QueryHandler + EmbeddedClient.
- Secret store: process-global `Arc<RwLock<HashMap<String, Secret>>>`. Memory only. Zeroized on drop.
- Catalog mount: dispatches to `iceberg-catalog-{rest,glue,s3tables,hms,sql}` and SQE's hadoop backend. AWS chain via `aws_config::defaults`.

**Branch:** `feat/attach-catalog-secrets` (already created)

---

## Phase A: SQL parser + AST (sqe-sql)

### Task A1: AST types

**Files:**
- Create: `crates/sqe-sql/src/attach.rs`
- Modify: `crates/sqe-sql/src/lib.rs` (re-export, extend `StatementKind`)

`crates/sqe-sql/src/attach.rs`:

```rust
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachStatement { pub name: String }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecretStatement {
    pub name: String,
    pub kind: SecretKind,
    pub options: BTreeMap<String, OptionValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSecretStatement { pub name: String }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind { IcebergRest, Glue, S3Tables, Hms, Jdbc, Sqlite, Hadoop }

impl CatalogKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "iceberg_rest" | "rest" => Some(Self::IcebergRest),
            "glue"     => Some(Self::Glue),
            "s3tables" => Some(Self::S3Tables),
            "hms" | "hive" => Some(Self::Hms),
            "jdbc"     => Some(Self::Jdbc),
            "sqlite"   => Some(Self::Sqlite),
            "hadoop"   => Some(Self::Hadoop),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::IcebergRest => "iceberg_rest",
            Self::Glue => "glue", Self::S3Tables => "s3tables",
            Self::Hms => "hms", Self::Jdbc => "jdbc",
            Self::Sqlite => "sqlite", Self::Hadoop => "hadoop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind { Aws, Bearer, Basic }
impl SecretKind {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "aws"    => Some(Self::Aws),
            "bearer" => Some(Self::Bearer),
            "basic"  => Some(Self::Basic),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    String(String),
    SecretRef(String),  // unquoted identifier
}

impl OptionValue {
    pub fn as_str(&self) -> Option<&str> {
        match self { Self::String(s) => Some(s), _ => None }
    }
    pub fn as_secret_ref(&self) -> Option<&str> {
        match self { Self::SecretRef(s) => Some(s), _ => None }
    }
}
```

Add to `StatementKind` enum (in `sqe-sql/src/lib.rs` or wherever it lives): `Attach(Box<AttachStatement>)`, `Detach(Box<DetachStatement>)`, `CreateSecret(Box<CreateSecretStatement>)`, `DropSecret(Box<DropSecretStatement>)`, `ShowSecrets`.

`StatementKind::name()` should return `attach`, `detach`, `create_secret`, `drop_secret`, `show_secrets`.

- [ ] Step 1: Write a failing test asserting `AttachStatement` and `CatalogKind::parse("glue")` work
- [ ] Step 2: FAIL
- [ ] Step 3: Add the AST types and `StatementKind` variants
- [ ] Step 4: PASS
- [ ] Step 5: `cargo clippy -p sqe-sql --all-targets -- -D warnings` clean
- [ ] Step 6: Commit `feat(sql): AST for ATTACH/DETACH/SECRET`

### Task A2: Parser hooks

**Files:**
- Modify: `crates/sqe-sql/src/lib.rs` (or `classifier.rs`) to recognise the shapes

sqlparser-rs already has `Statement::Attach` for SQLite-style `ATTACH '<location>' AS <name>`. We extend post-parse: if the statement's tail contains `(TYPE <kind>, ...)`, classify as `StatementKind::Attach`. Otherwise fall through unchanged.

Detection happens by re-tokenising the original SQL (or by inspecting `Statement::Attach.options` if sqlparser exposes them in the version SQE pins). Look at how `parse_and_classify` already handles GRANT/REVOKE for the established post-process pattern.

For DETACH, sqlparser has `Statement::Detach`. Extract the name; emit `Detach(stmt)`.

For CREATE SECRET / DROP SECRET / SHOW SECRETS, sqlparser does NOT have these. Match on the raw token stream:

```rust
fn try_parse_secret_stmt(sql: &str) -> Option<StatementKind> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("CREATE SECRET ") {
        // Parse: CREATE SECRET <name> (TYPE <kind>, <key>=<value>, ...)
        // ...
    } else if upper.starts_with("DROP SECRET ") {
        // ...
    } else if upper.trim_end() == "SHOW SECRETS" {
        return Some(StatementKind::ShowSecrets);
    }
    None
}
```

Implement a small hand-rolled parser for the option list. Same shape `(KEY = 'value', KEY2 = ident)` with case-insensitive keys, single-quoted strings for `OptionValue::String`, bare identifiers for `OptionValue::SecretRef` (only when key is `SECRET`).

- [ ] Step 1: Failing tests covering ATTACH iceberg_rest, ATTACH glue with SECRET, DETACH, CREATE SECRET aws, DROP SECRET, SHOW SECRETS, and rejection of invalid kinds
- [ ] Step 2: FAIL
- [ ] Step 3: Implement post-parse hooks in `classifier.rs`
- [ ] Step 4: PASS, all parser tests green
- [ ] Step 5: Clippy clean
- [ ] Step 6: Commit `feat(sql): parse ATTACH/DETACH/SECRET statements`

---

## Phase B: Secret store (sqe-core)

### Task B1: Secret enum + zeroize

**Files:**
- Create: `crates/sqe-core/src/secret.rs`
- Modify: `crates/sqe-core/Cargo.toml` (add `zeroize = "1"`)
- Modify: `crates/sqe-core/src/lib.rs` (re-export)

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use zeroize::Zeroize;

#[derive(Debug, Clone)]
pub enum Secret {
    Aws {
        access_key: Option<String>,
        secret_key: Option<String>,
        session_token: Option<String>,
        region: Option<String>,
        profile: Option<String>,
    },
    Bearer { token: String },
    Basic  { username: String, password: String },
}

impl Secret {
    pub fn type_name(&self) -> &'static str {
        match self { Self::Aws { .. } => "aws", Self::Bearer { .. } => "bearer", Self::Basic { .. } => "basic" }
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        match self {
            Self::Aws { access_key, secret_key, session_token, .. } => {
                if let Some(s) = access_key.as_mut() { s.zeroize() }
                if let Some(s) = secret_key.as_mut() { s.zeroize() }
                if let Some(s) = session_token.as_mut() { s.zeroize() }
            }
            Self::Bearer { token } => token.zeroize(),
            Self::Basic { password, .. } => password.zeroize(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SecretStore { inner: Arc<RwLock<HashMap<String, Secret>>> }

impl SecretStore {
    pub fn create(&self, name: &str, secret: Secret) -> Result<(), String> {
        let mut w = self.inner.write().map_err(|_| "secret store poisoned".to_string())?;
        if w.contains_key(name) {
            return Err(format!("secret '{name}' already exists"));
        }
        w.insert(name.to_string(), secret);
        Ok(())
    }
    pub fn drop_secret(&self, name: &str, in_use_by: &[String]) -> Result<(), String> {
        if !in_use_by.is_empty() {
            return Err(format!("secret '{name}' is referenced by attached catalogs: {}",
                               in_use_by.join(", ")));
        }
        let mut w = self.inner.write().map_err(|_| "secret store poisoned".to_string())?;
        w.remove(name).ok_or_else(|| format!("secret '{name}' not found"))?;
        Ok(())
    }
    pub fn get(&self, name: &str) -> Result<Secret, String> {
        let r = self.inner.read().map_err(|_| "secret store poisoned".to_string())?;
        r.get(name).cloned().ok_or_else(|| format!("secret '{name}' not found"))
    }
    pub fn list(&self) -> Vec<(String, &'static str)> {
        let r = self.inner.read().expect("secret store");
        r.iter().map(|(n, s)| (n.clone(), s.type_name())).collect()
    }
}
```

Tests:
- create + get round-trip
- create twice with same name -> error
- drop while in_use_by non-empty -> error
- list returns names + types but no values
- secret bytes zeroized after drop (verify by holding a `*const u8` pointer? skip; zeroize is well-tested upstream)

- [ ] Step 1: failing test for SecretStore round-trip
- [ ] Step 2: FAIL
- [ ] Step 3: Implement Secret + SecretStore
- [ ] Step 4: PASS
- [ ] Step 5: Clippy clean
- [ ] Step 6: Commit `feat(core): in-memory SecretStore with zeroize`

---

## Phase C: Catalog mount API (sqe-catalog)

### Task C1: AWS credential layering

**Files:**
- Create: `crates/sqe-catalog/src/aws_config.rs`
- Modify: `crates/sqe-catalog/Cargo.toml` (ensure `aws-config` dep present; should already be)

```rust
use sqe_core::{Secret, SecretStore};
use sqe_sql::OptionValue;
use std::collections::BTreeMap;

pub async fn build_aws_config(
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<aws_config::SdkConfig, String> {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(secret_ref) = options.get("SECRET").and_then(|v| v.as_secret_ref()) {
        let secret = secrets.get(secret_ref)?;
        if let Secret::Aws { access_key, secret_key, session_token, region, profile } = secret {
            if let (Some(ak), Some(sk)) = (access_key.clone(), secret_key.clone()) {
                let creds = aws_credential_types::Credentials::new(
                    ak, sk, session_token, None, "sqe-secret",
                );
                loader = loader.credentials_provider(creds);
            }
            if let Some(r) = region { loader = loader.region(aws_config::Region::new(r)); }
            if let Some(p) = profile { loader = loader.profile_name(p); }
        } else {
            return Err(format!("secret '{secret_ref}' is not of type aws"));
        }
    }

    if let Some(r) = options.get("REGION").and_then(|v| v.as_str()) {
        loader = loader.region(aws_config::Region::new(r.to_string()));
    }

    Ok(loader.load().await)
}
```

The `aws_credential_types` crate name may need adjustment to match the version in Cargo.lock. Check `aws-sdk-glue` transitive deps and use whatever it brings.

Tests: simulate scenarios via `aws_config::test_credentials::Credentials` or by setting env vars in the test harness.

- [ ] Step 1: failing test verifying `build_aws_config` with explicit SECRET produces SdkConfig with those creds (use `loader.identity_cache(...)` to inspect)
- [ ] Step 2: FAIL
- [ ] Step 3: Implement
- [ ] Step 4: PASS
- [ ] Step 5: Clippy clean
- [ ] Step 6: Commit `feat(catalog): AWS credential chain layering`

### Task C2: Catalog mount dispatch

**Files:**
- Create: `crates/sqe-catalog/src/mount.rs`

Single async function that dispatches by `CatalogKind` and produces an `Arc<dyn iceberg::Catalog>`. Reuses the same `iceberg-catalog-{rest,glue,s3tables,hms,sql}` builders the cluster's `flattened_catalogs` already calls. For Sqlite: re-uses the existing pattern from `embedded.rs::attach_sqlite_catalog`.

For Hadoop: SQE-native, see `crates/sqe-catalog/src/backends/hadoop.rs` for the existing builder.

```rust
pub async fn build_catalog(
    location: &str,
    kind: CatalogKind,
    options: &BTreeMap<String, OptionValue>,
    secrets: &SecretStore,
) -> Result<Arc<dyn iceberg::Catalog>, String> {
    match kind {
        CatalogKind::IcebergRest => build_rest(location, options, secrets).await,
        CatalogKind::Glue        => build_glue(location, options, secrets).await,
        CatalogKind::S3Tables    => build_s3tables(location, options, secrets).await,
        CatalogKind::Hms         => build_hms(location, options, secrets).await,
        CatalogKind::Jdbc        => build_jdbc(location, options, secrets).await,
        CatalogKind::Sqlite      => build_sqlite(location, options).await,
        CatalogKind::Hadoop      => build_hadoop(location, options).await,
    }
}
```

Each `build_*` function follows the same internal shape as the corresponding `flattened_catalogs` arm. Keep the per-backend code small; defer hard cases (KERBEROS for HMS, mTLS for REST) to follow-ups.

- [ ] Step 1: failing test that builds an iceberg_rest catalog against a wiremock REST endpoint and verifies it speaks
- [ ] Step 2: FAIL
- [ ] Step 3: Implement build_rest, build_sqlite first (simplest); wire dispatch
- [ ] Step 4: PASS for those two
- [ ] Step 5: Implement build_glue, build_s3tables, build_hms, build_jdbc, build_hadoop in subsequent commits, one per type, each with a failing test
- [ ] Step 6: Final clippy clean
- [ ] Step 7: Commit `feat(catalog): runtime catalog mount API`

---

## Phase D: Runtime catalog registry (sqe-coordinator)

### Task D1: RuntimeCatalogRegistry

**Files:**
- Create: `crates/sqe-coordinator/src/runtime_catalog.rs`

```rust
use sqe_catalog::{build_catalog, AttachedCatalog};
use sqe_core::SecretStore;
use sqe_sql::{AttachStatement, CatalogKind};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct AttachedCatalog {
    pub name: String,
    pub kind: CatalogKind,
    pub catalog: Arc<dyn iceberg::Catalog>,
    pub secret_ref: Option<String>,
}

#[derive(Default, Clone)]
pub struct RuntimeCatalogRegistry {
    inner: Arc<RwLock<HashMap<String, AttachedCatalog>>>,
}

impl RuntimeCatalogRegistry {
    pub async fn attach(
        &self,
        stmt: &AttachStatement,
        secrets: &SecretStore,
        ctx: &datafusion::execution::context::SessionContext,
    ) -> Result<(), String> {
        // 1. Refuse duplicate name
        // 2. Build catalog via build_catalog
        // 3. Wrap in WritableIcebergCatalog (existing in sqe-cli; lift it to sqe-catalog)
        // 4. ctx.register_catalog(name, provider)
        // 5. Insert into self.inner
    }

    pub fn detach(
        &self,
        name: &str,
        ctx: &datafusion::execution::context::SessionContext,
    ) -> Result<(), String> {
        // ctx.deregister_catalog(name) and remove from map
    }

    pub fn list(&self) -> Vec<String> { ... }
    pub fn referenced_secrets(&self, secret_name: &str) -> Vec<String> { ... }
}
```

`WritableIcebergCatalog` currently lives at `crates/sqe-cli/src/writable_iceberg_catalog.rs`. Lift it into `sqe-catalog` so both embedded and cluster paths share it.

- [ ] Step 1: lift WritableIcebergCatalog to `sqe-catalog`. Keep `sqe-cli` re-export shim for compat.
- [ ] Step 2: Commit `refactor(catalog): move WritableIcebergCatalog from sqe-cli to sqe-catalog`
- [ ] Step 3: Failing test for RuntimeCatalogRegistry::attach + detach
- [ ] Step 4: FAIL
- [ ] Step 5: Implement
- [ ] Step 6: PASS
- [ ] Step 7: Commit `feat(coordinator): RuntimeCatalogRegistry`

---

## Phase E: Coordinator handlers

### Task E1: Plumb registry + secrets into QueryHandler

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs`

Add fields:
```rust
runtime_catalogs: RuntimeCatalogRegistry,
secrets: SecretStore,
```

Constructor accepts them as new args (default-ed `Default::default()` everywhere existing call sites use). All existing tests keep passing.

- [ ] Step 1: Add fields + constructor args; pass `Default::default()` at every call site
- [ ] Step 2: `cargo build -p sqe-coordinator` clean
- [ ] Step 3: `cargo test -p sqe-coordinator` all green
- [ ] Step 4: Commit `refactor(coordinator): plumb RuntimeCatalogRegistry + SecretStore`

### Task E2: ATTACH/DETACH handlers

**Files:**
- Create: `crates/sqe-coordinator/src/handlers/attach.rs`
- Modify: `query_handler.rs` (dispatch arms)

```rust
StatementKind::Attach(stmt) => self.handle_attach(session, stmt).await,
StatementKind::Detach(stmt) => self.handle_detach(session, stmt).await,
```

`handle_attach`:
1. Authorisation check (admin only in cluster, anyone in embedded)
2. Build the SessionContext (existing path)
3. Call `runtime_catalogs.attach(stmt, &self.secrets, &ctx)`
4. Return a one-row result: `attached '<name>' (TYPE <kind>)`

`handle_detach`: analogous; returns `detached '<name>'`.

- [ ] Step 1: Failing test in `tests/attach_integration_test.rs` (wiremock + REST attach)
- [ ] Step 2: FAIL
- [ ] Step 3: Implement
- [ ] Step 4: PASS
- [ ] Step 5: Commit `feat(coordinator): ATTACH/DETACH handlers`

### Task E3: CREATE SECRET / DROP SECRET / SHOW SECRETS handlers

**Files:**
- Create: `crates/sqe-coordinator/src/handlers/secret.rs`
- Modify: `query_handler.rs` (dispatch arms)

`handle_create_secret`:
1. Authorisation check
2. Convert AST options to `Secret::Aws { ... }` / `Bearer { ... }` / `Basic { ... }`
3. `secrets.create(&stmt.name, secret)?`
4. Return one-row result

`handle_drop_secret`:
1. Authorisation check
2. `let in_use = self.runtime_catalogs.referenced_secrets(&stmt.name);`
3. `secrets.drop_secret(&stmt.name, &in_use)?`

`handle_show_secrets`: returns RecordBatch with columns (`name`, `type`).

- [ ] Step 1: Failing tests for each
- [ ] Step 2: FAIL
- [ ] Step 3: Implement
- [ ] Step 4: PASS
- [ ] Step 5: Commit `feat(coordinator): CREATE/DROP/SHOW SECRETS handlers`

---

## Phase F: Embedded mode wiring (sqe-cli)

### Task F1: Plumb registry + secrets into EmbeddedClient

**Files:**
- Modify: `crates/sqe-cli/src/embedded.rs`

Add fields and constructor args matching QueryHandler. `--catalog NAME=PATH` becomes a synthetic `ATTACH '<path>' AS <name> (TYPE sqlite)` issued at startup; backward compat preserved.

- [ ] Step 1: Plumb registry + secrets through
- [ ] Step 2: Failing test: ATTACH inside embedded mode adds the catalog and queries succeed
- [ ] Step 3: FAIL
- [ ] Step 4: Implement
- [ ] Step 5: PASS
- [ ] Step 6: Commit `feat(cli): SQL-driven catalog attach in embedded mode`

---

## Phase G: Integration tests

### Task G1: tests/attach_integration_test.rs

Cases (each its own `#[tokio::test]`):

1. `attach_iceberg_rest_against_wiremock`: ATTACH, query 3-part name, verify expected REST traffic.
2. `attach_glue_with_explicit_secret`: `aws-smithy-mocks` based stub OR LocalStack; verify SigV4 headers from the secret's keys.
3. `attach_glue_uses_aws_credential_chain_when_no_secret`: set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` env vars before ATTACH, verify the catalog uses them.
4. `attach_sqlite_equivalent_to_legacy_flag`: ATTACH sqlite + a CREATE TABLE, then a query; same outcome as the existing `--catalog NAME=PATH` flow.
5. `detach_unregisters_catalog`: ATTACH, DETACH, verify SHOW CATALOGS omits it and subsequent queries error.
6. `drop_secret_in_use_errors`: CREATE SECRET + ATTACH glue with SECRET ref + DROP SECRET fails until DETACH.
7. `re_attach_after_detach`. ATTACH, DETACH, ATTACH again all succeed.

- [ ] Step 1: write all tests
- [ ] Step 2: PASS
- [ ] Step 3: Commit `test(coordinator): ATTACH integration tests`

---

## Phase H: Documentation

### Task H1: mdBook chapter

**Files:**
- Create: `docs/book/src/operations/catalogs.md`
- Modify: `docs/book/src/SUMMARY.md`

Cover: ATTACH/DETACH syntax, per-backend examples, SHOW CATALOGS output, CREATE/DROP/SHOW SECRETS, AWS credential chain explanation, troubleshooting (catalog already exists, secret in use, etc.).

Voice rules: no emdash/endash/Unicode arrows, no AI tells.

- [x] Step 1: Write — `docs/book/src/operations/catalogs.md` (operator reference) + `docs/ebook/chapters/06c-attaching-at-runtime.md` (narrative)
- [x] Step 2: `grep -nE '(—|–|→)' ...` zero hits
- [x] Step 3: `mdbook build` clean
- [x] Step 4: Commit `docs(book): ATTACH catalog and secrets chapter`

### Task H2: README + cli-embedded.md + roadmap

- README.md: add an ATTACH example to the lead.
- docs/cli-embedded.md: section linking to the new chapter; note that the URL/ARN forms of `--catalog` are now redundant.
- docs/roadmap.md: move the embedded-catalog item from "Open" to "Shipped".

- [x] Step 1: Edit (README.md, docs/cli-embedded.md, docs/roadmap.md)
- [x] Step 2: Voice check (zero emdash / endash / arrow hits)
- [x] Step 3: Commit `docs: README + cli-embedded.md + roadmap for ATTACH`

### Task H3: Blog post

**Files:**
- Create: `docs/blog/2026-05-10-attach-catalog-and-secrets.md`

Operator-facing post. Leads with the analyst's use case, walks through the parser, secret store, the lifecycle bug found in Phase G, and the registry pattern. Closes with v2 deferrals (persistence, encryption, OPA-gated ATTACH).

- [x] Step 1: Write
- [x] Step 2: Voice check (zero emdash + zero AI tells)
- [x] Step 3: Commit `docs(blog): ATTACH catalog and secrets`

---

## Phase I: Final + push + MR

### Task I1: Full sweep

- [ ] `cargo build --all` clean
- [ ] `cargo test -p sqe-sql -p sqe-core -p sqe-catalog -p sqe-coordinator -p sqe-cli` all green
- [ ] `cargo clippy -p sqe-sql -p sqe-core -p sqe-catalog -p sqe-coordinator -p sqe-cli --all-targets -- -D warnings` clean

### Task I2: Push + MR

```bash
git push -u origin feat/attach-catalog-secrets \
  -o merge_request.create \
  -o merge_request.target=main \
  -o merge_request.title="feat: SQL ATTACH catalog and CREATE SECRET (DuckDB-inspired)" \
  -o merge_request.remove_source_branch
```

Then `glab mr update <MR> --description "..."` (or via web UI) to attach the full PR description from the test plan in this doc.

---

## Self-review notes

- **Spec coverage:** every spec section maps to at least one task. §3 (SQL syntax) -> Phase A; §4 (per-backend options) -> Phase C; §5 (implementation) -> Phases C+D+E; §6 (auth) -> handler step in E2/E3; §7 (testing) -> Phase G; §8 (file layout) -> all phases.
- **Placeholder scan:** zero `TBD/TODO/FIXME` in this plan beyond explicit deferred items in §9 of the spec.
- **Type consistency:** AttachStatement, DetachStatement, CreateSecretStatement, DropSecretStatement, CatalogKind, SecretKind, OptionValue, Secret, SecretStore, AttachedCatalog, RuntimeCatalogRegistry. All defined in early tasks, used consistently in later tasks.
- **Risks:** the `aws-credential-types` crate version is pinned by `aws-sdk-glue`'s transitive deps. If the API differs, the implementer should match the version in Cargo.lock at attach-time, not chase a particular version.
