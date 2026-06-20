# Polaris Dynamic Catalog Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a warehouse the caller is authorized for be queryable as `"<warehouse>".<ns>.<tbl>` with no static `[catalogs.*]` entry and no restart, by lazily probing Polaris on an unknown 3-part reference.

**Architecture:** When `[query] catalog_discovery = "polaris-auto"`, the coordinator pre-flight fetches the caller's cached session `ctx`, treats `ctx.catalog_names()` as the set of known catalogs, and for any still-unknown qualifier builds the *same* `SqeCatalogProvider` static catalogs use (template config + warehouse override, resolved with the caller's bearer) and registers it into that per-user `ctx`. Polaris enforces authz; any probe failure falls through to the existing "unknown catalog" error (no info leak).

**Tech Stack:** Rust, DataFusion `SessionContext`, `sqe-catalog` (`SessionCatalog`, `SqeCatalogProvider`), iceberg REST catalog (Polaris).

**Spec:** `docs/superpowers/specs/2026-05-29-polaris-catalog-discovery-design.md`

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/sqe-core/src/config.rs` | config types | add `CatalogDiscovery` enum + `QueryConfig.catalog_discovery` |
| `crates/sqe-coordinator/src/session_context.rs` | per-session ctx build | extract `build_catalog_provider` helper (shared by the static loop and discovery) |
| `crates/sqe-coordinator/src/query_handler.rs` | statement execution + pre-flight | discovery hook: resolve unknown qualifiers into the session `ctx` |
| `crates/sqe-coordinator/tests/catalog_discovery_test.rs` | integration tests | new |
| `quickstart/sqe/assets/sqe-config/sqe.toml` | quickstart config | set `catalog_discovery = "polaris-auto"` |

---

## Task 1: Config — `CatalogDiscovery` enum + `[query] catalog_discovery`

**Files:**
- Modify: `crates/sqe-core/src/config.rs` (QueryConfig struct ~line 107; add enum near `QueryEngine` ~line 84)

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `crates/sqe-core/src/config.rs`:

```rust
#[test]
fn catalog_discovery_parses_and_defaults() {
    assert_eq!(CatalogDiscovery::parse("polaris-auto"), CatalogDiscovery::PolarisAuto);
    assert_eq!(CatalogDiscovery::parse("static"), CatalogDiscovery::Static);
    assert_eq!(CatalogDiscovery::parse("PoLaRiS-AuTo"), CatalogDiscovery::PolarisAuto);
    // Unknown -> safe default (static).
    assert_eq!(CatalogDiscovery::parse("nonsense"), CatalogDiscovery::Static);
    assert_eq!(CatalogDiscovery::default(), CatalogDiscovery::Static);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sqe-core catalog_discovery_parses_and_defaults`
Expected: FAIL — `cannot find type CatalogDiscovery`.

- [ ] **Step 3: Add the enum**

Insert after the `QueryEngine` enum/impl (around line 90, `impl QueryEngine` block end) in `config.rs`:

```rust
/// How the coordinator resolves a catalog name that is not statically
/// declared in `[catalogs.*]`. `Static` (default) errors on an unknown
/// 3-part identifier. `PolarisAuto` lazily probes Polaris for a warehouse
/// of that name using the caller's bearer (see the catalog-discovery design).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogDiscovery {
    #[default]
    Static,
    PolarisAuto,
}

impl CatalogDiscovery {
    /// Parse from a config string; unknown values fall back to `Static`
    /// (fail-closed — discovery is opt-in).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "polaris-auto" => Self::PolarisAuto,
            _ => Self::Static,
        }
    }
}
```

- [ ] **Step 4: Add the QueryConfig field**

In `pub struct QueryConfig` (after `default_catalog`, ~line 116) add:

```rust
    /// How catalogs not declared in `[catalogs.*]` resolve. `"static"`
    /// (default) errors on an unknown 3-part identifier; `"polaris-auto"`
    /// lazily probes Polaris for a warehouse of that name with the caller's
    /// bearer. See `docs/superpowers/specs/2026-05-29-polaris-catalog-discovery-design.md`.
    #[serde(default)]
    pub catalog_discovery: CatalogDiscovery,
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test -p sqe-core catalog_discovery_parses_and_defaults`
Expected: PASS.

- [ ] **Step 6: Confirm existing config tests still pass**

Run: `cargo test -p sqe-core`
Expected: all pass (the `#[serde(default)]` keeps every existing TOML valid).

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat(config): add [query] catalog_discovery (static|polaris-auto)"
```

---

## Task 2: Extract `build_catalog_provider` helper (shared construction)

**Goal:** The static loop (`session_context.rs:213-263`) and discovery must build a `SqeCatalogProvider` identically. Extract the per-catalog body into one async helper so they cannot drift.

**Files:**
- Modify: `crates/sqe-coordinator/src/session_context.rs` (extract helper; call it from the existing loop)

- [ ] **Step 1: Add the helper function**

Add a module-level async fn in `session_context.rs` (above `create_session_context`). This is the exact body of the current loop, parameterized:

```rust
/// Build one `SqeCatalogProvider` for `cat_cfg` exactly as the per-session
/// loop does: resolve the per-catalog bearer, pick storage, open the
/// `SessionCatalog`, wrap it with policy, and apply metrics + scan knobs.
/// Shared by the static registration loop and dynamic catalog discovery so
/// the two paths produce identical providers. Returns the provider plus the
/// `SessionCatalog` (the caller decides whether it is the primary).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_catalog_provider(
    cat_cfg: &sqe_core::config::CatalogConfig,
    session: &sqe_auth::Session,
    global_storage: &sqe_core::config::StorageConfig,
    prefetch_concurrency: usize,
    table_cache: Option<&sqe_catalog::TableMetadataCache>,
    policy_store: Option<&std::sync::Arc<dyn sqe_policy::PolicyStore>>,
    prom_metrics: Option<&std::sync::Arc<sqe_metrics::MetricsRegistry>>,
) -> Result<(SqeCatalogProvider, std::sync::Arc<sqe_catalog::SessionCatalog>), std::sync::Arc<sqe_core::SqeError>> {
    use std::sync::Arc;
    let auth = cat_cfg.auth.clone().unwrap_or_default();
    let bearer = sqe_auth::per_catalog::resolve_bearer(&auth, session.access_token().expose())
        .await
        .map_err(Arc::new)?;
    let storage = cat_cfg.storage.clone().unwrap_or_else(|| global_storage.clone());

    let session_catalog = Arc::new(
        sqe_catalog::SessionCatalog::for_session_with(cat_cfg, &storage, table_cache.cloned(), &bearer)
            .await
            .map_err(Arc::new)?,
    );

    let mut catalog_provider = SqeCatalogProvider::try_new_with_policy(
        session_catalog.clone(),
        storage.clone(),
        cat_cfg.warehouse.clone(),
        policy_store.cloned(),
        Some(session.user.clone()),
    )
    .await
    .map_err(Arc::new)?;
    if let Some(m) = prom_metrics {
        catalog_provider = catalog_provider.with_metrics(Arc::clone(m));
    }
    let small_file_threshold_bytes = cat_cfg.small_file_threshold_mb.saturating_mul(1024 * 1024);
    catalog_provider = catalog_provider.with_small_file_threshold(small_file_threshold_bytes);
    catalog_provider = catalog_provider.with_manifest_concurrency(cat_cfg.manifest_concurrency);
    catalog_provider = catalog_provider.with_prefetch_concurrency(prefetch_concurrency);

    Ok((catalog_provider, session_catalog))
}
```

> Adjust the `Result` error type / import paths to match the file's existing aliases (the loop currently maps to `Arc<...>` via `map_err(Arc::new)` — keep that exact error type). If `sqe_policy::PolicyStore` / `sqe_metrics::MetricsRegistry` are imported under different names in this file, use those.

- [ ] **Step 2: Replace the loop body with a call to the helper**

In `create_session_context`, replace lines ~213-262 (the per-catalog construction) with:

```rust
            for (cat_name, cat_cfg) in &flattened {
                let (catalog_provider, session_catalog) = build_catalog_provider(
                    cat_cfg,
                    session,
                    &global_storage,
                    config.storage.prefetch_concurrency,
                    table_cache,
                    policy_store,
                    prom_metrics,
                )
                .await?;
                ctx.register_catalog(cat_name, std::sync::Arc::new(catalog_provider));
                if primary_session_catalog.is_none() {
                    primary_session_catalog = Some(session_catalog);
                }
            }
```

> Match the exact names/types of `table_cache`, `policy_store`, `prom_metrics` as they appear in `create_session_context` (they were passed as `table_cache.cloned()`, `policy_store.cloned()`, `Arc::clone(m)` in the original — pass references and let the helper clone).

- [ ] **Step 3: Build**

Run: `cargo build -p sqe-coordinator`
Expected: compiles.

- [ ] **Step 4: Run the existing session/catalog tests (behavior unchanged)**

Run: `cargo test -p sqe-coordinator --features test-sqlite`
Expected: PASS (this is a pure refactor; existing tests cover the construction).

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/session_context.rs
git commit -m "refactor(coordinator): extract build_catalog_provider helper"
```

---

## Task 3: Discovery resolver + template selection

**Goal:** A function that, given the session + an unknown warehouse name, builds and returns a `SqeCatalogProvider` for it using a template `CatalogConfig`, or `None` if it cannot be resolved.

**Files:**
- Modify: `crates/sqe-coordinator/src/session_context.rs` (add `discover_catalog_provider` + `discovery_template`)

- [ ] **Step 1: Add the template selector**

Add to `session_context.rs`:

```rust
/// Pick the `CatalogConfig` to clone as the template for a discovered
/// warehouse: the configured default catalog (`query.default_catalog`) if it
/// names a REST catalog, else the first flattened REST catalog. Returns
/// `None` when no REST catalog is configured (discovery only targets Polaris/
/// REST). Only `warehouse` differs on the clone; `catalog_url` + `auth` +
/// backend are inherited.
pub(crate) fn discovery_template(config: &sqe_core::SqeConfig) -> Option<sqe_core::config::CatalogConfig> {
    let flattened = config.flattened_catalogs();
    let pick = config
        .query
        .default_catalog
        .as_deref()
        .and_then(|name| flattened.iter().find(|(n, _)| n == name))
        .or_else(|| {
            flattened
                .iter()
                .find(|(_, c)| matches!(c.backend, sqe_core::config::CatalogBackend::Rest))
        })
        .or_else(|| flattened.first());
    pick.map(|(_, c)| (*c).clone())
        .filter(|c| matches!(c.backend, sqe_core::config::CatalogBackend::Rest))
}
```

- [ ] **Step 2: Add the resolver**

```rust
/// Attempt to resolve `warehouse` as a Polaris catalog and build its
/// `SqeCatalogProvider` using the discovery template + the caller's bearer.
/// Returns `Ok(None)` (not an error) when discovery is off, no template
/// exists, or Polaris rejects the warehouse (unauthorized / nonexistent) —
/// the caller turns `None` into the existing "unknown catalog" error so an
/// unauthorized warehouse is indistinguishable from a missing one.
pub(crate) async fn discover_catalog_provider(
    warehouse: &str,
    config: &sqe_core::SqeConfig,
    session: &sqe_auth::Session,
    table_cache: Option<&sqe_catalog::TableMetadataCache>,
    policy_store: Option<&std::sync::Arc<dyn sqe_policy::PolicyStore>>,
    prom_metrics: Option<&std::sync::Arc<sqe_metrics::MetricsRegistry>>,
) -> Option<SqeCatalogProvider> {
    if config.query.catalog_discovery != sqe_core::config::CatalogDiscovery::PolarisAuto {
        return None;
    }
    let mut cfg = discovery_template(config)?;
    cfg.warehouse = warehouse.to_string();

    match build_catalog_provider(
        &cfg,
        session,
        &config.storage,
        config.storage.prefetch_concurrency,
        table_cache,
        policy_store,
        prom_metrics,
    )
    .await
    {
        Ok((provider, _)) => Some(provider),
        Err(e) => {
            tracing::info!(
                warehouse,
                error = %e,
                "catalog discovery: Polaris did not resolve warehouse (treated as unknown catalog)"
            );
            None
        }
    }
}
```

- [ ] **Step 3: Build**

Run: `cargo build -p sqe-coordinator`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/session_context.rs
git commit -m "feat(coordinator): catalog discovery resolver + template selection"
```

---

## Task 4: Pre-flight discovery hook

**Goal:** When discovery is on, resolve unknown qualifiers into the caller's session `ctx` before erroring; use `ctx.catalog_names()` as truth so a second reference never re-probes.

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (pre-flight block, lines 563-578)

- [ ] **Step 1: Replace the pre-flight block**

Replace lines 563-578 with:

```rust
        if let Some(stmt) = kind.statement() {
            let qualifiers = sqe_sql::extract_catalog_qualifiers(stmt);
            if !qualifiers.is_empty() {
                // Authority for "known" is the caller's session ctx: it already
                // has every static + attached catalog registered, plus any
                // catalog discovered earlier in this session (so a second
                // reference never re-probes Polaris).
                let (ctx, _) = self.create_session_context(session).await?;
                let mut known: std::collections::HashSet<String> =
                    ctx.catalog_names().into_iter().collect();
                known.insert("system".to_string());
                known.insert("datafusion".to_string());

                for q in &qualifiers {
                    if known.contains(q) {
                        continue;
                    }
                    // Unknown: try discovery (no-op + None unless polaris-auto).
                    if let Some(provider) = crate::session_context::discover_catalog_provider(
                        q,
                        &self.config,
                        session,
                        self.table_cache.as_ref(),
                        self.policy_store.as_ref(),
                        self.prom_metrics.as_ref(),
                    )
                    .await
                    {
                        ctx.register_catalog(q.clone(), std::sync::Arc::new(provider));
                        known.insert(q.clone());
                        tracing::info!(catalog = %q, "catalog discovery: registered Polaris warehouse for session");
                    }
                }

                if let Some(unknown) = qualifiers.iter().find(|q| !known.contains(*q)) {
                    let mut names: Vec<String> = known.into_iter().collect();
                    names.sort();
                    return Err(SqeError::Catalog(format!(
                        "unknown catalog '{}' in 3-part identifier; configured \
                         catalogs are {:?}. Declare it via TOML `[catalogs.<name>]`, \
                         `ATTACH` it, or enable `[query] catalog_discovery = \"polaris-auto\"`.",
                        unknown, names
                    )));
                }
            }
        }
```

> Field access notes: confirm the `QueryHandler` field names for `table_cache`, `policy_store`, `prom_metrics`. They are the same values passed into `create_session_context` elsewhere in this file — grep `self\.` near the existing `create_session_context` call sites (e.g. line 292, 896) to read the exact field names and `.as_ref()` shapes, and match them. If a value is not currently a `QueryHandler` field but is threaded some other way, thread it the same way the existing `create_session_context` call does.

- [ ] **Step 2: Build**

Run: `cargo build -p sqe-coordinator`
Expected: compiles. Fix field-name mismatches per the note above if it does not.

- [ ] **Step 3: Confirm static-mode behavior unchanged with a unit-ish check**

Run: `cargo test -p sqe-coordinator --features test-sqlite`
Expected: PASS. (Discovery is `None` unless `polaris-auto`, so the loop reduces to the old known/unknown check; the error string changed text only.)

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(coordinator): lazy Polaris catalog discovery in pre-flight"
```

---

## Task 5: Integration tests (quickstart Polaris + S3 stack)

**Files:**
- Create: `crates/sqe-coordinator/tests/catalog_discovery_test.rs`

> These mirror the existing integration-test harness. Read `crates/sqe-coordinator/tests/` for the current pattern (how a `QueryHandler` + `Session` are built against the test stack, e.g. `runtime_catalog_test.rs` and the quack/e2e tests) and reuse that scaffolding rather than inventing a new one. Gate behind the same feature/ignore convention those tests use if they require the live stack.

- [ ] **Step 1: Write the tests**

```rust
//! Integration tests for `[query] catalog_discovery = "polaris-auto"`.
//! Requires the quickstart Polaris + S3 stack (docker-compose.test.yml).

// Test 1: lazy-resolve hit — a warehouse with no [catalogs.*] entry resolves.
//   - Create Polaris warehouse `disco_test` out of band (or via the helper the
//     other integration tests use to seed a warehouse).
//   - Build a QueryHandler with config.query.catalog_discovery = PolarisAuto.
//   - Run: SELECT 1 FROM "disco_test"."ns"."tbl"  (against a seeded table)
//   - Assert: Ok, returns rows (not an "unknown catalog" error).
//
// Test 2: miss — a nonexistent warehouse errors.
//   - Run: SELECT 1 FROM "no_such_wh"."ns"."tbl"
//   - Assert: Err contains "unknown catalog".
//
// Test 3: static mode unchanged.
//   - config.query.catalog_discovery = Static (default).
//   - Run: SELECT 1 FROM "disco_test"."ns"."tbl"
//   - Assert: Err contains "unknown catalog" (no probe attempted).
//
// Test 4: in-session reuse — second reference does not re-probe.
//   - PolarisAuto; reference "disco_test" twice in one session.
//   - Assert: both succeed; (optional) assert only one "registered Polaris
//     warehouse" log / one Polaris config call via a counting test catalog.
```

Fill each test body using the existing harness helpers. Each test's concrete setup (seeding `disco_test`, the table, the bearer) follows whatever the neighboring integration tests already do — do not hand-roll a new Polaris client.

- [ ] **Step 2: Run the tests against the stack**

```bash
docker compose -f docker-compose.test.yml up -d
cargo test -p sqe-coordinator --features test-sqlite --test catalog_discovery_test -- --nocapture
```
Expected: all pass (Test 1/4 succeed, Test 2/3 see "unknown catalog").

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/tests/catalog_discovery_test.rs
git commit -m "test(coordinator): catalog discovery integration tests"
```

---

## Task 6: Quickstart config + docs

**Files:**
- Modify: `quickstart/sqe/assets/sqe-config/sqe.toml`
- Modify: `README.md` (roadmap), `nextsteps.md`

- [ ] **Step 1: Enable discovery in the quickstart**

In `quickstart/sqe/assets/sqe-config/sqe.toml`, under `[query]` add:

```toml
[query]
# ... existing keys ...
catalog_discovery = "polaris-auto"
```

(If there is no `[query]` block, add one.)

- [ ] **Step 2: Update roadmap docs**

Mark Polaris dynamic catalog discovery done in `README.md` and shift the NEXT pointer in `nextsteps.md` (per the repo's "After Completing Work" convention).

- [ ] **Step 3: Full gate before PR**

Run: `cargo clippy --all-targets --all-features -- -D warnings`
Run: `cargo test -p sqe-core -p sqe-coordinator --features test-sqlite`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add quickstart/sqe/assets/sqe-config/sqe.toml README.md nextsteps.md
git commit -m "docs(quickstart): enable polaris-auto catalog discovery + roadmap"
```

---

## Self-Review

- **Spec coverage:** config flag (Task 1) ✓; lazy resolve reusing static path (Tasks 2-4) ✓; per-user session scoping (Task 4 registers into `ctx` from `create_session_context`, keyed per user+token) ✓; authz/no-leak via fall-through error (Task 3 `None` + Task 4 error) ✓; caching/drop-out via session ctx + `ctx.catalog_names()` (Task 4) ✓; tests (Task 5) ✓; static default (Task 1) ✓.
- **Placeholder scan:** Task 5 bodies are described, not coded, because they must bind to the existing integration harness — the step explicitly directs reading the neighboring tests; concrete setup helpers are not inventable here without that harness. All production-code steps carry full code.
- **Type consistency:** `build_catalog_provider` (Task 2) is the single constructor called by both the static loop (Task 2 Step 2) and `discover_catalog_provider` (Task 3); `CatalogDiscovery::PolarisAuto` (Task 1) is matched in Task 3; `ctx.catalog_names()` + `ctx.register_catalog` (Task 4) are DataFusion `SessionContext` methods.
- **Field-name caveat:** Tasks 2 and 4 flag that `table_cache` / `policy_store` / `prom_metrics` names must be confirmed against `create_session_context` and the `QueryHandler` struct — the exact bindings live there and the implementer must match them.
