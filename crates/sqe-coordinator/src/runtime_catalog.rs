//! Runtime catalog registry for SQL `ATTACH` / `DETACH`.
//!
//! Catalogs registered here are process-local: they survive across
//! queries within the same process but are wiped on restart.
//! Operators that want persistence keep using the TOML
//! `[catalogs.*]` config; ATTACH is additive, not a replacement.
//!
//! Spec: `docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md` §5.2
//!
//! ## DataFusion catalog-list deregistration
//!
//! DataFusion 53.1's `CatalogProviderList` trait has `register_catalog`
//! but no `deregister_catalog`. To keep `SHOW CATALOGS` consistent with
//! `DETACH`, this module downcasts the session's catalog list to
//! `MemoryCatalogProviderList` (the default backing store) and
//! removes the entry directly from its inner `DashMap`. If the
//! session has wrapped the list in something else (`DynamicFileCatalog`
//! from `enable_url_table`, or a custom wrapper), the downcast fails
//! and the catalog stays visible in `SHOW CATALOGS` even though our
//! registry forgets it. The registry is the source of truth; query
//! resolution against a detached catalog will surface the right error
//! once Phase E wires the registry into the SQL pipeline.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::execution::context::SessionContext;
use sqe_catalog::mount::build_catalog;
use sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog;
use sqe_core::SecretStore;
use sqe_sql::{AttachStatement, CatalogKind, OptionValue};
use tracing::{info, warn};

/// One attached catalog tracked by the registry.
pub struct AttachedCatalog {
    pub name: String,
    pub kind: CatalogKind,
    /// Underlying iceberg::Catalog used by writes / metadata calls.
    pub catalog: Arc<dyn iceberg::Catalog>,
    /// Secret name referenced via `SECRET <name>` in ATTACH options.
    /// Tracked so DROP SECRET can refuse to drop a secret while
    /// catalogs depend on it.
    pub secret_ref: Option<String>,
}

impl std::fmt::Debug for AttachedCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachedCatalog")
            .field("name", &self.name)
            .field("kind", &self.kind.name())
            .field("secret_ref", &self.secret_ref)
            .finish_non_exhaustive()
    }
}

/// Process-local registry of catalogs added by `ATTACH` at runtime.
///
/// Cloning shares the same backing map; safe to thread through
/// `QueryHandler` and `EmbeddedClient` alike.
#[derive(Default, Clone)]
pub struct RuntimeCatalogRegistry {
    inner: Arc<RwLock<HashMap<String, AttachedCatalog>>>,
}

impl RuntimeCatalogRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a catalog from the AST and register it with the live
    /// `SessionContext`. Errors if a catalog with the same name is
    /// already attached, or if the underlying builder rejects the
    /// options.
    ///
    /// On success the catalog appears in `SHOW CATALOGS` and can be
    /// referenced with three-part SQL identifiers
    /// (`<name>.<schema>.<table>`).
    pub async fn attach(
        &self,
        stmt: &AttachStatement,
        secrets: &SecretStore,
        ctx: &SessionContext,
    ) -> Result<(), String> {
        // 1) Refuse duplicate name (case-sensitive). The check is
        //    redundant with DataFusion's silent overwrite, but a
        //    pre-flight error is friendlier than a surprise.
        {
            let r = self
                .inner
                .read()
                .map_err(|_| "registry poisoned".to_string())?;
            if r.contains_key(&stmt.name) {
                return Err(format!(
                    "catalog '{}' is already attached; DETACH it first",
                    stmt.name
                ));
            }
        }

        // 2) Build the iceberg::Catalog via the per-backend dispatch.
        let catalog = build_catalog(&stmt.location, stmt.kind, &stmt.options, secrets).await?;

        // 3) Wrap in WritableIcebergCatalog so DataFusion can run
        //    CREATE SCHEMA / CREATE TABLE through its normal pipeline.
        let provider = WritableIcebergCatalog::try_new(catalog.clone())
            .await
            .map_err(|e| format!("failed to wrap catalog '{}' for DataFusion: {e}", stmt.name))?;

        // 4) Register with the SessionContext. DataFusion replaces
        //    any existing registration silently; we already guarded
        //    against duplicates above so a successful insert is the
        //    truth source.
        ctx.register_catalog(stmt.name.clone(), Arc::new(provider));

        // 5) Capture the optional SECRET reference so DROP SECRET can
        //    refuse to drop a secret while a catalog depends on it.
        let secret_ref = stmt.options.get("SECRET").and_then(|v| match v {
            OptionValue::SecretRef(s) => Some(s.clone()),
            _ => None,
        });

        // 6) Insert under write lock.
        {
            let mut w = self
                .inner
                .write()
                .map_err(|_| "registry poisoned".to_string())?;
            w.insert(
                stmt.name.clone(),
                AttachedCatalog {
                    name: stmt.name.clone(),
                    kind: stmt.kind,
                    catalog,
                    secret_ref,
                },
            );
        }

        info!(catalog = %stmt.name, kind = %stmt.kind.name(), "Catalog attached at runtime");
        Ok(())
    }

    /// Detach a catalog by name. Errors if the name is unknown.
    ///
    /// On success the catalog is removed from this registry and, when
    /// the `SessionContext`'s catalog list is the default
    /// `MemoryCatalogProviderList`, removed from DataFusion too. If
    /// the list has been wrapped (e.g. by `enable_url_table()`), the
    /// DataFusion-side entry survives but our registry no longer
    /// considers the name attached.
    pub fn detach(&self, name: &str, ctx: &SessionContext) -> Result<(), String> {
        // 1) Remove from our own map first so the registry view of
        //    the world is consistent even if the DataFusion-side
        //    cleanup fails downstream.
        {
            let mut w = self
                .inner
                .write()
                .map_err(|_| "registry poisoned".to_string())?;
            if w.remove(name).is_none() {
                return Err(format!("catalog '{name}' is not attached"));
            }
        }

        // 2) Best-effort cleanup of DataFusion's catalog list.
        let removed_in_df = remove_from_session_context(ctx, name);
        if removed_in_df {
            info!(catalog = %name, "Catalog detached at runtime");
        } else {
            warn!(
                catalog = %name,
                "registry detached the catalog but the DataFusion catalog list could not be \
                 modified (probably wrapped); SHOW CATALOGS may still list it until the session \
                 is rebuilt"
            );
        }
        Ok(())
    }

    /// Names of all currently-attached catalogs, sorted.
    pub fn list(&self) -> Vec<String> {
        let r = self.inner.read().expect("registry poisoned");
        let mut names: Vec<_> = r.keys().cloned().collect();
        names.sort();
        names
    }

    /// Names of attached catalogs that reference the given secret.
    /// Used by `DROP SECRET` to enforce the in-use guard.
    pub fn referenced_secrets(&self, secret_name: &str) -> Vec<String> {
        let r = self.inner.read().expect("registry poisoned");
        r.values()
            .filter(|c| c.secret_ref.as_deref() == Some(secret_name))
            .map(|c| c.name.clone())
            .collect()
    }
}

/// Try to remove a catalog entry from the `SessionContext`'s catalog
/// list. Returns `true` on success.
///
/// DataFusion 53.1 has no `deregister_catalog` on the trait, so this
/// helper downcasts to the default `MemoryCatalogProviderList` and
/// removes from its inner `DashMap` directly. Any wrapper around the
/// list (`DynamicFileCatalog`, custom wrappers) defeats the downcast
/// and the function returns `false`. Callers treat that as a soft
/// failure: the registry is the authoritative source of truth.
fn remove_from_session_context(ctx: &SessionContext, name: &str) -> bool {
    let state = ctx.state();
    let list = state.catalog_list();
    if let Some(memlist) = list.as_any().downcast_ref::<MemoryCatalogProviderList>() {
        memlist.catalogs.remove(name).is_some()
    } else {
        false
    }
}
