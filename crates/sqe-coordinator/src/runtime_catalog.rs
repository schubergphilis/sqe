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

use datafusion::catalog::CatalogProvider;
use sqe_catalog::mount::build_catalog;
use sqe_catalog::writable_iceberg_catalog::WritableIcebergCatalog;
use sqe_core::SecretStore;
use sqe_sql::{AttachStatement, CatalogKind, OptionValue};
use tracing::info;

/// One attached catalog tracked by the registry.
pub struct AttachedCatalog {
    pub name: String,
    pub kind: CatalogKind,
    /// Underlying iceberg::Catalog used by writes / metadata calls.
    pub catalog: Arc<dyn iceberg::Catalog>,
    /// DataFusion CatalogProvider pre-built at attach time. Cloned
    /// into every new SessionContext via `create_session_context`.
    pub provider: Arc<dyn CatalogProvider>,
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

/// Shorthand alias used internally.
type ProviderList = Vec<(String, Arc<dyn CatalogProvider>)>;

/// Process-local registry of catalogs added by `ATTACH` at runtime.
///
/// Cloning shares the same backing map; safe to thread through
/// `QueryHandler` and `EmbeddedClient` alike.
#[derive(Default, Clone)]
pub struct RuntimeCatalogRegistry {
    inner: Arc<RwLock<HashMap<String, AttachedCatalog>>>,
    /// The Hive lane. See the "Hive lane" section below for why this is a
    /// second map rather than a second field on `AttachedCatalog`.
    hive_roots: Arc<RwLock<HashMap<String, sqe_catalog::hive::source::HiveRoot>>>,
}

impl RuntimeCatalogRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a catalog from the AST and store it in the registry.
    ///
    /// The built `WritableIcebergCatalog` provider is stored alongside
    /// the raw `iceberg::Catalog` so that `create_session_context` can
    /// register it into every fresh `SessionContext` without re-connecting
    /// to the backend. Errors if a catalog with the same name is already
    /// attached, or if the underlying builder rejects the options.
    pub async fn attach(
        &self,
        stmt: &AttachStatement,
        secrets: &SecretStore,
    ) -> Result<(), String> {
        // 1) Refuse duplicate name (case-sensitive). Checks BOTH lanes: the
        //    two share one SQL catalog-name namespace (see the "Hive lane"
        //    section below), and checking only `self.inner` here would let
        //    `ATTACH ... TYPE iceberg_rest` steal a name an attached Hive
        //    root already holds. `attach_hive_root` already checks this
        //    direction; this is the other half of that symmetry.
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
        {
            let hive = self
                .hive_roots
                .read()
                .map_err(|_| "hive registry poisoned".to_string())?;
            if hive.contains_key(&stmt.name) {
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
        let provider: Arc<dyn CatalogProvider> = Arc::new(
            WritableIcebergCatalog::try_new(catalog.clone())
                .await
                .map_err(|e| {
                    format!("failed to wrap catalog '{}' for DataFusion: {e}", stmt.name)
                })?,
        );

        // 4) Capture the optional SECRET reference so DROP SECRET can
        //    refuse to drop a secret while a catalog depends on it.
        let secret_ref = stmt.options.get("SECRET").and_then(|v| match v {
            OptionValue::SecretRef(s) => Some(s.clone()),
            _ => None,
        });

        // 5) Insert under write lock.
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
                    provider,
                    secret_ref,
                },
            );
        }

        info!(catalog = %stmt.name, kind = %stmt.kind.name(), "Catalog attached at runtime");
        Ok(())
    }

    /// Detach a catalog by name. Errors if the name is unknown.
    ///
    /// Removes the entry from the registry. `create_session_context`
    /// invalidation is the caller's responsibility; the next rebuilt
    /// `SessionContext` will not re-register this catalog because it is
    /// no longer in `providers()`.
    pub fn detach(&self, name: &str) -> Result<(), String> {
        let mut w = self
            .inner
            .write()
            .map_err(|_| "registry poisoned".to_string())?;
        if w.remove(name).is_none() {
            return Err(format!("catalog '{name}' is not attached"));
        }
        info!(catalog = %name, "Catalog detached at runtime");
        Ok(())
    }

    /// Names of all currently-attached catalogs, sorted.
    pub fn list(&self) -> Result<Vec<String>, String> {
        let r = self
            .inner
            .read()
            .map_err(|_| "registry poisoned".to_string())?;
        let mut names: Vec<_> = r.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    /// All attached `(name, provider)` pairs, sorted by name.
    ///
    /// Called by `create_session_context` to inject every attached
    /// catalog into a freshly built `SessionContext`.
    pub fn providers(&self) -> Result<ProviderList, String> {
        let r = self
            .inner
            .read()
            .map_err(|_| "registry poisoned".to_string())?;
        let mut pairs: ProviderList = r
            .values()
            .map(|c| (c.name.clone(), Arc::clone(&c.provider)))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }

    /// Names of attached catalogs that reference the given secret.
    /// Used by `DROP SECRET` to enforce the in-use guard.
    pub fn referenced_secrets(&self, secret_name: &str) -> Result<Vec<String>, String> {
        let r = self
            .inner
            .read()
            .map_err(|_| "registry poisoned".to_string())?;
        Ok(r.values()
            .filter(|c| c.secret_ref.as_deref() == Some(secret_name))
            .map(|c| c.name.clone())
            .collect())
    }

    // -----------------------------------------------------------------------
    // Hive lane
    //
    // A Hive root's `HiveCatalogProvider` is rebuilt PER SESSION by
    // `create_session_context`. NOT because each session has its own
    // `RuntimeEnv` -- production shares one across every session, so that
    // is not the reason -- but because the provider's data-store factory
    // embeds that session's `TvfCaller`, and `TvfPolicy::check_path` gates
    // every table load on it (see `mount.rs`'s module doc comment above
    // `build_hive_root` for the full case). A provider built once and
    // reused across sessions would carry one session's caller into every
    // other session's table loads. Only the underlying `HiveManifestStore`
    // is session-independent, so that is all this lane stores -- but
    // "session-independent" does not mean caller-free. `HiveManifestStore`
    // (`hive/store.rs`) DOES carry a `TvfCaller` (its `caller` field, used
    // in `put`/`get`'s `check_path` calls): the caller of whichever session
    // ran the `ATTACH` (or `trusted_startup_config()` for a `[[hive.roots]]`
    // entry), baked in once and shared by every later session's reads. For
    // a config-attached root that caller is `trusted`, so `check_path`'s
    // object-store branch never evaluates `allowed_object_store_prefixes`
    // at all (`file://`/`http(s)://` are still gated); for an
    // `ATTACH`-created root it is the attaching session's real caller, so
    // every later reader's manifest load is authorized against whoever
    // attached, not against themselves -- fail-closed (a false denial for
    // a more-privileged later reader), not an escalation.
    //
    // It is a second map, not a second
    // `AttachedCatalog` field, because the two kinds cannot share one
    // representation: an Iceberg entry's `provider` is exactly the thing
    // every session reuses unchanged, and a Hive entry's `store` is
    // exactly the thing no session may reuse the provider built from.
    //
    // Both lanes share one SQL catalog-name namespace, so attach/detach on
    // either checks the other for a collision.

    /// Attach a Hive-style external-table warehouse root's manifest store
    /// under `name`. Errors if `name` is already attached, in either lane.
    pub fn attach_hive_root(
        &self,
        name: &str,
        root: sqe_catalog::hive::source::HiveRoot,
    ) -> Result<(), String> {
        {
            let iceberg = self
                .inner
                .read()
                .map_err(|_| "registry poisoned".to_string())?;
            if iceberg.contains_key(name) {
                return Err(format!(
                    "catalog '{name}' is already attached; DETACH it first"
                ));
            }
        }
        let mut hive = self
            .hive_roots
            .write()
            .map_err(|_| "hive registry poisoned".to_string())?;
        if hive.contains_key(name) {
            return Err(format!(
                "catalog '{name}' is already attached; DETACH it first"
            ));
        }
        hive.insert(name.to_string(), root);
        info!(catalog = %name, kind = "hive_external", "Catalog attached at runtime");
        Ok(())
    }

    /// Detach a Hive root by name. Errors if the name is not an attached
    /// Hive root (including when it names an attached Iceberg catalog
    /// instead -- callers distinguish the two lanes with `is_hive_root`
    /// before choosing which detach to call).
    pub fn detach_hive_root(&self, name: &str) -> Result<(), String> {
        let mut hive = self
            .hive_roots
            .write()
            .map_err(|_| "hive registry poisoned".to_string())?;
        if hive.remove(name).is_none() {
            return Err(format!("catalog '{name}' is not attached"));
        }
        info!(catalog = %name, "Catalog detached at runtime");
        Ok(())
    }

    /// Whether `name` is an attached Hive root. Used by `DETACH` dispatch
    /// to pick the right lane, and by callers that need to tell a Hive
    /// catalog name apart from an Iceberg one without a full policy-key
    /// resolution.
    pub fn is_hive_root(&self, name: &str) -> Result<bool, String> {
        let hive = self
            .hive_roots
            .read()
            .map_err(|_| "hive registry poisoned".to_string())?;
        Ok(hive.contains_key(name))
    }

    /// All attached `(name, store)` pairs, sorted by name.
    ///
    /// Called by `create_session_context` to build and register a fresh,
    /// session-scoped `HiveCatalogProvider` over each root for every
    /// `SessionContext` it builds -- the Hive-lane counterpart of
    /// `providers()` above, which registers the Iceberg lane's providers
    /// as-is instead of rebuilding them.
    pub fn hive_roots(
        &self,
    ) -> Result<Vec<(String, sqe_catalog::hive::source::HiveRoot)>, String> {
        let hive = self
            .hive_roots
            .read()
            .map_err(|_| "hive registry poisoned".to_string())?;
        let mut pairs: Vec<_> = hive
            .iter()
            .map(|(name, root)| (name.clone(), root.clone()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }
}

/// Attach every `[[hive.roots]]` configured root at coordinator startup,
/// mirroring the `[catalogs.*]` static-config loop for Iceberg catalogs
/// (`create_session_context`'s `flattened` loop). Shared with the
/// `sqe-server` binary (`crates/sqe-server/src/main.rs`) so they cannot
/// drift on this the way their `hive_catalogs`/`hive_manifest_cache`
/// constructions above them are already (independently) duplicated.
///
/// `TvfCaller::trusted_startup_config()` is required, not incidental:
/// `build_hive_root`'s `check_path` gate is fail-closed by default and
/// there is no session/user identity yet at startup to build a `for_user`
/// caller from. An operator's own TOML is trusted the same way the
/// embedded CLI trusts `--hive-root`; a local-path root still needs
/// `[storage.tvf] allow_local_paths = true` regardless, since `trusted`
/// only bypasses the object-store branch of `check_path` (see that
/// function's own comment).
///
/// Also inserts each attached name into `hive_catalogs`, the security
/// registry `resolve_policy_key` reads live: a root missing from it would
/// key every table under the Iceberg namespace convention instead (#7's
/// escalation, reached from the config-file door instead of `ATTACH`).
///
/// `default_catalog` must be `config.resolve_default_catalog()`. A root
/// named the same as the session's default catalog is refused outright
/// (see the name check below): `resolve_policy_key` (`sqe-policy`) and
/// `CatalogRoutedTagSource` (`hive_tag_source.rs`) both dispatch on
/// `table_ref.catalog()` alone, which is `None` for a two-part reference
/// (`sales.orders`) -- DataFusion preserves the AS-WRITTEN reference in
/// the scan, it does not fill in the resolved default catalog. A two-part
/// reference under a Hive-named default catalog would therefore take the
/// Iceberg branch of BOTH functions regardless of what the reference
/// actually resolves against, either denying every row (no tag-source
/// cache entry -> `lit(false)`) or, worse, inheriting whatever grants
/// exist for a same-named Iceberg namespace. Refusing the name collision
/// here closes both symptoms at the one place a two-part reference could
/// ever reach a Hive table this way, without touching either dispatch
/// site (see the report for why promoting the reference inside the
/// policy path is deliberately deferred, not overlooked).
pub async fn attach_configured_hive_roots(
    hive_config: &sqe_core::config::HiveConfig,
    storage: &sqe_core::config::StorageConfig,
    registry: &RuntimeCatalogRegistry,
    hive_catalogs: &sqe_policy::plan_rewriter::HiveCatalogs,
    default_catalog: &str,
) -> Result<(), String> {
    let limits = sqe_catalog::hive::manifest::ManifestLimits {
        max_bytes: hive_config.manifest_max_bytes,
        max_columns: hive_config.manifest_max_columns,
        max_partition_keys: hive_config.manifest_max_partition_keys,
        max_partition_index_entries: hive_config.manifest_max_partition_index_entries,
    };
    let metadata_timeout = (hive_config.metadata_timeout_secs > 0)
        .then(|| std::time::Duration::from_secs(hive_config.metadata_timeout_secs));
    for root in &hive_config.roots {
        if root.name == default_catalog {
            return Err(format!(
                "[[hive.roots]] '{}' collides with the session default catalog '{default_catalog}': \
                 a two-part table reference under this default resolves to a Hive table without \
                 either the policy key or the tag route ever seeing its catalog name, so a root \
                 cannot share a name with the default catalog. Rename the root or set \
                 query.default_catalog to something else",
                root.name
            ));
        }
        let attached = match root.kind {
            sqe_core::config::HiveRootKind::Sidecar => {
                let store = sqe_catalog::mount::build_hive_root(
                    &root.location,
                    &std::collections::BTreeMap::new(),
                    storage,
                    limits,
                    sqe_core::config::TvfCaller::trusted_startup_config(),
                )
                .await
                .map_err(|e| format!("[[hive.roots]] {}={}: {e}", root.name, root.location))?;
                sqe_catalog::hive::source::HiveRoot::from_store(store)
            }
            sqe_core::config::HiveRootKind::Glue => {
                build_glue_hive_root(root, limits, metadata_timeout).await?
            }
        };
        // Every metadata read on the root is cut at `metadata_timeout`
        // (issue #666); zero disables the bound.
        let attached = match metadata_timeout {
            Some(t) => attached.with_metadata_timeout(t),
            None => attached,
        };
        registry.attach_hive_root(&root.name, attached)?;
        hive_catalogs.insert(&root.name);
    }
    Ok(())
}

/// A `kind = "glue"` root. Feature-gated because the Glue client is a large
/// dependency the slim build deliberately omits; without it the operator
/// gets a message naming the feature rather than a root that silently
/// resolves nothing.
#[cfg(feature = "glue")]
async fn build_glue_hive_root(
    root: &sqe_core::config::HiveRootConfig,
    limits: sqe_catalog::hive::manifest::ManifestLimits,
    timeout: Option<std::time::Duration>,
) -> Result<sqe_catalog::hive::source::HiveRoot, String> {
    let source = sqe_catalog::mount::build_glue_root(
        root.region.as_deref(),
        root.catalog_id.clone(),
        root.databases.clone(),
        limits,
        timeout,
    )
    .await;
    Ok(sqe_catalog::hive::source::HiveRoot::from_source(source))
}

#[cfg(not(feature = "glue"))]
async fn build_glue_hive_root(
    root: &sqe_core::config::HiveRootConfig,
    _limits: sqe_catalog::hive::manifest::ManifestLimits,
    _timeout: Option<std::time::Duration>,
) -> Result<sqe_catalog::hive::source::HiveRoot, String> {
    Err(format!(
        "[[hive.roots]] '{}' sets kind = \"glue\", which needs the `glue` cargo feature on \
         sqe-catalog (build with --features glue)",
        root.name
    ))
}

#[cfg(test)]
mod hive_lane_tests {
    use super::*;

    async fn local_hive_store(
        dir: &std::path::Path,
    ) -> Arc<sqe_catalog::hive::store::HiveManifestStore> {
        let mut storage = sqe_core::config::StorageConfig::default();
        storage.tvf.allow_local_paths = true;
        sqe_catalog::mount::build_hive_root(
            &format!("file://{}", dir.display()),
            &std::collections::BTreeMap::new(),
            &storage,
            sqe_catalog::hive::manifest::ManifestLimits::default(),
            sqe_core::config::TvfCaller::trusted_local(),
        )
        .await
        .expect("build_hive_root")
    }

    #[tokio::test]
    async fn attach_hive_root_then_lists_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = RuntimeCatalogRegistry::new();
        let store = local_hive_store(dir.path()).await;

        registry.attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store)).expect("attach");

        assert!(registry.is_hive_root("lake").expect("is_hive_root"));
        let roots = registry.hive_roots().expect("hive_roots");
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].0, "lake");
    }

    #[tokio::test]
    async fn attach_hive_root_rejects_duplicate_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = RuntimeCatalogRegistry::new();
        let store = local_hive_store(dir.path()).await;
        registry
            .attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(Arc::clone(&store)))
            .expect("first attach");

        let err = registry
            .attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store))
            .expect_err("duplicate attach must fail");
        assert!(err.contains("already attached"), "{err}");
    }

    /// Minimal `iceberg::Catalog` stub so the cross-lane collision test below
    /// can populate the Iceberg lane without a real backend (no SQLite
    /// engine feature, no network). Every method is unreachable: the test only
    /// exercises registry bookkeeping, never catalog behavior.
    #[derive(Debug)]
    struct UnreachableCatalog;

    #[async_trait::async_trait]
    impl iceberg::Catalog for UnreachableCatalog {
        async fn list_namespaces(
            &self,
            _parent: Option<&iceberg::NamespaceIdent>,
        ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
            unimplemented!("test stub")
        }
        async fn create_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!("test stub")
        }
        async fn get_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            unimplemented!("test stub")
        }
        async fn namespace_exists(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<bool> {
            unimplemented!("test stub")
        }
        async fn update_namespace(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unimplemented!("test stub")
        }
        async fn drop_namespace(&self, _namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
            unimplemented!("test stub")
        }
        async fn list_tables(
            &self,
            _namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
            unimplemented!("test stub")
        }
        async fn create_table(
            &self,
            _namespace: &iceberg::NamespaceIdent,
            _creation: iceberg::TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            unimplemented!("test stub")
        }
        async fn load_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<iceberg::table::Table> {
            unimplemented!("test stub")
        }
        async fn drop_table(&self, _table: &iceberg::TableIdent) -> iceberg::Result<()> {
            unimplemented!("test stub")
        }
        async fn table_exists(&self, _table: &iceberg::TableIdent) -> iceberg::Result<bool> {
            unimplemented!("test stub")
        }
        async fn rename_table(
            &self,
            _src: &iceberg::TableIdent,
            _dest: &iceberg::TableIdent,
        ) -> iceberg::Result<()> {
            unimplemented!("test stub")
        }
        async fn register_table(
            &self,
            _table: &iceberg::TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            unimplemented!("test stub")
        }
        async fn update_table(&self, _commit: iceberg::TableCommit) -> iceberg::Result<iceberg::table::Table> {
            unimplemented!("test stub")
        }
    }

    /// The other half of the symmetry: `attach()` (the Iceberg lane) must
    /// also refuse a name an attached Hive root already holds, not just the
    /// reverse (`attach_hive_root_rejects_name_already_attached_as_iceberg`
    /// below). Before this check existed, `ATTACH ... AS lake (TYPE
    /// iceberg_rest)` would succeed while `lake` was an attached Hive root
    /// -- self-consistent rather than an escalation (the Hive provider
    /// still wins DataFusion's catalog registration, and `hive_catalogs`
    /// still keys those tables Hive-side), but only by accident of which
    /// registration loop runs last, which a later refactor could silently
    /// reverse.
    ///
    /// `TYPE sqlite` needs a SQLite engine feature (`sql-sqlite-rs` or
    /// `sql-sqlite`) to actually build a
    /// catalog, but the collision check runs before `build_catalog` is
    /// ever called, so this test does not need that feature: it proves the
    /// check fires, not that a sqlite catalog can be built.
    #[tokio::test]
    async fn attach_rejects_name_already_attached_as_hive_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = RuntimeCatalogRegistry::new();
        let store = local_hive_store(dir.path()).await;
        registry
            .attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store))
            .expect("hive attach");

        let secrets = sqe_core::SecretStore::new();
        let attach_stmt = sqe_sql::AttachStatement {
            name: "lake".to_string(),
            location: "sqlite://:memory:".to_string(),
            kind: sqe_sql::CatalogKind::Sqlite,
            options: std::collections::BTreeMap::new(),
        };
        let err = registry
            .attach(&attach_stmt, &secrets)
            .await
            .expect_err("must not shadow an attached hive root");
        assert!(err.contains("already attached"), "{err}");
    }

    /// The two lanes share one SQL catalog-name namespace: a Hive root
    /// cannot steal a name an Iceberg `ATTACH` already holds, and (proven
    /// separately by `detach_hive_root_then_forgets_it` below) neither can
    /// `DETACH` cross the boundary and silently no-op the wrong lane.
    ///
    /// Populates the Iceberg lane directly (module-private field access)
    /// rather than through `attach()`, which would need a real backend
    /// (a SQLite engine feature, or network for a REST/Glue/HMS catalog) --
    /// this test cares about the name-collision check, not catalog
    /// construction.
    #[tokio::test]
    async fn attach_hive_root_rejects_name_already_attached_as_iceberg() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = RuntimeCatalogRegistry::new();
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(UnreachableCatalog);
        let provider: Arc<dyn CatalogProvider> = Arc::new(
            datafusion::catalog::MemoryCatalogProvider::new(),
        );
        registry.inner.write().unwrap().insert(
            "lake".to_string(),
            AttachedCatalog {
                name: "lake".to_string(),
                kind: sqe_sql::CatalogKind::IcebergRest,
                catalog: iceberg_catalog,
                provider,
                secret_ref: None,
            },
        );

        let store = local_hive_store(dir.path()).await;
        let err = registry
            .attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store))
            .expect_err("must not shadow an attached iceberg catalog");
        assert!(err.contains("already attached"), "{err}");
    }

    #[tokio::test]
    async fn detach_hive_root_then_forgets_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let registry = RuntimeCatalogRegistry::new();
        let store = local_hive_store(dir.path()).await;
        registry.attach_hive_root("lake", sqe_catalog::hive::source::HiveRoot::from_store(store)).expect("attach");

        registry.detach_hive_root("lake").expect("detach");

        assert!(!registry.is_hive_root("lake").expect("is_hive_root"));
        assert!(registry.hive_roots().expect("hive_roots").is_empty());
    }

    #[tokio::test]
    async fn detach_hive_root_unknown_name_errors() {
        let registry = RuntimeCatalogRegistry::new();
        let err = registry
            .detach_hive_root("nope")
            .expect_err("unknown name must error");
        assert!(err.contains("not attached"), "{err}");
    }

    /// Proves the actual startup wiring this task adds: `[[hive.roots]]`
    /// entries land in the registry AND in `hive_catalogs` (the live set
    /// `resolve_policy_key` reads), not just one or the other.
    #[tokio::test]
    async fn attach_configured_hive_roots_populates_both_registries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut storage = sqe_core::config::StorageConfig::default();
        storage.tvf.allow_local_paths = true;
        let hive_config = sqe_core::config::HiveConfig {
            roots: vec![sqe_core::config::HiveRootConfig {
                kind: Default::default(),
                region: None,
                catalog_id: None,
                databases: Vec::new(),
                name: "lake".to_string(),
                location: format!("file://{}", dir.path().display()),
                secret: None,
            }],
            ..sqe_core::config::HiveConfig::default()
        };
        let registry = RuntimeCatalogRegistry::new();
        let hive_catalogs = sqe_policy::plan_rewriter::HiveCatalogs::new();

        attach_configured_hive_roots(&hive_config, &storage, &registry, &hive_catalogs, "iceberg")
            .await
            .expect("attach_configured_hive_roots");

        assert!(registry.is_hive_root("lake").expect("is_hive_root"));
        assert!(hive_catalogs.contains_name(Some("lake")));
    }

    /// The collision this task's security review found: a root named the
    /// same as the session default catalog must be refused, naming both,
    /// rather than attached silently. Mutation-checked: removing the
    /// `root.name == default_catalog` check made this test fail (the
    /// attach succeeded).
    #[tokio::test]
    async fn attach_configured_hive_roots_refuses_a_name_matching_the_default_catalog() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut storage = sqe_core::config::StorageConfig::default();
        storage.tvf.allow_local_paths = true;
        let hive_config = sqe_core::config::HiveConfig {
            roots: vec![sqe_core::config::HiveRootConfig {
                kind: Default::default(),
                region: None,
                catalog_id: None,
                databases: Vec::new(),
                name: "lake".to_string(),
                location: format!("file://{}", dir.path().display()),
                secret: None,
            }],
            ..sqe_core::config::HiveConfig::default()
        };
        let registry = RuntimeCatalogRegistry::new();
        let hive_catalogs = sqe_policy::plan_rewriter::HiveCatalogs::new();

        let err = attach_configured_hive_roots(&hive_config, &storage, &registry, &hive_catalogs, "lake")
            .await
            .expect_err("a root named like the default catalog must be refused");
        assert!(err.contains("lake"), "{err}");
        assert!(!registry.is_hive_root("lake").unwrap_or(false));
        assert!(!hive_catalogs.contains_name(Some("lake")));
    }

    /// An unreachable location (no `allow_local_paths`, so `check_path`
    /// fails closed) must fail the whole startup loop with a message that
    /// names the offending root, not silently skip it or panic.
    #[tokio::test]
    async fn attach_configured_hive_roots_fails_closed_on_a_bad_root() {
        let hive_config = sqe_core::config::HiveConfig {
            roots: vec![sqe_core::config::HiveRootConfig {
                kind: Default::default(),
                region: None,
                catalog_id: None,
                databases: Vec::new(),
                name: "lake".to_string(),
                location: "/no/such/warehouse".to_string(),
                secret: None,
            }],
            ..sqe_core::config::HiveConfig::default()
        };
        let registry = RuntimeCatalogRegistry::new();
        let hive_catalogs = sqe_policy::plan_rewriter::HiveCatalogs::new();

        let err = attach_configured_hive_roots(
            &hive_config,
            &sqe_core::config::StorageConfig::default(),
            &registry,
            &hive_catalogs,
            "iceberg",
        )
        .await
        .expect_err("local paths are disabled by default");
        assert!(err.contains("lake"), "{err}");
        assert!(!hive_catalogs.contains_name(Some("lake")));
    }
}
