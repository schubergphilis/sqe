//! Catalogs read with the engine's keys (`[catalogs.*] credentials`, #934).
//! Their tables default-deny `select`: only an explicit Ranger access grant
//! lets a person read, so a table nobody wrote a policy for answers with no
//! rows instead of the engine's keys.

use std::collections::HashSet;

use sqe_core::system_catalog::{is_virtual_system_ref, DATAFUSION_CATALOG};

use super::RangerStore;

/// Which catalogs default-deny `select`.
#[derive(Debug, Clone)]
pub enum EngineCatalogs {
    /// Only these names: the configured catalogs with engine credentials.
    Listed(HashSet<String>),
    /// Every catalog except these names (configured catalogs without engine
    /// credentials, their warehouse aliases and the configured Hive roots)
    /// and SQE's virtual schemas. Used when discovered warehouses may read with the
    /// engine's keys (`[query] discovery_engine_read = "template"`): any name
    /// a caller can spell is denied from startup, before a warehouse is ever
    /// discovered.
    AllExcept(HashSet<String>),
}

impl Default for EngineCatalogs {
    fn default() -> Self {
        Self::Listed(HashSet::new())
    }
}

impl EngineCatalogs {
    /// The mode name for the startup log.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Listed(_) => "listed",
            Self::AllExcept(_) => "all-except",
        }
    }
}

/// The policy namespace is one of SQE's virtual schemas, served in memory
/// and never by a catalog: `datafusion`, `system`'s reserved schemas, and
/// `information_schema` (bare, or under any catalog). Any other `system.*`
/// namespace belongs to a Polaris catalog named `system` and is not virtual.
/// Catalog and `information_schema` names match exactly, as DataFusion
/// resolves them: a quoted differently cased name is a real catalog or
/// namespace, so it stays governed.
fn is_virtual_policy_namespace(first: &str, second: Option<&str>) -> bool {
    const INFO_SCHEMA: &str = "information_schema";
    first == DATAFUSION_CATALOG
        || first == INFO_SCHEMA
        || second.is_some_and(|s| s == INFO_SCHEMA || is_virtual_system_ref(first, s))
}

impl RangerStore {
    /// Set the catalogs that default-deny `select`. Needs `catalog_scoped`,
    /// whose policy keys start with the catalog name; startup validation
    /// requires it.
    #[must_use = "with_engine_catalogs consumes self; bind the returned store"]
    pub fn with_engine_catalogs(mut self, catalogs: EngineCatalogs) -> Self {
        self.engine_catalogs = catalogs;
        self
    }

    /// Whether a table whose policy namespace is `namespace` sits under an
    /// engine-keyed catalog. Without `catalog_scoped` the first component is
    /// a namespace, not a catalog, so it never matches.
    pub(super) fn under_engine_catalog(&self, namespace: &str) -> bool {
        if !self.catalog_scoped {
            return false;
        }
        let mut parts = namespace.split('.');
        let first = parts.next().unwrap_or_default();
        match &self.engine_catalogs {
            EngineCatalogs::Listed(names) => names.contains(first),
            // Only the Hive roots in `open` (those from config) are exempt.
            // A root ATTACHed at runtime is not: its name could equal a
            // discovered warehouse that reads with the engine's keys.
            EngineCatalogs::AllExcept(open) => {
                !(open.contains(first) || is_virtual_policy_namespace(first, parts.next()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::resolve_from_bundle_with;
    use super::super::tests::{access_bundle, user};
    use super::{EngineCatalogs, RangerStore};
    use sqe_core::config::RangerPolicyConfig;

    fn names(list: &[&str]) -> std::collections::HashSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn store_with(
        catalog_scoped: bool,
        hive_access_policies: bool,
        catalogs: EngineCatalogs,
    ) -> RangerStore {
        RangerStore::from_config(&RangerPolicyConfig {
            url: "http://ranger:6080".into(),
            catalog_scoped,
            hive_access_policies,
            ..RangerPolicyConfig::default()
        })
        .unwrap()
        .with_engine_catalogs(catalogs)
    }

    fn engine_store(catalog_scoped: bool, hive_access_policies: bool) -> RangerStore {
        store_with(
            catalog_scoped,
            hive_access_policies,
            EngineCatalogs::Listed(names(&["lake"])),
        )
    }

    /// `AllExcept` with `iceberg` and its warehouse alias `iceberg_wh` open.
    fn all_except_store() -> RangerStore {
        store_with(
            true,
            false,
            EngineCatalogs::AllExcept(names(&["iceberg", "iceberg_wh"])),
        )
    }

    #[test]
    fn engine_catalog_tables_always_need_a_grant() {
        let store = engine_store(true, true);
        assert!(store.requires_access_grant("lake.sales"), "engine catalog");
        assert!(store.requires_access_grant("lake.a.b"), "nested namespace");
        assert!(
            !store.requires_access_grant("other.sales"),
            "other catalog stays open"
        );
        // The default-deny does not hang on the Hive switch.
        assert!(engine_store(true, false).requires_access_grant("lake.sales"));
    }

    #[test]
    fn engine_default_deny_needs_catalog_scoped_keys() {
        // Without catalog_scoped the first component is a namespace, not the
        // catalog, so it must not be mistaken for one.
        assert!(!engine_store(false, true).requires_access_grant("lake.sales"));
        let unscoped = store_with(false, false, EngineCatalogs::AllExcept(names(&[])));
        assert!(!unscoped.requires_access_grant("ws1.sales"));
    }

    #[test]
    fn an_engine_catalog_table_with_no_grant_reads_as_denied() {
        let policy = resolve_from_bundle_with(
            &access_bundle(),
            &user("nobody", &[]),
            "orders",
            "lake.sales",
            engine_store(true, true).requires_access_grant("lake.sales"),
            true,
        );
        assert!(crate::plan_rewriter::policy_denies_all(&policy));
    }

    #[test]
    fn all_except_denies_an_unlisted_and_a_differently_spelled_name() {
        let store = all_except_store();
        assert!(store.requires_access_grant("ws1.sales"), "discovered");
        assert!(store.requires_access_grant("ws1.a.b"), "nested namespace");
        assert!(store.requires_access_grant("ICEBERG.sales"), "case differs");
        assert!(store.requires_access_grant("iceberg-x.sales"));
        assert!(store.requires_access_grant(""), "empty key");
    }

    #[test]
    fn all_except_leaves_configured_catalogs_without_credentials_open() {
        let store = all_except_store();
        assert!(!store.requires_access_grant("iceberg.sales"));
        assert!(!store.requires_access_grant("iceberg_wh.sales"), "alias");
    }

    #[test]
    fn all_except_opens_configured_hive_roots_but_not_attached_ones() {
        let roots = std::sync::Arc::new(crate::plan_rewriter::HiveCatalogs::new());
        roots.insert("glue");
        roots.insert("attached");
        let open = EngineCatalogs::AllExcept(names(&["glue"]));
        let store =
            store_with(true, false, open.clone()).with_hive_catalogs(std::sync::Arc::clone(&roots));
        assert!(!store.requires_access_grant("glue.db"), "configured root");
        assert!(store.requires_access_grant("attached.db"), "runtime root");
        let gated = store_with(true, true, open).with_hive_catalogs(roots);
        assert!(gated.requires_access_grant("glue.db"), "Hive switch on");
    }

    #[test]
    fn all_except_never_needs_a_grant_for_each_reserved_system_schema() {
        let store = all_except_store();
        for schema in sqe_core::system_catalog::RESERVED_SYSTEM_SCHEMAS {
            assert!(
                !store.requires_access_grant(&format!("system.{schema}")),
                "system.{schema}"
            );
        }
        assert!(
            !store.requires_access_grant("system.RUNTIME"),
            "schema case"
        );
    }

    #[test]
    fn all_except_still_governs_the_polaris_part_of_system() {
        let store = all_except_store();
        assert!(
            store.requires_access_grant("system.sales"),
            "Polaris system"
        );
        assert!(store.requires_access_grant("system"), "no schema");
        assert!(
            store.requires_access_grant("SYSTEM.runtime"),
            "only the exact catalog name is virtual"
        );
    }

    #[test]
    fn all_except_never_needs_a_grant_for_datafusion() {
        let store = all_except_store();
        assert!(!store.requires_access_grant("datafusion.public"));
        assert!(store.requires_access_grant("DataFusion.public"), "case");
    }

    #[test]
    fn all_except_never_needs_a_grant_for_information_schema() {
        let store = all_except_store();
        assert!(!store.requires_access_grant("ws1.information_schema"));
        assert!(!store.requires_access_grant("information_schema"), "bare");
        assert!(store.requires_access_grant("ws1.sales.information_schema"));
        assert!(
            store.requires_access_grant("ws1.INFORMATION_SCHEMA"),
            "case"
        );
    }

    #[test]
    fn engine_catalog_mode_names_the_variant() {
        assert_eq!(EngineCatalogs::default().mode(), "listed");
        assert_eq!(EngineCatalogs::AllExcept(names(&[])).mode(), "all-except");
    }
}
