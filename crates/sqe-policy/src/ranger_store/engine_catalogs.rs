//! Catalogs read with the engine's keys (`[catalogs.*] credentials`, #934).
//! Their tables default-deny `select`: only an explicit Ranger access grant
//! lets a person read, so a table nobody wrote a policy for answers with no
//! rows instead of the engine's keys.

use super::RangerStore;

impl RangerStore {
    /// Name the catalogs that read with the engine's keys. Needs
    /// `catalog_scoped`, whose policy keys start with the catalog name;
    /// startup validation requires it.
    #[must_use = "with_engine_catalogs consumes self; bind the returned store"]
    pub fn with_engine_catalogs(mut self, catalogs: impl IntoIterator<Item = String>) -> Self {
        self.engine_catalogs = catalogs.into_iter().collect();
        self
    }

    /// Whether a table whose policy namespace starts with `first` sits under
    /// an engine-keyed catalog. Without `catalog_scoped` the first component
    /// is a namespace, not a catalog, so it never matches.
    pub(super) fn under_engine_catalog(&self, first: Option<&str>) -> bool {
        self.catalog_scoped && first.is_some_and(|c| self.engine_catalogs.contains(c))
    }
}

#[cfg(test)]
mod tests {
    use super::super::resolve_from_bundle_with;
    use super::super::tests::{access_bundle, user};
    use super::RangerStore;
    use sqe_core::config::RangerPolicyConfig;

    fn engine_store(catalog_scoped: bool, hive_access_policies: bool) -> RangerStore {
        RangerStore::from_config(&RangerPolicyConfig {
            url: "http://ranger:6080".into(),
            catalog_scoped,
            hive_access_policies,
            ..RangerPolicyConfig::default()
        })
        .unwrap()
        .with_engine_catalogs(["lake".to_string()])
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
}
