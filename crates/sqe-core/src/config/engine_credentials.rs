//! Startup checks for the per-catalog engine credential identity (#934).
//!
//! `[catalogs.<name>.credentials]` makes SQE fetch read keys as the engine
//! after the policy engine allows the person `select`. That is only safe when
//! the policy engine denies by default, which only the Ranger backend with
//! catalog-scoped keys does.

use super::{CatalogAuthConfig, CatalogConfig, PolicyEngine, SqeConfig};

impl SqeConfig {
    /// Names of the catalogs (`[catalog]` is `"default"` only in messages) that
    /// carry an engine credential identity, as `(config section, block)`.
    fn engine_credential_catalogs(&self) -> Vec<(String, &CatalogConfig)> {
        let mut found: Vec<(String, &CatalogConfig)> = self
            .catalogs
            .iter()
            .filter(|(_, c)| c.credentials.is_some())
            .map(|(name, c)| (format!("[catalogs.{name}]"), c))
            .collect();
        found.sort_by(|a, b| a.0.cmp(&b.0));
        if self.catalog.credentials.is_some() {
            found.insert(0, ("[catalog]".to_string(), &self.catalog));
        }
        found
    }

    /// Every SQL name an engine-identity catalog answers to: its config key
    /// and its `warehouse`. The policy store default-denies `select` under
    /// these names. A catalog is also reached by its warehouse alias
    /// (`configured_session_catalog` matches either), and the read gate then
    /// asks under the alias, so listing only the key left the alias open.
    pub fn engine_read_catalog_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for (key, catalog) in self.flattened_catalogs() {
            if catalog.credentials.is_none() {
                continue;
            }
            for name in [key, catalog.warehouse.clone()] {
                if !name.is_empty() && !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    /// Refuse an unsafe engine credential identity: a mode other than
    /// `client_credentials`, a catalog that does not require vended
    /// credentials, a policy backend without default-deny `select`, or Ranger
    /// keys that do not name the catalog.
    pub(super) fn validate_engine_credentials(&self, errors: &mut Vec<String>) {
        let identities = self.engine_credential_catalogs();
        if identities.is_empty() {
            return;
        }
        for (section, catalog) in &identities {
            if !matches!(
                catalog.credentials,
                Some(CatalogAuthConfig::ClientCredentials { .. })
            ) {
                errors.push(format!(
                    "{section}.credentials: only type = \"client_credentials\" is accepted \
                     for the engine identity"
                ));
            }
            if !catalog.require_vended_credentials {
                errors.push(format!(
                    "{section}.credentials needs {section} require_vended_credentials = true \
                     (the engine keys are vended per table)"
                ));
            }
        }
        // `in_memory` has no default-deny `select`; it is a test store and the
        // integration harness builds its own config without `validate()`.
        if self.policy.engine != PolicyEngine::Ranger {
            errors.push(
                "[catalogs.*.credentials] needs policy.engine = \"ranger\": only the Ranger \
                 backend denies `select` by default, so with any other engine every person \
                 with metadata access would read every table"
                    .to_string(),
            );
        } else if !self.policy.ranger.catalog_scoped {
            errors.push(
                "[catalogs.*.credentials] needs [policy.ranger] catalog_scoped = true: bare \
                 policy keys cannot tell catalogs apart, so default deny cannot be limited \
                 to the catalog with the engine identity"
                    .to_string(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> CatalogAuthConfig {
        CatalogAuthConfig::ClientCredentials {
            token_endpoint: "http://idp/token".into(),
            client_id: "sqe-engine".into(),
            client_secret: "s".into(),
            scope: None,
        }
    }

    fn config(
        credentials: Option<CatalogAuthConfig>,
        vended: bool,
        engine: PolicyEngine,
    ) -> SqeConfig {
        let mut cfg = SqeConfig::embedded_defaults();
        let mut cat = cfg.catalog.clone();
        cat.credentials = credentials;
        cat.require_vended_credentials = vended;
        cfg.catalogs.insert("lake".into(), cat);
        cfg.policy.engine = engine;
        cfg.policy.ranger.catalog_scoped = true;
        cfg
    }

    fn errors(cfg: &SqeConfig) -> Vec<String> {
        let mut errors = Vec::new();
        cfg.validate_engine_credentials(&mut errors);
        errors
    }

    #[test]
    fn engine_read_names_cover_key_and_warehouse_alias() {
        let mut cfg = config(Some(identity()), true, PolicyEngine::Ranger);
        cfg.catalogs.get_mut("lake").expect("lake").warehouse = "lake_wh".into();
        let mut open = cfg.catalog.clone();
        open.credentials = None;
        open.warehouse = "open_wh".into();
        cfg.catalogs.insert("open".into(), open);
        let names = cfg.engine_read_catalog_names();
        assert!(names.contains(&"lake".to_string()), "{names:?}");
        assert!(names.contains(&"lake_wh".to_string()), "{names:?}");
        assert!(
            !names.iter().any(|n| n == "open" || n == "open_wh"),
            "{names:?}"
        );
    }

    #[test]
    fn unset_identity_is_never_checked() {
        assert!(errors(&config(None, false, PolicyEngine::Passthrough)).is_empty());
    }

    #[test]
    fn identity_on_ranger_with_scoped_keys_is_accepted() {
        assert!(errors(&config(Some(identity()), true, PolicyEngine::Ranger)).is_empty());
    }

    #[test]
    fn identity_refuses_other_modes() {
        let errs = errors(&config(
            Some(CatalogAuthConfig::SessionBearer),
            true,
            PolicyEngine::Ranger,
        ));
        assert!(
            errs.iter().any(|e| e.contains("client_credentials")),
            "{errs:?}"
        );
    }

    #[test]
    fn identity_needs_vended_credentials() {
        let errs = errors(&config(Some(identity()), false, PolicyEngine::Ranger));
        assert!(
            errs.iter()
                .any(|e| e.contains("require_vended_credentials")),
            "{errs:?}"
        );
    }

    #[test]
    fn identity_refuses_passthrough_and_in_memory_policy() {
        for engine in [PolicyEngine::Passthrough, PolicyEngine::InMemory] {
            let errs = errors(&config(Some(identity()), true, engine));
            assert!(errs.iter().any(|e| e.contains("policy.engine")), "{errs:?}");
        }
    }

    #[test]
    fn identity_needs_catalog_scoped_ranger_keys() {
        let mut cfg = config(Some(identity()), true, PolicyEngine::Ranger);
        cfg.policy.ranger.catalog_scoped = false;
        let errs = errors(&cfg);
        assert!(
            errs.iter().any(|e| e.contains("catalog_scoped")),
            "{errs:?}"
        );
    }
}
