//! Engine reads for discovered warehouses (`[query] discovery_engine_read`).
//!
//! With `catalog_discovery = "polaris-auto"`, a warehouse that is not in the
//! configuration is built from a template catalog and read with the person's
//! delegated keys. `discovery_engine_read = "template"` lets such a warehouse
//! read with the template's engine credentials instead, once the coordinator
//! has proved the warehouse is allowlisted, is not a configured name, and
//! resolves to the same catalog for both the person and the engine. The
//! Ranger store then default-denies `select` on every catalog except the
//! configured ones without engine credentials.

use serde::Deserialize;

use super::{
    CatalogAuthConfig, CatalogBackend, CatalogConfig, CatalogDiscovery, PolicyEngine, QueryConfig,
    SqeConfig,
};

/// Whether a discovered warehouse may read with the template's engine
/// credentials. `Off` (the default) reads with the person's delegated keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryEngineRead {
    #[default]
    Off,
    Template,
}

/// `name` matches `pattern`: exact, or with `*` standing for any run of
/// characters (empty included). Case-sensitive.
pub fn warehouse_pattern_matches(pattern: &str, name: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = name.strip_prefix(first) else {
        return false;
    };
    let tail: Vec<&str> = parts.collect();
    let Some((last, middle)) = tail.split_last() else {
        return rest.is_empty();
    };
    for part in middle {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

impl QueryConfig {
    /// `warehouse` is on the `discovery_engine_warehouses` allowlist.
    pub fn discovery_engine_allows(&self, warehouse: &str) -> bool {
        self.discovery_engine_warehouses
            .iter()
            .any(|p| warehouse_pattern_matches(p, warehouse))
    }
}

impl SqeConfig {
    /// The catalog a discovered warehouse is cloned from: the default catalog
    /// when it is REST, else the first REST catalog. `None` without one.
    pub fn discovery_template_catalog(&self) -> Option<(String, &CatalogConfig)> {
        let flattened = self.flattened_catalogs();
        let is_rest = |c: &CatalogConfig| matches!(c.backend, CatalogBackend::Rest);
        let pick = (self.query.default_catalog.as_deref())
            .and_then(|name| flattened.iter().position(|(n, _)| n == name))
            .or_else(|| flattened.iter().position(|(_, c)| is_rest(c)))
            .or((!flattened.is_empty()).then_some(0))?;
        let (name, catalog) = flattened.into_iter().nth(pick)?;
        is_rest(catalog).then_some((name, catalog))
    }

    /// Every name the configuration gives a catalog: config keys, warehouse
    /// aliases and Hive roots. A discovered warehouse with one of these names
    /// never gets the engine identity.
    pub fn configured_catalog_names(&self) -> Vec<String> {
        self.catalog_names_where(|_| true)
    }

    /// The configured names that read without engine credentials, plus the
    /// Hive roots. With `discovery_engine_read = "template"`, the Ranger
    /// store default-denies `select` on every catalog except these (and
    /// SQE's virtual schemas). A subset of [`Self::configured_catalog_names`].
    pub fn discovery_engine_open_catalogs(&self) -> Vec<String> {
        self.catalog_names_where(|c| c.credentials.is_none())
    }

    fn catalog_names_where(&self, keep: impl Fn(&CatalogConfig) -> bool) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        let mut push = |name: &str| {
            if !name.is_empty() && !names.iter().any(|n| n == name) {
                names.push(name.to_string());
            }
        };
        for (key, catalog) in self.flattened_catalogs() {
            if keep(catalog) {
                push(&key);
                push(&catalog.warehouse);
            }
        }
        for root in &self.hive.roots {
            push(&root.name);
        }
        names
    }

    /// Refuse `discovery_engine_read = "template"` unless discovery is on,
    /// the template holds engine `client_credentials` with vended
    /// credentials required, Ranger denies by catalog, and the allowlist is
    /// not empty.
    pub(super) fn validate_discovery_engine(&self, errors: &mut Vec<String>) {
        if self.query.discovery_engine_read == DiscoveryEngineRead::Off {
            return;
        }
        let field = "[query] discovery_engine_read = \"template\"";
        if self.query.catalog_discovery != CatalogDiscovery::PolarisAuto {
            errors.push(format!(
                "{field} needs catalog_discovery = \"polaris-auto\""
            ));
        }
        match self.discovery_template_catalog() {
            None => errors.push(format!("{field} needs a REST template catalog")),
            Some((name, template)) => {
                if !matches!(
                    template.credentials,
                    Some(CatalogAuthConfig::ClientCredentials { .. })
                ) {
                    errors.push(format!(
                        "{field} needs template catalog '{name}' to set credentials \
                         of type \"client_credentials\""
                    ));
                }
                if !template.require_vended_credentials {
                    errors.push(format!(
                        "{field} needs template catalog '{name}' to set \
                         require_vended_credentials = true"
                    ));
                }
            }
        }
        if self.policy.engine != PolicyEngine::Ranger || !self.policy.ranger.catalog_scoped {
            errors.push(format!(
                "{field} needs policy.engine = \"ranger\" with [policy.ranger] \
                 catalog_scoped = true: only then is `select` denied by default"
            ));
        }
        if self.query.discovery_engine_warehouses.is_empty() {
            errors.push(format!(
                "{field} needs a non-empty [query] discovery_engine_warehouses"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_exact_names_and_globs() {
        assert!(warehouse_pattern_matches("ws1", "ws1"));
        assert!(!warehouse_pattern_matches("ws1", "ws10"));
        assert!(!warehouse_pattern_matches("ws1", "WS1"), "case-sensitive");
        assert!(warehouse_pattern_matches("ws-*", "ws-sales"));
        assert!(warehouse_pattern_matches("ws-*", "ws-"));
        assert!(!warehouse_pattern_matches("ws-*", "xws-sales"));
        assert!(warehouse_pattern_matches("*-prod", "sales-prod"));
        assert!(!warehouse_pattern_matches("*-prod", "sales-prod2"));
        assert!(warehouse_pattern_matches("a*b*c", "a-x-b-y-c"));
        assert!(!warehouse_pattern_matches("a*b*c", "a-c"));
        assert!(!warehouse_pattern_matches("ab*ba", "aba"), "no overlap");
        assert!(warehouse_pattern_matches("*", "anything"));
    }

    fn engine() -> CatalogAuthConfig {
        CatalogAuthConfig::ClientCredentials {
            token_endpoint: "http://idp/token".into(),
            client_id: "sqe-engine".into(),
            client_secret: "s".into(),
            client_secret_file: None,
            scope: None,
        }
    }

    /// Template `lake` (first REST catalog, engine credentials), `open`
    /// (warehouse `open_wh`, no credentials), `other` (warehouse alias
    /// `other_wh`, engine credentials) and a Hive root `glue`; every check on.
    fn template_config() -> SqeConfig {
        let mut cfg = SqeConfig::embedded_defaults();
        for (name, warehouse, credentials) in [
            ("lake", "lake", Some(engine())),
            ("open", "open_wh", None),
            ("other", "other_wh", Some(engine())),
        ] {
            let mut cat = cfg.catalog.clone();
            cat.warehouse = warehouse.into();
            cat.require_vended_credentials = credentials.is_some();
            cat.credentials = credentials;
            cfg.catalogs.insert(name.into(), cat);
        }
        cfg.query.catalog_discovery = CatalogDiscovery::PolarisAuto;
        cfg.query.discovery_engine_read = DiscoveryEngineRead::Template;
        cfg.query.discovery_engine_warehouses = vec!["ws-*".into()];
        cfg.policy.engine = PolicyEngine::Ranger;
        cfg.policy.ranger.catalog_scoped = true;
        cfg.hive
            .roots
            .push(toml::from_str("name = \"glue\"\nlocation = \"s3://glue/\"").expect("hive root"));
        cfg
    }

    fn errors(cfg: &SqeConfig) -> Vec<String> {
        let mut errors = Vec::new();
        cfg.validate_discovery_engine(&mut errors);
        errors
    }

    #[test]
    fn default_is_off_and_off_checks_nothing() {
        let cfg = SqeConfig::embedded_defaults();
        assert_eq!(cfg.query.discovery_engine_read, DiscoveryEngineRead::Off);
        assert!(cfg.query.discovery_engine_warehouses.is_empty());
        assert!(errors(&cfg).is_empty());
        let parsed: QueryConfig =
            toml::from_str("discovery_engine_read = \"template\"").expect("parse");
        assert_eq!(parsed.discovery_engine_read, DiscoveryEngineRead::Template);
    }

    #[test]
    fn a_complete_template_config_passes() {
        assert!(
            errors(&template_config()).is_empty(),
            "{:?}",
            errors(&template_config())
        );
    }

    /// What the refusal names, and how the config loses it.
    type Case = (&'static str, fn(&mut SqeConfig));

    #[test]
    fn each_missing_requirement_is_refused() {
        let cases: [Case; 6] = [
            ("polaris-auto", |c| {
                c.query.catalog_discovery = CatalogDiscovery::Static
            }),
            ("client_credentials", |c| {
                c.catalogs.get_mut("lake").expect("lake").credentials = None
            }),
            ("require_vended_credentials", |c| {
                c.catalogs
                    .get_mut("lake")
                    .expect("lake")
                    .require_vended_credentials = false
            }),
            ("policy.engine", |c| {
                c.policy.engine = PolicyEngine::InMemory
            }),
            ("catalog_scoped", |c| c.policy.ranger.catalog_scoped = false),
            ("discovery_engine_warehouses", |c| {
                c.query.discovery_engine_warehouses.clear()
            }),
        ];
        for (needle, break_it) in cases {
            let mut cfg = template_config();
            break_it(&mut cfg);
            let errs = errors(&cfg);
            assert!(
                errs.iter().any(|e| e.contains(needle)),
                "{needle}: {errs:?}"
            );
        }
    }

    #[test]
    fn allowlist_uses_the_glob_rules() {
        let cfg = template_config();
        assert!(cfg.query.discovery_engine_allows("ws-sales"));
        assert!(!cfg.query.discovery_engine_allows("sales"));
        assert!(!cfg.query.discovery_engine_allows("WS-sales"));
    }

    #[test]
    fn open_catalogs_are_configured_names_without_engine_credentials() {
        let cfg = template_config();
        let configured = cfg.configured_catalog_names();
        let open = cfg.discovery_engine_open_catalogs();
        for name in ["lake", "other", "other_wh", "open", "open_wh", "glue"] {
            assert!(configured.contains(&name.to_string()), "{name}");
        }
        assert_eq!(open, ["open", "open_wh", "glue"]);
        // The exception set never names a catalog the proof would accept.
        assert!(open.iter().all(|n| configured.contains(n)));
    }

    #[test]
    fn without_a_default_the_template_is_the_first_rest_catalog() {
        let cfg = template_config();
        let (name, _) = cfg.discovery_template_catalog().expect("template");
        assert_eq!(name, "lake");
    }
}
