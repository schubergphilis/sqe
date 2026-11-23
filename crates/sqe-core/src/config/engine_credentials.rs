//! Startup checks for the per-catalog engine credential identity (#934).
//!
//! `[catalogs.<name>.credentials]` makes SQE fetch read keys as the engine
//! after the policy engine allows the person `select`. That is only safe when
//! the policy engine denies by default, which only the Ranger backend with
//! catalog-scoped keys does.

use super::{CatalogAuthConfig, CatalogConfig, PolicyEngine, SqeConfig};

impl CatalogAuthConfig {
    /// The `client_credentials` secret, or `None` for other variants. With
    /// `client_secret_file` the file is read on every call, so a rotated file
    /// is used by the next token request without a restart (#937). Only the
    /// trailing line break is dropped. An unreadable or empty file is an error
    /// naming the path, never a fall back to the inline secret.
    pub fn client_secret_value(&self) -> Result<Option<String>, String> {
        let CatalogAuthConfig::ClientCredentials {
            client_secret,
            client_secret_file,
            ..
        } = self
        else {
            return Ok(None);
        };
        match client_secret_file.as_deref() {
            None if client_secret.is_empty() => {
                Err("client_credentials needs client_secret or client_secret_file".to_string())
            }
            None => Ok(Some(client_secret.expose().to_string())),
            Some(_) if !client_secret.is_empty() => {
                Err("set either client_secret or client_secret_file, not both".to_string())
            }
            Some(path) => {
                let raw = std::fs::read_to_string(path)
                    .map_err(|e| format!("client_secret_file {path}: {e}"))?;
                let secret = raw.trim_end_matches(['\n', '\r']);
                if secret.is_empty() {
                    return Err(format!("client_secret_file {path} is empty"));
                }
                Ok(Some(secret.to_string()))
            }
        }
    }
}

impl SqeConfig {
    /// Every `client_credentials` block (`auth` and `credentials`, on
    /// `[catalog]` and each `[catalogs.<name>]`) names exactly one secret
    /// source, and a `client_secret_file` is readable at boot.
    fn validate_client_secret_sources(&self, errors: &mut Vec<String>) {
        let mut blocks: Vec<_> = (self.catalogs.iter())
            .map(|(n, c)| (format!("[catalogs.{n}]"), c))
            .collect();
        blocks.sort_by(|a, b| a.0.cmp(&b.0));
        blocks.insert(0, ("[catalog]".to_string(), &self.catalog));
        for (section, catalog) in blocks {
            for (field, auth) in [
                ("auth", &catalog.auth),
                ("credentials", &catalog.credentials),
            ] {
                let Some(auth) = auth else { continue };
                if let Some(problem) = auth.client_secret_value().err() {
                    errors.push(format!("{section}.{field}: {problem}"));
                }
            }
        }
    }

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
        self.validate_client_secret_sources(errors);
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
            client_secret_file: None,
            scope: None,
        }
    }

    /// A file under the temp dir, unique per test, holding `body`.
    fn secret_file(name: &str, body: &str) -> String {
        let path = std::env::temp_dir().join(format!("sqe-937-{}-{name}", std::process::id()));
        std::fs::write(&path, body).expect("write secret file");
        path.to_string_lossy().into_owned()
    }

    fn from_file(path: &str) -> CatalogAuthConfig {
        CatalogAuthConfig::ClientCredentials {
            token_endpoint: "http://idp/token".into(),
            client_id: "sqe-engine".into(),
            client_secret: "".into(),
            client_secret_file: Some(path.to_string()),
            scope: None,
        }
    }

    #[test]
    fn secret_file_is_read_on_every_call_and_keeps_inner_text() {
        let path = secret_file("reread", "old secret\n");
        let auth = from_file(&path);
        assert_eq!(auth.client_secret_value(), Ok(Some("old secret".into())));
        std::fs::write(&path, "new secret\r\n").expect("rotate");
        assert_eq!(auth.client_secret_value(), Ok(Some("new secret".into())));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unreadable_or_empty_secret_file_is_an_error_naming_the_path() {
        let missing = from_file("/nonexistent/sqe-937-secret");
        let err = missing.client_secret_value().expect_err("missing file");
        assert!(err.contains("/nonexistent/sqe-937-secret"), "{err}");
        let path = secret_file("empty", "\n");
        let err = from_file(&path).client_secret_value().expect_err("empty");
        assert!(err.contains(&path) && err.contains("empty"), "{err}");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn secret_sources_are_exactly_one_in_auth_and_credentials() {
        let mut cfg = config(None, false, PolicyEngine::Passthrough);
        let lake = cfg.catalogs.get_mut("lake").expect("lake");
        lake.auth = Some(CatalogAuthConfig::ClientCredentials {
            token_endpoint: "http://idp/token".into(),
            client_id: "partner".into(),
            client_secret: "inline".into(),
            client_secret_file: Some("/run/secrets/partner".into()),
            scope: None,
        });
        let errs = errors(&cfg);
        assert!(
            errs.iter()
                .any(|e| e.contains("[catalogs.lake].auth") && e.contains("not both")),
            "{errs:?}"
        );

        let lake = cfg.catalogs.get_mut("lake").expect("lake");
        lake.auth = None;
        // Neither source: an empty inline secret and no file.
        lake.credentials = Some(CatalogAuthConfig::ClientCredentials {
            token_endpoint: "http://idp/token".into(),
            client_id: "sqe-engine".into(),
            client_secret: "".into(),
            client_secret_file: None,
            scope: None,
        });
        let errs = errors(&cfg);
        assert!(
            errs.iter()
                .any(|e| e.contains("[catalogs.lake].credentials")
                    && e.contains("needs client_secret or client_secret_file")),
            "{errs:?}"
        );
    }

    #[test]
    fn readable_secret_file_passes_startup_and_missing_one_fails() {
        let path = secret_file("boot", "s3cret");
        let mut cfg = config(Some(from_file(&path)), true, PolicyEngine::Ranger);
        assert!(errors(&cfg).is_empty(), "{:?}", errors(&cfg));
        cfg.catalogs.get_mut("lake").expect("lake").credentials =
            Some(from_file("/nonexistent/sqe-937-boot"));
        let errs = errors(&cfg);
        assert!(
            errs.iter().any(|e| e.contains("/nonexistent/sqe-937-boot")),
            "{errs:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn client_secret_file_deserializes_without_inline_secret() {
        let auth: CatalogAuthConfig = toml::from_str(
            "type = \"client_credentials\"\ntoken_endpoint = \"http://idp/token\"\n\
             client_id = \"sqe-engine\"\nclient_secret_file = \"/run/secrets/engine\"\n",
        )
        .expect("deserialize");
        match auth {
            CatalogAuthConfig::ClientCredentials {
                client_secret,
                client_secret_file,
                ..
            } => {
                assert!(client_secret.is_empty());
                assert_eq!(client_secret_file.as_deref(), Some("/run/secrets/engine"));
            }
            other => panic!("wrong variant: {other:?}"),
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
