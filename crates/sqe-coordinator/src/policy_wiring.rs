//! AUTH-01: build the policy enforcer + store from `config.policy.engine`.
//!
//! Shared by both coordinator binaries (`main.rs`, `bin/sqe_server.rs`) so the
//! enforcement wiring cannot drift between them. Returns the enforcer that the
//! query pipeline runs AND the same `Arc<dyn PolicyStore>` so GRANT/REVOKE can
//! invalidate its cache.

use std::sync::Arc;

use sqe_catalog::grant_chameleon::ChameleonGrantBackend;
use sqe_core::config::{PolicyConfig, PolicyEngine};
use sqe_core::SqeConfig;
use sqe_policy::grants::{polaris::PolarisGrantBackend, ranger::RangerGrantBackend, GrantBackend};
use sqe_policy::plan_rewriter::PolicyPlanRewriter;
use sqe_policy::{PassthroughEnforcer, PolicyEnforcer, PolicyStore};

/// Construct the policy enforcer and (optionally) the backing store.
/// Passthrough returns `(PassthroughEnforcer, None)`.
///
/// `table_cache` is the global `TableMetadataCache`. When `Some`, a
/// `CacheTagSource` is wired into the rewriter so tag-based column masks and
/// row filters are resolved from Iceberg table properties. When `None` (or
/// `Passthrough` engine), `NoopTagSource` is used (no tag masking).
///
/// `metrics`, when present, is attached to the Ranger store so policy resolve
/// latency, cache hit/miss, and circuit-breaker state are exported. Pass `None`
/// in tests that do not serve metrics.
#[allow(clippy::type_complexity)]
pub fn build_policy_enforcer(
    config: &PolicyConfig,
    table_cache: Option<sqe_catalog::TableMetadataCache>,
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
) -> anyhow::Result<(Arc<dyn PolicyEnforcer>, Option<Arc<dyn PolicyStore>>)> {
    let mask_key: Option<Arc<Vec<u8>>> = if config.mask_key.is_empty() {
        None
    } else {
        Some(Arc::new(config.mask_key.as_bytes().to_vec()))
    };

    let store: Option<Arc<dyn PolicyStore>> = match config.engine {
        PolicyEngine::Passthrough => None,
        PolicyEngine::InMemory => {
            Some(Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new()))
        }
        PolicyEngine::Ranger => {
            let rc = &config.ranger;
            if rc.url.is_empty() {
                anyhow::bail!("policy.engine = ranger requires policy.ranger.url");
            }
            // Issue #37 (non-breaking): a Ranger MASK_HASH column mask falls back
            // to plain unsalted SHA-256 when no `policy.mask_key` is set. That is
            // brute-forceable via rainbow tables on low-entropy columns (SSN,
            // phone, small enums). Warn at startup and recommend a key; we do NOT
            // default-deny Hash, since that would break existing deployments that
            // rely on the unkeyed behaviour. Setting a key upgrades Hash to HMAC.
            if config.mask_key.is_empty() {
                tracing::warn!(
                    "policy.engine = ranger with no policy.mask_key: MASK_HASH column \
                     masks fall back to UNSALTED SHA-256, which is brute-forceable on \
                     low-entropy columns (issue #37). Set policy.mask_key to upgrade \
                     Hash masks to keyed HMAC."
                );
            }
            let store = sqe_policy::ranger_store::RangerStore::from_config(rc)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let store = match &metrics {
                Some(m) => store.with_metrics(m.clone()),
                None => store,
            };
            Some(Arc::new(store))
        }
    };

    match store {
        None => Ok((Arc::new(PassthroughEnforcer), None)),
        Some(store) => {
            let mut rewriter = PolicyPlanRewriter::new(store.clone())
                .with_mask_key(mask_key)
                .with_mask_precedence(config.mask_precedence);

            // Wire the tag source. `CacheTagSource` reads `sqe.column-tags`
            // table properties from the shared metadata cache with zero extra
            // network calls. When no cache is available (e.g. in-process tests
            // that construct a rewriter without a full coordinator), fall back to
            // `NoopTagSource` (already the default; this block is explicit for
            // clarity).
            if let Some(cache) = table_cache {
                let tag_src = Arc::new(crate::tag_source_impl::CacheTagSource::new(
                    Arc::new(cache),
                ));
                rewriter = rewriter.with_tag_source(tag_src);
            }
            // else: NoopTagSource stays (set in PolicyPlanRewriter::new).

            Ok((Arc::new(rewriter), Some(store)))
        }
    }
}

/// Construct the GRANT/REVOKE backend from `config.access_control`.
///
/// Shared by both coordinator binaries (`main.rs`, `bin/sqe_server.rs`) and by
/// the access-control e2e test, for the same reason `build_policy_enforcer` is
/// shared: three copies of this wiring would drift.
pub fn build_grant_backend(
    config: &SqeConfig,
) -> anyhow::Result<Option<Arc<dyn GrantBackend>>> {
    use sqe_core::config::AccessControlBackend;
    match config.access_control.backend {
        AccessControlBackend::Chameleon if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "chameleon",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            let client = Arc::new(sqe_catalog::AccessControlClient::new(
                &config.access_control.url,
            )?);
            Ok(Some(Arc::new(ChameleonGrantBackend::new(client))))
        }
        AccessControlBackend::Polaris if !config.access_control.url.is_empty() => {
            tracing::info!(
                backend = "polaris",
                url = %config.access_control.url,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(PolarisGrantBackend::new(
                &config.access_control.url,
                config.access_control.client_id.clone(),
                config.access_control.client_secret.clone(),
            )?)))
        }
        AccessControlBackend::Ranger if !config.access_control.url.is_empty() => {
            let r = &config.access_control.ranger;
            tracing::info!(
                backend = "ranger",
                url = %config.access_control.url,
                service = %r.service_name,
                "Access control backend configured"
            );
            Ok(Some(Arc::new(RangerGrantBackend::new(
                &config.access_control.url,
                &r.service_name,
                &r.admin_user,
                r.admin_password.expose(),
                &r.realm,
                r.timeout_secs,
                r.accept_invalid_certs,
                config.access_control.grant_authority,
            )?)))
        }
        AccessControlBackend::None
        | AccessControlBackend::Chameleon
        | AccessControlBackend::Polaris
        | AccessControlBackend::Ranger => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::{PolicyConfig, PolicyEngine};

    #[test]
    fn passthrough_yields_no_store() {
        let config = PolicyConfig::default();
        let (_enforcer, store) = build_policy_enforcer(&config, None, None).unwrap();
        assert!(store.is_none());
    }

    #[test]
    fn ranger_without_url_errors() {
        let config = PolicyConfig {
            engine: PolicyEngine::Ranger,
            ..Default::default()
        };
        assert!(build_policy_enforcer(&config, None, None).is_err());
    }

    #[test]
    fn in_memory_yields_store() {
        let config = PolicyConfig {
            engine: PolicyEngine::InMemory,
            ..Default::default()
        };
        let (_enforcer, store) = build_policy_enforcer(&config, None, None).unwrap();
        assert!(store.is_some());
    }

    /// Fix 5 (non-breaking): Ranger + empty mask_key must still BUILD (the build
    /// emits a startup warning recommending a key, but does NOT default-deny
    /// Hash, which would break existing deployments).
    #[test]
    fn ranger_with_empty_mask_key_still_builds() {
        let mut config = PolicyConfig {
            engine: PolicyEngine::Ranger,
            ..Default::default()
        };
        config.ranger.url = "http://ranger.example:6080".to_string();
        config.mask_key = String::new();
        let result = build_policy_enforcer(&config, None, None);
        assert!(
            result.is_ok(),
            "ranger + empty mask_key must build (warn, not reject): {:?}",
            result.err()
        );
    }

    // ── build_grant_backend ────────────────────────────────────────────────
    // These two run in the DEFAULT suite (`cargo test -p sqe-coordinator
    // --lib`), so they must pass with nothing listening on port 26080.
    // `RangerGrantBackend::new` only builds a reqwest client and copies
    // strings (crates/sqe-policy/src/grants/ranger.rs:190): no I/O.

    const RANGER_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59997"

[access_control]
backend = "ranger"
url = "http://localhost:26080"

[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
realm = "*"
"#;

    const PASSTHROUGH_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59997"
"#;

    #[test]
    fn ranger_config_yields_a_grant_backend() {
        let config: sqe_core::SqeConfig = toml::from_str(RANGER_TOML).expect("parse ranger toml");
        let backend = build_grant_backend(&config).expect("build ranger grant backend");
        assert!(
            backend.is_some(),
            "access_control.backend = ranger with a url must yield a grant backend"
        );
    }

    #[test]
    fn no_access_control_config_yields_no_backend() {
        let config: sqe_core::SqeConfig =
            toml::from_str(PASSTHROUGH_TOML).expect("parse passthrough toml");
        let backend = build_grant_backend(&config).expect("build passthrough grant backend");
        assert!(
            backend.is_none(),
            "no access_control backend configured must yield None, not a live client"
        );
    }

    /// `opa` and `cedar` are GONE, not merely unwired.
    ///
    /// They were config values from an earlier design that never shipped: OPA
    /// had a `PolicyStore` implementation nothing ever constructed, and Cedar
    /// had no implementation at all. Both used to parse and then fail at
    /// startup. Removing the variants makes the bad state unrepresentable
    /// instead of representable-and-rejected, and moves the error from "your
    /// coordinator will not boot" to "your config file is wrong", which is a
    /// better place for it.
    ///
    /// The error must name what IS valid, because an operator who typed `opa`
    /// needs to know the answer is `ranger`, not just that `opa` failed.
    #[test]
    fn opa_and_cedar_are_not_valid_policy_engines() {
        for gone in ["opa", "cedar", "Cedar", "OPA"] {
            let err = gone
                .parse::<PolicyEngine>()
                .expect_err("a removed engine must not parse");
            assert!(
                err.contains("ranger") && err.contains("in-memory"),
                "the error must name the engines that DO exist, got: {err}"
            );
        }
        // The negative control: the engines that remain still parse, so the
        // test is not passing because everything fails.
        for ok in ["passthrough", "in-memory", "ranger"] {
            assert!(ok.parse::<PolicyEngine>().is_ok(), "{ok} must still parse");
        }
    }

    /// `ranger` without a URL is a misconfiguration, not a reason to enforce
    /// nothing. Same argument as above.
    #[test]
    fn ranger_without_a_url_is_rejected() {
        let config = PolicyConfig {
            engine: PolicyEngine::Ranger,
            ..Default::default()
        };
        let msg = match build_policy_enforcer(&config, None, None) {
            Ok(_) => panic!("ranger with an empty url must be rejected"),
            Err(e) => format!("{e}"),
        };
        assert!(msg.contains("policy.ranger.url"), "got: {msg}");
    }

    /// Fix 1: attaching a metrics registry to the Ranger store must not break
    /// construction (the store is wired via `with_metrics`).
    #[test]
    fn ranger_accepts_metrics_registry() {
        let mut config = PolicyConfig {
            engine: PolicyEngine::Ranger,
            ..Default::default()
        };
        config.ranger.url = "http://ranger.example:6080".to_string();
        let metrics = std::sync::Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let (_enforcer, store) =
            build_policy_enforcer(&config, None, Some(metrics)).unwrap();
        assert!(store.is_some());
    }
}
