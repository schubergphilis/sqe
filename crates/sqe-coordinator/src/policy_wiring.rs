//! AUTH-01: build the policy enforcer + store from `config.policy.engine`.
//!
//! Shared by both coordinator binaries (`main.rs`, `bin/sqe_server.rs`) so the
//! enforcement wiring cannot drift between them. Returns the enforcer that the
//! query pipeline runs AND the same `Arc<dyn PolicyStore>` so GRANT/REVOKE can
//! invalidate its cache.

use std::sync::Arc;

use sqe_core::config::{PolicyConfig, PolicyEngine};
use sqe_policy::plan_rewriter::PolicyPlanRewriter;
use sqe_policy::{PassthroughEnforcer, PolicyEnforcer, PolicyStore};

/// Construct the policy enforcer and (optionally) the backing store.
/// Passthrough returns `(PassthroughEnforcer, None)`.
///
/// `table_cache` is the global `TableMetadataCache`. When `Some`, a
/// `CacheTagSource` is wired into the rewriter so tag-based column masks and
/// row filters are resolved from Iceberg table properties. When `None` (or
/// `Passthrough` engine), `NoopTagSource` is used (no tag masking).
#[allow(clippy::type_complexity)]
pub fn build_policy_enforcer(
    config: &PolicyConfig,
    table_cache: Option<sqe_catalog::TableMetadataCache>,
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
        PolicyEngine::Opa => {
            anyhow::bail!(
                "policy.engine = opa selected but OPA wiring is not part of this change; \
                 use ranger or in-memory"
            )
        }
        PolicyEngine::Cedar => {
            anyhow::bail!("policy.engine = cedar is not implemented")
        }
        PolicyEngine::Ranger => {
            let rc = &config.ranger;
            if rc.url.is_empty() {
                anyhow::bail!("policy.engine = ranger requires policy.ranger.url");
            }
            let store = sqe_policy::ranger_store::RangerStore::from_config(rc)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Some(Arc::new(store))
        }
    };

    match store {
        None => Ok((Arc::new(PassthroughEnforcer), None)),
        Some(store) => {
            let mut rewriter = PolicyPlanRewriter::new(store.clone()).with_mask_key(mask_key);

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

#[cfg(test)]
mod tests {
    use super::*;
    use sqe_core::config::{PolicyConfig, PolicyEngine};

    #[test]
    fn passthrough_yields_no_store() {
        let config = PolicyConfig::default();
        let (_enforcer, store) = build_policy_enforcer(&config, None).unwrap();
        assert!(store.is_none());
    }

    #[test]
    fn ranger_without_url_errors() {
        let config = PolicyConfig {
            engine: PolicyEngine::Ranger,
            ..Default::default()
        };
        assert!(build_policy_enforcer(&config, None).is_err());
    }

    #[test]
    fn in_memory_yields_store() {
        let config = PolicyConfig {
            engine: PolicyEngine::InMemory,
            ..Default::default()
        };
        let (_enforcer, store) = build_policy_enforcer(&config, None).unwrap();
        assert!(store.is_some());
    }
}
