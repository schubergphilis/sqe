//! Finding 902 follow-up: governing engines expose a policy fingerprint so
//! the coordinator can keep the materialized result cache on and bind every
//! entry to the policy state it was produced under.

use std::sync::Arc;

use sqe_policy::plan_rewriter::PolicyPlanRewriter;
use sqe_policy::policy_store::InMemoryPolicyStore;
use sqe_policy::{PassthroughEnforcer, PolicyEnforcer, PolicyStore, ResolvedPolicy};

#[test]
fn passthrough_has_a_constant_fingerprint_and_permits_the_cache() {
    let p = PassthroughEnforcer;
    assert!(p.permits_result_cache());
    assert_eq!(p.policy_fingerprint().as_deref(), Some("passthrough"));
}

#[tokio::test]
async fn in_memory_store_fingerprint_changes_on_every_mutation() {
    let store = Arc::new(InMemoryPolicyStore::new());
    let before = store
        .policy_fingerprint()
        .expect("in-memory store can fingerprint");
    store
        .add_table_policy("hr", "employees", ResolvedPolicy::default())
        .await;
    let after_table = store.policy_fingerprint().unwrap();
    assert_ne!(before, after_table);
    store
        .add_role_policy("analyst", ResolvedPolicy::default())
        .await;
    let after_role = store.policy_fingerprint().unwrap();
    assert_ne!(after_table, after_role);

    // The rewriter derives its fingerprint from the store, so a policy change
    // invalidates every cached result produced before it.
    let rewriter = PolicyPlanRewriter::new(store.clone() as Arc<dyn PolicyStore>);
    assert!(!rewriter.permits_result_cache());
    let fp1 = rewriter.policy_fingerprint().unwrap();
    assert!(fp1.starts_with(&format!("{after_role}|")), "{fp1}");
    store
        .add_role_policy("auditor", ResolvedPolicy::default())
        .await;
    assert_ne!(rewriter.policy_fingerprint().unwrap(), fp1);
}

/// A store that cannot vouch for its policy state keeps the cache off.
struct OpaqueStore;

#[async_trait::async_trait]
impl PolicyStore for OpaqueStore {
    async fn resolve(
        &self,
        _user: &sqe_core::SessionUser,
        _table_name: &str,
        _namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        Ok(ResolvedPolicy::default())
    }
}

#[test]
fn stores_without_a_fingerprint_keep_the_result_cache_off() {
    let rewriter = PolicyPlanRewriter::new(Arc::new(OpaqueStore));
    assert!(rewriter.policy_fingerprint().is_none());
    assert!(!rewriter.permits_result_cache());
}
