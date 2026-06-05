//! In-memory policy store for testing and development.
//!
//! Allows programmatic registration of policies without an external
//! policy engine. Useful for integration tests and local development.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use sqe_core::SessionUser;

use crate::{PolicyStore, ResolvedPolicy};

/// In-memory policy store backed by a concurrent HashMap.
/// Policies are keyed by (namespace, table_name).
pub struct InMemoryPolicyStore {
    /// Policies keyed by "namespace.table_name"
    policies: Arc<RwLock<HashMap<String, ResolvedPolicy>>>,
    /// Role-based policies keyed by role name, applied to all tables
    role_policies: Arc<RwLock<HashMap<String, ResolvedPolicy>>>,
}

impl InMemoryPolicyStore {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            role_policies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a policy for a specific table.
    pub async fn add_table_policy(
        &self,
        namespace: &str,
        table_name: &str,
        policy: ResolvedPolicy,
    ) {
        let key = format!("{}.{}", namespace, table_name);
        self.policies.write().await.insert(key, policy);
    }

    /// Register a policy for a role (applied to all tables for users with that role).
    pub async fn add_role_policy(&self, role: &str, policy: ResolvedPolicy) {
        self.role_policies
            .write()
            .await
            .insert(role.to_string(), policy);
    }
}

impl Default for InMemoryPolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PolicyStore for InMemoryPolicyStore {
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        let key = format!("{}.{}", namespace, table_name);

        // Check table-specific policies first
        if let Some(policy) = self.policies.read().await.get(&key) {
            return Ok(policy.clone());
        }

        // Check role-based policies. A user may hold several roles; resolving
        // only the first match (the previous behaviour) silently dropped the
        // restrictions of every other role and made the effective policy
        // order-dependent, over-granting access (AUTH-03). Union all matching
        // role policies so the result is the most restrictive combination:
        // union the restricted columns and column masks, and AND the row
        // filters (every matching role's filters all apply).
        let role_policies = self.role_policies.read().await;
        let mut merged = ResolvedPolicy::default();
        let mut matched = false;
        for role in &user.roles {
            if let Some(policy) = role_policies.get(role) {
                matched = true;

                // Union restricted columns (dedup).
                for col in &policy.restricted_columns {
                    if !merged.restricted_columns.contains(col) {
                        merged.restricted_columns.push(col.clone());
                    }
                }

                // Union column masks. On a collision the later role's mask
                // wins; restricting a column entirely still takes precedence
                // downstream (restricted columns are dropped before masks are
                // applied in the plan rewriter).
                for (col, mask) in &policy.column_masks {
                    merged.column_masks.insert(col.clone(), mask.clone());
                }

                // AND the row filters: every matching role's filters apply.
                merged
                    .row_filters
                    .extend(policy.row_filters.iter().cloned());
            }
        }

        if matched {
            return Ok(merged);
        }

        // No policy found — allow all (passthrough)
        Ok(ResolvedPolicy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_policy() {
        let store = InMemoryPolicyStore::new();
        let policy = ResolvedPolicy {
            restricted_columns: vec!["ssn".to_string()],
            ..Default::default()
        };
        store.add_table_policy("hr", "employees", policy).await;

        let user = SessionUser {
            username: "alice".to_string(),
            roles: vec![],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        assert_eq!(resolved.restricted_columns, vec!["ssn"]);
    }

    #[tokio::test]
    async fn test_no_policy_is_passthrough() {
        let store = InMemoryPolicyStore::new();
        let user = SessionUser {
            username: "alice".to_string(),
            roles: vec![],
        };
        let resolved = store.resolve(&user, "orders", "sales").await.unwrap();
        assert!(resolved.row_filters.is_empty());
        assert!(resolved.column_masks.is_empty());
        assert!(resolved.restricted_columns.is_empty());
    }

    #[tokio::test]
    async fn test_role_policy_applied_to_user_with_matching_role() {
        let store = InMemoryPolicyStore::new();
        let policy = ResolvedPolicy {
            restricted_columns: vec!["salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("analyst", policy).await;

        let user = SessionUser {
            username: "bob".to_string(),
            roles: vec!["analyst".to_string()],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        assert_eq!(resolved.restricted_columns, vec!["salary"]);
    }

    #[tokio::test]
    async fn test_role_policy_not_applied_when_no_matching_role() {
        let store = InMemoryPolicyStore::new();
        let policy = ResolvedPolicy {
            restricted_columns: vec!["salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("analyst", policy).await;

        let user = SessionUser {
            username: "charlie".to_string(),
            roles: vec!["viewer".to_string()],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        assert!(resolved.restricted_columns.is_empty());
    }

    #[tokio::test]
    async fn test_table_policy_takes_priority_over_role_policy() {
        let store = InMemoryPolicyStore::new();

        let role_policy = ResolvedPolicy {
            restricted_columns: vec!["salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("analyst", role_policy).await;

        let table_policy = ResolvedPolicy {
            restricted_columns: vec!["ssn".to_string(), "dob".to_string()],
            ..Default::default()
        };
        store.add_table_policy("hr", "employees", table_policy).await;

        let user = SessionUser {
            username: "dave".to_string(),
            roles: vec!["analyst".to_string()],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        // Table-specific policy wins; salary should NOT be restricted, ssn and dob should be
        assert!(resolved.restricted_columns.contains(&"ssn".to_string()));
        assert!(resolved.restricted_columns.contains(&"dob".to_string()));
        assert!(!resolved.restricted_columns.contains(&"salary".to_string()));
    }

    #[tokio::test]
    async fn test_user_with_one_matching_role_among_several() {
        let store = InMemoryPolicyStore::new();

        let analyst_policy = ResolvedPolicy {
            restricted_columns: vec!["salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("analyst", analyst_policy).await;

        let user = SessionUser {
            username: "eve".to_string(),
            roles: vec!["viewer".to_string(), "analyst".to_string()],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        assert_eq!(resolved.restricted_columns, vec!["salary"]);
    }

    // AUTH-03: when a user holds multiple roles that each carry a policy, the
    // resolved policy must be the union of all of them (most restrictive),
    // not just the first iterated role.
    #[tokio::test]
    async fn test_user_with_multiple_matching_roles_unions_policies() {
        let store = InMemoryPolicyStore::new();

        let analyst_policy = ResolvedPolicy {
            restricted_columns: vec!["salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("analyst", analyst_policy).await;

        let restricted_policy = ResolvedPolicy {
            restricted_columns: vec!["ssn".to_string(), "salary".to_string()],
            ..Default::default()
        };
        store.add_role_policy("restricted", restricted_policy).await;

        let user = SessionUser {
            username: "frank".to_string(),
            roles: vec!["analyst".to_string(), "restricted".to_string()],
        };
        let resolved = store.resolve(&user, "employees", "hr").await.unwrap();
        // Union of both role policies, deduped.
        assert!(resolved.restricted_columns.contains(&"salary".to_string()));
        assert!(resolved.restricted_columns.contains(&"ssn".to_string()));
        assert_eq!(resolved.restricted_columns.len(), 2, "must dedup salary");
    }

    // AUTH-03: the union must not depend on the order roles are listed.
    #[tokio::test]
    async fn test_multi_role_union_is_order_independent() {
        let store = InMemoryPolicyStore::new();
        store
            .add_role_policy(
                "a",
                ResolvedPolicy {
                    restricted_columns: vec!["col_a".to_string()],
                    ..Default::default()
                },
            )
            .await;
        store
            .add_role_policy(
                "b",
                ResolvedPolicy {
                    restricted_columns: vec!["col_b".to_string()],
                    ..Default::default()
                },
            )
            .await;

        let forward = SessionUser {
            username: "u1".to_string(),
            roles: vec!["a".to_string(), "b".to_string()],
        };
        let reverse = SessionUser {
            username: "u2".to_string(),
            roles: vec!["b".to_string(), "a".to_string()],
        };
        let mut f = store
            .resolve(&forward, "t", "ns")
            .await
            .unwrap()
            .restricted_columns;
        let mut r = store
            .resolve(&reverse, "t", "ns")
            .await
            .unwrap()
            .restricted_columns;
        f.sort();
        r.sort();
        assert_eq!(f, r);
        assert_eq!(f, vec!["col_a".to_string(), "col_b".to_string()]);
    }

    #[tokio::test]
    async fn test_default_store_same_as_new() {
        let store: InMemoryPolicyStore = Default::default();
        let user = SessionUser {
            username: "alice".to_string(),
            roles: vec![],
        };
        let resolved = store.resolve(&user, "t", "ns").await.unwrap();
        assert!(resolved.row_filters.is_empty());
    }
}
