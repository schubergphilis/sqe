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

        // Check role-based policies
        let role_policies = self.role_policies.read().await;
        for role in &user.roles {
            if let Some(policy) = role_policies.get(role) {
                return Ok(policy.clone());
            }
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
}
