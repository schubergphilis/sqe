pub mod grants;
pub mod plan_rewriter;
pub mod policy_breaker;
pub mod policy_expr;
pub mod policy_store;
pub mod opa;
pub mod ranger_store;
pub mod mask_udf;
pub mod sha256_udf;
pub mod session_udf;
pub mod write_predicates;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use sqe_core::SessionUser;

/// Policy enforcement trait. Implementations receive a user and a logical plan,
/// and return a (possibly rewritten) plan with security filters applied.
///
/// The evaluate() call sits in the query pipeline between planning and
/// optimization. This means:
/// - Row filters are injected ABOVE the TableScan
/// - Column masks replace column references with expressions
/// - Column restrictions remove columns from projections
/// - The optimizer can push user predicates through row filters (safe)
/// - The optimizer CANNOT push predicates through column masks (security)
#[async_trait]
pub trait PolicyEnforcer: Send + Sync {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan>;
}

/// No-op enforcer that returns the plan unchanged. Used when policy
/// enforcement is disabled or during development/testing.
pub struct PassthroughEnforcer;

#[async_trait]
impl PolicyEnforcer for PassthroughEnforcer {
    async fn evaluate(
        &self,
        _user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<LogicalPlan> {
        Ok(plan)
    }
}

/// Resolved policy for a (user, table) pair. Contains the filters,
/// masks, and restrictions that should be applied to the plan.
#[derive(Debug, Clone, Default)]
pub struct ResolvedPolicy {
    /// Row filter expressions — injected as Filter nodes above TableScan.
    /// Multiple filters are ANDed together.
    pub row_filters: Vec<datafusion::logical_expr::Expr>,

    /// Column masks — map from column name to masking expression.
    /// The original column reference is replaced with this expression.
    pub column_masks: std::collections::HashMap<String, MaskType>,

    /// Restricted columns — removed from the plan entirely.
    /// The column does not appear in any projection, as if it doesn't exist.
    pub restricted_columns: Vec<String>,
}

/// Types of column masking supported.
#[derive(Debug, Clone)]
pub enum MaskType {
    /// Replace with NULL
    Nullify,
    /// Replace with a constant string (e.g., "***")
    Redact(String),
    /// Replace with SHA-256 hash (requires sha256 UDF registered)
    Hash,
    /// Replace with a custom expression
    Custom(datafusion::logical_expr::Expr),
    /// Hive-style partial mask. Keep the first `show_first` and last `show_last`
    /// characters; mask the rest by char class (ASCII upper->`upper`,
    /// lower->`lower`, digit->`digit`). Realized by
    /// `mask_udf::mask_partial_udf`. String-only; non-string columns fall back
    /// to a typed NULL (see `plan_rewriter::apply_mask`).
    PartialMask {
        show_first: u32,
        show_last: u32,
        upper: char,
        lower: char,
        digit: char,
    },
    /// Show only the year of a date/timestamp (`YYYY-01-01`), via
    /// `date_trunc('year', col)`. Non-temporal columns fall back to typed NULL.
    DateShowYear,
}

/// Trait for policy storage backends. Implementations resolve policies
/// for a given (user, table) pair from an external system.
#[async_trait]
pub trait PolicyStore: Send + Sync {
    /// Look up the resolved policy for a user accessing a specific table.
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy>;

    /// Invalidate any cached policy decisions so the next `resolve()` call
    /// re-reads from the backing engine.
    ///
    /// Called after a GRANT/REVOKE (or any policy-mutating statement) so the
    /// change takes effect immediately rather than after the cache TTL elapses
    /// (issue #207). Cacheless stores (e.g. `InMemoryPolicyStore`) inherit the
    /// default no-op. The method is synchronous because moka's
    /// `invalidate_all()` is synchronous (it only marks existing entries stale).
    fn invalidate_all(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_user(username: &str, roles: &[&str]) -> SessionUser {
        SessionUser {
            username: username.to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
        }
    }

    // Build a trivial LogicalPlan (EmptyRelation) that we can pass through the enforcer.
    fn empty_plan() -> LogicalPlan {
        use datafusion::logical_expr::LogicalPlanBuilder;
        LogicalPlanBuilder::empty(false)
            .build()
            .expect("Failed to build empty plan")
    }

    // PassthroughEnforcer tests

    #[tokio::test]
    async fn test_passthrough_returns_plan_unchanged() {
        let enforcer = PassthroughEnforcer;
        let user = make_user("alice", &[]);
        let plan = empty_plan();
        // We compare the debug representation since LogicalPlan doesn't implement PartialEq.
        let before = format!("{plan:?}");
        let result = enforcer.evaluate(&user, plan).await.unwrap();
        let after = format!("{result:?}");
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn test_passthrough_ignores_user_roles() {
        let enforcer = PassthroughEnforcer;
        let admin = make_user("admin", &["superuser", "admin", "data_owner"]);
        let plan = empty_plan();
        let before = format!("{plan:?}");
        let result = enforcer.evaluate(&admin, plan).await.unwrap();
        assert_eq!(before, format!("{result:?}"));
    }

    #[tokio::test]
    async fn test_passthrough_with_no_roles() {
        let enforcer = PassthroughEnforcer;
        let guest = make_user("guest", &[]);
        let plan = empty_plan();
        let before = format!("{plan:?}");
        let result = enforcer.evaluate(&guest, plan).await.unwrap();
        assert_eq!(before, format!("{result:?}"));
    }

    // ResolvedPolicy default is empty (allow-all passthrough)

    #[test]
    fn test_resolved_policy_default_is_empty() {
        let policy = ResolvedPolicy::default();
        assert!(policy.row_filters.is_empty());
        assert!(policy.column_masks.is_empty());
        assert!(policy.restricted_columns.is_empty());
    }

    // MaskType variants are constructible and debug-printable

    #[test]
    fn test_mask_type_variants_debug() {
        let nullify = MaskType::Nullify;
        let redact = MaskType::Redact("***".to_string());
        let hash = MaskType::Hash;
        let custom = MaskType::Custom(datafusion::logical_expr::lit("custom"));

        assert!(format!("{nullify:?}").contains("Nullify"));
        assert!(format!("{redact:?}").contains("Redact"));
        assert!(format!("{hash:?}").contains("Hash"));
        assert!(format!("{custom:?}").contains("Custom"));

        let partial = MaskType::PartialMask { show_first: 0, show_last: 4, upper: 'x', lower: 'x', digit: 'x' };
        let date_year = MaskType::DateShowYear;
        assert!(format!("{partial:?}").contains("PartialMask"));
        assert!(format!("{date_year:?}").contains("DateShowYear"));
    }
}
