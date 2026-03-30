pub mod plan_rewriter;
pub mod policy_store;
pub mod opa;
pub mod sha256_udf;

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
}
