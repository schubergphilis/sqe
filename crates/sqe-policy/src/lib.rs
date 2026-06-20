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
pub mod tag_source;
pub mod write_predicates;

pub use tag_source::{NoopTagSource, TagSource};

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use datafusion::logical_expr::{Expr, LogicalPlan};
use sqe_core::SessionUser;

/// What a single `evaluate()` call did, summarised for the audit log.
///
/// Aggregated across every scanned table in the plan. Populated by the
/// `PolicyPlanRewriter` from the resolved policies it injected; the
/// `PassthroughEnforcer` returns the default (all-zero, not denied). The
/// coordinator copies these counts/names into the `AuditEntry` so an operator
/// can answer "was user X's access to table T filtered, masked, restricted, or
/// denied" instead of seeing a bare `status:"success"` with zero rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicySummary {
    /// Total row-filter expressions injected across all scans (excludes the
    /// `lit(false)` deny-all sentinel, which is reflected in `denied`).
    pub row_filters_applied: usize,
    /// Names of columns that were masked (sorted, deduplicated).
    pub columns_masked: Vec<String>,
    /// Names of columns that were restricted/dropped (sorted, deduplicated).
    pub columns_restricted: Vec<String>,
    /// True when at least one scan was denied (a deny-all `lit(false)` row
    /// filter was injected: resolve failure, breaker-open, unknown-tag state,
    /// or fully-restricted table).
    pub denied: bool,
}

/// Policy enforcement trait. Implementations receive a user and a logical plan,
/// and return a (possibly rewritten) plan with security filters applied, plus a
/// [`PolicySummary`] describing what was applied (for the audit log).
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
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)>;
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
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
        Ok((plan, PolicySummary::default()))
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

/// Tag-level mask carrier used by `PolicyStore::resolve_tags`.
///
/// Tag-based CUSTOM masks cannot be resolved to a full `MaskType::Custom(expr)` at
/// tag-resolve time because the actual column name (needed for `{col}` substitution)
/// is only known when the rewriter iterates over table columns. This enum defers the
/// substitution until `merge_tag_masks` in the plan rewriter, where the column name
/// is available.
///
/// Non-CUSTOM tag masks are wrapped in `Ready` so the rewriter can use them without
/// any further processing.
#[derive(Debug, Clone)]
pub enum TagMaskSpec {
    /// A fully-resolved mask ready to insert into `ResolvedPolicy::column_masks`.
    Ready(MaskType),
    /// A CUSTOM Ranger mask whose `value_expr` template still contains `{col}`.
    /// The rewriter substitutes the real column name and parses the resulting SQL
    /// expression. On parse failure the column is restricted (fail-closed).
    Custom(String),
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

    /// Resolve tag-based mask and row-filter policies for a user given a set
    /// of tag names present on columns in a table.
    ///
    /// Returns:
    /// - `HashMap<tag_name, TagMaskSpec>` — mask specs keyed by TAG (not column name).
    ///   `TagMaskSpec::Ready` holds a fully-resolved `MaskType`. `TagMaskSpec::Custom`
    ///   holds the raw `{col}`-template string; the rewriter substitutes the real column
    ///   name and parses the expression at merge time.
    ///   The `PlanRewriter` maps tag -> column using the Iceberg schema's
    ///   `column -> tags` map from the `TagSource`.
    /// - `Vec<Expr>` — row filter expressions to AND into the resolved policy.
    /// - `HashSet<tag_name>` — tags that matched the user but whose mask could
    ///   NOT be mapped to any supported spec (genuinely unsupported type). The
    ///   rewriter MUST restrict any column bearing one of these tags (fail-closed):
    ///   without this, a column whose only protection is an unmappable tag mask would
    ///   be returned RAW. This mirrors the resource path where an unmappable mask adds
    ///   the column to `restricted_columns`. CUSTOM tags are no longer in this set;
    ///   they appear in the masks map as `TagMaskSpec::Custom` instead.
    ///
    /// Default implementation returns empty (non-Ranger stores need no change).
    /// RangerStore overrides this to fetch the bundle and call `resolve_tag_policies`.
    ///
    /// Fail-closed contract: on any fetch/parse failure the implementation
    /// MUST return `(HashMap::new(), vec![lit(false)], HashSet::new())` — the
    /// `lit(false)` row filter denies all rows, consistent with how `resolve()`
    /// handles errors. Returning an empty filter vec on failure would show
    /// tagged columns unmasked while resource-policy cache-hits still work
    /// (fail-open on the security path).
    async fn resolve_tags(
        &self,
        _user: &SessionUser,
        _tags: &HashSet<String>,
    ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
        (HashMap::new(), vec![], HashSet::new())
    }

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
        let (result, summary) = enforcer.evaluate(&user, plan).await.unwrap();
        let after = format!("{result:?}");
        assert_eq!(before, after);
        assert_eq!(summary, PolicySummary::default());
    }

    #[tokio::test]
    async fn test_passthrough_ignores_user_roles() {
        let enforcer = PassthroughEnforcer;
        let admin = make_user("admin", &["superuser", "admin", "data_owner"]);
        let plan = empty_plan();
        let before = format!("{plan:?}");
        let (result, _summary) = enforcer.evaluate(&admin, plan).await.unwrap();
        assert_eq!(before, format!("{result:?}"));
    }

    #[tokio::test]
    async fn test_passthrough_with_no_roles() {
        let enforcer = PassthroughEnforcer;
        let guest = make_user("guest", &[]);
        let plan = empty_plan();
        let before = format!("{plan:?}");
        let (result, _summary) = enforcer.evaluate(&guest, plan).await.unwrap();
        assert_eq!(before, format!("{result:?}"));
    }

    #[test]
    fn test_policy_summary_default_is_not_denied() {
        let s = PolicySummary::default();
        assert_eq!(s.row_filters_applied, 0);
        assert!(s.columns_masked.is_empty());
        assert!(s.columns_restricted.is_empty());
        assert!(!s.denied);
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
