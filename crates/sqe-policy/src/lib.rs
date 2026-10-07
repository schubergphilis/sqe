pub mod grants;
mod leakproof;
pub mod mask_udf;
pub mod plan_rewriter;
pub mod policy_expr;
pub mod policy_store;
pub mod ranger_store;
pub mod session_udf;
pub mod sha256_udf;
pub mod tag_projector;
pub mod tag_source;
pub mod write_predicates;

pub use tag_source::{NoopTagSource, TagSource};

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
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
    /// Every masked or restricted column with the scan it was applied to, as
    /// `(table, column)` where `table` is the scan's `TableReference` display
    /// (`catalog.ns.t`, `ns.t` or `t`, exactly as the planner spells it).
    /// Sorted, deduplicated. Column lineage uses it to report those source
    /// columns as MASKED instead of identity passthrough (architecture review
    /// 610); the bare name lists above stay for the audit entry.
    pub masked_fields: Vec<(String, String)>,
}

impl PolicySummary {
    /// Fold another summary into this one (a MERGE audits its source SELECT
    /// and its target policy as one statement).
    pub fn merge(&mut self, other: &PolicySummary) {
        self.row_filters_applied += other.row_filters_applied;
        self.columns_masked
            .extend(other.columns_masked.iter().cloned());
        self.columns_masked.sort();
        self.columns_masked.dedup();
        self.columns_restricted
            .extend(other.columns_restricted.iter().cloned());
        self.columns_restricted.sort();
        self.columns_restricted.dedup();
        self.denied |= other.denied;
        self.masked_fields
            .extend(other.masked_fields.iter().cloned());
        self.masked_fields.sort();
        self.masked_fields.dedup();
    }
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
    /// Cached batches cannot be re-filtered safely after a policy change.
    /// Enforcers must explicitly opt in only when no local policy can change
    /// the result. Catalog access is still revalidated by the coordinator.
    fn permits_result_cache(&self) -> bool {
        false
    }

    /// Stable identifier of the policy state this enforcer applies (finding
    /// 902 follow-up). Cached results are stored with the fingerprint that was
    /// current when they were produced and served only while it is unchanged,
    /// so a governing engine can keep the materialized result cache on. `None`
    /// (the default) means the enforcer cannot vouch for policy stability and
    /// the cache stays off unless `permits_result_cache` is true.
    fn policy_fingerprint(&self) -> Option<String> {
        None
    }

    /// `scope` is the default catalog and schema the plan's table names were
    /// resolved against. DataFusion keeps a scan's reference as written, so
    /// without it `SELECT * FROM t` under `USE sales` is governed as
    /// `default.t`, not `sales.t`.
    async fn evaluate(
        &self,
        user: &SessionUser,
        scope: &TableScope,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)>;
}

/// The default catalog and schema a plan's partial table names resolve
/// against. `TableScope::default()` (no scope) keeps a bare name in the
/// `default` namespace, which is what every caller got before the scope
/// existed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableScope {
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

impl TableScope {
    /// The scope DataFusion itself resolves against for this session, read
    /// from the context's own config so it cannot disagree with how the plan
    /// was actually resolved.
    pub fn of(ctx: &datafusion::prelude::SessionContext) -> Self {
        let state = ctx.state_ref();
        let state = state.read();
        let c = &state.config().options().catalog;
        Self {
            catalog: Some(c.default_catalog.clone()),
            schema: Some(c.default_schema.clone()),
        }
    }

    /// `table` with its missing catalog / schema filled from the scope. A
    /// reserved catalog (`system`, `datafusion`) is never filled in: it would
    /// make a user table look like a reserved virtual one and skip its tag
    /// lookup, and neither Iceberg policy keys nor Hive routing need it.
    pub fn qualify(
        &self,
        table: &datafusion::common::TableReference,
    ) -> datafusion::common::TableReference {
        use datafusion::common::TableReference;
        let catalog = self
            .catalog
            .as_deref()
            .filter(|c| !c.eq_ignore_ascii_case("system") && !c.eq_ignore_ascii_case("datafusion"));
        match table {
            TableReference::Bare { table: t } => match (catalog, self.schema.as_deref()) {
                (Some(c), Some(s)) => TableReference::full(c, s, t.as_ref()),
                (None, Some(s)) => TableReference::partial(s, t.as_ref()),
                _ => table.clone(),
            },
            TableReference::Partial { schema, table: t } => match catalog {
                Some(c) => TableReference::full(c, schema.as_ref(), t.as_ref()),
                None => table.clone(),
            },
            TableReference::Full { .. } => table.clone(),
        }
    }
}

/// No-op enforcer that returns the plan unchanged. Used when policy
/// enforcement is disabled or during development/testing.
pub struct PassthroughEnforcer;

#[async_trait]
impl PolicyEnforcer for PassthroughEnforcer {
    fn permits_result_cache(&self) -> bool {
        true
    }

    fn policy_fingerprint(&self) -> Option<String> {
        Some("passthrough".to_string())
    }

    async fn evaluate(
        &self,
        _user: &SessionUser,
        _scope: &TableScope,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
        Ok((plan, PolicySummary::default()))
    }
}

/// The policy-store contract and its result types live in `sqe-policy-api`
/// (build-time item C1) so `sqe-catalog` can depend on them without waiting
/// for this crate to compile. Re-exported under the old paths.
pub use sqe_policy_api::{
    MaskPolicyInfo, MaskType, PolicyStore, ResolvedPolicy, TablePrivilege, TagMaskSpec,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn make_user(username: &str, roles: &[&str]) -> SessionUser {
        SessionUser {
            username: username.to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            subject: None,
            email: None,
            groups: vec![],
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
        let (result, summary) = enforcer
            .evaluate(&user, &TableScope::default(), plan)
            .await
            .unwrap();
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
        let (result, _summary) = enforcer
            .evaluate(&admin, &TableScope::default(), plan)
            .await
            .unwrap();
        assert_eq!(before, format!("{result:?}"));
    }

    #[tokio::test]
    async fn test_passthrough_with_no_roles() {
        let enforcer = PassthroughEnforcer;
        let guest = make_user("guest", &[]);
        let plan = empty_plan();
        let before = format!("{plan:?}");
        let (result, _summary) = enforcer
            .evaluate(&guest, &TableScope::default(), plan)
            .await
            .unwrap();
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

        let partial = MaskType::PartialMask {
            show_first: 0,
            show_last: 4,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        };
        let date_year = MaskType::DateShowYear;
        assert!(format!("{partial:?}").contains("PartialMask"));
        assert!(format!("{date_year:?}").contains("DateShowYear"));
    }
}
