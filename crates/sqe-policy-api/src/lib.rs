//! Policy and grant interfaces shared by `sqe-catalog` and `sqe-policy`.
//!
//! Build-time program item C1 (`docs/internal/plans/2026-09-04-build-test-time-program.md`).
//! `sqe-catalog` used to depend on all of `sqe-policy` for five items: the
//! [`PolicyStore`] trait with its result types, the [`grants::GrantBackend`]
//! trait with its statement types, and [`HiveCatalogs`]. That edge put the
//! 22 s policy compile in front of the 47 s catalog compile on every cold
//! build. This crate holds exactly those items, depends on the shared tier
//! only, and lets the two compile side by side. `sqe-policy` re-exports
//! everything here under its old paths, so no other crate changes.
//!
//! Nothing in here has behaviour beyond `Grantee::name` and the
//! `HiveCatalogs` set; the trait default methods are the fail-closed
//! refusals they always were.

pub mod grants;
pub mod hive_catalogs;

pub use hive_catalogs::HiveCatalogs;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use datafusion::logical_expr::Expr;
use sqe_core::SessionUser;

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

/// One data-mask policy as WRITTEN in the policy store, for introspection.
///
/// Not a resolved decision. `SHOW MASKING POLICIES` lists these so an operator can
/// see what is configured; what actually applies to a user is `SHOW EFFECTIVE
/// POLICY`, which accounts for deny items, role membership, and resource-versus-tag
/// precedence. Reading this and concluding "alice sees xxx-xx-1111" is the mistake
/// the two statements exist to keep apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaskPolicyInfo {
    /// Policy name in the backing store, which is how an operator refers to it.
    pub name: String,
    /// False when the policy is disabled in the console, in which case it is
    /// listed and does nothing.
    pub enabled: bool,
    /// The TAG this policy is keyed on, for a tag-service mask policy. Empty for a
    /// resource policy, and the two are mutually exclusive: a tag policy names a
    /// tag and no table, a resource policy the reverse. So this field is also how a
    /// reader tells the two kinds apart.
    pub tag: String,
    /// Resource the policy covers. Comma-joined when the policy names several
    /// values at a level, which Ranger allows. Empty for a tag policy.
    pub database: String,
    pub table: String,
    pub column: String,
    /// Ranger `dataMaskType`, e.g. `MASK_NULL`, `CUSTOM`, or a component-qualified
    /// `hive:CUSTOM` on a tag policy.
    pub mask_type: String,
    /// The `{col}` template for a CUSTOM mask; empty for a named type.
    pub expression: String,
    /// Who it applies to, as `USER x` / `ROLE y` / `GROUP z`, comma-joined.
    pub grantees: String,
}

/// Trait for policy storage backends. Implementations resolve policies
/// for a given (user, table) pair from an external system.
#[async_trait]
pub trait PolicyStore: Send + Sync {
    /// Stable identifier of the store's current policy set (finding 902
    /// follow-up): must change whenever any resolved policy could change.
    /// `None` means the store cannot tell, which keeps the result cache off.
    fn policy_fingerprint(&self) -> Option<String> {
        None
    }

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

    /// List the data-mask policies as WRITTEN in the backing store.
    ///
    /// Powers `SHOW MASKING POLICIES`. The default REFUSES rather than returning an
    /// empty list, and the distinction matters more than it looks: an empty result is
    /// indistinguishable from "no masks are configured", so a backend that simply
    /// cannot answer would hand an auditor a clean bill of health. Refusing says
    /// "this deployment cannot tell you", which is the truth.
    ///
    /// A backend gains the statement by overriding this. Only the Ranger store does.
    async fn list_mask_policies(&self) -> sqe_core::Result<Vec<MaskPolicyInfo>> {
        Err(sqe_core::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed,
            "SHOW MASKING POLICIES is not available: the configured policy backend \
             cannot list mask policies. It needs [policy] engine = \"ranger\"."
                .to_string(),
        ))
    }

    /// Create or replace a fine-grained row-filter / column-mask policy.
    async fn create_policy(
        &self,
        _stmt: &sqe_sql::policy_ddl::CreatePolicyStatement,
    ) -> sqe_core::Result<()> {
        Err(sqe_core::SqeError::NotImplemented(
            "CREATE POLICY requires [policy] engine = \"ranger\"".to_string(),
        ))
    }

    /// Drop a fine-grained policy by name.
    async fn drop_policy(
        &self,
        _stmt: &sqe_sql::policy_ddl::DropPolicyStatement,
    ) -> sqe_core::Result<()> {
        Err(sqe_core::SqeError::NotImplemented(
            "DROP POLICY requires [policy] engine = \"ranger\"".to_string(),
        ))
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
