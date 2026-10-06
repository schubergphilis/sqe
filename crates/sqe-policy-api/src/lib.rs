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

impl ResolvedPolicy {
    /// True when `column` is restricted. Entries may be Ranger column
    /// patterns (`*`, `card_*`), matched like Ranger: wildcards, any case.
    pub fn is_restricted(&self, column: &str) -> bool {
        self.restricted_columns.iter().any(|p| wildcard_match(p, column))
    }

    /// The mask that applies to `column`: an exact key first, else the
    /// matching pattern that sorts first (deterministic across runs).
    pub fn mask_for(&self, column: &str) -> Option<&MaskType> {
        if let Some(mask) = self.column_masks.get(column) {
            return Some(mask);
        }
        self.column_masks
            .iter()
            .filter(|(p, _)| wildcard_match(p, column))
            .min_by_key(|(p, _)| *p)
            .map(|(_, mask)| mask)
    }
}

/// Ranger wildcard match: `*` matches any run of characters (including none),
/// `?` matches exactly one.
///
/// Semantics taken from the live `hive` service definition, whose `database`,
/// `table` and `column` resources all declare
/// `matcherOptions: {wildCard: "true", ignoreCase: "true"}`. Ranger implements
/// that with commons-io `FilenameUtils.wildcardMatch` and `IOCase.INSENSITIVE`,
/// where no character other than `*` and `?` is special.
///
/// Case folding is part of the same contract, not a separate liberty: with
/// `ignoreCase: true`, a policy written on `Orders` fires for `orders` in Ranger,
/// and SQE comparing case-sensitively would silently not mask.
///
/// Iterative with a single backtrack point, so a pattern like `*a*a*a*a*` cannot
/// blow up: policies are operator input reaching a per-query path.
pub fn wildcard_match(pattern: &str, target: &str) -> bool {
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let t: Vec<char> = target.to_lowercase().chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut resume = 0usize;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            pi += 1;
            resume = ti;
        } else if let Some(s) = star {
            // Backtrack: let the last `*` swallow one more character.
            pi = s + 1;
            resume += 1;
            ti = resume;
        } else {
            return false;
        }
    }
    // Trailing `*`s may match nothing.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
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

/// A table-level DDL privilege, for statements that change or remove a table
/// definition rather than read its rows. Only tables whose definition SQE
/// owns (Hive-root tables) ask for one; an Iceberg table's DDL is decided by
/// its catalog under the caller's own bearer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TablePrivilege {
    /// Change the definition in place (Ranger `alter`).
    Alter,
    /// Remove or replace the definition (Ranger `drop`).
    Drop,
}

impl TablePrivilege {
    /// The `hive` servicedef access type that carries this privilege. `all`
    /// carries every one of them.
    pub fn access_type(self) -> &'static str {
        match self {
            Self::Alter => "alter",
            Self::Drop => "drop",
        }
    }
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

    /// Does the policy engine grant `user` the DDL `privilege` on this table?
    ///
    /// `namespace` is keyed exactly as `resolve` is (`{root}.{database}` for
    /// a Hive-root table). This is one way to hold DDL authority over a table,
    /// next to an admin role and ownership; it can only add authority, never
    /// take an admin's or an owner's away.
    ///
    /// The default answers `false`: a backend without a privilege model
    /// grants nothing, so its callers fall back to admin or owner. A backend
    /// gains it by overriding this.
    async fn grants_table_privilege(
        &self,
        _user: &SessionUser,
        _table_name: &str,
        _namespace: &str,
        _privilege: TablePrivilege,
    ) -> sqe_core::Result<bool> {
        Ok(false)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Ranger column resources are patterns: `*`, prefix wildcards and any case
    /// must hit the real column, or the mask/restriction never fires.
    #[test]
    fn column_patterns_match_like_ranger() {
        let mut p = ResolvedPolicy::default();
        p.column_masks.insert("card_*".to_string(), MaskType::Hash);
        p.column_masks.insert("SSN".to_string(), MaskType::Nullify);
        p.restricted_columns.push("*".to_string());
        assert!(matches!(p.mask_for("card_number"), Some(MaskType::Hash)));
        assert!(matches!(p.mask_for("ssn"), Some(MaskType::Nullify)));
        assert!(p.mask_for("name").is_none());
        assert!(p.is_restricted("anything"));

        let mut q = ResolvedPolicy::default();
        q.restricted_columns.push("Notes".to_string());
        assert!(q.is_restricted("notes"));
        assert!(!q.is_restricted("notes2"));
    }
}
