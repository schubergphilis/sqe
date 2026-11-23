//! Policy plan rewriter — injects row filters, column masks, and column
//! restrictions into a DataFusion LogicalPlan before optimization.
//!
//! The rewriter walks the plan tree top-down. When it encounters a TableScan,
//! it looks up the resolved policy for that table and user, then:
//! 1. Injects Filter nodes above the TableScan for row-level security
//! 2. Wraps column references in mask expressions for column masking
//! 3. Removes restricted columns from projections
//!
//! This happens BEFORE the optimizer runs, so:
//! - User predicates that cannot fail are pushed through row filters; when
//!   any can fail, a row-filtered row reads as all NULL (no error oracle,
//!   see `crate::leakproof`)
//! - User predicates CANNOT push through column masks (expression boundary)
//! - Restricted columns are invisible (not errors)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{col, lit, when, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::scalar::ScalarValue;
use tracing::{debug, warn};

use sqe_core::SessionUser;

use crate::policy_expr::parse_sql_predicate;
use crate::session_udf::SessionIdentity;
use crate::{
    MaskType, NoopTagSource, PolicyEnforcer, PolicyStore, PolicySummary, ResolvedPolicy,
    TagMaskSpec, TagSource,
};

/// Plan-rewriting policy enforcer. Uses a PolicyStore to resolve policies
/// and rewrites the LogicalPlan accordingly.
pub struct PolicyPlanRewriter {
    store: Arc<dyn PolicyStore>,
    /// HMAC key used by `MaskType::Hash`. When `None` the rewriter falls
    /// back to plain SHA-256, which is vulnerable to offline brute force
    /// against low-entropy columns (issue #37).
    mask_key: Option<Arc<Vec<u8>>>,
    /// Tag source used to look up column -> tags for each scanned table.
    /// Defaults to `NoopTagSource` (no tag-based masking). Replaced at
    /// startup with `CacheTagSource` when a `TableMetadataCache` is available.
    tag_source: Arc<dyn TagSource>,
    /// Which mask wins when a column has both a resource mask and a tag mask.
    mask_precedence: sqe_core::config::MaskPrecedence,
    /// Live registry of Hive-external catalog names, consulted by
    /// `resolve_policy_key` so a Hive table keys under its warehouse root
    /// instead of the Iceberg convention. Defaults to empty, which is why an
    /// Iceberg-only deployment never takes the Hive branch.
    hive_catalogs: Arc<HiveCatalogs>,
    /// Maps a metadata TVF provider (`table_files('ns','t')`, ...) to the
    /// `(namespace, table)` it describes. `None` when not wired (unit tests).
    metadata_target: Option<MetadataTarget>,
    /// Recognises a file table function's provider (`read_csv(...)`, ...).
    /// `None` when not wired (unit tests).
    file_table: Option<FileTable>,
    /// Reads column tags off the provider a scan was planned with. `None`
    /// when not wired (unit tests), in which case every lookup goes to
    /// `tag_source`.
    scan_tags: Option<ScanTags>,
}

/// Column tags read off a scan's own provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanTagState {
    /// Not a provider the hook knows; ask the `TagSource`.
    NotRecognised,
    /// The provider's tag property is unreadable. Refuse, as for a `None`
    /// from the `TagSource`. Never fall back: the cache could hold an older,
    /// readable value.
    Unknown,
    /// The provider's tags. Empty means the table carries no tags.
    Known(HashMap<String, Vec<String>>),
}

impl ScanTagState {
    /// Combine the states of two scans of the same table name. An unknown
    /// state wins; known maps union, which can only add masks.
    fn merge(self, other: ScanTagState) -> ScanTagState {
        use ScanTagState::*;
        match (self, other) {
            (Unknown, _) | (_, Unknown) => Unknown,
            (NotRecognised, s) | (s, NotRecognised) => s,
            (Known(mut a), Known(b)) => {
                for (column, tags) in b {
                    let entry = a.entry(column).or_default();
                    for tag in tags {
                        if !entry.contains(&tag) {
                            entry.push(tag);
                        }
                    }
                }
                Known(a)
            }
        }
    }
}

#[cfg(test)]
mod scan_tag_state_tests {
    use super::ScanTagState::{self, *};
    use std::collections::HashMap;

    fn known(pairs: &[(&str, &[&str])]) -> ScanTagState {
        Known(
            pairs
                .iter()
                .map(|(c, ts)| (c.to_string(), ts.iter().map(|t| t.to_string()).collect()))
                .collect(),
        )
    }

    #[test]
    fn unknown_wins_over_everything() {
        assert_eq!(known(&[("a", &["x"])]).merge(Unknown), Unknown);
        assert_eq!(Unknown.merge(known(&[])), Unknown);
        assert_eq!(NotRecognised.merge(Unknown), Unknown);
    }

    #[test]
    fn not_recognised_yields_to_the_other_state() {
        assert_eq!(NotRecognised.merge(known(&[])), known(&[]));
        assert_eq!(known(&[]).merge(NotRecognised), known(&[]));
        assert_eq!(NotRecognised.merge(NotRecognised), NotRecognised);
    }

    #[test]
    fn known_maps_union_their_tags() {
        let Known(got) = known(&[("a", &["x"]), ("b", &["y"])])
            .merge(known(&[("a", &["x", "z"]), ("c", &["w"])]))
        else {
            panic!("two known states stay known");
        };
        let want: HashMap<String, Vec<String>> =
            [("a", vec!["x", "z"]), ("b", vec!["y"]), ("c", vec!["w"])]
                .into_iter()
                .map(|(c, ts)| (c.to_string(), ts.into_iter().map(String::from).collect()))
                .collect();
        assert_eq!(got, want);
    }
}

/// Reads column tags off a scan's provider. Injected because the Iceberg
/// provider type lives in `sqe-catalog`.
pub type ScanTags = fn(&dyn datafusion::catalog::TableProvider) -> ScanTagState;

/// `(catalog, namespace, table)` a metadata TVF provider reads, `None` for
/// any other provider. The catalog is the one the call named
/// (`table_files('ns', 't', 'cat')`), `None` when it named none. Injected
/// because the TVF type lives in `sqe-catalog`.
pub type MetadataTarget =
    fn(&dyn datafusion::catalog::TableProvider) -> Option<(Option<String>, String, String)>;

/// Whether a provider is a file table function's. Injected because the type
/// lives in `sqe-catalog`.
pub type FileTable = fn(&dyn datafusion::catalog::TableProvider) -> bool;

impl PolicyPlanRewriter {
    pub fn new(store: Arc<dyn PolicyStore>) -> Self {
        Self {
            store,
            mask_key: None,
            tag_source: Arc::new(NoopTagSource),
            mask_precedence: sqe_core::config::MaskPrecedence::default(),
            hive_catalogs: Arc::new(HiveCatalogs::default()),
            metadata_target: None,
            file_table: None,
            scan_tags: None,
        }
    }

    /// Inject the scan-provider tag reader. The Iceberg provider a scan was
    /// planned with already holds the table's current properties, loaded
    /// with the caller's token, so its tags are known even when the metadata
    /// cache is disabled or has evicted the table (#842). Scans it does not
    /// recognise still go to the `TagSource`.
    #[must_use = "with_scan_tags consumes self; bind the returned rewriter"]
    pub fn with_scan_tags(mut self, scan_tags: ScanTags) -> Self {
        self.scan_tags = Some(scan_tags);
        self
    }

    /// Inject the file table function lookup. A file carries no Iceberg
    /// column tags and its access is governed by the `[storage.tvf]` path
    /// policy, so its scan is not denied for an unknown tag state.
    #[must_use = "with_file_table consumes self; bind the returned rewriter"]
    pub fn with_file_table(mut self, file_table: FileTable) -> Self {
        self.file_table = Some(file_table);
        self
    }

    /// Inject the metadata TVF target lookup. With it, `evaluate` refuses a
    /// plan whose metadata TVF names a table the caller is denied all
    /// access to, on every path that enforces policy (#750).
    #[must_use = "with_metadata_target consumes self; bind the returned rewriter"]
    pub fn with_metadata_target(mut self, metadata_target: MetadataTarget) -> Self {
        self.metadata_target = Some(metadata_target);
        self
    }

    /// Choose which mask wins when a resource mask and a tag mask cover the same
    /// column. Defaults to `Tag`, which is what Hive and Spark/Kyuubi do.
    #[must_use = "with_mask_precedence consumes self; bind the returned rewriter"]
    pub fn with_mask_precedence(mut self, precedence: sqe_core::config::MaskPrecedence) -> Self {
        self.mask_precedence = precedence;
        self
    }

    /// Set the HMAC key used by Hash-type column masks. Pass `None` to
    /// keep the legacy unsalted SHA-256 behavior.
    #[must_use = "with_mask_key consumes self; bind the returned rewriter"]
    pub fn with_mask_key(mut self, mask_key: Option<Arc<Vec<u8>>>) -> Self {
        self.mask_key = mask_key;
        self
    }

    /// Inject the `TagSource` used to resolve column tags from Iceberg table
    /// metadata. Defaults to `NoopTagSource` (no tag-based masking).
    #[must_use = "with_tag_source consumes self; bind the returned rewriter"]
    pub fn with_tag_source(mut self, tag_source: Arc<dyn TagSource>) -> Self {
        self.tag_source = tag_source;
        self
    }

    /// Inject the live registry of Hive-external catalog names. `ATTACH ...
    /// TYPE hive_external` and `DETACH` update the SAME `Arc` at runtime, so
    /// the rewriter (built once, process-wide) sees a root attached after
    /// startup instead of keying it under the Iceberg convention forever.
    #[must_use = "with_hive_catalogs consumes self; bind the returned rewriter"]
    pub fn with_hive_catalogs(mut self, hive_catalogs: Arc<HiveCatalogs>) -> Self {
        self.hive_catalogs = hive_catalogs;
        self
    }
}

#[async_trait]
impl PolicyEnforcer for PolicyPlanRewriter {
    fn policy_fingerprint(&self) -> Option<String> {
        // The store fingerprint covers resource and tag policies; mask
        // precedence and the presence of a mask key change rendered values,
        // so they are part of the identity too. Table-level tag changes go
        // through DDL, which evicts that table's cached results.
        self.store.policy_fingerprint().map(|f| {
            format!(
                "{f}|precedence={:?}|keyed_hash={}",
                self.mask_precedence,
                self.mask_key.is_some()
            )
        })
    }

    // Written out the way `#[async_trait]` expands it, and delegating to a
    // non-generic inherent method: see `PassthroughEnforcer::evaluate`.
    fn evaluate<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        user: &'life1 SessionUser,
        scope: &'life2 crate::TableScope,
        plan: LogicalPlan,
    ) -> crate::BoxedFuture<'async_trait, sqe_core::Result<(LogicalPlan, PolicySummary)>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.evaluate_boxed(user, scope, plan)
    }
}

impl PolicyPlanRewriter {
    fn evaluate_boxed<'a>(
        &'a self,
        user: &'a SessionUser,
        scope: &'a crate::TableScope,
        plan: LogicalPlan,
    ) -> crate::BoxedFuture<'a, sqe_core::Result<(LogicalPlan, PolicySummary)>> {
        Box::pin(self.evaluate_plan(user, scope, plan))
    }

    async fn evaluate_plan(
        &self,
        user: &SessionUser,
        scope: &crate::TableScope,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
        let store = self.store.clone();
        let mask_key = self.mask_key.clone();
        let tag_source = self.tag_source.clone();
        let hive_catalogs = self.hive_catalogs.clone();
        let username = user.username.clone();
        let user_clone = user.clone();

        // Collect all TableScan nodes (keyed by their stringified reference,
        // which is what the rewrite phase below matches on) together with the
        // structured `TableReference`. We resolve from the structured form so
        // multi-level Iceberg namespaces survive instead of being lost to a
        // naive split on '.' (issue #205).
        let mut table_refs: HashMap<String, datafusion::common::TableReference> = HashMap::new();
        // Defense-in-depth for the `view-bypass-policy` finding. DataFusion 54
        // inlines views into their base scans at `ctx.sql` planning time, so the
        // rewriter normally sees the governed base `TableScan`. But a scan whose
        // provider is still a `ViewTable` (a hand-built scan, or any residual
        // un-inlined view) would be keyed by the VIEW name, whose policy is
        // usually empty, leaking the base table raw. Mark such scans for a
        // fail-closed deny rather than trusting the view-name policy.
        let mut view_scans: HashSet<String> = HashSet::new();
        // Tables described by metadata TVF scans. The scan itself is keyed by
        // the function name, so the target's own policy is checked below.
        // The target lives in the catalog the call names
        // (`table_files('ns', 't', 'cat')`), else in the catalog the function
        // name resolves to.
        let mut metadata_targets: Vec<(Option<String>, String, String)> = Vec::new();
        // Scan names some file table function scan carries, and scan names
        // some other scan carries. Policies are keyed by scan name, so a name
        // in both (a table named `read_csv()` beside a `read_csv(...)` call)
        // is governed as a table.
        let mut file_scans: HashSet<String> = HashSet::new();
        let mut other_scans: HashSet<String> = HashSet::new();
        // Tags read off each scan's provider, per scan name.
        let mut scan_tag_states: HashMap<String, ScanTagState> = HashMap::new();
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let table_name = scan.table_name.to_string();
                let scan_tags = datafusion::datasource::source_as_provider(&scan.source)
                    .ok()
                    .zip(self.scan_tags)
                    .map_or(ScanTagState::NotRecognised, |(provider, f)| {
                        f(provider.as_ref())
                    });
                let merged = match scan_tag_states.remove(&table_name) {
                    Some(seen) => seen.merge(scan_tags),
                    None => scan_tags,
                };
                scan_tag_states.insert(table_name.clone(), merged);
                let is_file = datafusion::datasource::source_as_provider(&scan.source)
                    .ok()
                    .zip(self.file_table)
                    .is_some_and(|(provider, f)| f(provider.as_ref()));
                if is_file {
                    file_scans.insert(table_name.clone());
                } else {
                    other_scans.insert(table_name.clone());
                }
                if let Ok(provider) = datafusion::datasource::source_as_provider(&scan.source) {
                    if let Some((named, ns, table)) =
                        self.metadata_target.and_then(|f| f(provider.as_ref()))
                    {
                        // The catalog the call named, else the one the
                        // function name resolved in.
                        let catalog = named.or_else(|| {
                            scope
                                .qualify(&scan.table_name)
                                .catalog()
                                .map(str::to_string)
                        });
                        let target = (catalog, ns, table);
                        if !metadata_targets.contains(&target) {
                            metadata_targets.push(target);
                        }
                    }
                    // TableProvider has `Any` as a supertrait but no `as_any()`
                    // method; upcast the reference to downcast to ViewTable.
                    let any_provider: &dyn std::any::Any = provider.as_ref();
                    if any_provider
                        .downcast_ref::<datafusion::datasource::ViewTable>()
                        .is_some()
                    {
                        view_scans.insert(table_name.clone());
                    }
                }
                // Governed as the table it resolved to: a name written
                // without its schema/catalog is qualified from the scope the
                // plan was resolved in.
                table_refs
                    .entry(table_name)
                    .or_insert_with(|| scope.qualify(&scan.table_name));
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .map_err(|e| sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Plan traversal failed: {e}")))?;

        // A metadata TVF exposes file paths, partition values and row counts
        // of its target, so a caller denied all access to the target is
        // refused, with the missing-table wording so "denied" is
        // indistinguishable from "never existed" (#750). Table-level only.
        // A `catalog_scoped` store keys the target as `{catalog}.{namespace}`;
        // with no catalog to key on, the target is refused (fail closed).
        let no_tags = HashMap::new();
        for (catalog, ns, table) in &metadata_targets {
            let key_ns = match (store.catalog_scoped(), catalog) {
                (false, _) => Some(ns.clone()),
                (true, Some(catalog)) => Some(format!("{catalog}.{ns}")),
                (true, None) => None,
            };
            let denied = match key_ns {
                Some(key_ns) => policy_denies_all(
                    &resolve_effective_policy(
                        store.as_ref(),
                        user,
                        catalog.as_deref(),
                        table,
                        &key_ns,
                        &no_tags,
                        self.mask_precedence,
                    )
                    .await,
                ),
                None => true,
            };
            if denied {
                return Err(sqe_core::error::SqeError::with_code(
                    sqe_core::SqeErrorCode::TableNotFound,
                    format!("table not found: {ns}.{table}"),
                ));
            }
        }

        // Resolve policies for all tables
        let mut table_policies: HashMap<String, ResolvedPolicy> = HashMap::new();
        for (table_name, table_ref) in &table_refs {
            // Un-inlined view scan: deny rather than govern by the (ungoverned)
            // view name. See `view_scans` above.
            if view_scans.contains(table_name) {
                warn!(
                    user = %username,
                    table = %table_name,
                    "Un-inlined view scan reached the rewriter; denying access (fail-closed)"
                );
                let mut deny = ResolvedPolicy::default();
                deny.row_filters.push(lit(false));
                table_policies.insert(table_name.clone(), deny);
                continue;
            }
            // Derive the (namespace, table) policy key from the structured
            // reference. This MUST match the write path's scheme
            // (the full dotted namespace, see `resolve_policy_key`), otherwise
            // reads and writes resolve different policies for the same table.
            let Some((namespace, table)) =
                resolve_policy_key_scoped(table_ref, &hive_catalogs, store.catalog_scoped())
            else {
                // FAIL CLOSED: a reference we cannot confidently map to a
                // policy key must never pass through. Deny all rows rather
                // than risk leaking an unguarded table (issue #205).
                warn!(
                    user = %username,
                    table = %table_name,
                    "Could not resolve policy key for table reference, denying access"
                );
                let mut deny = ResolvedPolicy::default();
                deny.row_filters.push(lit(false));
                table_policies.insert(table_name.clone(), deny);
                continue;
            };

            // Diagnostic: log the EXACT (database, table) lookup keys sent to the
            // policy store. `namespace` is the FULL dotted schema, which is what
            // Ranger `database` resource values must match. When a policy silently
            // does not fire, operators compare `lookup_database` against the Ranger
            // policy resource: a policy written against the old last-component key
            // (`ns2` for schema `ns1.ns2`) still matches, but logs that it did so.
            // No row values are logged.
            debug!(
                user = %username,
                full_ref = %table_name,
                lookup_database = %namespace,
                lookup_table = %table,
                "Resolving policy with flattened lookup keys"
            );

            match store.resolve(&user_clone, &table, &namespace).await {
                Ok(mut policy) => {
                    debug!(
                        user = %username,
                        table = %table_name,
                        namespace = %namespace,
                        row_filters = policy.row_filters.len(),
                        column_masks = policy.column_masks.len(),
                        restricted = policy.restricted_columns.len(),
                        "Resolved policy for table"
                    );

                    // Tag-based masking: look up column -> tags from Iceberg
                    // metadata (via the injected TagSource), then resolve tag
                    // policies from the store and merge them into the resolved
                    // policy.
                    //
                    // Identity threading: use the FULL namespace path from the
                    // TableReference (split the schema string on '.') — NOT the
                    // last-component `namespace` used by `resolve_policy_key`.
                    // The CacheTagSource builds NamespaceIdent::from_vec which
                    // Display-joins with '.', matching the cache key format
                    // `{token}|{ns_display}.{table}`. Passing the reduced
                    // last-component would silently miss for multi-level
                    // namespaces (e.g. "ns1.ns2" -> "ns2" != cache key "ns1.ns2.t").
                    let catalog = table_ref.catalog();
                    let ns_path: Vec<String> = match table_ref.schema() {
                        Some(s) if !s.is_empty() => s.split('.').map(str::to_string).collect(),
                        _ => Vec::new(),
                    };
                    // Reserved virtual schemas (information_schema, the `system`
                    // / `datafusion` catalogs) are served by in-memory providers
                    // that are NEVER inserted into the table metadata cache, so
                    // `column_tags` would always return None for them. They also
                    // cannot host an Iceberg table (DataFusion/SQE reserve these
                    // names), so they carry no tags by construction. Treat them
                    // as known-no-tags rather than letting the fail-closed None
                    // branch deny every information_schema query (which dbt and
                    // SHOW COLUMNS / SHOW CREATE TABLE issue constantly).
                    // A file table function's scan is never in that cache
                    // either, and a file has no Iceberg tags.
                    let file_scan =
                        file_scans.contains(table_name) && !other_scans.contains(table_name);
                    // An Iceberg scan's own provider holds the properties
                    // planning loaded for this query; only a scan the hook
                    // does not recognise asks the tag source (#842).
                    let tag_lookup = if is_reserved_virtual_ref(table_ref) || file_scan {
                        Some(HashMap::new())
                    } else {
                        match scan_tag_states.get(table_name) {
                            Some(ScanTagState::Known(tags)) => Some(tags.clone()),
                            Some(ScanTagState::Unknown) => None,
                            Some(ScanTagState::NotRecognised) | None => {
                                tag_source.column_tags(catalog, &ns_path, table_ref.table())
                            }
                        }
                    };

                    match tag_lookup {
                        // Tag state UNKNOWN (cache miss / disabled cache /
                        // unreadable metadata). Fail CLOSED: a tag mask or tag
                        // row-filter may exist that we cannot see, so the scan
                        // must not run. Skipping tag work here would silently
                        // bypass a security control. Refuse the statement
                        // rather than inject `lit(false)`: a deny-all row
                        // filter returned an empty result that a client (or a
                        // CTAS) took for real data, with only a WARN to show
                        // for it (#840).
                        None => {
                            warn!(
                                user = %username,
                                table = %table_name,
                                namespace = %namespace,
                                "Tag state unknown (cache miss or disabled); refusing the statement"
                            );
                            return Err(sqe_core::error::SqeError::with_code(
                                sqe_core::SqeErrorCode::AccessDenied,
                                format!(
                                    "tag state unknown for table {table_name}: its column \
                                     tags could not be read, so the statement was refused \
                                     rather than run without its tag policies"
                                ),
                            ));
                        }
                        // Known: at least one column carries tags. Resolve tag
                        // policies and merge them into the resolved policy.
                        Some(col_tags) if !col_tags.is_empty() => {
                            let all_tags: HashSet<String> =
                                col_tags.values().flatten().cloned().collect();

                            if !all_tags.is_empty() {
                                let (tag_masks_by_tag, tag_filters, unmappable_tags) =
                                    crate::tag_scope::resolve_tags_in_catalog(
                                        store.as_ref(),
                                        &user_clone,
                                        &all_tags,
                                        table_ref.catalog(),
                                    )
                                    .await;

                                debug!(
                                    user = %username,
                                    table = %table_name,
                                    tag_masks = tag_masks_by_tag.len(),
                                    tag_filters = tag_filters.len(),
                                    unmappable_tags = unmappable_tags.len(),
                                    "Resolved tag-based policies"
                                );

                                let identity = SessionIdentity {
                                    username: user_clone.username.clone(),
                                    roles: user_clone.roles.clone(),
                                    database: None,
                                    schema: None,
                                };
                                let conflicts = merge_tag_masks(
                                    &mut policy,
                                    &col_tags,
                                    &tag_masks_by_tag,
                                    tag_filters,
                                    &unmappable_tags,
                                    &identity,
                                    self.mask_precedence,
                                );
                                // Policies that disagree fail the statement
                                // before any row is read: masks under `refuse`
                                // (#918), row filters always (TA-13).
                                if let Some(conflict) = conflicts.first() {
                                    warn!(
                                        user = %username,
                                        table = %table_name,
                                        conflict = ?conflict,
                                        "Conflicting policies; refusing the statement"
                                    );
                                    return Err(conflict.to_error(&table_ref.to_string()));
                                }
                            }
                        }
                        // Known: no tags on this table. Proceed with the
                        // resource-path policy unchanged, no tag work.
                        Some(_) => {}
                    }

                    table_policies.insert(table_name.clone(), policy);
                }
                Err(e) => {
                    warn!(
                        user = %username,
                        table = %table_name,
                        error = %e,
                        "Failed to resolve policy, denying access"
                    );
                    // On policy resolution failure, inject a FALSE filter (deny all)
                    let mut deny = ResolvedPolicy::default();
                    deny.row_filters.push(lit(false));
                    table_policies.insert(table_name.clone(), deny);
                }
            }
        }

        // Summarise what the resolved policies do, for the audit log. Folded
        // out of `table_policies` here (we hold every ResolvedPolicy) so the
        // coordinator can record per-query masked/restricted/denied state
        // instead of a bare `status:"success"` with zero rows. `denied` is true
        // when any scan carries a deny-all `lit(false)` row filter (resolve
        // failure, breaker-open, unknown-tag state, or an un-inlined view scan);
        // those sentinels are NOT counted in `row_filters_applied`. A restricted
        // column is NOT a deny: it stays in the schema as a forced NULL.
        let summary = summarise_policies(&table_policies);

        // Whether any caller expression the optimizer can move below a row
        // filter could fail on a hidden row (see `crate::leakproof`). Only
        // then are row-filtered tables read through the guard (step 1b),
        // which costs those tables their scan pruning. A plan-wide switch,
        // not per column: an alias in a view or subquery hides which scan
        // column a predicate reads.
        let guard_rows = table_policies.values().any(|p| !p.row_filters.is_empty())
            && crate::leakproof::plan_has_fallible_predicate(&plan);

        // Rewrite the plan
        let rewritten = plan
            .transform_down_with_subqueries(|node| {
                if let LogicalPlan::TableScan(ref scan) = node {
                    let table_name = scan.table_name.to_string();
                    if let Some(policy) = table_policies.get(&table_name) {
                        if policy.row_filters.is_empty()
                            && policy.column_masks.is_empty()
                            && policy.restricted_columns.is_empty()
                        {
                            return Ok(Transformed::no(node));
                        }

                        // Build the security wrappers with LogicalPlanBuilder so
                        // injected expressions are NORMALIZED against the real
                        // scan schema. An Iceberg TableScan exposes fully
                        // qualified fields (`catalog.schema.table.col`), while
                        // the policy row filters (parsed schema-free) and the
                        // mask UDF args use BARE column names. Manual
                        // Filter/Projection construction left those refs
                        // unqualified, so physical planning failed with
                        // "No field named <qualified>.col". `filter()` and
                        // `project()` both run `normalize_col`, qualifying every
                        // column reference (including those nested inside Hash /
                        // Custom mask expressions) to the child schema.
                        // Predicates already pushed into the scan (a view body is
                        // optimized when it is planned) run on RAW values at file,
                        // row-group and row level, here and on workers. One that
                        // reads a masked or restricted column would filter on the
                        // value the mask hides, so it leaves the scan and is
                        // re-applied above the mask (step 2b). So does a
                        // fallible one on a row-filtered table: in the scan it
                        // runs on rows the row filter hides (step 1b).
                        let guard = guard_rows && !policy.row_filters.is_empty();
                        let source_schema = if guard && !scan.filters.is_empty() {
                            datafusion::common::DFSchema::try_from_qualified_schema(
                                scan.table_name.clone(),
                                &scan.source.schema(),
                            )
                            .ok()
                        } else {
                            None
                        };
                        let raw_on_governed: Vec<Expr> = scan
                            .filters
                            .iter()
                            .filter(|f| {
                                f.column_refs().iter().any(|c| {
                                    policy.is_restricted(&c.name)
                                        || policy.mask_for(&c.name).is_some()
                                }) || (guard
                                    && source_schema.as_ref().is_none_or(|s| {
                                        crate::leakproof::is_fallible(f, s)
                                    }))
                            })
                            .cloned()
                            .collect();
                        let mut scan = scan.clone();
                        let node = if raw_on_governed.is_empty() {
                            node
                        } else {
                            scan.filters.retain(|f| !raw_on_governed.contains(f));
                            LogicalPlan::TableScan(scan.clone())
                        };
                        // Widen the scan's projection if a row filter (or a moved
                        // predicate) needs a column the query does not select (a
                        // narrow view). The restore list puts the output schema
                        // back afterwards.
                        let needed: Vec<Expr> =
                            policy.row_filters.iter().chain(&raw_on_governed).cloned().collect();
                        let (node, restore) = match widen_scan_for_row_filters(&scan, &needed) {
                            Some((wider, restore)) => (wider, Some(restore)),
                            None => (node, None),
                        };
                        let mut builder = LogicalPlanBuilder::from(node);

                        // 1. Inject row filters above the TableScan.
                        for filter_expr in &policy.row_filters {
                            builder = builder.filter(filter_expr.clone()).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to create policy filter: {e}"
                                ))
                            })?;
                        }

                        // 1b. Read every column through the row filter:
                        //     `CASE WHEN <row filter> THEN col END`. The
                        //     optimizer merges the Filter above with the
                        //     caller's predicates into one conjunction (and may
                        //     reorder it), and AND evaluates its right side over
                        //     the whole batch unless few rows pass. A fallible
                        //     caller predicate would then run on hidden rows and
                        //     its error would reveal their values. Guarded, a
                        //     hidden row is all NULL to everything above, wherever
                        //     the predicate ends up. Cost: caller predicates on a
                        //     row-filtered table no longer reach scan pruning,
                        //     so the guard is only added when the plan holds a
                        //     fallible expression (`guard_rows`).
                        //     `write_predicates::unparse_predicates` recognises
                        //     the guard by its shape.
                        let guard_expr = if guard {
                            conjunction(policy.row_filters.iter().cloned())
                        } else {
                            None
                        };
                        if let Some(guard) = guard_expr {
                            let schema = builder.schema().clone();
                            let exprs = schema
                                .iter()
                                .map(|(qualifier, field)| -> datafusion::error::Result<Expr> {
                                    let column = Expr::Column(datafusion::common::Column::new(
                                        qualifier.cloned(),
                                        field.name(),
                                    ));
                                    Ok(when(guard.clone(), column)
                                        .end()?
                                        .alias_qualified(qualifier.cloned(), field.name().clone()))
                                })
                                .collect::<datafusion::error::Result<Vec<Expr>>>()?;
                            builder = builder.project(exprs).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to create policy row-filter guard: {e}"
                                ))
                            })?;
                        }

                        // 2. Apply column masks and restrictions via a
                        //    projection. A RESTRICTED column is kept in the
                        //    schema but forced to a typed NULL (restriction is a
                        //    forced Nullify), so `SELECT *` and any reference to
                        //    it resolve and the raw value is never returned.
                        //    Dropping the column (the old behavior) made any
                        //    outer reference fail with FieldNotFound. A MASKED
                        //    column gets its mask expression; everything else
                        //    passes through with its real (qualified) reference.
                        //    Restriction wins over a mask on the same column.
                        if !policy.column_masks.is_empty() || !policy.restricted_columns.is_empty()
                        {
                            let schema = builder.schema().clone();
                            let exprs: Vec<Expr> = schema
                                .iter()
                                .map(|(qualifier, field)| {
                                    let name = field.name();
                                    // Alias mask/null exprs to the column's
                                    // QUALIFIED name so the output field keeps
                                    // the scan's qualifier. A bare `.alias(name)`
                                    // produces an unqualified field, which breaks
                                    // the user's outer `SELECT t.col` reference
                                    // (planned against the qualified scan) with
                                    // "No field named <qualified>.col".
                                    if policy.is_restricted(name) {
                                        apply_mask(
                                            name,
                                            field.data_type(),
                                            &MaskType::Nullify,
                                            mask_key.clone(),
                                        )
                                        .alias_qualified(qualifier.cloned(), name.clone())
                                    } else if let Some(mask) = policy.mask_for(name) {
                                        apply_mask(name, field.data_type(), mask, mask_key.clone())
                                            .alias_qualified(qualifier.cloned(), name.clone())
                                    } else {
                                        Expr::Column(datafusion::common::Column::new(
                                            qualifier.cloned(),
                                            name,
                                        ))
                                    }
                                })
                                .collect();

                            if !exprs.is_empty() {
                                builder = builder.project(exprs).map_err(|e| {
                                    datafusion::error::DataFusionError::Internal(format!(
                                        "Failed to create policy projection: {e}"
                                    ))
                                })?;
                            }
                        }

                        // 2b. Re-apply the predicates taken off the scan, now on
                        //     the masked values.
                        if let Some(pred) =
                            datafusion::logical_expr::utils::conjunction(raw_on_governed)
                        {
                            builder = builder.filter(pred).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to re-apply a scan predicate above the mask: {e}"
                                ))
                            })?;
                        }

                        // 3. Restore the original output columns when the scan's
                        //    projection was widened to give a row filter the column
                        //    it needed. The widening must not be observable: a
                        //    caller that selected two columns gets two, and the
                        //    column the filter needed never reaches the result.
                        if let Some(restore) = restore {
                            builder = builder.project(restore).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to restore the projection after widening it \
                                     for a row filter: {e}"
                                ))
                            })?;
                        }

                        let current = builder.build().map_err(|e| {
                            datafusion::error::DataFusionError::Internal(format!(
                                "Failed to build policy plan: {e}"
                            ))
                        })?;

                        // Jump: skip descending into the wrappers we just
                        // injected. Continue would re-enter the inner
                        // TableScan and rewrap forever.
                        return Ok(Transformed::new(current, true, TreeNodeRecursion::Jump));
                    }
                }
                Ok(Transformed::no(node))
            })
            .map_err(|e| {
                sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::ExecutionFailed, format!("Plan rewrite failed: {e}"))
            })?;

        Ok((rewritten.data, summary))
    }
}

/// True when `policy`'s row filters carry the deny-all `lit(false)` sentinel
/// (resolve failure, breaker-open, unknown-tag state, or a fully-restricted
/// table). The one place this check lives: `summarise_policies` below, the
/// `SHOW EFFECTIVE POLICY` diagnostic, and any table-level gate outside the
/// plan rewriter (e.g. a Hive DDL/introspection path with no LogicalPlan to
/// run the rewriter over) all call this instead of re-deriving the
/// literal-equality check by hand.
pub fn policy_denies_all(policy: &ResolvedPolicy) -> bool {
    policy.row_filters.iter().any(is_deny_all_expr)
}

/// The single `lit(false)` equality check `policy_denies_all` and
/// `summarise_policies`'s per-filter loop both need. Not `pub`: callers
/// outside this module want the whole-policy answer (`policy_denies_all`),
/// not a per-`Expr` one.
fn is_deny_all_expr(f: &Expr) -> bool {
    let deny = lit(false);
    f == &deny
}

/// Fold the per-table resolved policies into a single audit summary.
///
/// `denied` is true when any policy injected a deny-all `lit(false)` row filter
/// (resolve failure, breaker-open, unknown-tag state, or a column-isExcludes
/// datamask). The `lit(false)` sentinel is excluded from `row_filters_applied`
/// so the count reflects real, user-visible row filters only. Masked and
/// restricted column names are deduplicated and sorted for stable audit output.
fn summarise_policies(table_policies: &HashMap<String, ResolvedPolicy>) -> PolicySummary {
    let mut row_filters_applied = 0usize;
    let mut masked: HashSet<String> = HashSet::new();
    let mut restricted: HashSet<String> = HashSet::new();
    let mut masked_fields: HashSet<(String, String)> = HashSet::new();
    let mut denied = false;

    for (table, policy) in table_policies {
        for f in &policy.row_filters {
            if is_deny_all_expr(f) {
                denied = true;
            } else {
                row_filters_applied += 1;
            }
        }
        for col in policy.column_masks.keys() {
            masked.insert(col.clone());
            masked_fields.insert((table.clone(), col.clone()));
        }
        for col in &policy.restricted_columns {
            restricted.insert(col.clone());
            masked_fields.insert((table.clone(), col.clone()));
        }
    }

    let mut columns_masked: Vec<String> = masked.into_iter().collect();
    columns_masked.sort();
    let mut columns_restricted: Vec<String> = restricted.into_iter().collect();
    columns_restricted.sort();
    let mut masked_fields: Vec<(String, String)> = masked_fields.into_iter().collect();
    masked_fields.sort();

    PolicySummary {
        row_filters_applied,
        columns_masked,
        columns_restricted,
        denied,
        masked_fields,
    }
}

/// Resolve the effective policy for a single (user, table) pair for the
/// `SHOW EFFECTIVE POLICY` diagnostic, mirroring the per-table resolution the
/// `PlanRewriter` performs at query time.
///
/// The flow matches the rewriter exactly: `store.resolve()` for the resource
/// policy, then (when `col_tags` is non-empty) `store.resolve_tags()` followed
/// by `merge_tag_masks` with the same precedence contract. The caller supplies
/// `col_tags` (the table's `column -> tags` map, read from the loaded Iceberg
/// metadata) rather than threading the rewriter's `TagSource` cache. On a
/// resolve failure this returns the same deny-all sentinel (`lit(false)` row
/// filter) the rewriter would inject, so the diagnostic never under-reports.
///
/// `namespace`/`table` must be the SAME policy key the rewriter uses
/// (`resolve_policy_key` -> the full dotted namespace), or the diagnostic would
/// describe a different policy than enforcement applies. `catalog` is the
/// planner's name for the table's catalog: under `catalog_scoped` it picks the
/// catalog's tag policies (#919, [`crate::tag_scope`]). It is passed rather
/// than read off `namespace`, which not every caller prefixes.
pub async fn resolve_effective_policy(
    store: &dyn PolicyStore,
    user: &sqe_core::session::SessionUser,
    catalog: Option<&str>,
    table: &str,
    namespace: &str,
    col_tags: &HashMap<String, Vec<String>>,
    precedence: sqe_core::config::MaskPrecedence,
) -> ResolvedPolicy {
    let mut policy = match store.resolve(user, table, namespace).await {
        Ok(p) => p,
        Err(_) => {
            // Same deny-all sentinel the rewriter injects on resolve failure.
            let mut deny = ResolvedPolicy::default();
            deny.row_filters.push(lit(false));
            return deny;
        }
    };

    let all_tags: HashSet<String> = col_tags.values().flatten().cloned().collect();
    if !all_tags.is_empty() {
        let (tag_masks_by_tag, tag_filters, unmappable_tags) =
            crate::tag_scope::resolve_tags_in_catalog(store, user, &all_tags, catalog).await;
        let identity = SessionIdentity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            database: None,
            schema: None,
        };
        let conflicts = merge_tag_masks(
            &mut policy,
            col_tags,
            &tag_masks_by_tag,
            tag_filters,
            &unmappable_tags,
            &identity,
            precedence,
        );
        // A statement on this table is refused (#918, TA-13); the callers here only
        // read the policy, so they see it denied, fail closed.
        if !conflicts.is_empty() {
            let mut deny = ResolvedPolicy::default();
            deny.row_filters.push(lit(false));
            return deny;
        }
    }

    policy
}

/// Merge tag-derived masks and row filters into an existing `ResolvedPolicy`.
///
/// # Precedence (LOCKED — security contract)
///
/// 1. **Restricted columns always win.** A column already in
///    `policy.restricted_columns` stays dropped; no tag can un-restrict it.
/// 2. **Resource masks win over tag masks.** If `policy.column_masks` already
///    contains a mask for a column (from `store.resolve()`), that resource-
///    level mask is more specific and MUST NOT be overwritten by a tag mask.
/// 3. **One row filter per table and user.** A tag row filter and a
///    different resource filter (or two different tag filters) are a
///    [`PolicyConflict::RowFilter`] under every precedence (TA-13); see
///    [`merge_tag_row_filters`]. Until TA-13 they were ANDed.
/// 4. **Several tag masks on one column join to the most restrictive.** The
///    masks of all the column's tags are collected (a tag with no mask for the
///    user adds nothing) and joined by [`join_masks`], whatever the order of
///    the tags: `Nullify` beats every mask, `Redact` beats the rest, partial
///    masks join to their meet (fewest characters shown, no character class
///    unless all agree), and masks that cannot be joined (for example `Hash`
///    next to a `PartialMask`) are a [`PolicyConflict::Mask`]:
///    the column is restricted and the conflict returned. So adding a tag never
///    loosens a column's mask through another tag. If any tag's CUSTOM template
///    fails to parse, no tag mask is applied (rule 6).
/// 5. **Unmappable tags fail closed.** A tag whose mask type is genuinely
///    unsupported appears in `unmappable_tags`. A column carrying such a tag is
///    RESTRICTED (dropped) when it has no resource mask — mirroring the resource
///    path's `Err(())` -> `restricted_columns` behaviour. A resource mask still
///    wins (precedence rule 2): a more-specific resource mask is sufficient
///    protection, so the column is not dropped.
/// 6. **CUSTOM tag masks are now supported.** `TagMaskSpec::Custom(template)`
///    carries the raw `{col}` template from Ranger. For each column bearing the
///    tag, the column name is substituted into the template and the result is
///    parsed as a SQL expression via `parse_sql_predicate`. On success the column
///    receives `MaskType::Custom(expr)`. On any parse failure the column is
///    restricted (fail-closed) unless a resource mask covers it.
///
/// Under [`MaskPrecedence::Refuse`](sqe_core::config::MaskPrecedence::Refuse)
/// nothing wins: see [`merge_tag_masks_refusing`]. Under every other
/// precedence the returned conflicts hold the row filter conflict (rule 3) and
/// the columns whose tag masks cannot be joined (rule 4); a resource mask and
/// a tag mask never conflict there.
pub(crate) fn merge_tag_masks(
    policy: &mut ResolvedPolicy,
    col_tags: &HashMap<String, Vec<String>>,
    tag_masks_by_tag: &HashMap<String, TagMaskSpec>,
    tag_filters: Vec<datafusion::logical_expr::Expr>,
    unmappable_tags: &HashSet<String>,
    identity: &SessionIdentity,
    precedence: sqe_core::config::MaskPrecedence,
) -> Vec<PolicyConflict> {
    if precedence == sqe_core::config::MaskPrecedence::Refuse {
        return merge_tag_masks_refusing(
            policy,
            col_tags,
            tag_masks_by_tag,
            tag_filters,
            unmappable_tags,
            identity,
        );
    }
    let mut conflicts = Vec::new();
    for (column, tags) in col_tags {
        // Restricted columns always win — tag cannot un-restrict. This is not
        // configurable: restriction means SQE could not establish a safe value
        // to return, and no tag rule can answer that question for it.
        if policy.is_restricted(column) {
            continue;
        }
        // A resource mask already covers this column. Under `Resource` the more
        // specific rule stands and the tag is skipped; under `Tag` (the default,
        // matching Hive/Kyuubi) the tag mask overwrites it below.
        //
        // Either way the column stays masked, so an unmappable tag must NOT
        // restrict it: the resource mask is sufficient protection and dropping
        // the column would be a strictly worse answer than masking it.
        let resource_masked = policy.mask_for(column).is_some();
        if resource_masked && precedence == sqe_core::config::MaskPrecedence::Resource {
            continue;
        }
        // Fail-closed: if any of the column's tags is unmappable (genuinely
        // unsupported type, no resource mask above), restrict the column rather
        // than leak it raw. Mirrors the resource path's `Err(())` ->
        // restricted_columns behaviour.
        //
        // `!resource_masked` is load-bearing under `Tag` precedence, which no
        // longer skips masked columns early: an unmappable tag on a column that
        // already has a working resource mask must leave that mask in place.
        // Restricting it would replace a readable masked value with NULL and
        // call it an improvement.
        if !resource_masked && tags.iter().any(|t| unmappable_tags.contains(t)) {
            policy.restricted_columns.push(column.clone());
            continue;
        }
        // Collect the mask of EVERY tag on the column and join them into the
        // most restrictive one (rule 4). A tag without a mask for this user
        // (for example `MASK_NONE`) contributes nothing.
        let mut masks: Vec<MaskType> = Vec::new();
        let mut broken = false;
        for tag in tags {
            let mask = match tag_masks_by_tag.get(tag) {
                Some(TagMaskSpec::Ready(mask)) => mask.clone(),
                Some(TagMaskSpec::Custom(template)) => {
                    // Substitute the real column name for the `{col}` placeholder
                    // and parse the resulting expression.
                    let substituted = template.replace("{col}", column);
                    match parse_sql_predicate(&substituted, identity) {
                        Ok(expr) => MaskType::Custom(expr),
                        Err(e) => {
                            // Do not log `template`: a CUSTOM mask body can embed
                            // sensitive literals or keyed values. Column + tag are
                            // enough to locate the offending Ranger policy.
                            warn!(
                                column = %column,
                                tag = %tag,
                                error = %e,
                                "CUSTOM tag mask expression failed to parse"
                            );
                            broken = true;
                            continue;
                        }
                    }
                }
                None => continue,
            };
            if !masks.contains(&mask) {
                masks.push(mask);
            }
        }
        if broken {
            // A broken tag template must not cost the column a working resource
            // mask, and the other tags' masks are not applied: the broken one
            // may have been the strictest. Restrict only when nothing else
            // masks the column (fail-closed).
            if !resource_masked {
                policy.restricted_columns.push(column.clone());
            }
            continue;
        }
        if masks.is_empty() {
            continue;
        }
        match join_masks(&masks) {
            Some(mask) => {
                policy.column_masks.insert(column.clone(), mask);
            }
            None => {
                // The masks cannot be joined (e.g. `Hash` next to a partial
                // mask). Same outcome as `Refuse` for this column.
                policy.column_masks.remove(column);
                policy.restricted_columns.push(column.clone());
                conflicts.push(PolicyConflict::Mask(column.clone()));
            }
        }
    }
    // One row filter per table and user, whatever the mask precedence (TA-13).
    conflicts.extend(merge_tag_row_filters(policy, tag_filters));
    conflicts.sort();
    conflicts
}

/// Join the masks that several tags put on one column into the single most
/// restrictive one. The result does not depend on the order of `masks`.
/// `None` means the masks cannot be joined: the caller treats that as a
/// conflict.
///
/// - `Nullify` beats every other mask.
/// - `Redact(s)` beats every mask except `Nullify`. Two different redaction
///   strings join to `Nullify`: both reveal nothing, and `Nullify` is at least
///   as restrictive as either.
/// - Partial masks join to their meet, see [`meet_partial_masks`].
/// - Any other mix of distinct masks (`Hash`, `DateShowYear`, `Custom`, or one
///   of them next to a partial mask) cannot be joined.
fn join_masks(masks: &[MaskType]) -> Option<MaskType> {
    if masks.iter().any(|m| matches!(m, MaskType::Nullify)) {
        return Some(MaskType::Nullify);
    }
    let mut redactions: Vec<&String> = Vec::new();
    for m in masks {
        if let MaskType::Redact(s) = m {
            if !redactions.contains(&s) {
                redactions.push(s);
            }
        }
    }
    match redactions.as_slice() {
        [] => {}
        [one] => return Some(MaskType::Redact((*one).clone())),
        _ => return Some(MaskType::Nullify),
    }
    let first = masks.first()?;
    if masks.iter().all(|m| m == first) {
        return Some(first.clone());
    }
    meet_partial_masks(masks)
}

/// The meet of several partial masks: the fewest characters shown at either
/// end, so it reveals no more than any input. The replacement chars stay when
/// every input uses the same ones; otherwise the meet reveals no character
/// class and replaces every character with one char (the smallest class-less
/// char among the inputs, else Ranger's `x`). Ranger's `MASK` (no characters
/// shown, classes visible) and `MASK_SHOW_LAST_4` (last four shown, no class)
/// therefore join to a mask that shows nothing and no class. `None` when a
/// mask is not a partial mask.
fn meet_partial_masks(masks: &[MaskType]) -> Option<MaskType> {
    let mut parts = Vec::with_capacity(masks.len());
    for m in masks {
        match m {
            MaskType::PartialMask {
                show_first,
                show_last,
                upper,
                lower,
                digit,
            } => parts.push((*show_first, *show_last, (*upper, *lower, *digit))),
            _ => return None,
        }
    }
    let show_first = parts.iter().map(|p| p.0).min()?;
    let show_last = parts.iter().map(|p| p.1).min()?;
    let chars = parts[0].2;
    let (upper, lower, digit) = if parts.iter().all(|p| p.2 == chars) {
        chars
    } else {
        let c = parts
            .iter()
            .filter(|p| p.2 .0 == p.2 .1 && p.2 .1 == p.2 .2)
            .map(|p| p.2 .0)
            .min()
            .unwrap_or('x');
        (c, c, c)
    };
    Some(MaskType::PartialMask {
        show_first,
        show_last,
        upper,
        lower,
        digit,
    })
}

/// A column or table for which policies resolve to different answers.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PolicyConflict {
    /// Two different masks resolve for this column, under
    /// [`MaskPrecedence::Refuse`](sqe_core::config::MaskPrecedence::Refuse),
    /// or several tag masks on it cannot be joined, under every precedence.
    Mask(String),
    /// Tag row filters and a different filter resolve for the table, under
    /// every precedence (TA-13).
    RowFilter,
}

impl PolicyConflict {
    /// The statement error for this conflict on `table` (the qualified name).
    pub fn to_error(&self, table: &str) -> sqe_core::error::SqeError {
        let message = match self {
            Self::Mask(column) => format!(
                "mask conflict on {table}.{column}: more than one distinct column mask \
                 applies to this column for this user"
            ),
            Self::RowFilter => format!(
                "row filter conflict on {table}: more than one distinct row filter \
                 applies to this table for this user"
            ),
        };
        sqe_core::error::SqeError::with_code(sqe_core::SqeErrorCode::AccessDenied, message)
    }
}

/// `merge_tag_masks` under `Refuse` (#918): every mask that resolves for a
/// column (its resource mask and each of its tags' masks) is collected, and
/// so is every row filter once a tag contributes one. One distinct mask (or
/// filter) applies as usual; the same mask from two sources is one mask.
/// More than one is a conflict: the column is restricted as well, so a
/// caller that only reads the policy (metadata, `SHOW STATS`) fails closed.
///
/// The fail-closed rules of the other modes hold: a restricted column stays
/// restricted, an unmappable tag on a column with no resource mask restricts
/// it, and a CUSTOM template that does not parse restricts the column when
/// nothing else masks it (and is otherwise ignored, as under `Tag`).
fn merge_tag_masks_refusing(
    policy: &mut ResolvedPolicy,
    col_tags: &HashMap<String, Vec<String>>,
    tag_masks_by_tag: &HashMap<String, TagMaskSpec>,
    tag_filters: Vec<datafusion::logical_expr::Expr>,
    unmappable_tags: &HashSet<String>,
    identity: &SessionIdentity,
) -> Vec<PolicyConflict> {
    let mut conflicts = Vec::new();
    for (column, tags) in col_tags {
        if policy.is_restricted(column) {
            continue;
        }
        let resource_mask = policy.mask_for(column).cloned();
        if resource_mask.is_none() && tags.iter().any(|t| unmappable_tags.contains(t)) {
            policy.restricted_columns.push(column.clone());
            continue;
        }
        let mut masks: Vec<MaskType> = resource_mask.into_iter().collect();
        let mut broken = false;
        for tag in tags {
            let mask = match tag_masks_by_tag.get(tag) {
                Some(TagMaskSpec::Ready(mask)) => mask.clone(),
                Some(TagMaskSpec::Custom(template)) => {
                    match parse_sql_predicate(&template.replace("{col}", column), identity) {
                        Ok(expr) => MaskType::Custom(expr),
                        Err(e) => {
                            warn!(
                                column = %column,
                                tag = %tag,
                                error = %e,
                                "CUSTOM tag mask expression failed to parse"
                            );
                            broken = true;
                            continue;
                        }
                    }
                }
                None => continue,
            };
            if !masks.contains(&mask) {
                masks.push(mask);
            }
        }
        match masks.as_slice() {
            [] if broken => policy.restricted_columns.push(column.clone()),
            [] => {}
            [mask] => {
                policy.column_masks.insert(column.clone(), mask.clone());
            }
            [_, _, ..] => {
                policy.column_masks.remove(column);
                policy.restricted_columns.push(column.clone());
                conflicts.push(PolicyConflict::Mask(column.clone()));
            }
        }
    }

    conflicts.extend(merge_tag_row_filters(policy, tag_filters));
    conflicts.sort();
    conflicts
}

/// One row filter per table and user (TA-13), under every precedence, the
/// rule Unity Catalog ABAC follows. Once a tag contributes a row filter,
/// every distinct filter that resolves for the table (the resource filter and
/// each tag's) is counted, and more than one is a conflict. The same filter
/// from two sources is one filter. The deny-all sentinel is not a filter a
/// policy chose, so it is kept as is and never counted.
///
/// Every filter stays in the policy, so a caller that ignores the conflict
/// still reads the most restrictive combination. Resource filters with no tag
/// filter are left alone: several resource policies matching one user still
/// AND, as before.
fn merge_tag_row_filters(
    policy: &mut ResolvedPolicy,
    tag_filters: Vec<datafusion::logical_expr::Expr>,
) -> Option<PolicyConflict> {
    if tag_filters.is_empty() {
        return None;
    }
    let (mut sentinels, mut filters): (Vec<_>, Vec<_>) = std::mem::take(&mut policy.row_filters)
        .into_iter()
        .partition(is_deny_all_expr);
    for f in tag_filters {
        if is_deny_all_expr(&f) {
            sentinels.push(f);
        } else if !filters.contains(&f) {
            filters.push(f);
        }
    }
    let conflict = (filters.len() > 1).then_some(PolicyConflict::RowFilter);
    policy.row_filters = sentinels;
    policy.row_filters.extend(filters);
    conflict
}

/// Derive the `(namespace, table)` policy key from a structured DataFusion
/// `TableReference`.
///
/// DataFusion's `TableReference` carries at most three slots: catalog,
/// schema, table. Multi-level Iceberg namespaces (`a.b`) are registered as a
/// single DataFusion schema name `"a.b"` (see
/// `sqe-catalog/catalog_provider.rs`), so a 4-part qualified name like
/// `cat.a.b.t` lands here as schema `"a.b"`, table `"t"`.
///
/// The policy key MUST match the write path, which keys by
/// the full dotted namespace (see `resolve_policy_key`). For a schema of
/// `"a.b"` the last namespace component is `"b"`. We take the last dotted
/// component of the schema so reads and writes resolve the same policy.
///
/// Returns `None` when no table component can be determined. The caller
/// treats `None` as fail-closed (deny all rows) rather than passthrough.
/// True when `table_ref` names a reserved, virtual (non-Iceberg) relation that
/// cannot carry Iceberg column tags and is never present in the table metadata
/// cache: `information_schema.*` (DataFusion reserves this schema in every
/// catalog), SQE's virtual `system` schemas (`system.runtime.*`,
/// `system.metadata.*`, `system.jdbc.*`), and anything under `datafusion`.
///
/// These names cannot be used for a user Iceberg table (DataFusion reserves the
/// schema; the `system` composite never serves a Polaris namespace of a
/// reserved name; `datafusion` cannot be attached), so skipping the tag lookup
/// here introduces no governance bypass: a real Iceberg table can never match.
/// The resource policy (`store.resolve`) still runs for these refs.
///
/// Any other schema under `system` is a Polaris catalog `system` (#901) and is
/// governed like every catalog. The match is the one the composite routes by
/// (`sqe_core::system_catalog`), so the two cannot disagree.
///
/// `pg_catalog` is intentionally NOT matched: SQE does not register it as a
/// virtual provider, so a user namespace named `pg_catalog` must still be tag-
/// governed.
fn is_reserved_virtual_ref(table_ref: &datafusion::common::TableReference) -> bool {
    if let Some(cat) = table_ref.catalog() {
        if cat.eq_ignore_ascii_case(sqe_core::system_catalog::DATAFUSION_CATALOG) {
            return true;
        }
        if let Some(schema) = table_ref.schema() {
            if sqe_core::system_catalog::is_virtual_system_ref(cat, schema) {
                return true;
            }
        }
    }
    // information_schema is reserved in every catalog. Match on the LAST schema
    // component so `cat.information_schema.tables` (qualified) and
    // `information_schema.tables` (bare) both match.
    matches!(
        table_ref.schema(),
        Some(schema)
            if schema
                .rsplit('.')
                .next()
                .unwrap_or(schema)
                .eq_ignore_ascii_case("information_schema")
    )
}
/// Defined in `sqe-policy-api` (build-time item C1) so `sqe-catalog` can
/// register Hive roots without depending on this crate; see its module doc for
/// why it is read through a lock on every key resolution.
pub use sqe_policy_api::HiveCatalogs;

/// Derive the `(namespace, table)` policy key from a structured DataFusion
/// `TableReference`.
///
/// The Iceberg branch is unchanged from issue #205: the FULL (possibly
/// multi-level) namespace string, falling back to `"default"` for a bare
/// table. This used to take only the last dotted component, which collided
/// (`sales` and `a.b.sales` both resolved to the Ranger database `sales`), so
/// truncating again would reopen that hole.
///
/// The Hive branch is new: when `table_ref`'s catalog name is registered in
/// `hive_catalogs`, the key carries that root name (`"{root}.{namespace}"`)
/// instead. Whoever can write a Hive manifest picks the table's name and its
/// `LOCATION`, so sharing a key with a same-named Iceberg table would be
/// privilege escalation, not a coincidence to shrug off. The asymmetry is
/// deliberate: every Ranger policy already written against the
/// `hive_database` convention keys on the bare Iceberg namespace, and making
/// both branches symmetric would be its own migration, not this one.
pub fn resolve_policy_key(
    table_ref: &datafusion::common::TableReference,
    hive_catalogs: &HiveCatalogs,
) -> Option<(String, String)> {
    resolve_policy_key_scoped(table_ref, hive_catalogs, false)
}

/// `resolve_policy_key` for a store that may key on the catalog
/// (`PolicyStore::catalog_scoped`, `[policy.ranger] catalog_scoped`).
///
/// With `catalog_scoped` the Iceberg branch returns `"{catalog}.{namespace}"`,
/// the same shape the Hive branch always had, so a portal running one Iceberg
/// catalog per workspace with identical `namespace.table` names gets a distinct
/// key, and so a distinct Ranger `database`, per catalog. A reference that
/// carries no catalog cannot be keyed that way and returns `None`, which every
/// caller turns into a deny: falling back to the bare namespace would match
/// nothing written for the scoped convention, and a policy that does not match
/// is a mask that does not apply. The exceptions are a reserved virtual
/// reference (`information_schema`, the `system` catalog), which holds no user
/// rows and is keyed as before, and a schema that already starts with a
/// registered Hive root (the Hive write probe's `{root}.{database}`).
///
/// The rewriter qualifies an unqualified scan with the session's catalog and
/// schema (`TableScope::qualify`) before calling this, so `SELECT * FROM t`
/// under `USE ws_a.finance` keys as `ws_a.finance`.
pub fn resolve_policy_key_scoped(
    table_ref: &datafusion::common::TableReference,
    hive_catalogs: &HiveCatalogs,
    catalog_scoped: bool,
) -> Option<(String, String)> {
    let table = table_ref.table();
    if table.is_empty() {
        return None;
    }

    let namespace = match table_ref.schema() {
        Some(schema) if !schema.is_empty() => schema.to_string(),
        _ => "default".to_string(),
    };

    if hive_catalogs.contains_name(table_ref.catalog()) {
        let root = table_ref.catalog().unwrap_or_default();
        return Some((format!("{root}.{namespace}"), table.to_string()));
    }

    if catalog_scoped {
        match table_ref.catalog() {
            Some(catalog) if !catalog.is_empty() => {
                return Some((format!("{catalog}.{namespace}"), table.to_string()));
            }
            _ if is_reserved_virtual_ref(table_ref) => {}
            // A Hive write probe passes its `{root}.{database}` key as the
            // schema with no catalog; that key already names the root.
            None if hive_catalogs.contains_name(namespace.split('.').next()) => {}
            _ => {
                warn!(
                    table = %table_ref,
                    "catalog-scoped policy key needs a catalog and the reference has none; denying"
                );
                return None;
            }
        }
    }

    Some((namespace, table.to_string()))
}

/// The same key rule for callers that hold an Iceberg-style identity
/// (`catalog`, namespace path, table) instead of a DataFusion
/// `TableReference`: DML handlers, `information_schema.columns`, the
/// write-predicate probe. Every policy lookup in the engine must go through
/// one of these two entry points; the review found three hand-copied
/// "last namespace component" variants that had drifted from the read path
/// (architecture review 201), so the rule is now called, not copied.
///
/// The namespace path is display-joined with `.`, exactly as the SQL planner
/// spells a multi-level namespace in the scan's `TableReference`.
pub fn resolve_policy_key_for_ident(
    catalog: Option<&str>,
    namespace: &[String],
    table: &str,
    hive_catalogs: &HiveCatalogs,
) -> Option<(String, String)> {
    resolve_policy_key_for_ident_scoped(catalog, namespace, table, hive_catalogs, false)
}

/// `resolve_policy_key_for_ident` with the store's `catalog_scoped` flag; see
/// `resolve_policy_key_scoped`.
pub fn resolve_policy_key_for_ident_scoped(
    catalog: Option<&str>,
    namespace: &[String],
    table: &str,
    hive_catalogs: &HiveCatalogs,
    catalog_scoped: bool,
) -> Option<(String, String)> {
    use datafusion::common::TableReference;
    let namespace = namespace.join(".");
    let table_ref = match (catalog, namespace.is_empty()) {
        (Some(catalog), false) => TableReference::full(catalog, namespace, table),
        (_, false) => TableReference::partial(namespace, table),
        (_, true) => TableReference::bare(table),
    };
    resolve_policy_key_scoped(&table_ref, hive_catalogs, catalog_scoped)
}

/// Widen a `TableScan`'s projection so a row filter can reference a column the
/// query itself does not select, returning the columns to project back to.
///
/// A row filter is enforced above the scan, and `LogicalPlanBuilder::filter`
/// normalises it against the scan's schema. When the scan carries a pushed-down
/// projection -- which is what a narrow VIEW expands to -- a filter on an
/// unselected column failed the whole query with "Schema error: No field named
/// region". Fail-closed, so never a leak, but it made a row filter and a narrow
/// view over the same table mutually exclusive, and the message named neither the
/// policy nor the view.
///
/// Widening is internal: the returned expression list restores the original output
/// columns, so the caller sees exactly the schema it asked for and the extra column
/// cannot reach the result. Returns `None` when nothing needs widening, which is
/// every scan without a projection -- including a direct query, because the
/// rewriter runs BEFORE projection pushdown.
///
/// Dropping the filter instead would turn a hard error into a silent leak, which is
/// the one outcome worse than the error being replaced.
fn widen_scan_for_row_filters(
    scan: &datafusion::logical_expr::TableScan,
    row_filters: &[Expr],
) -> Option<(LogicalPlan, Vec<Expr>)> {
    let projection = scan.projection.as_ref()?;
    if row_filters.is_empty() {
        return None;
    }
    let source_schema = scan.source.schema();

    // Columns the filters need, by name.
    let mut wanted: Vec<String> = Vec::new();
    for f in row_filters {
        for c in f.column_refs() {
            if !wanted.contains(&c.name) {
                wanted.push(c.name.clone());
            }
        }
    }

    // Which of those exist in the table but are absent from the projection.
    let mut widened = projection.clone();
    for name in &wanted {
        let Ok(idx) = source_schema.index_of(name) else {
            // Not a column of this table. Leave it alone: `filter()` will report
            // it, and inventing a column would be worse than a clear failure.
            continue;
        };
        if !widened.contains(&idx) {
            widened.push(idx);
        }
    }
    if widened.len() == projection.len() {
        return None;
    }

    // What the caller must see afterwards: the ORIGINAL columns, qualified as the
    // scan exposes them.
    let restore: Vec<Expr> = scan
        .projected_schema
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(datafusion::common::Column::new(
                qualifier.cloned(),
                field.name(),
            ))
        })
        .collect();

    // DataFusion 55 deprecated TableScan::try_new in favour of TableScanBuilder.
    // This is a copy of `scan` with a wider projection, so carry the statistics
    // requests across too: the builder defaults them to empty, and silently
    // dropping them here would quietly change what the scan asks for.
    let wider = datafusion::logical_expr::TableScanBuilder::new(
        scan.table_name.clone(),
        scan.source.clone(),
    )
    .with_projection(Some(widened))
    .with_filters(scan.filters.clone())
    .with_fetch(scan.fetch)
    .with_statistics_requests(scan.statistics_requests.clone())
    .build()
    .ok()?;
    Some((LogicalPlan::TableScan(wider), restore))
}

/// A typed NULL literal of `data_type` (so projection output type == column
/// type). Falls back to a Utf8 NULL cast into the target type for Arrow types
/// that have no direct ScalarValue::try_from.
fn typed_null(data_type: &DataType) -> Expr {
    match ScalarValue::try_from(data_type) {
        Ok(scalar) => lit(scalar),
        Err(_) => Expr::Cast(Cast::new(
            Box::new(lit(ScalarValue::Utf8(None))),
            data_type.clone(),
        )),
    }
}

/// Apply a mask type to a column, returning the masking expression.
///
/// `data_type` is the Arrow type of the original column. The returned
/// expression has the same type so downstream operators (Filter, Join,
/// GroupBy) see the column shape they expect. A Utf8 NULL substituted for
/// a BIGINT column would either fail optimization or, worse, coerce both
/// sides of a predicate to Utf8 and leak masked rows.
fn apply_mask(
    column_name: &str,
    data_type: &DataType,
    mask: &MaskType,
    mask_key: Option<Arc<Vec<u8>>>,
) -> Expr {
    match mask {
        MaskType::Nullify => typed_null(data_type),
        MaskType::Redact(value) => {
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                lit(value.clone())
            } else {
                Expr::Cast(Cast::new(Box::new(lit(value.clone())), data_type.clone()))
            }
        }
        MaskType::Hash => {
            // sha256 UDF returns Utf8; cast back to the column type so the
            // projection schema matches. Non-string columns will fail the
            // cast at runtime, which is the correct signal: hashing a
            // BIGINT into itself is meaningless. The UDF runs HMAC-SHA256
            // when `mask_key` is `Some`, plain SHA-256 otherwise. Plain
            // mode is vulnerable to offline brute force on low-entropy
            // values (issue #37).
            let hash =
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::sha256_udf::sha256_udf(mask_key)),
                    vec![col(column_name)],
                ));
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                hash
            } else {
                Expr::Cast(Cast::new(Box::new(hash), data_type.clone()))
            }
        }
        MaskType::Custom(expr) => expr.clone(),
        MaskType::PartialMask {
            show_first,
            show_last,
            upper,
            lower,
            digit,
        } => {
            let mask_expr = |inner: Expr| {
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::mask_udf::mask_partial_udf(
                        *show_first,
                        *show_last,
                        *upper,
                        *lower,
                        *digit,
                    )),
                    vec![inner],
                ))
            };
            match data_type {
                // Utf8 column: mask directly (UDF returns Utf8 == column type).
                DataType::Utf8 => mask_expr(col(column_name)),
                // LargeUtf8 / Utf8View column (Iceberg scans read strings as
                // Utf8View by default): the UDF only accepts Utf8, and the
                // output must keep the column type. Cast in and back. The
                // named target field keeps the cast in, so a JSON column is
                // masked as its text: `sqe_json_cast` plans only unnamed
                // (query-written) casts as Trino's JSON-to-varchar.
                DataType::LargeUtf8 | DataType::Utf8View => Expr::Cast(Cast::new(
                    Box::new(mask_expr(Expr::Cast(Cast::new_from_field(
                        Box::new(col(column_name)),
                        Arc::new(Field::new(column_name, DataType::Utf8, true)),
                    )))),
                    data_type.clone(),
                )),
                // Non-string column: a char-class mask is meaningless; fall back
                // to a typed NULL so the value is hidden AND output type ==
                // column type (no type_coercion failure).
                _ => {
                    warn!(column = %column_name, ?data_type,
                        "PartialMask on non-string column; falling back to NULL");
                    typed_null(data_type)
                }
            }
        }
        MaskType::DateShowYear => match data_type {
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
                let truncated =
                    datafusion::functions::expr_fn::date_trunc(lit("year"), col(column_name));
                // date_trunc returns a Timestamp; cast back to the column's exact
                // type so the projection schema matches (Date32 stays Date32).
                Expr::Cast(Cast::new(Box::new(truncated), data_type.clone()))
            }
            _ => {
                warn!(column = %column_name, ?data_type,
                    "DateShowYear on non-temporal column; falling back to NULL");
                typed_null(data_type)
            }
        },
    }
}

#[cfg(test)]
mod tests;
