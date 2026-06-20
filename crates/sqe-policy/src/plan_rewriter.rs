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
//! - User predicates CAN push through row filters (same semantics)
//! - User predicates CANNOT push through column masks (expression boundary)
//! - Restricted columns are invisible (not errors)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::DataType;
use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{col, lit, Cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::scalar::ScalarValue;
use tracing::{debug, warn};

use sqe_core::SessionUser;

use crate::policy_expr::parse_sql_predicate;
use crate::session_udf::SessionIdentity;
use crate::{
    MaskType, NoopTagSource, PolicyEnforcer, PolicySummary, PolicyStore, ResolvedPolicy,
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
}

impl PolicyPlanRewriter {
    pub fn new(store: Arc<dyn PolicyStore>) -> Self {
        Self {
            store,
            mask_key: None,
            tag_source: Arc::new(NoopTagSource),
        }
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
}

#[async_trait]
impl PolicyEnforcer for PolicyPlanRewriter {
    async fn evaluate(
        &self,
        user: &SessionUser,
        plan: LogicalPlan,
    ) -> sqe_core::Result<(LogicalPlan, PolicySummary)> {
        let store = self.store.clone();
        let mask_key = self.mask_key.clone();
        let tag_source = self.tag_source.clone();
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
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let table_name = scan.table_name.to_string();
                if let Ok(provider) =
                    datafusion::datasource::source_as_provider(&scan.source)
                {
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
                table_refs
                    .entry(table_name)
                    .or_insert_with(|| scan.table_name.clone());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .map_err(|e| sqe_core::error::SqeError::Execution(format!("Plan traversal failed: {e}")))?;

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
            // (write_handler.rs keys by `namespace().last()`), otherwise
            // reads and writes resolve different policies for the same table.
            let Some((namespace, table)) = resolve_policy_key(table_ref) else {
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
            // policy store. `namespace` here is the flattened last dotted
            // component of the schema (see `resolve_policy_key`), which is what
            // Ranger `database` resource values must match. When a policy silently
            // does not fire, operators compare `lookup_database` against the
            // Ranger policy resource to catch a multi-level-namespace mismatch
            // (e.g. schema "ns1.ns2" flattens to "ns2"). No row values are logged.
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
                        Some(s) if !s.is_empty() => {
                            s.split('.').map(str::to_string).collect()
                        }
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
                    let tag_lookup = if is_reserved_virtual_ref(table_ref) {
                        Some(HashMap::new())
                    } else {
                        tag_source.column_tags(catalog, &ns_path, table_ref.table())
                    };

                    match tag_lookup {
                        // Tag state UNKNOWN (cache miss / disabled cache /
                        // unreadable metadata). Fail CLOSED exactly like the
                        // resolve-failure path below: a tag mask or tag
                        // row-filter may exist that we cannot see, so denying
                        // is the only safe answer. Skipping tag work here would
                        // silently bypass a security control.
                        None => {
                            warn!(
                                user = %username,
                                table = %table_name,
                                namespace = %namespace,
                                "Tag state unknown (cache miss or disabled); denying access"
                            );
                            let mut deny = ResolvedPolicy::default();
                            deny.row_filters.push(lit(false));
                            policy = deny;
                        }
                        // Known: at least one column carries tags. Resolve tag
                        // policies and merge them into the resolved policy.
                        Some(col_tags) if !col_tags.is_empty() => {
                            let all_tags: HashSet<String> =
                                col_tags.values().flatten().cloned().collect();

                            if !all_tags.is_empty() {
                                let (tag_masks_by_tag, tag_filters, unmappable_tags) =
                                    store.resolve_tags(&user_clone, &all_tags).await;

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
                                merge_tag_masks(
                                    &mut policy,
                                    &col_tags,
                                    &tag_masks_by_tag,
                                    tag_filters,
                                    &unmappable_tags,
                                    &identity,
                                );
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

        // Rewrite the plan
        let rewritten = plan
            .transform_down(|node| {
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
                        let mut builder = LogicalPlanBuilder::from(node);

                        // 1. Inject row filters above the TableScan.
                        for filter_expr in &policy.row_filters {
                            builder = builder.filter(filter_expr.clone()).map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Failed to create policy filter: {e}"
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
                        if !policy.column_masks.is_empty()
                            || !policy.restricted_columns.is_empty()
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
                                    if policy.restricted_columns.contains(name) {
                                        apply_mask(
                                            name,
                                            field.data_type(),
                                            &MaskType::Nullify,
                                            mask_key.clone(),
                                        )
                                        .alias_qualified(qualifier.cloned(), name.clone())
                                    } else if let Some(mask) = policy.column_masks.get(name) {
                                        apply_mask(
                                            name,
                                            field.data_type(),
                                            mask,
                                            mask_key.clone(),
                                        )
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
                sqe_core::error::SqeError::Execution(format!("Plan rewrite failed: {e}"))
            })?;

        Ok((rewritten.data, summary))
    }
}

/// Fold the per-table resolved policies into a single audit summary.
///
/// `denied` is true when any policy injected a deny-all `lit(false)` row filter
/// (resolve failure, breaker-open, unknown-tag state, or a column-isExcludes
/// datamask). The `lit(false)` sentinel is excluded from `row_filters_applied`
/// so the count reflects real, user-visible row filters only. Masked and
/// restricted column names are deduplicated and sorted for stable audit output.
fn summarise_policies(table_policies: &HashMap<String, ResolvedPolicy>) -> PolicySummary {
    let deny = lit(false);
    let mut row_filters_applied = 0usize;
    let mut masked: HashSet<String> = HashSet::new();
    let mut restricted: HashSet<String> = HashSet::new();
    let mut denied = false;

    for policy in table_policies.values() {
        for f in &policy.row_filters {
            if f == &deny {
                denied = true;
            } else {
                row_filters_applied += 1;
            }
        }
        for col in policy.column_masks.keys() {
            masked.insert(col.clone());
        }
        for col in &policy.restricted_columns {
            restricted.insert(col.clone());
        }
    }

    let mut columns_masked: Vec<String> = masked.into_iter().collect();
    columns_masked.sort();
    let mut columns_restricted: Vec<String> = restricted.into_iter().collect();
    columns_restricted.sort();

    PolicySummary {
        row_filters_applied,
        columns_masked,
        columns_restricted,
        denied,
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
/// `namespace`/`table` must be the SAME flattened policy key the rewriter uses
/// (`resolve_policy_key` -> last dotted namespace component), or the diagnostic
/// would describe a different policy than enforcement applies.
pub async fn resolve_effective_policy(
    store: &dyn PolicyStore,
    user: &sqe_core::session::SessionUser,
    table: &str,
    namespace: &str,
    col_tags: &HashMap<String, Vec<String>>,
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
            store.resolve_tags(user, &all_tags).await;
        let identity = SessionIdentity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            database: None,
            schema: None,
        };
        merge_tag_masks(
            &mut policy,
            col_tags,
            &tag_masks_by_tag,
            tag_filters,
            &unmappable_tags,
            &identity,
        );
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
/// 3. **Tag row filters are ANDed with resource row filters.** Both sets
///    apply; the result is the most restrictive combination.
/// 4. **Within-column tag ordering is deterministic.** The first tag in the
///    stored tag list that has a matching mask in `tag_masks_by_tag` wins.
///    This is stable because `col_tags` comes from Iceberg property JSON
///    (parsed order preserved) and the caller iterates it in that order.
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
///    restricted (fail-closed).
pub(crate) fn merge_tag_masks(
    policy: &mut ResolvedPolicy,
    col_tags: &HashMap<String, Vec<String>>,
    tag_masks_by_tag: &HashMap<String, TagMaskSpec>,
    tag_filters: Vec<datafusion::logical_expr::Expr>,
    unmappable_tags: &HashSet<String>,
    identity: &SessionIdentity,
) {
    for (column, tags) in col_tags {
        // Restricted columns always win — tag cannot un-restrict.
        if policy.restricted_columns.contains(column) {
            continue;
        }
        // Resource mask wins — do not overwrite with a tag mask, and do NOT
        // restrict even if an unmappable tag is present: the resource mask is
        // more specific and is sufficient protection.
        if policy.column_masks.contains_key(column) {
            continue;
        }
        // Fail-closed: if any of the column's tags is unmappable (genuinely
        // unsupported type, no resource mask above), restrict the column rather
        // than leak it raw. Mirrors the resource path's `Err(())` ->
        // restricted_columns behaviour.
        if tags.iter().any(|t| unmappable_tags.contains(t)) {
            policy.restricted_columns.push(column.clone());
            continue;
        }
        // Apply the first tag that has a matching mask spec (deterministic:
        // first match in stored tag order).
        for tag in tags {
            match tag_masks_by_tag.get(tag) {
                Some(TagMaskSpec::Ready(mask)) => {
                    policy.column_masks.insert(column.clone(), mask.clone());
                    break;
                }
                Some(TagMaskSpec::Custom(template)) => {
                    // Substitute the real column name for the `{col}` placeholder
                    // and parse the resulting expression. On parse failure restrict
                    // the column (fail-closed) rather than return it raw.
                    let substituted = template.replace("{col}", column);
                    match parse_sql_predicate(&substituted, identity) {
                        Ok(expr) => {
                            policy.column_masks.insert(column.clone(), MaskType::Custom(expr));
                        }
                        Err(e) => {
                            // Do not log `template`: a CUSTOM mask body can embed
                            // sensitive literals or keyed values. Column + tag are
                            // enough to locate the offending Ranger policy.
                            warn!(
                                column = %column,
                                tag = %tag,
                                error = %e,
                                "CUSTOM tag mask expression failed to parse; \
                                 restricting column (fail-closed)"
                            );
                            policy.restricted_columns.push(column.clone());
                        }
                    }
                    break;
                }
                None => { /* tag has no mask for this user; try the next tag */ }
            }
        }
    }
    // Tag row filters AND with resource row filters (most restrictive).
    policy.row_filters.extend(tag_filters);
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
/// `TableIdent::namespace().last()` (`write_handler.rs`). For a schema of
/// `"a.b"` the last namespace component is `"b"`. We take the last dotted
/// component of the schema so reads and writes resolve the same policy.
///
/// Returns `None` when no table component can be determined. The caller
/// treats `None` as fail-closed (deny all rows) rather than passthrough.
/// True when `table_ref` names a reserved, virtual (non-Iceberg) relation that
/// cannot carry Iceberg column tags and is never present in the table metadata
/// cache: `information_schema.*` (DataFusion reserves this schema in every
/// catalog), and anything under the reserved `system` / `datafusion` catalogs.
///
/// These names cannot be used for a user Iceberg table (DataFusion reserves the
/// schema; the coordinator reserves the catalog names), so skipping the tag
/// lookup here introduces no governance bypass: a real Iceberg table can never
/// match. The resource policy (`store.resolve`) still runs for these refs.
///
/// `pg_catalog` is intentionally NOT matched: SQE does not register it as a
/// virtual provider, so a user namespace named `pg_catalog` must still be tag-
/// governed.
fn is_reserved_virtual_ref(table_ref: &datafusion::common::TableReference) -> bool {
    // Reserved catalogs (cannot host a user Iceberg table).
    if let Some(cat) = table_ref.catalog() {
        if cat.eq_ignore_ascii_case("system") || cat.eq_ignore_ascii_case("datafusion") {
            return true;
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

fn resolve_policy_key(
    table_ref: &datafusion::common::TableReference,
) -> Option<(String, String)> {
    let table = table_ref.table();
    if table.is_empty() {
        return None;
    }

    // `schema()` is the (possibly multi-level) namespace string. Take its
    // last dotted component to match the write path's `namespace().last()`.
    // A bare table name with no schema falls back to "default", preserving
    // the existing 1-part behavior.
    let namespace = match table_ref.schema() {
        Some(schema) if !schema.is_empty() => {
            schema.rsplit('.').next().unwrap_or(schema).to_string()
        }
        _ => "default".to_string(),
    };

    Some((namespace, table.to_string()))
}

/// A typed NULL literal of `data_type` (so projection output type == column
/// type). Falls back to a Utf8 NULL cast into the target type for Arrow types
/// that have no direct ScalarValue::try_from.
fn typed_null(data_type: &DataType) -> Expr {
    match ScalarValue::try_from(data_type) {
        Ok(scalar) => lit(scalar),
        Err(_) => Expr::Cast(Cast::new(Box::new(lit(ScalarValue::Utf8(None))), data_type.clone())),
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
                Expr::Cast(Cast::new(
                    Box::new(lit(value.clone())),
                    data_type.clone(),
                ))
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
            let hash = Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::sha256_udf::sha256_udf(mask_key)),
                    vec![col(column_name)],
                ),
            );
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                hash
            } else {
                Expr::Cast(Cast::new(Box::new(hash), data_type.clone()))
            }
        }
        MaskType::Custom(expr) => expr.clone(),
        MaskType::PartialMask { show_first, show_last, upper, lower, digit } => {
            let mask_expr = |inner: Expr| {
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::mask_udf::mask_partial_udf(
                        *show_first, *show_last, *upper, *lower, *digit,
                    )),
                    vec![inner],
                ))
            };
            match data_type {
                // Utf8 column: mask directly (UDF returns Utf8 == column type).
                DataType::Utf8 => mask_expr(col(column_name)),
                // LargeUtf8 column: the UDF only accepts Utf8, and the output
                // must be LargeUtf8 to match the column type. Cast in and back.
                DataType::LargeUtf8 => Expr::Cast(Cast::new(
                    Box::new(mask_expr(Expr::Cast(Cast::new(
                        Box::new(col(column_name)),
                        DataType::Utf8,
                    )))),
                    DataType::LargeUtf8,
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
                let truncated = datafusion::functions::expr_fn::date_trunc(
                    lit("year"),
                    col(column_name),
                );
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
mod tests {
    use super::*;

    // ── summarise_policies (audit summary) tests ──────────────────────────────

    #[test]
    fn summary_counts_masks_filters_and_restrictions() {
        let mut policy = ResolvedPolicy::default();
        policy.row_filters.push(col("region").eq(lit("EU")));
        policy.column_masks.insert("ssn".to_string(), MaskType::Hash);
        policy.column_masks.insert("salary".to_string(), MaskType::Nullify);
        policy.restricted_columns.push("notes".to_string());
        let mut table_policies = HashMap::new();
        table_policies.insert("sales.orders".to_string(), policy);

        let s = summarise_policies(&table_policies);
        assert_eq!(s.row_filters_applied, 1);
        assert_eq!(s.columns_masked, vec!["salary".to_string(), "ssn".to_string()]);
        assert_eq!(s.columns_restricted, vec!["notes".to_string()]);
        assert!(!s.denied, "a real row filter is not a deny");
    }

    #[test]
    fn summary_marks_denied_on_deny_all_filter() {
        let mut policy = ResolvedPolicy::default();
        policy.row_filters.push(lit(false));
        let mut table_policies = HashMap::new();
        table_policies.insert("sales.orders".to_string(), policy);

        let s = summarise_policies(&table_policies);
        assert!(s.denied, "lit(false) deny-all must set denied=true");
        assert_eq!(
            s.row_filters_applied, 0,
            "the deny-all sentinel must not be counted as a real row filter"
        );
    }

    #[test]
    fn summary_default_when_no_policies() {
        let table_policies: HashMap<String, ResolvedPolicy> = HashMap::new();
        assert_eq!(summarise_policies(&table_policies), PolicySummary::default());
    }

    // ── merge_tag_masks precedence tests ──────────────────────────────────────

    fn make_col_tags(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(col, tags)| (col.to_string(), tags.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    fn make_tag_masks(pairs: &[(&str, MaskType)]) -> HashMap<String, TagMaskSpec> {
        pairs
            .iter()
            .map(|(t, m)| (t.to_string(), TagMaskSpec::Ready(m.clone())))
            .collect()
    }

    fn no_unmappable() -> HashSet<String> {
        HashSet::new()
    }

    fn default_identity() -> SessionIdentity {
        SessionIdentity {
            username: "test_user".to_string(),
            roles: vec![],
            database: None,
            schema: None,
        }
    }

    #[test]
    fn merge_tag_masks_applies_tag_mask_to_unmasked_column() {
        let mut policy = ResolvedPolicy::default();
        let col_tags = make_col_tags(&[("email", &["PII"])]);
        let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        assert!(
            matches!(policy.column_masks.get("email"), Some(MaskType::Nullify)),
            "tag mask must be applied when no resource mask exists"
        );
    }

    #[test]
    fn merge_tag_masks_resource_mask_wins_over_tag_mask() {
        let mut policy = ResolvedPolicy::default();
        // Resource mask: Hash on email
        policy.column_masks.insert("email".to_string(), MaskType::Hash);
        let col_tags = make_col_tags(&[("email", &["PII"])]);
        // Tag mask: Nullify — MUST NOT overwrite the resource mask.
        let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        assert!(
            matches!(policy.column_masks.get("email"), Some(MaskType::Hash)),
            "resource mask must win over tag mask"
        );
    }

    #[test]
    fn merge_tag_masks_restricted_column_stays_restricted() {
        let mut policy = ResolvedPolicy::default();
        policy.restricted_columns.push("ssn".to_string());
        let col_tags = make_col_tags(&[("ssn", &["PII"])]);
        let tag_masks = make_tag_masks(&[("PII", MaskType::Nullify)]);
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        // No mask added; restricted stays restricted.
        assert!(
            !policy.column_masks.contains_key("ssn"),
            "restricted column must not gain a mask from tags"
        );
        assert!(
            policy.restricted_columns.contains(&"ssn".to_string()),
            "column must remain in restricted_columns"
        );
    }

    #[test]
    fn merge_tag_masks_tag_filters_appended() {
        let mut policy = ResolvedPolicy::default();
        // Pre-existing resource filter.
        policy.row_filters.push(lit(true));
        let col_tags = make_col_tags(&[("region", &["RESTRICTED"])]);
        let tag_masks: HashMap<String, TagMaskSpec> = HashMap::new(); // no masks
        let tag_filter = datafusion::logical_expr::col("region").eq(lit("EU"));
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![tag_filter], &no_unmappable(), &default_identity());
        assert_eq!(
            policy.row_filters.len(),
            2,
            "tag filter must be ANDed (appended) with resource filters"
        );
    }

    #[test]
    fn merge_tag_masks_first_matching_tag_wins() {
        // Column has two tags; only the second has a mask.
        let mut policy = ResolvedPolicy::default();
        let col_tags = make_col_tags(&[("salary", &["INTERNAL", "PII"])]);
        // Only PII has a mask.
        let tag_masks = make_tag_masks(&[("PII", MaskType::Hash)]);
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        assert!(
            matches!(policy.column_masks.get("salary"), Some(MaskType::Hash)),
            "first matching tag mask in stored order must be applied"
        );
    }

    #[test]
    fn merge_tag_masks_empty_col_tags_is_noop() {
        let mut policy = ResolvedPolicy::default();
        merge_tag_masks(&mut policy, &HashMap::new(), &HashMap::new(), vec![], &no_unmappable(), &default_identity());
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    // ── unmappable-tag fail-closed tests (security regression) ────────────────

    #[test]
    fn merge_tag_masks_unmappable_tag_restricts_unmasked_column() {
        // A column whose only protection is an unmappable (unsupported type)
        // tag mask must be RESTRICTED, not returned raw.
        let mut policy = ResolvedPolicy::default();
        let col_tags = make_col_tags(&[("ssn", &["PII"])]);
        let tag_masks: HashMap<String, TagMaskSpec> = HashMap::new(); // PII produced no mask (unmappable)
        let unmappable: HashSet<String> = ["PII".to_string()].into_iter().collect();
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &unmappable, &default_identity());
        assert!(
            policy.restricted_columns.contains(&"ssn".to_string()),
            "column with unmappable tag mask must be restricted (fail-closed)"
        );
        assert!(
            !policy.column_masks.contains_key("ssn"),
            "restricted column must not also carry a mask"
        );
    }

    #[test]
    fn merge_tag_masks_unmappable_tag_resource_mask_wins_not_restricted() {
        // Same unmappable tag, but the column already has a resource mask. The
        // resource mask is more specific and sufficient — the column must NOT
        // be restricted.
        let mut policy = ResolvedPolicy::default();
        policy.column_masks.insert("ssn".to_string(), MaskType::Hash);
        let col_tags = make_col_tags(&[("ssn", &["PII"])]);
        let tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
        let unmappable: HashSet<String> = ["PII".to_string()].into_iter().collect();
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &unmappable, &default_identity());
        assert!(
            matches!(policy.column_masks.get("ssn"), Some(MaskType::Hash)),
            "resource mask must win over unmappable-tag restriction"
        );
        assert!(
            !policy.restricted_columns.contains(&"ssn".to_string()),
            "column with a resource mask must NOT be restricted by an unmappable tag"
        );
    }

    // ── CUSTOM tag mask tests ──────────────────────────────────────────────────

    #[test]
    fn merge_tag_masks_custom_template_applied_to_column() {
        // A CUSTOM TagMaskSpec with a valid {col} template must produce a
        // MaskType::Custom expression for the column — NOT restrict it.
        let mut policy = ResolvedPolicy::default();
        let col_tags = make_col_tags(&[("email", &["PII"])]);
        let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
        tag_masks.insert("PII".to_string(), TagMaskSpec::Custom("concat('***', {col})".to_string()));
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        assert!(
            matches!(policy.column_masks.get("email"), Some(MaskType::Custom(_))),
            "CUSTOM tag mask with valid expression must produce MaskType::Custom"
        );
        assert!(
            !policy.restricted_columns.contains(&"email".to_string()),
            "column with a valid CUSTOM mask must NOT be restricted"
        );
    }

    #[test]
    fn merge_tag_masks_custom_template_bad_expr_restricts_column() {
        // A CUSTOM TagMaskSpec whose expression cannot be parsed must RESTRICT
        // the column (fail-closed), not leave it unmasked.
        let mut policy = ResolvedPolicy::default();
        let col_tags = make_col_tags(&[("ssn", &["PII"])]);
        let mut tag_masks: HashMap<String, TagMaskSpec> = HashMap::new();
        // Invalid SQL expression — parser will reject it.
        tag_masks.insert("PII".to_string(), TagMaskSpec::Custom("!!!INVALID SQL!!!".to_string()));
        merge_tag_masks(&mut policy, &col_tags, &tag_masks, vec![], &no_unmappable(), &default_identity());
        assert!(
            policy.restricted_columns.contains(&"ssn".to_string()),
            "column with unparseable CUSTOM tag mask must be restricted (fail-closed)"
        );
        assert!(
            !policy.column_masks.contains_key("ssn"),
            "restricted column must not also carry a mask"
        );
    }

    #[test]
    fn test_apply_mask_nullify_utf8() {
        let expr = apply_mask("ssn", &DataType::Utf8, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Utf8(None), _) => {}
            other => panic!("Expected Utf8 NULL literal, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_int64_produces_typed_null() {
        let expr = apply_mask("customer_id", &DataType::Int64, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Int64(None), _) => {}
            other => panic!("Expected Int64 NULL literal, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_decimal_produces_typed_null() {
        let dt = DataType::Decimal128(18, 2);
        let expr = apply_mask("salary", &dt, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::Decimal128(None, 18, 2), _) => {}
            other => panic!("Expected Decimal128(None,18,2), got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_nullify_timestamp_produces_typed_null() {
        let dt = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
        let expr = apply_mask("ts", &dt, &MaskType::Nullify, None);
        match expr {
            Expr::Literal(ScalarValue::TimestampMicrosecond(None, None), _) => {}
            other => panic!("Expected TimestampMicrosecond NULL, got: {other:?}"),
        }
    }

    #[test]
    fn test_apply_mask_redact_utf8_returns_literal() {
        let expr = apply_mask(
            "ssn",
            &DataType::Utf8,
            &MaskType::Redact("***".to_string()),
            None,
        );
        assert!(matches!(expr, Expr::Literal(ScalarValue::Utf8(Some(_)), _)));
    }

    #[test]
    fn test_apply_mask_redact_int_wraps_in_cast() {
        let expr = apply_mask(
            "id",
            &DataType::Int64,
            &MaskType::Redact("0".to_string()),
            None,
        );
        assert!(
            matches!(expr, Expr::Cast(_)),
            "Expected Cast for Redact on Int64, got: {expr:?}"
        );
    }

    #[test]
    fn test_apply_mask_hash_utf8_produces_scalar_function() {
        let expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, None);
        assert!(
            matches!(expr, Expr::ScalarFunction(_)),
            "Expected ScalarFunction for Hash mask, got: {expr:?}"
        );
    }

    /// Regression for #37: when a mask key is configured the Hash branch
    /// must still produce a scalar function, but the registered UDF
    /// carries the key so it runs HMAC-SHA256 at execution time. We can't
    /// reach into the boxed ScalarFunction from the LogicalPlan layer, so
    /// the assertion focuses on the structural invariant (still a
    /// function call wrapped in a Cast for non-string types) plus the
    /// keyed-vs-unkeyed branch in the underlying UDF (covered in
    /// `sha256_udf::tests`).
    #[test]
    fn test_apply_mask_hash_with_key_still_produces_scalar_function() {
        let key = Some(Arc::new(b"deployment-key".to_vec()));
        let utf8_expr = apply_mask("email", &DataType::Utf8, &MaskType::Hash, key.clone());
        assert!(matches!(utf8_expr, Expr::ScalarFunction(_)));

        let int_expr = apply_mask("id", &DataType::Int64, &MaskType::Hash, key);
        assert!(matches!(int_expr, Expr::Cast(_)));
    }

    // ── resolve_policy_key (issue #205) ──────────────────────────
    // The policy key MUST match the write path, which uses
    // `namespace().last()`. For a multi-level schema "ns1.ns2" the key is
    // ("ns2", table). A bare table falls back to "default".

    #[test]
    fn resolve_policy_key_bare_table_uses_default_namespace() {
        let r = datafusion::common::TableReference::bare("employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("default".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_two_part_uses_schema_as_namespace() {
        let r = datafusion::common::TableReference::partial("hr", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("hr".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_three_part_uses_schema_as_namespace() {
        let r = datafusion::common::TableReference::full("cat", "hr", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("hr".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_multilevel_takes_last_namespace_component() {
        // cat.ns1.ns2.employees -> schema "ns1.ns2" -> last component "ns2".
        let r = datafusion::common::TableReference::full("cat", "ns1.ns2", "employees");
        assert_eq!(
            resolve_policy_key(&r),
            Some(("ns2".to_string(), "employees".to_string()))
        );
    }

    #[test]
    fn resolve_policy_key_empty_table_fails_closed() {
        // An empty table component cannot be confidently keyed; the caller
        // treats None as deny-all rather than passthrough.
        let r = datafusion::common::TableReference::bare("");
        assert_eq!(resolve_policy_key(&r), None);
    }

    // ── is_reserved_virtual_ref ──────────────────────────────────
    // Reserved virtual relations are skipped from the tag lookup so an
    // unknown tag state never denies information_schema / system queries.
    // The skip must be tight: a real Iceberg ref must never match.

    #[test]
    fn reserved_ref_matches_bare_information_schema() {
        let r = datafusion::common::TableReference::partial("information_schema", "tables");
        assert!(is_reserved_virtual_ref(&r));
    }

    #[test]
    fn reserved_ref_matches_qualified_information_schema() {
        let r =
            datafusion::common::TableReference::full("cat", "information_schema", "columns");
        assert!(is_reserved_virtual_ref(&r));
    }

    #[test]
    fn reserved_ref_matches_system_and_datafusion_catalogs() {
        let sys = datafusion::common::TableReference::full("system", "runtime", "queries");
        assert!(is_reserved_virtual_ref(&sys));
        let df = datafusion::common::TableReference::full("datafusion", "info", "version");
        assert!(is_reserved_virtual_ref(&df));
    }

    #[test]
    fn reserved_ref_does_not_match_real_iceberg_table() {
        // A normal user table must be tag-governed.
        let r = datafusion::common::TableReference::full("cat", "ns1.ns2", "employees");
        assert!(!is_reserved_virtual_ref(&r));
        // Bare and two-part user tables too.
        assert!(!is_reserved_virtual_ref(&datafusion::common::TableReference::bare("employees")));
        assert!(!is_reserved_virtual_ref(&datafusion::common::TableReference::partial(
            "hr", "employees"
        )));
    }

    #[test]
    fn reserved_ref_does_not_match_namespace_ending_in_runtime() {
        // The fix keys on reserved CATALOG names, not on the last namespace
        // component, so a user namespace ending in "runtime" is NOT a bypass.
        let r = datafusion::common::TableReference::full("cat", "app.runtime", "events");
        assert!(!is_reserved_virtual_ref(&r));
    }

    #[test]
    fn reserved_ref_does_not_match_pg_catalog() {
        // SQE does not register pg_catalog as a virtual provider, so a user
        // namespace named pg_catalog must still be tag-governed.
        let r = datafusion::common::TableReference::partial("pg_catalog", "pg_class");
        assert!(!is_reserved_virtual_ref(&r));
    }

    #[test]
    fn test_apply_mask_custom_returns_provided_expr() {
        let custom_expr = datafusion::logical_expr::lit("REDACTED");
        let result = apply_mask(
            "secret",
            &DataType::Utf8,
            &MaskType::Custom(custom_expr.clone()),
            None,
        );
        assert_eq!(
            format!("{result:?}"),
            format!("{custom_expr:?}"),
            "Custom mask should return the provided expression unchanged"
        );
    }

    #[tokio::test]
    async fn custom_mask_referencing_sibling_resolves_end_to_end() {
        use crate::policy_store::InMemoryPolicyStore;
        use crate::session_udf::SessionIdentity;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::TableReference;
        use datafusion::logical_expr::builder::table_scan;
        use sqe_core::SessionUser;

        let schema = Schema::new(vec![
            Field::new("salary", DataType::Utf8, true),
            Field::new("department", DataType::Utf8, true),
        ]);
        let scan = table_scan(
            Some(TableReference::partial("hr", "employees")),
            &schema,
            None,
        )
        .unwrap()
        .build()
        .unwrap();

        let identity = SessionIdentity {
            username: "bob".to_string(),
            roles: vec![],
            database: Some("db".to_string()),
            schema: Some("hr".to_string()),
        };
        let mask_expr = parse_sql_predicate(
            "CASE WHEN department = 'HR' THEN salary ELSE '0' END",
            &identity,
        )
        .unwrap();

        let mut policy = ResolvedPolicy::default();
        policy
            .column_masks
            .insert("salary".to_string(), MaskType::Custom(mask_expr));

        let store = InMemoryPolicyStore::new();
        store.add_table_policy("hr", "employees", policy).await;

        let rewriter = PolicyPlanRewriter::new(Arc::new(store));
        let user = SessionUser {
            username: "bob".to_string(),
            roles: vec![],
        };

        let (rewritten, _summary) = rewriter
            .evaluate(&user, scan)
            .await
            .expect("rewrite with sibling-referencing CUSTOM mask must succeed");

        let rendered = format!("{}", rewritten.display_indent());
        assert!(
            rendered.contains("department"),
            "rewritten plan must reference the sibling column, got:\n{rendered}"
        );
    }

    // ── resolve_effective_policy (SHOW EFFECTIVE POLICY diagnostic) ───────
    // The diagnostic must report the SAME resolution the rewriter applies:
    // resource policy from store.resolve, plus tag-merged masks/filters.

    fn diag_user(roles: &[&str]) -> sqe_core::session::SessionUser {
        sqe_core::session::SessionUser {
            username: "alice".to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn resolve_effective_policy_surfaces_resource_masks_and_filters() {
        let store = crate::policy_store::InMemoryPolicyStore::new();
        let mut p = ResolvedPolicy::default();
        p.column_masks.insert("ssn".to_string(), MaskType::Hash);
        p.row_filters.push(col("region").eq(lit("EU")));
        p.restricted_columns.push("notes".to_string());
        store.add_table_policy("hr", "employees", p).await;

        let got = resolve_effective_policy(
            &store,
            &diag_user(&[]),
            "employees",
            "hr",
            &HashMap::new(),
        )
        .await;

        assert!(matches!(got.column_masks.get("ssn"), Some(MaskType::Hash)));
        assert_eq!(got.row_filters.len(), 1);
        assert_eq!(got.restricted_columns, vec!["notes".to_string()]);
    }

    #[tokio::test]
    async fn resolve_effective_policy_no_policy_is_empty_passthrough() {
        let store = crate::policy_store::InMemoryPolicyStore::new();
        let got = resolve_effective_policy(
            &store,
            &diag_user(&[]),
            "orders",
            "sales",
            &HashMap::new(),
        )
        .await;
        assert!(got.column_masks.is_empty());
        assert!(got.row_filters.is_empty());
        assert!(got.restricted_columns.is_empty());
    }

    #[tokio::test]
    async fn resolve_effective_policy_matches_rewriter_key_via_namespace() {
        // The store keys on "{namespace}.{table}"; the caller must pass the
        // last namespace component (as resolve_policy_key derives), so a policy
        // registered under "ns2" is found when the table is cat.ns1.ns2.t.
        let store = crate::policy_store::InMemoryPolicyStore::new();
        let mut p = ResolvedPolicy::default();
        p.column_masks.insert("email".to_string(), MaskType::Nullify);
        store.add_table_policy("ns2", "t", p).await;

        let got =
            resolve_effective_policy(&store, &diag_user(&[]), "t", "ns2", &HashMap::new()).await;
        assert!(matches!(got.column_masks.get("email"), Some(MaskType::Nullify)));
    }
}
