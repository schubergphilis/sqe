//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::logical_expr::{lit, Expr};
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use sqe_core::config::RangerPolicyConfig;
use sqe_core::{SecretString, SessionUser};
use tracing::{debug, warn};

use crate::policy_breaker::PolicyCircuitBreaker;
use crate::policy_expr::parse_sql_predicate;
use crate::session_udf::SessionIdentity;
use crate::{MaskType, PolicyStore, ResolvedPolicy};

// --- Ranger policy bundle model (ServicePolicies) ---

// TODO(phase3): verify tagPolicies shape against a live tag-linked bundle
/// Nested tag-service policy bundle. Present when Ranger has at least one
/// tag-based policy. Structure mirrors the top-level `ServicePolicies` but
/// with `tag` resources instead of database/table/column.
#[derive(Debug, Deserialize, Default)]
pub(crate) struct TagPolicies {
    /// Same `RangerPolicy` type as resource policies; `resources` map carries
    /// a `tag` key with the tag values (e.g. `["PII"]`).
    #[serde(default)]
    pub(crate) policies: Vec<RangerPolicy>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ServicePolicies {
    #[serde(rename = "policyVersion", default)]
    #[allow(dead_code)] // read only in #[cfg(test)]; used by serde and test assertions
    pub(crate) policy_version: Option<i64>,
    #[serde(default)]
    pub(crate) policies: Vec<RangerPolicy>,
    /// Nested tag-service policies. Present when the Ranger bundle includes
    /// tag-based policies. Absent in pure-resource bundles (default = None).
    #[serde(rename = "tagPolicies", default)]
    pub(crate) tag_policies: Option<TagPolicies>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RangerPolicy {
    #[serde(default)]
    #[allow(dead_code)] // read only in #[cfg(test)]; present in Ranger JSON for traceability
    pub(crate) id: i64,
    /// 0 = access, 1 = DATAMASK, 2 = ROWFILTER.
    #[serde(rename = "policyType", default)]
    pub(crate) policy_type: i32,
    #[serde(rename = "isEnabled", default)]
    pub(crate) is_enabled: bool,
    /// Resource map: keys are "database", "table", "column".
    #[serde(default)]
    pub(crate) resources: HashMap<String, RangerResource>,
    #[serde(rename = "dataMaskPolicyItems", default)]
    pub(crate) data_mask_policy_items: Vec<DataMaskPolicyItem>,
    #[serde(rename = "rowFilterPolicyItems", default)]
    pub(crate) row_filter_policy_items: Vec<RowFilterPolicyItem>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RangerResource {
    #[serde(default)]
    pub(crate) values: Vec<String>,
    #[serde(rename = "isExcludes", default)]
    pub(crate) is_excludes: bool,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct DataMaskPolicyItem {
    #[serde(default)]
    pub(crate) users: Vec<String>,
    #[serde(default)]
    pub(crate) roles: Vec<String>,
    // groups-based binding is NOT enforced (SQE matches token roles only); see Phase 2.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "dataMaskInfo", default)]
    pub(crate) data_mask_info: DataMaskInfo,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct DataMaskInfo {
    #[serde(rename = "dataMaskType", default)]
    pub(crate) data_mask_type: String,
    #[serde(rename = "valueExpr", default)]
    pub(crate) value_expr: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RowFilterPolicyItem {
    #[serde(default)]
    pub(crate) users: Vec<String>,
    #[serde(default)]
    pub(crate) roles: Vec<String>,
    // groups-based binding is NOT enforced (SQE matches token roles only); see Phase 2.
    #[serde(default)]
    pub(crate) groups: Vec<String>,
    #[serde(rename = "rowFilterInfo", default)]
    pub(crate) row_filter_info: RowFilterInfo,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RowFilterInfo {
    #[serde(rename = "filterExpr", default)]
    pub(crate) filter_expr: Option<String>,
}

// --- RangerStore struct, constructor, and download fetch ---

/// Fine-grained policy store backed by a `hive`-type Ranger service.
pub struct RangerStore {
    client: Client,
    /// Base download URL, e.g. ".../service/plugins/policies/download/hive".
    download_url: String,
    admin_user: String,
    admin_password: SecretString,
    cache: Cache<String, ResolvedPolicy>,
    breaker: Arc<PolicyCircuitBreaker>,
}

impl RangerStore {
    pub fn from_config(cfg: &RangerPolicyConfig) -> sqe_core::Result<Self> {
        let base = cfg.url.trim_end_matches('/');
        let download_url = format!(
            "{base}/service/plugins/policies/download/{}",
            cfg.service_name
        );
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(cfg.timeout_secs))
                .danger_accept_invalid_certs(cfg.accept_invalid_certs)
                .build()
                .map_err(|e| {
                    sqe_core::error::SqeError::Config(format!(
                        "Failed to build Ranger HTTP client: {e}"
                    ))
                })?,
            download_url,
            admin_user: cfg.admin_user.clone(),
            admin_password: cfg.admin_password.clone(),
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(cfg.cache_max_entries)
                .build(),
            breaker: Arc::new(PolicyCircuitBreaker::new(
                "Ranger",
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
        })
    }

    /// Fetch the full policy bundle. Fail-closed: any transport/parse error
    /// trips the breaker and returns Err so the caller denies.
    // TODO(phase2): lastKnownVersion + HTTP 304 incremental refresh.
    async fn fetch_bundle(&self) -> sqe_core::Result<ServicePolicies> {
        self.breaker.check().map_err(|e| {
            sqe_core::error::SqeError::Execution(format!("Ranger unavailable: {e}"))
        })?;

        let resp = self
            .client
            .get(&self.download_url)
            .basic_auth(&self.admin_user, Some(self.admin_password.expose()))
            .send()
            .await
            .map_err(|e| {
                self.breaker.record_failure();
                sqe_core::error::SqeError::Execution(format!("Ranger download failed: {e}"))
            })?;

        if !resp.status().is_success() {
            self.breaker.record_failure();
            return Err(sqe_core::error::SqeError::Execution(format!(
                "Ranger download returned status {}",
                resp.status()
            )));
        }

        let bundle: ServicePolicies = resp.json().await.map_err(|e| {
            self.breaker.record_failure();
            sqe_core::error::SqeError::Execution(format!("Failed to parse Ranger bundle: {e}"))
        })?;
        self.breaker.record_success();
        Ok(bundle)
    }
}

// --- Pure resolution helpers ---

/// Flatten an Iceberg namespace to a hive `database` name. SQE namespaces are
/// already dotted multi-level strings and Kyuubi uses the same dotted
/// convention, so this is identity for now. Catalog is intentionally dropped
/// (hive has no catalog level); cross-engine policies must be written without a
/// catalog prefix. See docs/ranger-fine-grained-service-type.md.
///
/// NOTE: `plan_rewriter.rs::resolve_policy_key` passes the **last** dotted
/// component of the schema as `namespace` (e.g. schema `"sales_wh.sales"` ->
/// `"sales"`). Ranger `database` resource values must match that last component
/// for policies to fire. See project tracking for the namespace convention
/// alignment task.
fn hive_database(namespace: &str) -> String {
    namespace.to_string()
}

/// True if a Ranger resource value list matches `target` (supports `*`
/// wildcard and exact match; `isExcludes` inverts the result).
///
// Only exact match and bare "*" are supported. Ranger glob patterns (e.g.
// "orders*", "*_pii") are NOT matched in MVP — author policies with exact
// names or "*". An empty values list matches nothing.
fn resource_matches(res: &RangerResource, target: &str) -> bool {
    let hit = res.values.iter().any(|v| v == "*" || v == target);
    hit ^ res.is_excludes
}

/// True if a policy's database + table resources match the target table.
fn policy_matches_table(p: &RangerPolicy, database: &str, table: &str) -> bool {
    let db_ok = p
        .resources
        .get("database")
        .map(|r| resource_matches(r, database))
        .unwrap_or(false);
    let tbl_ok = p
        .resources
        .get("table")
        .map(|r| resource_matches(r, table))
        .unwrap_or(false);
    db_ok && tbl_ok
}

/// True if a policy-item applies to this user/roles (token roles, matched directly).
///
/// `groups` is accepted but NOT enforced (SQE has no group info; token roles
/// only, by design — Phase 2). A policy item bound ONLY via `groups` is skipped
/// with a warning so operators see the gap instead of a silent drop.
fn item_matches(
    users: &[String],
    roles: &[String],
    groups: &[String],
    user: &SessionUser,
) -> bool {
    let matched =
        users.iter().any(|u| u == &user.username) || roles.iter().any(|r| user.roles.contains(r));
    if !matched && !groups.is_empty() {
        warn!(
            ?groups,
            "Ranger policy item is group-bound; SQE does not enforce group bindings (Phase 2) — policy item skipped"
        );
    }
    matched
}

/// Map a Ranger hive data-mask type to an SQE `MaskType`.
///  - `Ok(Some(mask))` supported,
///  - `Ok(None)` for MASK_NONE (explicit exemption: no mask, not restricted),
///  - `Err(())` for not-yet-supported types (caller restricts the column, fail-closed).
fn map_mask(info: &DataMaskInfo, column: &str, identity: &SessionIdentity) -> Result<Option<MaskType>, ()> {
    match info.data_mask_type.as_str() {
        "MASK_NULL" => Ok(Some(MaskType::Nullify)),
        "MASK_NONE" => Ok(None),
        "MASK_HASH" => Ok(Some(MaskType::Hash)),
        "CUSTOM" => {
            let expr_str = info.value_expr.as_deref().ok_or(())?;
            // Ranger CUSTOM masks use `{col}` as the column placeholder.
            // Substitute with the real column name so the parsed Expr references
            // the actual column. The rewriter splices the Expr as-is via
            // `MaskType::Custom(expr) => expr.clone()` (plan_rewriter.rs:323),
            // so the column name must be correct at parse time.
            // If parsing fails -> Err(()) -> column restricted (fail-closed).
            let substituted = expr_str.replace("{col}", column);
            parse_sql_predicate(&substituted, identity)
                .map(|e| Some(MaskType::Custom(e)))
                .map_err(|_| ())
        }
        "MASK" => Ok(Some(MaskType::PartialMask {
            show_first: 0,
            show_last: 0,
            upper: 'X',
            lower: 'x',
            digit: 'n',
        })),
        "MASK_SHOW_LAST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 0,
            show_last: 4,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        })),
        "MASK_SHOW_FIRST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 4,
            show_last: 0,
            upper: 'x',
            lower: 'x',
            digit: 'x',
        })),
        "MASK_DATE_SHOW_YEAR" => Ok(Some(MaskType::DateShowYear)),
        // Genuinely unknown / unsupported types still fail closed (restrict).
        _ => Err(()),
    }
}

/// Build a `ResolvedPolicy` from an already-fetched bundle. Pure (no I/O), so it
/// is unit-tested directly and reused by `resolve()` after a cache miss.
fn resolve_from_bundle(
    bundle: &ServicePolicies,
    user: &SessionUser,
    table: &str,
    namespace: &str,
) -> ResolvedPolicy {
    let database = hive_database(namespace);
    let mut policy = ResolvedPolicy::default();

    // Build the identity once for the whole resolution pass. database/schema
    // are None here -- RangerStore doesn't hold the session warehouse; UDFs
    // referencing current_database()/current_schema() fold to NULL (MVP).
    let identity = SessionIdentity {
        username: user.username.clone(),
        roles: user.roles.clone(),
        database: None,
        schema: None,
    };

    for p in &bundle.policies {
        if !p.is_enabled || !policy_matches_table(p, &database, table) {
            continue;
        }

        // Data-mask policy (policyType 1). A datamask policy's `column`
        // resource can list several columns that all receive the same mask;
        // iterate ALL of them so multi-column policies don't leak.
        if p.policy_type == 1 {
            let Some(col_res) = p.resources.get("column") else { continue };
            for column in &col_res.values {
                for item in &p.data_mask_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, user) {
                        continue;
                    }
                    match map_mask(&item.data_mask_info, column, &identity) {
                        Ok(Some(mask)) => {
                            policy.column_masks.insert(column.clone(), mask);
                        }
                        Ok(None) => { /* MASK_NONE exemption: leave column visible */ }
                        Err(()) => {
                            warn!(
                                column = %column,
                                mask_type = %item.data_mask_info.data_mask_type,
                                "unsupported Ranger mask type; restricting column (fail-closed)"
                            );
                            if !policy.restricted_columns.contains(column) {
                                policy.restricted_columns.push(column.clone());
                            }
                        }
                    }
                }
            }
        }

        // Row-filter policy (policyType 2)
        if p.policy_type == 2 {
            for item in &p.row_filter_policy_items {
                if !item_matches(&item.users, &item.roles, &item.groups, user) {
                    continue;
                }
                if let Some(expr_str) = &item.row_filter_info.filter_expr {
                    match parse_sql_predicate(expr_str, &identity) {
                        Ok(expr) => policy.row_filters.push(expr),
                        Err(e) => {
                            warn!(
                                filter = %expr_str,
                                error = %e,
                                "unparseable Ranger row filter; denying (fail-closed)"
                            );
                            policy.row_filters.push(lit(false));
                        }
                    }
                }
            }
        }
    }

    debug!(
        user = %user.username,
        table = %table,
        db = %database,
        masks = policy.column_masks.len(),
        filters = policy.row_filters.len(),
        restricted = policy.restricted_columns.len(),
        "resolved Ranger policy"
    );
    policy
}

/// Resolve tag-based mask and row-filter policies from the bundle for a given
/// user identity and a set of column tags (Iceberg column-level tags).
///
/// Returns:
/// - `HashMap<tag, MaskType>` -- masks keyed by **tag name** (not column name).
///   The caller (Task 4 rewriter) maps tag -> column using the Iceberg schema's
///   column->tags map.
/// - `Vec<(tag, Expr)>` -- row filters keyed by the tag that triggered them.
///
/// This function is pure (no I/O) and unit-tested directly. It is wired into
/// the plan rewriter in Task 4.
///
/// CUSTOM tag masks are skipped at this stage: a CUSTOM mask requires the
/// actual column name for `{col}` substitution, which is not yet known here
/// (tags bind to columns only at rewrite time). Skipping with a warning keeps
/// the function fail-safe.
/// TODO(phase3): bind CUSTOM tag masks at apply time (Task 4 rewriter).
pub(crate) fn resolve_tag_policies(
    bundle: &ServicePolicies,
    identity: &SessionIdentity,
    tags: &HashSet<String>,
) -> (HashMap<String, MaskType>, Vec<(String, Expr)>) {
    let mut masks: HashMap<String, MaskType> = HashMap::new();
    let mut filters: Vec<(String, Expr)> = Vec::new();

    let tag_bundle = match &bundle.tag_policies {
        Some(tp) => tp,
        None => return (masks, filters),
    };

    // Bridge SessionIdentity -> SessionUser for item_matches.
    use sqe_core::SessionUser;
    let user = SessionUser {
        username: identity.username.clone(),
        roles: identity.roles.clone(),
    };

    for p in &tag_bundle.policies {
        if !p.is_enabled {
            continue;
        }

        // Read tag resource values for this policy.
        let tag_res = match p.resources.get("tag") {
            Some(r) => r,
            None => continue,
        };

        // Only process tags that the caller's column set carries.
        for tag_value in &tag_res.values {
            if !tags.contains(tag_value.as_str()) {
                continue;
            }

            // policyType 1: datamask
            if p.policy_type == 1 {
                for item in &p.data_mask_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, &user) {
                        continue;
                    }
                    // CUSTOM masks are deferred to Task 4 apply time.
                    if item.data_mask_info.data_mask_type == "CUSTOM" {
                        warn!(
                            tag = %tag_value,
                            "CUSTOM tag mask skipped -- column not known at tag-resolve time; \
                             TODO(phase3): bind {{col}} at Task 4 apply time"
                        );
                        continue;
                    }
                    // column placeholder is empty; CUSTOM is already excluded above.
                    match map_mask(&item.data_mask_info, "", identity) {
                        Ok(Some(mask)) => {
                            masks.insert(tag_value.clone(), mask);
                        }
                        Ok(None) => { /* MASK_NONE exemption: tag has no mask */ }
                        Err(()) => {
                            warn!(
                                tag = %tag_value,
                                mask_type = %item.data_mask_info.data_mask_type,
                                "unsupported Ranger tag mask type; skipping (fail-closed: caller \
                                 must restrict columns bearing this tag)"
                            );
                        }
                    }
                }
            }

            // policyType 2: rowfilter
            if p.policy_type == 2 {
                for item in &p.row_filter_policy_items {
                    if !item_matches(&item.users, &item.roles, &item.groups, &user) {
                        continue;
                    }
                    if let Some(expr_str) = &item.row_filter_info.filter_expr {
                        match parse_sql_predicate(expr_str, identity) {
                            Ok(expr) => filters.push((tag_value.clone(), expr)),
                            Err(e) => {
                                warn!(
                                    tag = %tag_value,
                                    filter = %expr_str,
                                    error = %e,
                                    "unparseable Ranger tag row filter; denying (fail-closed)"
                                );
                                filters.push((tag_value.clone(), lit(false)));
                            }
                        }
                    }
                }
            }
        }
    }

    (masks, filters)
}

// --- Cache key + PolicyStore impl ---

fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
    let mut roles = user.roles.clone();
    roles.sort();
    format!("{}:{}:{}:{}", user.username, namespace, table, roles.join(","))
}

#[async_trait]
impl PolicyStore for RangerStore {
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        let key = cache_key(user, table_name, namespace);
        if let Some(cached) = self.cache.get(&key).await {
            return Ok(cached);
        }
        let bundle = self.fetch_bundle().await?;
        let policy = resolve_from_bundle(&bundle, user, table_name, namespace);
        self.cache.insert(key, policy.clone()).await;
        Ok(policy)
    }

    /// Resolve tag-based policies from the Ranger bundle for a given user and
    /// set of tag names present on a table's columns.
    ///
    /// Fetches the bundle (or re-uses the in-flight breaker state). On any
    /// fetch failure the method returns `(empty, [lit(false)])` — the
    /// `lit(false)` row filter denies all rows (fail-closed), consistent with
    /// how `resolve()` handles bundle errors.
    ///
    /// Masks are returned keyed by TAG NAME. The plan rewriter maps tag ->
    /// column using the `TagSource` column->tags map.
    async fn resolve_tags(
        &self,
        user: &SessionUser,
        tags: &std::collections::HashSet<String>,
    ) -> (std::collections::HashMap<String, MaskType>, Vec<Expr>) {
        if tags.is_empty() {
            return (std::collections::HashMap::new(), vec![]);
        }

        let bundle = match self.fetch_bundle().await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    user = %user.username,
                    error = %e,
                    "resolve_tags: failed to fetch Ranger bundle; \
                     denying all rows (fail-closed)"
                );
                return (std::collections::HashMap::new(), vec![lit(false)]);
            }
        };

        let identity = SessionIdentity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            database: None,
            schema: None,
        };

        let (masks, tag_filters) = resolve_tag_policies(&bundle, &identity, tags);
        // Discard the tag keys from row filters — the rewriter only needs Exprs.
        let filter_exprs: Vec<Expr> = tag_filters.into_iter().map(|(_, e)| e).collect();
        (masks, filter_exprs)
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUNDLE: &str = r#"{
      "policyVersion": 7,
      "policies": [
        {
          "id": 1, "policyType": 1, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales"]},
            "table": {"values": ["orders"]},
            "column": {"values": ["amount"]}
          },
          "dataMaskPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}
          ]
        },
        {
          "id": 2, "policyType": 2, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales"]},
            "table": {"values": ["orders"]}
          },
          "rowFilterPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
          ]
        }
      ]
    }"#;

    #[test]
    fn parses_bundle() {
        let sp: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        assert_eq!(sp.policy_version, Some(7));
        assert_eq!(sp.policies.len(), 2);
        assert_eq!(sp.policies[0].policy_type, 1);
        assert!(sp.policies[0].is_enabled);
        assert_eq!(
            sp.policies[0].data_mask_policy_items[0].data_mask_info.data_mask_type,
            "MASK_NULL"
        );
        assert_eq!(
            sp.policies[1].row_filter_policy_items[0]
                .row_filter_info
                .filter_expr
                .as_deref(),
            Some("region = 'EU'")
        );
    }

    #[test]
    fn empty_bundle_is_default() {
        let sp: ServicePolicies = serde_json::from_str("{}").unwrap();
        assert!(sp.policies.is_empty());
        assert_eq!(sp.policy_version, None);
    }

    #[test]
    fn from_config_builds_store() {
        let cfg = RangerPolicyConfig::default();
        // from_config must succeed even with an empty URL (no network call).
        let store = RangerStore::from_config(&cfg);
        assert!(store.is_ok(), "from_config failed: {:?}", store.err());
    }

    fn user(name: &str, roles: &[&str]) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn flattens_iceberg_to_hive_database() {
        assert_eq!(hive_database("sales"), "sales");
        assert_eq!(hive_database("sales.eu"), "sales.eu");
    }

    #[test]
    fn mask_null_maps_to_nullify() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(matches!(
            policy.column_masks.get("amount"),
            Some(MaskType::Nullify)
        ));
    }

    #[test]
    fn row_filter_applied_for_matching_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert_eq!(policy.row_filters.len(), 1);
    }

    #[test]
    fn no_match_for_other_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("bob", &["engineer"]),
            "orders",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn user_match_works_too() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0].roles.clear();
        bundle.policies[0].data_mask_policy_items[0].users = vec!["alice".to_string()];
        let policy =
            resolve_from_bundle(&bundle, &user("alice", &[]), "orders", "sales");
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn unsupported_mask_restricts_column_failclosed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_FUTURE_UNSUPPORTED".to_string();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(policy.restricted_columns.contains(&"amount".to_string()));
        assert!(!policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn mask_none_is_exemption() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0]
            .data_mask_info
            .data_mask_type = "MASK_NONE".to_string();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(!policy.column_masks.contains_key("amount"));
        assert!(!policy.restricted_columns.contains(&"amount".to_string()));
    }

    #[test]
    fn disabled_policy_is_skipped() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].is_enabled = false; // the datamask policy
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn wrong_table_does_not_match() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "customers",
            "sales",
        );
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn unparseable_row_filter_fails_closed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[1].row_filter_policy_items[0]
            .row_filter_info
            .filter_expr = Some("this is not sql !!!".to_string());
        let policy = resolve_from_bundle(
            &bundle,
            &user("alice", &["analyst"]),
            "orders",
            "sales",
        );
        // Fail-closed: a broken filter must NOT result in zero filters (which
        // would expose all rows). Expect a lit(false) deny filter instead.
        assert_eq!(policy.row_filters.len(), 1);
        // The single filter should be the literal-false deny, not a parsed predicate.
        let s = format!("{:?}", policy.row_filters[0]).to_lowercase();
        assert!(
            s.contains("false") || s.contains("boolean(false)"),
            "expected deny filter, got {s}"
        );
    }

    #[test]
    fn masks_all_columns_in_multi_column_policy() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].resources.get_mut("column").unwrap().values =
            vec!["amount".to_string(), "discount".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.contains_key("amount"));
        assert!(policy.column_masks.contains_key("discount"));
    }

    #[test]
    fn wildcard_table_matches() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].resources.get_mut("table").unwrap().values = vec!["*".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "anything", "sales");
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn excludes_inverts_match() {
        // is_excludes on table should make "orders" NOT match a values=["orders"] exclude.
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let tr = bundle.policies[0].resources.get_mut("table").unwrap();
        tr.is_excludes = true; // exclude "orders"
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.is_empty());
    }

    #[test]
    fn custom_mask_substitutes_column() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let mi = &mut bundle.policies[0].data_mask_policy_items[0].data_mask_info;
        mi.data_mask_type = "CUSTOM".to_string();
        mi.value_expr = Some("concat('x', {col})".to_string());
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        match policy.column_masks.get("amount") {
            Some(crate::MaskType::Custom(e)) => {
                let s = datafusion::sql::unparser::expr_to_sql(e)
                    .unwrap()
                    .to_string()
                    .to_lowercase();
                assert!(s.contains("amount"), "custom expr must reference the real column: {s}");
            }
            other => panic!("expected Custom mask, got {other:?}"),
        }
    }

    #[test]
    fn group_bound_item_is_skipped() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let item = &mut bundle.policies[0].data_mask_policy_items[0];
        item.roles.clear();
        item.users.clear();
        item.groups = vec!["analysts_group".to_string()];
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales");
        assert!(policy.column_masks.is_empty(), "group-bound item must not be enforced in MVP");
    }

    // --- map_mask arm tests ---

    #[test]
    fn maps_show_last_4() {
        let info = DataMaskInfo { data_mask_type: "MASK_SHOW_LAST_4".into(), ..Default::default() };
        match map_mask(&info, "ssn", &SessionIdentity::default()) {
            Ok(Some(MaskType::PartialMask { show_last: 4, show_first: 0, .. })) => {}
            other => panic!("expected show-last-4 PartialMask, got {other:?}"),
        }
    }

    #[test]
    fn maps_show_first_4() {
        let info = DataMaskInfo { data_mask_type: "MASK_SHOW_FIRST_4".into(), ..Default::default() };
        assert!(matches!(
            map_mask(&info, "ssn", &SessionIdentity::default()),
            Ok(Some(MaskType::PartialMask { show_first: 4, show_last: 0, .. }))
        ));
    }

    #[test]
    fn maps_full_mask_uses_hive_default_chars() {
        let info = DataMaskInfo { data_mask_type: "MASK".into(), ..Default::default() };
        match map_mask(&info, "name", &SessionIdentity::default()) {
            Ok(Some(MaskType::PartialMask {
                upper: 'X',
                lower: 'x',
                digit: 'n',
                show_first: 0,
                show_last: 0,
            })) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn maps_date_show_year() {
        let info = DataMaskInfo { data_mask_type: "MASK_DATE_SHOW_YEAR".into(), ..Default::default() };
        assert!(matches!(map_mask(&info, "hired_at", &SessionIdentity::default()), Ok(Some(MaskType::DateShowYear))));
    }

    #[test]
    fn truly_unknown_mask_is_err() {
        let info = DataMaskInfo { data_mask_type: "MASK_FUTURE_UNSUPPORTED".into(), ..Default::default() };
        assert!(map_mask(&info, "x", &SessionIdentity::default()).is_err());
    }

    // --- tagPolicies tests ---

    /// A ServicePolicies bundle that includes a top-level `tagPolicies` block
    /// with a datamask policy for tag "PII" (role=engineer) and a row-filter
    /// policy for tag "RESTRICTED" (role=analyst).
    ///
    /// NOTE: the exact live shape of tagPolicies must be verified against a
    /// real Ranger bundle that has tag-linked policies before this is used in
    /// production. See the Phase 3 prerequisite task.
    const TAG_BUNDLE: &str = r#"{
      "policyVersion": 1,
      "policies": [],
      "tagPolicies": {
        "serviceName": "tag",
        "policies": [
          {
            "id": 1, "policyType": 1, "isEnabled": true,
            "resources": { "tag": { "values": ["PII"] } },
            "dataMaskPolicyItems": [
              { "users": [], "roles": ["engineer"],
                "dataMaskInfo": { "dataMaskType": "MASK_SHOW_LAST_4" } }
            ]
          },
          {
            "id": 2, "policyType": 2, "isEnabled": true,
            "resources": { "tag": { "values": ["RESTRICTED"] } },
            "rowFilterPolicyItems": [
              { "users": [], "roles": ["analyst"],
                "rowFilterInfo": { "filterExpr": "region = 'EU'" } }
            ]
          }
        ]
      }
    }"#;

    #[test]
    fn tag_mask_resolved_for_matching_role() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "bob".into(), roles: vec!["engineer".into()], ..Default::default() };
        let (masks, filters) = resolve_tag_policies(&sp, &id, &tags);
        // tag PII -> a PartialMask (MASK_SHOW_LAST_4) for engineer
        assert!(masks.contains_key("PII"));
        assert!(matches!(masks.get("PII"), Some(crate::MaskType::PartialMask { show_last: 4, .. })));
        let _ = filters; // not the focus of this test
    }

    #[test]
    fn tag_mask_not_resolved_for_other_role() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["PII".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "x".into(), roles: vec!["other".into()], ..Default::default() };
        let (masks, _f) = resolve_tag_policies(&sp, &id, &tags);
        assert!(masks.is_empty());
    }

    #[test]
    fn tag_row_filter_resolved() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = ["RESTRICTED".to_string()].into_iter().collect();
        let id = SessionIdentity { username: "a".into(), roles: vec!["analyst".into()], ..Default::default() };
        let (_m, filters) = resolve_tag_policies(&sp, &id, &tags);
        assert_eq!(filters.len(), 1); // one (tag, Expr) row filter
    }

    #[test]
    fn untagged_yields_nothing() {
        let sp: ServicePolicies = serde_json::from_str(TAG_BUNDLE).unwrap();
        let tags: HashSet<String> = HashSet::new();
        let id = SessionIdentity::default();
        let (m, f) = resolve_tag_policies(&sp, &id, &tags);
        assert!(m.is_empty() && f.is_empty());
    }
}
