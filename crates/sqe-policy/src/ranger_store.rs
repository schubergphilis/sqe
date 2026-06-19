//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::logical_expr::lit;
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use sqe_core::config::RangerPolicyConfig;
use sqe_core::{SecretString, SessionUser};
use tracing::{debug, warn};

use crate::policy_breaker::PolicyCircuitBreaker;
use crate::policy_expr::parse_sql_predicate;
use crate::{MaskType, PolicyStore, ResolvedPolicy};

// --- Ranger policy bundle model (ServicePolicies) ---

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ServicePolicies {
    #[serde(rename = "policyVersion", default)]
    #[allow(dead_code)] // read only in #[cfg(test)]; used by serde and test assertions
    pub(crate) policy_version: Option<i64>,
    #[serde(default)]
    pub(crate) policies: Vec<RangerPolicy>,
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
fn map_mask(info: &DataMaskInfo, column: &str) -> Result<Option<MaskType>, ()> {
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
            parse_sql_predicate(&substituted)
                .map(|e| Some(MaskType::Custom(e)))
                .map_err(|_| ())
        }
        // Phase 2: MASK, MASK_SHOW_LAST_4, MASK_SHOW_FIRST_4, MASK_DATE_SHOW_YEAR
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
                    match map_mask(&item.data_mask_info, column) {
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
                    match parse_sql_predicate(expr_str) {
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
            .data_mask_type = "MASK_SHOW_LAST_4".to_string();
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
}
