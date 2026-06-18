//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use sqe_core::config::RangerPolicyConfig;

use crate::policy_breaker::PolicyCircuitBreaker;
use crate::ResolvedPolicy;

// --- Ranger policy bundle model (ServicePolicies) ---

#[derive(Debug, Deserialize, Default)]
pub struct ServicePolicies {
    #[serde(rename = "policyVersion", default)]
    pub policy_version: Option<i64>,
    #[serde(default)]
    pub policies: Vec<RangerPolicy>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RangerPolicy {
    #[serde(default)]
    pub id: i64,
    /// 0 = access, 1 = DATAMASK, 2 = ROWFILTER.
    #[serde(rename = "policyType", default)]
    pub policy_type: i32,
    #[serde(rename = "isEnabled", default)]
    pub is_enabled: bool,
    /// Resource map: keys are "database", "table", "column".
    #[serde(default)]
    pub resources: HashMap<String, RangerResource>,
    #[serde(rename = "dataMaskPolicyItems", default)]
    pub data_mask_policy_items: Vec<DataMaskPolicyItem>,
    #[serde(rename = "rowFilterPolicyItems", default)]
    pub row_filter_policy_items: Vec<RowFilterPolicyItem>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RangerResource {
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(rename = "isExcludes", default)]
    pub is_excludes: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct DataMaskPolicyItem {
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(rename = "dataMaskInfo", default)]
    pub data_mask_info: DataMaskInfo,
}

#[derive(Debug, Deserialize, Default)]
pub struct DataMaskInfo {
    #[serde(rename = "dataMaskType", default)]
    pub data_mask_type: String,
    #[serde(rename = "valueExpr", default)]
    pub value_expr: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RowFilterPolicyItem {
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(rename = "rowFilterInfo", default)]
    pub row_filter_info: RowFilterInfo,
}

#[derive(Debug, Deserialize, Default)]
pub struct RowFilterInfo {
    #[serde(rename = "filterExpr", default)]
    pub filter_expr: Option<String>,
}

// --- RangerStore struct, constructor, and download fetch ---
// dead_code allowed until Task 5 wires PolicyStore::resolve.

/// Fine-grained policy store backed by a `hive`-type Ranger service.
#[allow(dead_code)]
pub struct RangerStore {
    client: Client,
    /// Base download URL, e.g. ".../service/plugins/policies/download/hive".
    download_url: String,
    admin_user: String,
    admin_password: String,
    cache: Cache<String, ResolvedPolicy>,
    breaker: Arc<PolicyCircuitBreaker>,
}

#[allow(dead_code)]
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
            admin_password: cfg.admin_password.expose().to_string(),
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
            .basic_auth(&self.admin_user, Some(&self.admin_password))
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

#[cfg(test)]
mod tests {
    use super::*;

    const BUNDLE: &str = r#"{
      "policyVersion": 7,
      "policies": [
        {
          "id": 1, "policyType": 1, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales_wh.sales"]},
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
            "database": {"values": ["sales_wh.sales"]},
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
}
