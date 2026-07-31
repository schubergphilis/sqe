//! Ranger Admin REST fixtures for the access-control e2e suite.
//!
//! Two test-owned services are used so the suite never touches the demo's
//! shared `hive` service: linking a tag service or adding policies there would
//! change the downloaded bundle for Spark/Kyuubi, which
//! `quickstart/polaris-ranger-keycloak/parity-test.sh` cross-compares against.
//!
//! Ranger facts encoded here:
//!   - state-changing requests need the `X-XSRF-HEADER` header, else 401
//!   - tag-based policies live in a service of type `tag`, which must be linked
//!     to the resource service via its `tagService` field for the resource
//!     service's policy-download bundle to carry a `tagPolicies` block
//!   - SQE reads only mask (policyType 1) and row-filter (policyType 2)
//!     policies from the hive-type service; access (policyType 0) policies are
//!     ignored by SQE (its coarse gate is the `polaris` service)

#![allow(dead_code)]

use anyhow::{bail, Context};
use serde_json::Value;

/// Name prefix for every policy this suite creates. Setup deletes all policies
/// carrying it, so a crashed run cannot poison the next one.
pub const PREFIX: &str = "sqe-ac-e2e-";
/// Test-owned resource service (fine-grained masks + row filters).
pub const HIVE_SERVICE: &str = "sqe_ac_hive";
/// Test-owned tag service, linked to `HIVE_SERVICE`.
pub const TAG_SERVICE: &str = "sqe_ac_tag";

pub struct RangerAdmin {
    base: String,
    user: String,
    pass: String,
    client: reqwest::Client,
}

impl RangerAdmin {
    /// Ranger Admin at `AC_RANGER_URL` (default `http://localhost:26080`) with
    /// the quickstart's admin credentials.
    pub fn from_env() -> Self {
        Self {
            base: std::env::var("AC_RANGER_URL")
                .unwrap_or_else(|_| "http://localhost:26080".to_string()),
            user: std::env::var("AC_RANGER_USER").unwrap_or_else(|_| "admin".to_string()),
            pass: std::env::var("RANGER_ADMIN_PASSWORD")
                .unwrap_or_else(|_| "rangerR0cks!".to_string()),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.base, path))
            .basic_auth(&self.user, Some(&self.pass))
            .header("X-XSRF-HEADER", "x")
            .header("Content-Type", "application/json")
    }

    /// Panic with the HTTP status when Ranger Admin is not answering. Called by
    /// every test: with `SQE_AC_E2E=1` set, an absent stack must fail, not skip.
    pub async fn require_reachable(&self) {
        let url = "/service/public/v2/api/servicedef";
        match self.req(reqwest::Method::GET, url).send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => panic!(
                "Ranger Admin at {} answered HTTP {} for {url}; start the stack with \
                 scripts/access-control-test.sh",
                self.base,
                r.status()
            ),
            Err(e) => panic!(
                "Ranger Admin at {} is unreachable ({e}); start the stack with \
                 scripts/access-control-test.sh",
                self.base
            ),
        }
    }

    async fn service_by_name(&self, name: &str) -> anyhow::Result<Option<Value>> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/public/v2/api/service/name/{name}"),
            )
            .send()
            .await
            .context("GET service by name")?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            bail!("GET service {name} -> HTTP {}", resp.status());
        }
        Ok(Some(resp.json().await.context("parse service json")?))
    }

    /// Create `sqe_ac_hive` (type hive) and `sqe_ac_tag` (type tag) if absent,
    /// then link the tag service to the hive service. Idempotent.
    pub async fn ensure_services(&self) -> anyhow::Result<()> {
        if self.service_by_name(HIVE_SERVICE).await?.is_none() {
            let body = serde_json::json!({
                "name": HIVE_SERVICE,
                "type": "hive",
                "configs": {
                    "username": "admin",
                    "password": "none",
                    "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
                    "jdbc.url": "none"
                },
                "isEnabled": true
            });
            let resp = self
                .req(reqwest::Method::POST, "/service/public/v2/api/service")
                .json(&body)
                .send()
                .await
                .context("POST hive service")?;
            if !resp.status().is_success() {
                bail!(
                    "create {HIVE_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }

        if self.service_by_name(TAG_SERVICE).await?.is_none() {
            let body = serde_json::json!({
                "name": TAG_SERVICE,
                "type": "tag",
                "configs": {},
                "isEnabled": true
            });
            let resp = self
                .req(reqwest::Method::POST, "/service/public/v2/api/service")
                .json(&body)
                .send()
                .await
                .context("POST tag service")?;
            if !resp.status().is_success() {
                bail!(
                    "create {TAG_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }

        // Link: set `tagService` on the hive service and PUT it back. Without
        // the link the hive bundle carries no tagPolicies block.
        let mut hive = self
            .service_by_name(HIVE_SERVICE)
            .await?
            .context("hive service must exist after creation")?;
        if hive.get("tagService").and_then(Value::as_str) != Some(TAG_SERVICE) {
            hive["tagService"] = Value::String(TAG_SERVICE.to_string());
            let id = hive["id"].as_i64().context("hive service id")?;
            let resp = self
                .req(
                    reqwest::Method::PUT,
                    &format!("/service/public/v2/api/service/{id}"),
                )
                .json(&hive)
                .send()
                .await
                .context("PUT hive service with tagService")?;
            if !resp.status().is_success() {
                bail!(
                    "link {TAG_SERVICE} to {HIVE_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }
        Ok(())
    }

    /// All policies of a service.
    pub async fn get_policies(&self, service: &str) -> anyhow::Result<Vec<Value>> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/public/v2/api/policy?serviceName={service}"),
            )
            .send()
            .await
            .context("GET policies")?;
        if !resp.status().is_success() {
            bail!("GET policies for {service} -> HTTP {}", resp.status());
        }
        resp.json().await.context("parse policies json")
    }

    /// Create a policy. Returns its Ranger id.
    pub async fn create_policy(&self, body: Value) -> anyhow::Result<i64> {
        let resp = self
            .req(reqwest::Method::POST, "/service/public/v2/api/policy")
            .json(&body)
            .send()
            .await
            .context("POST policy")?;
        if !resp.status().is_success() {
            bail!(
                "create policy {} -> HTTP {}: {}",
                body["name"],
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        let created: Value = resp.json().await.context("parse created policy")?;
        created["id"].as_i64().context("created policy id")
    }

    /// Replace a policy by id (used to add denyPolicyItems to an existing one).
    pub async fn update_policy(&self, id: i64, body: Value) -> anyhow::Result<()> {
        let resp = self
            .req(
                reqwest::Method::PUT,
                &format!("/service/public/v2/api/policy/{id}"),
            )
            .json(&body)
            .send()
            .await
            .context("PUT policy")?;
        if !resp.status().is_success() {
            bail!(
                "update policy {id} -> HTTP {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    /// Delete one policy by service + name. Missing is not an error.
    pub async fn delete_policy(&self, service: &str, name: &str) -> anyhow::Result<()> {
        let resp = self
            .req(
                reqwest::Method::DELETE,
                &format!("/service/public/v2/api/policy?servicename={service}&policyname={name}"),
            )
            .send()
            .await
            .context("DELETE policy")?;
        if resp.status().is_success() || resp.status().as_u16() == 404 {
            return Ok(());
        }
        bail!("delete policy {name} -> HTTP {}", resp.status());
    }

    /// Delete every `sqe-ac-e2e-` policy from both test-owned services.
    /// Returns how many were removed.
    pub async fn delete_test_policies(&self) -> anyhow::Result<usize> {
        let mut removed = 0;
        for service in [HIVE_SERVICE, TAG_SERVICE] {
            for p in self.get_policies(service).await? {
                let Some(name) = p["name"].as_str() else { continue };
                if name.starts_with(PREFIX) {
                    self.delete_policy(service, name).await?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// The policy bundle SQE downloads. Used to capture a real `tagPolicies`
    /// sample for `sqe-policy`'s unit test.
    pub async fn download_bundle(&self, service: &str) -> anyhow::Result<Value> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/plugins/policies/download/{service}"),
            )
            .send()
            .await
            .context("GET policy bundle")?;
        if !resp.status().is_success() {
            bail!("download bundle for {service} -> HTTP {}", resp.status());
        }
        resp.json().await.context("parse bundle json")
    }
}
