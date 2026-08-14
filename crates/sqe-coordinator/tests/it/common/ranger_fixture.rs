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

/// The COARSE-gate service, which this suite does NOT own. SQE's `GRANT`/`REVOKE`
/// write here (`[access_control] service-name` in the quickstart's `sqe.toml`) and
/// Polaris's embedded authorizer enforces it. Shared with the demo and with the
/// Spark suite, so only the coordinates below may be cleaned from it.
pub const COARSE_SERVICE: &str = "polaris";
/// Catalogs in which this suite owns the `ac` namespace.
const SUITE_CATALOGS: [&str; 2] = ["sales_wh", "ops_wh"];
/// The namespace this suite owns inside each of those catalogs.
const SUITE_NAMESPACE: &str = "ac";

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

    /// One entry point for everything the suite needs on the Ranger side:
    /// the tag servicedef's row-filter capability, the two test-owned services
    /// and their link, a clean `sqe-ac-e2e-` policy slate, and no coarse-gate
    /// grant left inside the suite's own namespaces. Idempotent, so every test
    /// can call it in setup.
    ///
    /// Returns how many stale policies were removed, across both the test-owned
    /// services and the coarse-gate service.
    pub async fn bootstrap(&self) -> anyhow::Result<usize> {
        self.ensure_tag_rowfilter_support().await?;
        self.ensure_services().await?;
        self.clear_projected_tag_resources().await?;
        let test_policies = self.delete_test_policies().await?;
        let suite_grants = self.delete_suite_grants().await?;
        Ok(test_policies + suite_grants)
    }

    /// Give the built-in `tag` servicedef a `rowFilterDef`, so tag-based ROW
    /// FILTER policies can be created at all.
    ///
    /// Ranger populates the tag servicedef from each component servicedef, but
    /// asymmetrically: `dataMaskDef` is propagated unconditionally, while
    /// `rowFilterDef` is propagated only when Ranger Admin runs with
    /// `ranger.servicedef.autopropagate.rowfilterdef.to.tag=true`
    /// (`AbstractServiceStore`, default FALSE). Out of the box the tag
    /// servicedef therefore has a populated `dataMaskDef` and an empty
    /// `rowFilterDef: {}`, and Ranger rejects a tag row-filter policy with
    /// "tag policy can specify values for one of the following resource sets:
    /// does not have any resource hierarchies".
    ///
    /// This mirrors `dataMaskDef`'s single `tag` resource hierarchy into
    /// `rowFilterDef` and adds `hive:select` as the row-filter access type,
    /// which is what the auto-propagation would have written. It is a
    /// TEST-ENVIRONMENT patch: a Ranger upgrade or `docker compose down -v`
    /// resets it. The durable fix for a real deployment is the config property,
    /// set in `ranger-admin-site.xml`.
    ///
    /// No-op when `rowFilterDef` already carries resources, so it survives both
    /// a previously-patched stack and a properly-configured Ranger.
    pub async fn ensure_tag_rowfilter_support(&self) -> anyhow::Result<()> {
        let Some(mut tag_def) = self.servicedef_by_name("tag").await? else {
            bail!("built-in `tag` servicedef is missing from this Ranger");
        };
        let already = tag_def["rowFilterDef"]["resources"]
            .as_array()
            .is_some_and(|r| !r.is_empty());
        if already {
            return Ok(());
        }
        let mask_resources = tag_def["dataMaskDef"]["resources"].clone();
        if !mask_resources.as_array().is_some_and(|r| !r.is_empty()) {
            bail!(
                "tag servicedef has no dataMaskDef.resources to mirror; Ranger has not \
                 propagated any component servicedef into it yet"
            );
        }
        tag_def["rowFilterDef"] = serde_json::json!({
            "resources": mask_resources,
            "accessTypes": [{"itemId": 1, "name": "hive:select", "label": "hive:select"}],
        });
        sanitize_aggregate_servicedef(&mut tag_def);
        let id = tag_def["id"].as_i64().context("tag servicedef id")?;
        let resp = self
            .req(
                reqwest::Method::PUT,
                &format!("/service/public/v2/api/servicedef/{id}"),
            )
            .json(&tag_def)
            .send()
            .await
            .context("PUT tag servicedef with rowFilterDef")?;
        if !resp.status().is_success() {
            bail!(
                "patch tag servicedef rowFilterDef -> HTTP {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    async fn servicedef_by_name(&self, name: &str) -> anyhow::Result<Option<Value>> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/public/v2/api/servicedef/name/{name}"),
            )
            .send()
            .await
            .context("GET servicedef by name")?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            bail!("GET servicedef {name} -> HTTP {}", resp.status());
        }
        Ok(Some(resp.json().await.context("parse servicedef json")?))
    }

    /// True when the tag servicedef can express row filters. Lets a test state
    /// the platform precondition it depends on instead of guessing from an error.
    pub async fn tag_rowfilter_supported(&self) -> anyhow::Result<bool> {
        let Some(tag_def) = self.servicedef_by_name("tag").await? else {
            return Ok(false);
        };
        Ok(tag_def["rowFilterDef"]["resources"]
            .as_array()
            .is_some_and(|r| !r.is_empty()))
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

        self.grant_object_level_defer_item().await?;
        Ok(())
    }

    /// Make Kyuubi defer object-level decisions to Polaris on the test-owned
    /// frontend service.
    ///
    /// Kyuubi checks its OWN privilege before Polaris is consulted and
    /// short-circuits without a matching `policyType-0` item, failing with
    /// `AccessControlException: Permission denied: user [bob] does not have
    /// [select] privilege on [...]` even where Polaris would allow the read. SQE
    /// ignores `policyType-0` entirely, so leaving this out makes the two engines
    /// disagree on every object-level grant and every Spark test refuse for the
    /// wrong reason.
    ///
    /// It grants no data access: Polaris still decides, which
    /// `object_denial_survives_the_frontend_defer_policy` proves.
    ///
    /// Written through the GRANT API rather than as a named policy because
    /// creating a hive-type service makes Ranger auto-generate
    /// `all - database, table, column` over exactly `database=*/table=*/column=*`
    /// (granted to `admin` and `{OWNER}` only, so useless to a query user). That
    /// policy owns the resource signature and a separately named one is refused
    /// with `error code[3010] Another policy already exists for matching
    /// resource`. The grant API merges an item into the existing match instead.
    ///
    /// Every access type Kyuubi may check has to be listed: `update` for INSERT,
    /// `create` for DDL, and a missing one short-circuits exactly as above.
    async fn grant_object_level_defer_item(&self) -> anyhow::Result<()> {
        // Authored through the AUTHENTICATED policy API, not
        // `/service/plugins/services/grant/*`. Ranger declares that endpoint
        // `security="none"`, so it takes no credentials at all, and Ranger 2.9.0
        // refuses it with HTTP 400 "Unauthenticated access not allowed" unless
        // `ranger.admin.allow.unauthenticated.access` is enabled. On 2.9.0 the old
        // form failed here and took all 42 cases down at setup.
        //
        // The grant API merged an item into the auto-generated policy server-side.
        // That merge happens here instead: find the policy owning the wildcard
        // signature, append the `public` item, and PUT it back. Reading the whole
        // policy first is what keeps the auto-generated `admin` / `{OWNER}` items
        // intact.
        let accesses: Vec<Value> = [
            "select", "update", "create", "drop", "alter", "index", "lock", "read", "write",
        ]
        .iter()
        .map(|t| serde_json::json!({"type": t, "isAllowed": true}))
        .collect();
        let defer_item = serde_json::json!({
            "groups": ["public"],
            "accesses": accesses,
            "delegateAdmin": false,
        });

        let wildcard = |p: &Value, key: &str| -> bool {
            p["resources"][key]["values"] == serde_json::json!(["*"])
        };
        let mut policy = self
            .get_policies(HIVE_SERVICE)
            .await?
            .into_iter()
            .find(|p| {
                wildcard(p, "database") && wildcard(p, "table") && wildcard(p, "column")
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no policy owns database=*/table=*/column=* on {HIVE_SERVICE}; Ranger \
                     normally auto-generates `all - database, table, column` when the \
                     service is created"
                )
            })?;

        let items = policy
            .get_mut("policyItems")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| anyhow::anyhow!("wildcard policy has no policyItems array"))?;
        // Idempotent: this runs in every test's setup.
        let already = items
            .iter()
            .any(|i| i["groups"] == serde_json::json!(["public"]));
        if !already {
            items.push(defer_item);
        }
        let id = policy["id"]
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("wildcard policy has no id"))?;
        self.update_policy(id, policy).await.context(
            "grant the object-level defer item; without it Kyuubi refuses every Spark read \
             before Polaris is consulted",
        )
    }

    /// Delete every projected tag ASSOCIATION for the test-owned frontend service.
    ///
    /// Ranger's tag store is global and PERSISTS across runs, and the fixture's
    /// policy cleanup does not touch it. Without this, a run that projected an
    /// association leaves it behind, and the next run's tag-parity test passes from
    /// the STALE association even with projection disabled. Verified: the parity
    /// test passed with `project-tags = false` until this was added, which made it
    /// prove nothing.
    ///
    /// Tag DEFINITIONS are left alone. They are global vocabulary shared with the
    /// demo, and a definition with no association grants nothing.
    async fn clear_projected_tag_resources(&self) -> anyhow::Result<()> {
        let resp = self
            .req(reqwest::Method::GET, "/service/tags/resources")
            .send()
            .await
            .context("GET tag resources")?;
        if !resp.status().is_success() {
            // A Ranger without the tag store reachable is not a reason to fail
            // every test; the parity test will fail on its own if it matters.
            return Ok(());
        }
        let _ = resp;

        // Delete via the BULK import with `op: delete`, feeding back Ranger's own
        // download document. `DELETE /service/tags/resource/{id}` answers 500 while
        // the resource still carries an association, so the obvious per-resource
        // loop silently does nothing.
        let bundle = self.download_tag_bundle(HIVE_SERVICE).await?;
        let has_resources = bundle
            .get("serviceResources")
            .and_then(Value::as_array)
            .is_some_and(|r| !r.is_empty());
        if !has_resources {
            return Ok(());
        }
        let mut doc = bundle;
        doc["op"] = Value::String("delete".to_string());
        let resp = self
            .req(reqwest::Method::PUT, "/service/tags/importservicetags")
            .json(&doc)
            .send()
            .await
            .context("PUT importservicetags op=delete")?;
        if !resp.status().is_success() {
            bail!(
                "clearing projected tag associations on {HIVE_SERVICE} -> HTTP {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    /// Ranger's tag bundle for a resource service, the same document its plugins
    /// download.
    async fn download_tag_bundle(&self, service: &str) -> anyhow::Result<Value> {
        let resp = self
            .req(
                reqwest::Method::GET,
                // SECURE path: the non-secure one is `security="none"` and serves
                // the whole tag store to an unauthenticated caller, and Ranger
                // 2.9.0 stops serving it without
                // `ranger.admin.allow.unauthenticated.download.access`.
                &format!("/service/tags/secure/download/{service}"),
            )
            .send()
            .await
            .context("GET tag bundle")?;
        if !resp.status().is_success() {
            bail!("GET tag bundle for {service} -> HTTP {}", resp.status());
        }
        resp.json().await.context("decode tag bundle")
    }

    /// True when the object-level defer item is present on the frontend service.
    ///
    /// The Spark guard test asserts this BEFORE asserting a Polaris denial. Absent
    /// the item, Kyuubi refuses first and the guard would pass while proving
    /// nothing about whether the blanket allow leaks object-level authority.
    ///
    /// `service` matters and is easy to get wrong. SQE reads the test-owned
    /// [`HIVE_SERVICE`]; Kyuubi reads whatever `ranger-spark-security.xml` names
    /// inside the Spark container, which is the quickstart's `query`. Checking the
    /// test-owned service and then asserting a Spark denial proves nothing, since
    /// Kyuubi never read it.
    #[allow(dead_code)]
    pub async fn object_level_defer_item_present(&self, service: &str) -> anyhow::Result<bool> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/plugins/policies/service/name/{service}"),
            )
            .send()
            .await
            .context("GET frontend policies")?;
        let body: Value = resp.json().await.context("decode frontend policies")?;
        let policies = body
            .get("policies")
            .and_then(Value::as_array)
            .or_else(|| body.as_array())
            .cloned()
            .unwrap_or_default();
        // The RESOURCE has to match, not merely "some type-0 policy grants public
        // select". Ranger seeds `Information_schema database tables columns` with
        // exactly a public select item, so the loose form returned true with the
        // defer item revoked and the guard test passed while proving nothing.
        let wildcard = |p: &Value, level: &str| {
            p["resources"][level]["values"]
                .as_array()
                .is_some_and(|v| v.iter().any(|x| x.as_str() == Some("*")))
        };
        Ok(policies.iter().any(|p| {
            p["policyType"].as_i64() == Some(0)
                && wildcard(p, "database")
                && wildcard(p, "table")
                && wildcard(p, "column")
                && p["policyItems"].as_array().is_some_and(|items| {
                    items.iter().any(|i| {
                        i["groups"]
                            .as_array()
                            .is_some_and(|g| g.iter().any(|x| x.as_str() == Some("public")))
                            && i["accesses"].as_array().is_some_and(|a| {
                                a.iter().any(|x| x["type"].as_str() == Some("select"))
                            })
                    })
                })
        }))
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

    /// Names of every coarse-gate policy scoped inside the suite's own
    /// namespaces. Exposed so a test can assert the slate is clean rather than
    /// infer it from a query result.
    ///
    /// A coarse service that is absent or renamed yields an empty list rather
    /// than an error: on such a stack there is nothing of ours to clean, and
    /// failing here would break every test in the suite.
    pub async fn suite_scoped_grants(&self) -> Vec<String> {
        self.get_policies(COARSE_SERVICE)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(is_suite_scoped)
            .filter_map(|p| p["name"].as_str().map(str::to_string))
            .collect()
    }

    /// Delete every coarse-gate policy scoped inside the suite's own namespaces.
    /// Returns how many were removed.
    ///
    /// WHY THIS EXISTS: `delete_test_policies` cleans by NAME PREFIX, and SQE's
    /// own `GRANT` never carries that prefix. It posts to Ranger's grant API
    /// (`/service/plugins/services/grant/<service>`), and Ranger names the policy
    /// it creates `grant-<epoch_ms>`. So every grant the suite made through SQL
    /// outlived the run that made it, and the next run started pre-authorized.
    /// Two tests whose whole job is to prove access is denied BEFORE a grant
    /// (`denied_before_any_grant`,
    /// `all_tables_in_schema_grant_covers_the_namespace`) cannot survive that:
    /// they timed out after 120 s with "still allowed for alice with 3 rows".
    ///
    /// A LIST failure is tolerated for the reason given on `suite_scoped_grants`.
    /// A DELETE failure is not: a policy identified as ours that will not go away
    /// is the exact condition this function exists to prevent.
    pub async fn delete_suite_grants(&self) -> anyhow::Result<usize> {
        let names = self.suite_scoped_grants().await;
        for name in &names {
            self.delete_policy(COARSE_SERVICE, name)
                .await
                .with_context(|| {
                    format!("removing leaked coarse-gate grant `{name}` from {COARSE_SERVICE}")
                })?;
        }
        Ok(names.len())
    }

    /// The policy bundle SQE downloads. Used to capture a real `tagPolicies`
    /// sample for `sqe-policy`'s unit test.
    pub async fn download_bundle(&self, service: &str) -> anyhow::Result<Value> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/plugins/secure/policies/download/{service}"),
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

/// The single `values` entry of a policy resource, or `None` when the resource is
/// absent, empty, or carries more than one value.
///
/// More than one value means the policy spans coordinates beyond a single one of
/// ours, so it is not exclusively the suite's to delete.
fn sole_resource_value(policy: &Value, key: &str) -> Option<String> {
    let values = policy["resources"][key]["values"].as_array()?;
    match values.as_slice() {
        [one] => one.as_str().map(str::to_string),
        _ => None,
    }
}

/// True when a coarse-gate policy is scoped INSIDE one of the suite's own
/// namespaces (`sales_wh.ac` or `ops_wh.ac`), and is therefore the suite's to
/// delete.
///
/// Scoped by RESOURCE rather than by name, deliberately. The bug this guards
/// against was a name-prefix clean that could not see policies Ranger named
/// itself, and adding a second prefix (`grant-`) would repeat the mistake the
/// first time Ranger changes that format or a grant merges into a policy someone
/// else named. The resource coordinate is what actually makes a policy ours.
///
/// Catalog-level and wildcard policies are deliberately NOT matched:
///   - `bootstrap-ranger.sh` seeds admin and baseline grants at `catalog: *`,
///     shared with the demo and with Polaris's own operation. Deleting those
///     breaks the stack for everything, not just this suite.
///   - a catalog-level policy confers namespace-NAME visibility, not table read,
///     so it is not what poisons a denial baseline.
///   - the parity demo works in namespace `acparity`, which does not match
///     `SUITE_NAMESPACE` and is therefore untouched.
fn is_suite_scoped(policy: &Value) -> bool {
    if sole_resource_value(policy, "namespace").as_deref() != Some(SUITE_NAMESPACE) {
        return false;
    }
    sole_resource_value(policy, "catalog")
        .is_some_and(|catalog| SUITE_CATALOGS.contains(&catalog.as_str()))
}

/// Make Ranger's auto-generated `tag` servicedef pass Ranger's OWN validator.
///
/// The tag servicedef is assembled by Ranger from every registered component
/// servicedef, and the result does not round-trip: submitting it back verbatim
/// is rejected. Observed on Ranger 2.8.0 with the stock set of component defs:
///
///   - duplicate access type name `ozone:assume_role` and duplicate itemId
///     201209 ("duplicate access type name ... in access types")
///   - elasticsearch implied grants naming access types that are absent from
///     the aggregate list (`elasticsearch:indices_bulk`,
///     `indices_search_shards`, `indices_index`, `indices_put`)
///
/// Both are defects in what Ranger generated, not in what we add. This drops the
/// duplicate entries (keeping the first of each name/itemId) and prunes implied
/// grants that reference access types the def does not declare. Nothing else is
/// touched, and the components involved (ozone, elasticsearch) are not part of
/// this stack.
fn sanitize_aggregate_servicedef(def: &mut Value) {
    let Some(access_types) = def["accessTypes"].as_array() else {
        return;
    };

    // Pass 1: dedupe by name AND by itemId, keeping the first occurrence.
    let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_item_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut deduped: Vec<Value> = Vec::with_capacity(access_types.len());
    for at in access_types {
        let name = at["name"].as_str().unwrap_or_default().to_string();
        let item_id = at["itemId"].as_i64().unwrap_or(-1);
        if !seen_names.insert(name) || !seen_item_ids.insert(item_id) {
            continue;
        }
        deduped.push(at.clone());
    }

    // Pass 2: prune implied grants that name an access type the def no longer
    // declares (or never declared).
    let declared: std::collections::HashSet<String> = deduped
        .iter()
        .filter_map(|at| at["name"].as_str().map(str::to_string))
        .collect();
    for at in &mut deduped {
        if let Some(implied) = at["impliedGrants"].as_array() {
            let kept: Vec<Value> = implied
                .iter()
                .filter(|g| g.as_str().is_some_and(|n| declared.contains(n)))
                .cloned()
                .collect();
            at["impliedGrants"] = Value::Array(kept);
        }
    }

    def["accessTypes"] = Value::Array(deduped);
}

/// Stop and start the `ranger-admin` container of the quickstart stack.
///
/// Used by the policy-breaker test, which needs a REAL outage: pointing a second
/// handler at a dead port does not work, because a fresh handler's cold
/// `TableMetadataCache` denies for an unrelated reason (unknown tag state) and
/// the test passes vacuously. The only way to isolate the outage is to take
/// Ranger away from a handler that is already warm.
pub struct RangerContainer {
    stack_dir: std::path::PathBuf,
}

impl RangerContainer {
    pub fn new() -> Self {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let root = manifest
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root");
        Self {
            stack_dir: root.join("quickstart/polaris-ranger-keycloak"),
        }
    }

    fn compose(&self, args: &[&str]) -> anyhow::Result<()> {
        let out = std::process::Command::new("docker")
            .arg("compose")
            .arg("--project-directory")
            .arg(&self.stack_dir)
            .arg("-f")
            .arg(self.stack_dir.join("docker-compose.yml"))
            .args(args)
            .output()
            .context("run docker compose")?;
        if !out.status.success() {
            bail!(
                "docker compose {:?} failed: {}",
                args,
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    pub fn stop(&self) -> anyhow::Result<()> {
        self.compose(&["stop", "ranger-admin"])
    }

    pub fn start(&self) -> anyhow::Result<()> {
        self.compose(&["start", "ranger-admin"])
    }
}

/// Restarts `ranger-admin` on drop, so a panicking test cannot leave the
/// container stopped and poison every test that follows. `Drop` cannot be async,
/// which is why the container control above is a blocking `std::process`.
pub struct RangerOutage {
    container: RangerContainer,
}

impl RangerOutage {
    /// Stop `ranger-admin` and return a guard that restarts it.
    pub fn begin() -> anyhow::Result<Self> {
        let container = RangerContainer::new();
        container.stop()?;
        Ok(Self { container })
    }
}

impl Drop for RangerOutage {
    fn drop(&mut self) {
        if let Err(e) = self.container.start() {
            eprintln!("WARNING: failed to restart ranger-admin after the outage test: {e}");
        }
    }
}

/// `is_suite_scoped` decides what gets DELETED from a service the suite does not
/// own, so its boundaries are unit-tested rather than trusted. Every JSON shape
/// here is the shape Ranger actually returns: `resources.<key>.values` is always
/// an array, and the grant API omits levels below the one granted.
#[cfg(test)]
mod suite_scope_tests {
    use super::*;

    /// Ranger's own shape for a policy the grant API created at a coordinate.
    fn policy(resources: Value) -> Value {
        serde_json::json!({"name": "grant-1786370165684", "resources": resources})
    }

    fn table_policy(catalog: &str, namespace: &str, table: &str) -> Value {
        policy(serde_json::json!({
            "root":      {"values": ["*"]},
            "catalog":   {"values": [catalog]},
            "namespace": {"values": [namespace]},
            "table":     {"values": [table]},
        }))
    }

    #[test]
    fn matches_a_leaked_table_grant_in_either_suite_catalog() {
        assert!(is_suite_scoped(&table_policy("sales_wh", "ac", "orders")));
        assert!(is_suite_scoped(&table_policy("ops_wh", "ac", "audit")));
    }

    /// `GRANT ... ON ALL TABLES IN SCHEMA sales_wh.ac` writes a namespace-level
    /// policy with no `table` resource. It is the leak behind
    /// `all_tables_in_schema_grant_covers_the_namespace`, so it must match.
    #[test]
    fn matches_a_namespace_level_grant_with_no_table() {
        let p = policy(serde_json::json!({
            "root":      {"values": ["*"]},
            "catalog":   {"values": ["sales_wh"]},
            "namespace": {"values": ["ac"]},
        }));
        assert!(is_suite_scoped(&p));
    }

    /// `GRANT ... ON ALL TABLES IN SCHEMA` writes a table WILDCARD inside a named
    /// namespace, which is not the same thing as a wildcard namespace. Observed
    /// live as `grant-1786441479476` at `sales_wh` / `ac` / `*`, left behind by
    /// `all_tables_in_schema_grant_covers_the_namespace`.
    #[test]
    fn matches_a_table_wildcard_inside_a_suite_namespace() {
        assert!(is_suite_scoped(&table_policy("sales_wh", "ac", "*")));
    }

    /// The baseline and admin grants `bootstrap-ranger.sh` seeds. Deleting any of
    /// these breaks the stack for the demo and for Polaris itself.
    #[test]
    fn spares_the_bootstrap_wildcard_and_catalog_level_grants() {
        for resources in [
            serde_json::json!({"root": {"values": ["*"]}}),
            serde_json::json!({"root": {"values": ["*"]}, "catalog": {"values": ["*"]}}),
            serde_json::json!({
                "root": {"values": ["*"]},
                "catalog": {"values": ["*"]},
                "namespace": {"values": ["*"]},
            }),
            serde_json::json!({
                "root": {"values": ["*"]},
                "catalog": {"values": ["*"]},
                "namespace": {"values": ["*"]},
                "table": {"values": ["*"]},
            }),
        ] {
            assert!(
                !is_suite_scoped(&policy(resources.clone())),
                "must not claim the bootstrap grant {resources}"
            );
        }
    }

    /// A catalog-level grant confers namespace-name visibility, not table read.
    #[test]
    fn spares_a_catalog_level_grant_with_no_namespace() {
        let p = policy(serde_json::json!({
            "root":    {"values": ["*"]},
            "catalog": {"values": ["sales_wh"]},
        }));
        assert!(!is_suite_scoped(&p));
    }

    /// The parity demo lives in `acparity`, and other suites in other namespaces.
    #[test]
    fn spares_other_namespaces_including_the_parity_demo() {
        assert!(!is_suite_scoped(&table_policy(
            "sales_wh", "acparity", "customers"
        )));
        assert!(!is_suite_scoped(&table_policy("sales_wh", "sales", "orders")));
    }

    /// `ac` in a catalog the suite does not own is not the suite's to delete.
    #[test]
    fn spares_the_ac_namespace_in_a_foreign_catalog() {
        assert!(!is_suite_scoped(&table_policy("other_wh", "ac", "orders")));
    }

    /// A multi-valued resource spans coordinates beyond one of ours.
    #[test]
    fn spares_a_policy_spanning_more_than_one_coordinate() {
        let multi_ns = policy(serde_json::json!({
            "catalog":   {"values": ["sales_wh"]},
            "namespace": {"values": ["ac", "sales"]},
        }));
        assert!(!is_suite_scoped(&multi_ns));
        let multi_cat = policy(serde_json::json!({
            "catalog":   {"values": ["sales_wh", "other_wh"]},
            "namespace": {"values": ["ac"]},
        }));
        assert!(!is_suite_scoped(&multi_cat));
    }

    /// Malformed or absent resources must not panic and must not match.
    #[test]
    fn spares_malformed_and_empty_resources() {
        assert!(!is_suite_scoped(&policy(serde_json::json!({}))));
        assert!(!is_suite_scoped(&serde_json::json!({"name": "x"})));
        assert!(!is_suite_scoped(&policy(serde_json::json!({
            "catalog":   {"values": []},
            "namespace": {"values": ["ac"]},
        }))));
        assert!(!is_suite_scoped(&policy(serde_json::json!({
            "catalog":   {"values": ["sales_wh"]},
            "namespace": {"values": [42]},
        }))));
    }
}
