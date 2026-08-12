//! RangerGrantBackend — translates GRANT/REVOKE/SHOW GRANTS into Apache Ranger
//! Admin REST calls. Enforcement is delegated to Polaris's embedded Ranger
//! authorizer; this backend only writes/reads Ranger policies.
//!
//! Ranger service-def: `polaris`. Resource hierarchy: root -> catalog ->
//! namespace -> table. Access types are Polaris-native hyphenated names.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::profile::{profile, PlannedPolicy};
use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantObjectKind,
    GrantStatement, Grantee, RevokeStatement,
};

/// The Ranger `GrantRevokeRequest` payload. Field renames match Ranger's
/// `org.apache.ranger.plugin.model.RangerPolicy.GrantRevokeRequest` JSON.
#[derive(Debug, Serialize)]
pub struct GrantRevokeRequest {
    pub grantor: String,
    pub resource: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    #[serde(rename = "accessTypes")]
    pub access_types: Vec<String>,
    #[serde(rename = "delegateAdmin")]
    pub delegate_admin: bool,
    #[serde(rename = "enableAudit")]
    pub enable_audit: bool,
    #[serde(rename = "replaceExistingPermissions")]
    pub replace_existing_permissions: bool,
    #[serde(rename = "isRecursive")]
    pub is_recursive: bool,
}

/// Map SQE's (object kind, SQL privilege) pair onto the profile's privilege name.
///
/// The profile has no separate view AXIS: a view privilege is its own name
/// (`SELECT VIEW`, `DROP VIEW`, `CREATE VIEW`) and the view's name goes in the
/// `table` resource slot, which is how Polaris models it and what SQE already did.
///
/// `ALL` on a view is deliberately NOT translated. v4 defines no view-scoped
/// `ALL`, so it stays `ALL`, which binds at the catalog level and is therefore
/// refused when a view is named. Inventing a view-scoped `ALL` here is exactly the
/// kind of local divergence adopting the profile exists to remove; the operator
/// gets an error naming the level instead of a privately-invented grant.
fn profile_privilege(object: GrantObjectKind, sql_priv: &str) -> String {
    let canonical = sql_priv.split_whitespace().collect::<Vec<_>>().join(" ").to_uppercase();
    if object == GrantObjectKind::View {
        return match canonical.as_str() {
            "SELECT" => "SELECT VIEW".to_string(),
            "DROP" => "DROP VIEW".to_string(),
            other => other.to_string(),
        };
    }
    canonical
}

/// The policies one statement has to write, outermost first, from the vendored
/// profile.
fn plan_for(
    object: GrantObjectKind,
    privilege: &str,
    realm: &str,
    catalog: &str,
    namespace: Option<&str>,
    table: Option<&str>,
) -> sqe_core::Result<Vec<PlannedPolicy>> {
    profile()
        .plan_grant(
            &profile_privilege(object, privilege),
            realm,
            catalog,
            namespace,
            table,
        )
        .map_err(sqe_core::SqeError::Execution)
}

/// The policy at the level the statement NAMES, which is the deepest in the plan.
///
/// REVOKE and DENY both act on this one only. The outer levels are traversal
/// shared with every other grant in the catalog: revoking them would strip
/// discovery from unrelated grants, and denying them would hide every object under
/// the namespace rather than the one named.
fn deepest_policy(
    object: GrantObjectKind,
    privilege: &str,
    realm: &str,
    catalog: &str,
    namespace: Option<&str>,
    table: Option<&str>,
) -> sqe_core::Result<PlannedPolicy> {
    let mut plan = plan_for(object, privilege, realm, catalog, namespace, table)?;
    plan.pop().ok_or_else(|| {
        sqe_core::SqeError::Execution(format!("Privilege '{privilege}' plans no policies"))
    })
}

/// Build a Ranger deny item for one grantee and access-type set.
///
/// Shared by DENY and by the REVOKE path that removes a denial, so the two agree
/// on the shape by construction rather than by two similar literals.
fn build_deny_item(grantee: &Grantee, access_types: &[String]) -> serde_json::Value {
    let accesses: Vec<serde_json::Value> = access_types
        .iter()
        .map(|t| serde_json::json!({"type": t, "isAllowed": true}))
        .collect();
    let mut item = serde_json::Map::new();
    match grantee {
        Grantee::User(n) => {
            item.insert("users".into(), serde_json::json!([n]));
        }
        Grantee::Role(n) => {
            item.insert("roles".into(), serde_json::json!([n]));
        }
        Grantee::Group(n) => {
            item.insert("groups".into(), serde_json::json!([n]));
        }
    }
    item.insert("accesses".into(), serde_json::Value::Array(accesses));
    serde_json::Value::Object(item)
}

/// Do two deny items name the same grantee with the same access types?
///
/// Ranger returns a stored policy item with every optional field populated, so
/// the item SQE builds is never byte-equal to the one Ranger echoes back. Only
/// the grantee lists and the access-type set carry meaning for deduplication.
fn deny_items_equivalent(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    fn names(v: &serde_json::Value, key: &str) -> Vec<String> {
        let mut out: Vec<String> = v
            .get(key)
            .and_then(serde_json::Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }
    fn accesses(v: &serde_json::Value) -> Vec<String> {
        let mut out: Vec<String> = v
            .get("accesses")
            .and_then(serde_json::Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.get("type").and_then(|t| t.as_str()).map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }
    names(a, "users") == names(b, "users")
        && names(a, "roles") == names(b, "roles")
        && names(a, "groups") == names(b, "groups")
        && accesses(a) == accesses(b)
}

/// Does this privilege name mean "everything"? `ALL PRIVILEGES` is the Unity
/// Catalog spelling and the profile aliases it to `ALL`.
fn is_all_privileges(privilege: &str) -> bool {
    let p = privilege.trim();
    p.eq_ignore_ascii_case("ALL") || p.eq_ignore_ascii_case("ALL PRIVILEGES")
}

/// Reject identifier values that could inject into the Ranger resource map.
/// Catalog/namespace/table/user/role names come from GRANT SQL and flow into
/// the JSON `resource` map body (not URL paths; the only URL-interpolated value
/// is `service_name`, which is operator-controlled config). Rejecting path
/// separators, control, and whitespace characters is defense-in-depth against
/// resource-map injection, matching the Polaris backend's `validate_url_identifier`.
fn validate_identifier(value: &str, what: &str) -> sqe_core::Result<()> {
    if value.is_empty() {
        return Err(sqe_core::SqeError::Execution(format!("{what} must not be empty")));
    }
    if let Some(bad) = value.chars().find(|c| {
        matches!(c, '/' | '?' | '#' | '%' | '\\') || c.is_whitespace() || c.is_control()
    }) {
        return Err(sqe_core::SqeError::Execution(format!(
            "{what} '{value}' contains invalid character {bad:?}"
        )));
    }
    Ok(())
}

/// Split a grantee into (users, roles) for a `GrantRevokeRequest`. Groups are
/// rejected: Polaris does not deliver groups to Ranger unless usersync runs.
/// Which level of a plan a resource map addresses. For error messages, so a 403
/// can say WHICH of the three policies Ranger refused.
fn level_name(resource: &BTreeMap<String, String>) -> &'static str {
    if resource.contains_key("table") {
        "table"
    } else if resource.contains_key("namespace") {
        "namespace"
    } else {
        "catalog"
    }
}

/// The grantor a mutation must be performed as.
///
/// Refuses rather than falling back to the configured admin user. Ranger
/// authorizes against this name, so the fallback is not a default -- it is a
/// different security decision (perform the mutation with SQE's own authority),
/// and with `grant_authority = "ranger-delegate"` it would be the only check
/// standing between an authenticated caller and any grant they cared to write.
/// Making it an error means "the coarse gate is off" and "acting as admin" cannot
/// both be true on any code path.
fn required_grantor<'a>(grantor: Option<&'a str>, op: &str) -> sqe_core::Result<&'a str> {
    let g = grantor.ok_or_else(|| {
        sqe_core::SqeError::Execution(format!(
            "internal error: Ranger {op} reached the backend with no grantor. Ranger \
             authorizes against the grantor, so performing this as SQE's admin user \
             would skip the per-resource authority check entirely."
        ))
    })?;
    validate_identifier(g, "grantor")?;
    Ok(g)
}

fn grantee_to_fields(grantee: &Grantee) -> sqe_core::Result<(Vec<String>, Vec<String>)> {
    match grantee {
        Grantee::User(n) => Ok((vec![n.clone()], vec![])),
        Grantee::Role(n) => Ok((vec![], vec![n.clone()])),
        // A GROUP goes in the ROLES field, not the groups field, and that is not a
        // workaround. Ranger usersync is not involved in this deployment: the
        // control plane materialises every Keycloak group as a Ranger ROLE of the
        // identical name (`access/group_sync.py`), and under Ranger no Polaris
        // principal-roles are created at all. Verified there is no name transform
        // on either call site, so the name typed in SQL IS the Ranger role name.
        //
        // The previous refusal cited usersync and was simply wrong about this
        // deployment. It also made `GRANT ... TO GROUP` fail on the write path while
        // group-bound policies authored in the Ranger console were enforced on the
        // read path, which is a confusing pair of behaviours to hold at once.
        //
        // Deliberately NOT auto-creating the role, unlike the platform's
        // `ensure_role_exists`: its grantee arrives from a validated API, ours from
        // free-text SQL, where auto-creating turns a typo into an empty Ranger role
        // and a grant that silently confers nothing. Ranger 403s on an unknown
        // grantee instead, and `post_grant_revoke_inner` turns that into a message.
        Grantee::Group(n) => Ok((vec![], vec![n.clone()])),
    }
}

/// Apache Ranger Admin grant backend.
pub struct RangerGrantBackend {
    client: Client,
    /// Ranger Admin base URL, e.g. `http://ranger-admin:6080`.
    admin_url: String,
    service_name: String,
    admin_user: String,
    admin_password: String,
    /// Value for the `root` resource level (empty = omit).
    realm: String,
}

impl RangerGrantBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        admin_url: &str,
        service_name: &str,
        admin_user: &str,
        admin_password: &str,
        realm: &str,
        timeout_secs: u64,
        accept_invalid_certs: bool,
    ) -> sqe_core::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .danger_accept_invalid_certs(accept_invalid_certs)
            .build()
            .map_err(|e| sqe_core::SqeError::Config(format!("Failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            service_name: service_name.to_string(),
            admin_user: admin_user.to_string(),
            admin_password: admin_password.to_string(),
            realm: realm.to_string(),
        })
    }

    /// Validate the resource identifiers in a grant/revoke statement and build
    /// the (resource_map, access_type, users, roles) tuple shared by grant and
    /// revoke.
    /// Table-object convenience wrapper, kept so existing callers and tests read
    /// unchanged.
    #[cfg(test)]
    fn build_grant_revoke(
        &self,
        privilege: &str,
        catalog: Option<&str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<GrantRevokeRequest> {
        self.build_grant_revoke_for(
            privilege,
            GrantObjectKind::Table,
            catalog,
            namespace,
            table,
            grantee,
        )
    }

    /// The single request for the level the statement NAMES. Used by REVOKE, which
    /// must not touch the traversal levels a grant also wrote.
    fn build_grant_revoke_for(
        &self,
        privilege: &str,
        object: GrantObjectKind,
        catalog: Option<&str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<GrantRevokeRequest> {
        let catalog = self.validated_catalog(catalog, namespace, table, grantee)?;
        let policy = deepest_policy(object, privilege, &self.realm, catalog, namespace, table)?;
        self.request_for(&policy, grantee)
    }

    /// Every policy one GRANT has to write, outermost level first.
    ///
    /// Outermost first because Ranger has no transaction across the calls: a plan
    /// can land partially. Outermost-first leaves "can list, nothing readable",
    /// which is inert; innermost-first would leave "has table access, table
    /// unreachable", the failure this whole mechanism removes.
    fn build_grant_plan(
        &self,
        privilege: &str,
        object: GrantObjectKind,
        catalog: Option<&str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<Vec<GrantRevokeRequest>> {
        let catalog = self.validated_catalog(catalog, namespace, table, grantee)?;
        plan_for(object, privilege, &self.realm, catalog, namespace, table)?
            .iter()
            .map(|p| self.request_for(p, grantee))
            .collect()
    }

    /// Validate the identifiers a statement names and return the catalog.
    ///
    /// Identifier validation is separate from planning on purpose: the profile
    /// decides what a privilege MEANS, and this decides whether the names are ones
    /// SQE will put in a Ranger resource map at all.
    fn validated_catalog<'a>(
        &self,
        catalog: Option<&'a str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<&'a str> {
        let catalog = catalog.ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Ranger GRANT requires a catalog (use catalog.namespace.table)".into(),
            )
        })?;
        validate_identifier(catalog, "catalog")?;
        if let Some(ns) = namespace {
            validate_identifier(ns, "namespace")?;
        }
        if let Some(t) = table {
            validate_identifier(t, "table")?;
        }
        validate_identifier(grantee.name(), "grantee")?;
        Ok(catalog)
    }

    /// Turn one planned policy into a Ranger grant/revoke request.
    fn request_for(
        &self,
        policy: &PlannedPolicy,
        grantee: &Grantee,
    ) -> sqe_core::Result<GrantRevokeRequest> {
        let (users, roles) = grantee_to_fields(grantee)?;
        Ok(GrantRevokeRequest {
            grantor: self.admin_user.clone(),
            resource: policy.resource.clone(),
            users,
            groups: vec![],
            roles,
            access_types: policy.access_types.clone(),
            delegate_admin: false,
            enable_audit: true,
            replace_existing_permissions: false,
            is_recursive: false,
        })
    }

    /// POST a GrantRevokeRequest to the grant or revoke endpoint.
    /// Strip any deny item equivalent to what `DENY` would have written for this
    /// statement. No-op when the resource has no policy or no matching item.
    async fn remove_deny_items(&self, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let Some(catalog) = stmt.catalog.as_deref() else {
            return Ok(());
        };
        // The named level only, matching what DENY writes.
        let policy = deepest_policy(
            stmt.object,
            &stmt.privilege,
            &self.realm,
            catalog,
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
        )?;
        let (resource, access_types) = (policy.resource, policy.access_types);
        let Some(mut policy) = self.policy_by_resource(&resource).await? else {
            return Ok(());
        };
        let target = build_deny_item(&stmt.grantee, &access_types);
        let Some(items) = policy
            .get("denyPolicyItems")
            .and_then(serde_json::Value::as_array)
        else {
            return Ok(());
        };
        let kept: Vec<serde_json::Value> = items
            .iter()
            .filter(|e| !deny_items_equivalent(e, &target))
            .cloned()
            .collect();
        if kept.len() == items.len() {
            return Ok(());
        }
        policy["denyPolicyItems"] = serde_json::Value::Array(kept);
        let id = policy
            .get("id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| sqe_core::SqeError::Execution("Ranger policy has no id".into()))?;
        self.put_policy(id, &policy).await
    }

    /// The policy whose resource map matches `want` EXACTLY, or `None`.
    ///
    /// Ranger permits only ONE policy per exact resource per service: creating a
    /// second is rejected with "Another policy already exists for matching
    /// resource". So a deny cannot live in a policy of its own and has to be
    /// merged into whichever policy already covers the resource. Discovered by
    /// trying the cleaner design first.
    ///
    /// Compared in Rust over the service's policy list rather than with Ranger's
    /// `resource:` query parameters, because those match by prefix and
    /// containment; an exact comparison is what the uniqueness rule actually is.
    /// The allow access types `grantee` already holds in the policy for EXACTLY
    /// this resource. `None` when there is no such policy, or it could not be read.
    ///
    /// Exact resource only, and that is the safe direction rather than a shortcut.
    /// A wildcard policy (`catalog = *`) can cover the same target, but deciding
    /// that needs Ranger's own matcher, and a wrong "already covered" here would
    /// skip writing a level the grantee does not hold -- a grant reporting success
    /// while conferring nothing. Under-reporting only ever costs a redundant POST.
    async fn held_access_types_at(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
    ) -> Option<BTreeSet<String>> {
        let policy = self.policy_by_resource(resource).await.ok()??;
        // A policy disabled in the Ranger console still returns its `policyItems`
        // (`isEnabled: false`, everything else intact), and enforcement ignores it.
        // Reading those items as held would skip a level the grantee does not
        // actually have, which is the "grant succeeds and confers nothing" failure
        // this whole check is written to avoid. Absent field means enabled, which is
        // how Ranger treats it.
        if policy.get("isEnabled").and_then(serde_json::Value::as_bool) == Some(false) {
            return None;
        }
        let (users, roles) = grantee_to_fields(grantee).ok()?;
        let named = |item: &serde_json::Value, field: &str| -> Vec<String> {
            item.get(field)
                .and_then(serde_json::Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mut held = BTreeSet::new();
        for item in policy
            .get("policyItems")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let is_theirs = users.iter().any(|u| named(item, "users").contains(u))
                || roles.iter().any(|r| named(item, "roles").contains(r));
            if !is_theirs {
                continue;
            }
            for access in item
                .get("accesses")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(t) = access.get("type").and_then(serde_json::Value::as_str) {
                    held.insert(t.to_string());
                }
            }
        }
        Some(held)
    }

    async fn policy_by_resource(
        &self,
        want: &BTreeMap<String, String>,
    ) -> sqe_core::Result<Option<serde_json::Value>> {
        let url = format!(
            "{}/service/public/v2/api/policy?serviceName={}",
            self.admin_url, self.service_name
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!("Ranger policy list failed: {e}"))
            })?;
        if !resp.status().is_success() {
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger policy list failed (HTTP {})",
                resp.status()
            )));
        }
        let policies: Vec<serde_json::Value> = resp.json().await.map_err(|e| {
            sqe_core::SqeError::Execution(format!("Ranger policy list parse failed: {e}"))
        })?;
        Ok(policies.into_iter().find(|p| {
            let Some(res) = p.get("resources").and_then(serde_json::Value::as_object) else {
                return false;
            };
            if res.len() != want.len() {
                return false;
            }
            want.iter().all(|(k, v)| {
                res.get(k)
                    .and_then(|e| e.get("values"))
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|vals| vals.len() == 1 && vals[0] == serde_json::json!(v))
            })
        }))
    }

    async fn post_policy(&self, body: &serde_json::Value) -> sqe_core::Result<()> {
        let url = format!("{}/service/public/v2/api/policy", self.admin_url);
        self.send_policy(self.client.post(&url), body, "create").await
    }

    async fn put_policy(&self, id: i64, body: &serde_json::Value) -> sqe_core::Result<()> {
        let url = format!("{}/service/public/v2/api/policy/{id}", self.admin_url);
        self.send_policy(self.client.put(&url), body, "update").await
    }

    async fn send_policy(
        &self,
        req: reqwest::RequestBuilder,
        body: &serde_json::Value,
        what: &str,
    ) -> sqe_core::Result<()> {
        let resp = req
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            // State-changing Ranger REST calls need the CSRF header.
            .header("X-XSRF-HEADER", "x")
            .json(body)
            .send()
            .await
            .map_err(|e| {
                sqe_core::SqeError::Execution(format!("Ranger policy {what} failed: {e}"))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, ranger_body = %text, "Ranger policy {what} failed");
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger policy {what} failed (HTTP {status}){}",
                if text.trim().is_empty() {
                    String::new()
                } else {
                    format!(". Ranger said: {}", text.trim())
                }
            )));
        }
        Ok(())
    }

    async fn post_grant_revoke_with_privilege(
        &self,
        op: &str,
        privilege: &str,
        body: &GrantRevokeRequest,
    ) -> sqe_core::Result<()> {
        self.post_grant_revoke_inner(op, Some(privilege), body).await
    }

    async fn post_grant_revoke_inner(
        &self,
        op: &str,
        privilege: Option<&str>,
        body: &GrantRevokeRequest,
    ) -> sqe_core::Result<()> {
        let url = format!(
            "{}/service/plugins/services/{op}/{}",
            self.admin_url, self.service_name
        );
        let resp = self
            .client
            .post(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .json(body)
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Execution(format!("Ranger {op} request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, ranger_body = %text, op, "Ranger {op} failed");
            // A bare "HTTP 400" tells the caller nothing. The overwhelmingly
            // common cause is a privilege with no mapping, which is sent through
            // as a literal access type the servicedef does not declare, so name
            // it and list what IS mapped.
            let hint = match (status.as_u16(), privilege) {
                (400, Some(p))
                    if profile()
                        .deepest_level(&profile().canonical_privilege(p))
                        .is_none() =>
                {
                    format!(
                        ". Privilege '{p}' has no mapping and was sent to Ranger as the \
                         access type '{}', which the service definition does not declare. \
                         Mapped privileges: {}. A native Polaris access type may also be \
                         named directly.",
                        p.to_lowercase(),
                        profile().known_privileges().join(", ")
                    )
                }
                // Ranger answers 403 when the GRANTOR lacks delegate admin on the
                // resource, which reads as a permissions bug unless it is named.
                (403, _) => format!(
                    ". The grantor '{}' needs delegate admin on this resource in Ranger \
                     (grant it WITH GRANT OPTION, or add a Ranger policy). Ranger said: {}",
                    body.grantor,
                    text.trim()
                ),
                _ if !text.trim().is_empty() => format!(". Ranger said: {}", text.trim()),
                _ => String::new(),
            };
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger {op} failed (HTTP {status}){hint}"
            )));
        }
        debug!(op, service = %self.service_name, "Ranger {op} completed");
        Ok(())
    }
}

// ── Ranger policy read model (subset) ─────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
pub struct RangerPolicy {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    resources: BTreeMap<String, RangerResourceValues>,
    #[serde(default, rename = "policyItems")]
    policy_items: Vec<RangerPolicyItem>,
    #[serde(default, rename = "denyPolicyItems")]
    deny_policy_items: Vec<RangerPolicyItem>,
    /// Who Ranger recorded as the policy's creator, in its display form
    /// ("carol sqe" for firstName=carol lastName=sqe). Ranger sets it from the
    /// grantor SQE sends, so it is the grantor rather than the admin account.
    ///
    /// ABSENT from the policy LIST endpoint and present only on the per-policy
    /// one, which is why `SHOW GRANTS` needs `fill_provenance` rather than just
    /// deserializing another field.
    #[serde(default, rename = "createdBy")]
    created_by: Option<String>,
    /// Creation time in epoch milliseconds. Same list-versus-detail caveat.
    #[serde(default, rename = "createTime")]
    create_time: Option<i64>,
}

/// Upper bound on per-policy provenance requests for one `SHOW GRANTS`.
///
/// `SHOW GRANTS ON <resource>` matches a handful of policies, but
/// `SHOW GRANTS TO <role>` can match every policy in the service, and Ranger
/// serves provenance one policy at a time. Rather than issue an unbounded fan-out
/// on a statement someone typed interactively, stop here and say so in the log:
/// the rows still come back, with `granted_by` and `granted_at` empty beyond the
/// bound. Silently truncating would read as "nobody granted this".
const PROVENANCE_DETAIL_LIMIT: usize = 200;

/// Render Ranger's epoch-millisecond `createTime` as UTC RFC 3339.
///
/// Returns `None` for a value outside the representable range rather than
/// substituting a wrong instant: an audit column that is empty is honest, one
/// showing 1970 is not.
fn format_grant_time(epoch_ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(epoch_ms)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

#[derive(Debug, Deserialize)]
struct RangerResourceValues {
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RangerPolicyItem {
    #[serde(default)]
    users: Vec<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    accesses: Vec<RangerAccess>,
}

#[derive(Debug, Deserialize)]
struct RangerAccess {
    #[serde(rename = "type")]
    access_type: String,
}

/// Render a policy's resources as `catalog.namespace.table` (skipping `root`).
fn format_policy_resource(resources: &BTreeMap<String, RangerResourceValues>) -> String {
    let mut parts = Vec::new();
    for key in ["catalog", "namespace", "table"] {
        if let Some(v) = resources.get(key) {
            if let Some(first) = v.values.first() {
                parts.push(first.clone());
            }
        }
    }
    parts.join(".")
}

/// Flatten Ranger policies into GrantEntry rows (allow + deny items).
pub fn policies_to_entries(policies: &[RangerPolicy]) -> Vec<GrantEntry> {
    let mut out = Vec::new();
    for p in policies {
        let resource = format_policy_resource(&p.resources);
        // Both stay None unless `fill_provenance` fetched the per-policy record,
        // because the list endpoint does not carry them.
        let granted_by = p.created_by.clone();
        let granted_at = p.create_time.and_then(format_grant_time);
        let mut push_items = |items: &[RangerPolicyItem], effect: &str| {
            for item in items {
                for access in &item.accesses {
                    for u in &item.users {
                        out.push(GrantEntry {
                            privilege: access.access_type.clone(),
                            resource: resource.clone(),
                            grantee_type: "USER".into(),
                            grantee_name: u.clone(),
                            effect: effect.into(),
                            granted_by: granted_by.clone(),
                            granted_at: granted_at.clone(),
                        });
                    }
                    for r in &item.roles {
                        out.push(GrantEntry {
                            privilege: access.access_type.clone(),
                            resource: resource.clone(),
                            grantee_type: "ROLE".into(),
                            grantee_name: r.clone(),
                            effect: effect.into(),
                            granted_by: granted_by.clone(),
                            granted_at: granted_at.clone(),
                        });
                    }
                }
            }
        };
        push_items(&p.policy_items, "ALLOW");
        push_items(&p.deny_policy_items, "DENY");
    }
    out
}

/// Does a dotted `resource` fall at or under `prefix`, matching on a dot
/// boundary? `SHOW GRANTS ON CATALOG "wh"` (prefix `wh`) returns `wh` and
/// `wh.sales.orders` but never sibling catalogs like `wharf.ns.t` or
/// `wholesale`. An empty prefix matches everything (no resource filter).
pub fn resource_matches_prefix(resource: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    resource == prefix || resource.starts_with(&format!("{prefix}."))
}

/// Could this policy contribute a row for `grantee`?
///
/// The policy-level twin of `entry_matches_grantee`, used to decide which policies
/// are worth a provenance request BEFORE flattening. Deliberately permissive: a
/// false positive costs one wasted request, while a false negative would silently
/// blank the audit columns on a row that does come back. The entry-level filter
/// still decides what is returned.
fn policy_names_grantee(policy: &RangerPolicy, grantee: &Grantee) -> bool {
    let name = grantee.name();
    policy
        .policy_items
        .iter()
        .chain(policy.deny_policy_items.iter())
        .any(|item| match grantee {
            Grantee::User(_) => item.users.iter().any(|u| u == name),
            Grantee::Role(_) => item.roles.iter().any(|r| r == name),
            // RangerPolicyItem carries no groups field, so a group grantee can
            // never produce an entry. Claim nothing rather than fetch for rows
            // that cannot exist.
            Grantee::Group(_) => false,
        })
}

/// Could this policy contribute a USER row for `user`? The policy-level twin of
/// the `show_effective` filter.
fn policy_names_user(policy: &RangerPolicy, user: &str) -> bool {
    policy
        .policy_items
        .iter()
        .chain(policy.deny_policy_items.iter())
        .any(|item| item.users.iter().any(|u| u == user))
}

/// Does an entry's grantee match the requested grantee (type + name)?
pub fn entry_matches_grantee(entry: &GrantEntry, grantee: &Grantee) -> bool {
    let want_type = match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) => "ROLE",
        Grantee::Group(_) => "GROUP",
    };
    entry.grantee_type == want_type && entry.grantee_name == grantee.name()
}

/// Prefix marking a Ranger policy label as access provenance.
///
/// `chm`, NOT `sqe`, and that is load-bearing rather than cosmetic. SQE and the
/// data-platform control plane write to the SAME Ranger `polaris` service, and
/// both consult these labels to decide what a REVOKE must hold back. A private
/// prefix would make each tool blind to the other's provenance and fall straight
/// back to the cascade this mechanism exists to prevent: SQE would strip a grant
/// the platform had made, and the platform would strip SQE's.
///
/// Mirrors `data-platform/backend/data_platform/access/provenance.py`
/// (`LABEL_PREFIX = "chm"`). Ranger labels are a shared namespace; keep the two
/// in lockstep.
const LABEL_PREFIX: &str = "chm";

/// Grantee types a provenance label may name.
///
/// USER and ROLE only, matching `provenance.py`'s `_GRANTEE_TYPES`. A GROUP is
/// NOT a third kind: the platform materialises every Keycloak group as a Ranger
/// role of the identical name, so a group grantee is labelled `ROLE`.
const LABEL_GRANTEE_TYPES: &[&str] = &["USER", "ROLE"];

/// The provenance label recording that ONE `GRANT` statement is responsible for
/// part of a policy: `sqe:<GRANTEE_TYPE>:<name>:<PRIVILEGE>`.
///
/// Ranger permits only one policy per resource, so every grant on a table lands
/// in the same policy and their access types union together. Without a record of
/// which statement contributed what, revoke cannot tell them apart, and the
/// overlap is total rather than partial: `WRITE_ACCESS` contains all three of
/// `READ_ACCESS`'s types, so
///
///   GRANT SELECT ON t TO USER dave;
///   GRANT INSERT ON t TO USER dave;
///   REVOKE INSERT ON t FROM USER dave;
///
/// used to leave dave with NOTHING. Reproduced live before this was written: the
/// third statement removed dave's policy item outright, so an admin narrowing a
/// user's write access silently took away their read access as well.
/// `grant_label`, taking the SQL privilege plus the object kind so the label
/// records what was actually granted.
///
/// A view `SELECT` and a table `SELECT` are different privileges (`SELECT VIEW` vs
/// `SELECT`) conferring disjoint access types, but both arrive here as the SQL word
/// "SELECT". Labelling the SQL word loses that, and anything later reasoning from
/// the label plans the wrong privilege: a revoke computes table access types for a
/// view grant, and the audit tool reports a legitimate view grant as over-broad.
/// Observed live -- `sales_wh.acdemo.orders_eu` (a view, labelled `SELECT`) was
/// reported as holding `view-properties-read` and `view-list` "beyond the profile".
fn grant_label_for(grantee: &Grantee, object: GrantObjectKind, privilege: &str) -> String {
    grant_label(grantee, &profile_privilege(object, privilege))
}

fn grant_label(grantee: &Grantee, privilege: &str) -> String {
    // A GROUP is labelled ROLE, not GROUP. The platform materialises every
    // Keycloak group as a Ranger role of the identical name, so the two tools must
    // agree that a group grantee's provenance lives under ROLE, or a revoke on one
    // side cannot see what the other granted.
    let kind = match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) | Grantee::Group(_) => "ROLE",
    };
    format!(
        "{LABEL_PREFIX}:{kind}:{}:{}",
        grantee.name(),
        privilege.trim().to_uppercase()
    )
}

/// Parse a provenance label back into (grantee-kind, name, privilege).
///
/// A grantee name may itself contain `:`, so the type is split off the front and
/// the privilege off the back; whatever is left in the middle is the name.
/// Anything that does not round-trip is rejected rather than guessed: labels are
/// editable strings in the Ranger console, and a mis-parse here would silently
/// under-revoke, which is worse than the cascade it replaces.
fn parse_grant_label(label: &str) -> Option<(String, String, String)> {
    let rest = label.strip_prefix(&format!("{LABEL_PREFIX}:"))?;
    let (kind, rest) = rest.split_once(':')?;
    let kind = kind.trim().to_uppercase();
    if !LABEL_GRANTEE_TYPES.contains(&kind.as_str()) {
        return None;
    }
    let (name, privilege) = rest.rsplit_once(':')?;
    if name.is_empty() || privilege.is_empty() {
        return None;
    }
    // The privilege must round-trip to one SQE actually maps. Without this a
    // label naming anything at all reaches `map_sql_to_ranger_access_for`, whose
    // pass-through arm turns an unrecognised privilege into a literal access type
    // -- so a forged or hand-edited label could make revoke hold back an access
    // type nobody granted, forever. That is an UNDER-revoke, the one direction
    // worse than the cascade this mechanism replaces, so it fails closed to "no
    // provenance" and the caller cascades.
    //
    // Deliberately stricter than the grant path, which allows a native Polaris
    // access type to be named directly: such a grant simply gets no provenance.
    let privilege = privilege.trim().to_uppercase();
    // Must round-trip to a privilege the PROFILE plans, so a hand-edited label
    // cannot push an arbitrary string into a revoke's held-back set.
    let canonical = profile().canonical_privilege(&privilege);
    if profile().deepest_level(&canonical).is_none() {
        warn!(%label, "ignoring access-provenance label: unknown privilege");
        return None;
    }
    Some((kind, name.to_string(), privilege))
}

/// Every Ranger role the named user belongs to, following nested roles.
///
/// Ranger's role objects carry `users`, `groups` and `roles`. A role listing a
/// role means membership is transitive, so a user in `analyst` where
/// `senior_analyst` contains `analyst` holds both. Walked breadth-first with a
/// seen-set, because Ranger does not stop an operator creating a cycle and a
/// naive walk would hang the CHECK ACCESS request rather than answer it.
///
/// Groups are deliberately not resolved. Ranger only knows a user's groups if
/// usersync runs, which it does not in this deployment, so a group-derived role
/// would be a guess. A role reached only through a group is therefore missed,
/// which keeps CHECK ACCESS conservative in the same direction it already was.
fn roles_for_user(roles: &[serde_json::Value], user: &str) -> Vec<String> {
    let name_of = |r: &serde_json::Value| -> Option<String> {
        r.get("name").and_then(|n| n.as_str()).map(str::to_string)
    };
    let members = |r: &serde_json::Value, key: &str| -> Vec<String> {
        r.get(key)
            .and_then(serde_json::Value::as_array)
            .map(|xs| {
                xs.iter()
                    .filter_map(|m| m.get("name").and_then(|n| n.as_str()).map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut held: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut queue: Vec<String> = roles
        .iter()
        .filter(|r| members(r, "users").iter().any(|u| u == user))
        .filter_map(name_of)
        .collect();

    while let Some(role) = queue.pop() {
        if !seen.insert(role.clone()) {
            continue;
        }
        held.push(role.clone());
        // Any role that lists this one as a member is also held.
        for r in roles {
            if members(r, "roles").iter().any(|m| m == &role) {
                if let Some(parent) = name_of(r) {
                    if !seen.contains(&parent) {
                        queue.push(parent);
                    }
                }
            }
        }
    }
    held.sort();
    held
}

impl RangerGrantBackend {
    /// Fetch the service's roles so `CHECK ACCESS` can resolve a target user's
    /// role membership. Ranger exposes roles service-wide, not per service.
    async fn fetch_roles(&self) -> sqe_core::Result<Vec<serde_json::Value>> {
        let url = format!("{}/service/public/v2/api/roles", self.admin_url);
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Execution(format!("Ranger role fetch failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            warn!(http_status = %status, "Ranger role fetch failed");
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger role fetch failed (HTTP {status})"
            )));
        }
        resp.json().await.map_err(|e| {
            sqe_core::SqeError::Execution(format!("Ranger role parse failed: {e}"))
        })
    }

    /// Record on the policy covering `resource` that this grant happened.
    ///
    /// Best-effort by design. The grant itself already succeeded, so failing the
    /// statement here would report failure for access the user now has. A missing
    /// label degrades revoke to the old cascade, which is logged and visible,
    /// rather than losing the grant.
    async fn add_grant_label(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
        object: GrantObjectKind,
        privilege: &str,
    ) {
        self.add_policy_label(resource, &grant_label_for(grantee, object, privilege))
            .await;
    }

    /// Add one label to the policy covering `resource`, if it is not already
    /// there. Best-effort, for the reasons on `add_grant_label`.
    async fn add_policy_label(&self, resource: &BTreeMap<String, String>, label: &str) {
        let Ok(Some(mut policy)) = self.policy_by_resource(resource).await else {
            warn!(%label, "no policy found after grant; provenance label not written");
            return;
        };
        let mut labels: Vec<String> = policy
            .get("policyLabels")
            .and_then(serde_json::Value::as_array)
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        if labels.iter().any(|l| l.as_str() == label) {
            return; // idempotent: re-granting must not stack labels
        }
        labels.push(label.to_string());
        policy["policyLabels"] = serde_json::json!(labels);
        let Some(id) = policy.get("id").and_then(serde_json::Value::as_i64) else {
            return;
        };
        if let Err(e) = self.put_policy(id, &policy).await {
            warn!(%label, error = %e, "could not write provenance label; revoke will fall back to the cascade");
        }
    }

    /// Access types the grantee must KEEP because another privilege they still
    /// hold on this resource requires them, plus this statement's label removed.
    ///
    /// Returns `None` when the policy carries no usable provenance, which means
    /// we cannot know what else is owed and must revoke exactly what was asked
    /// for (the old behaviour).
    async fn retained_access_types(
        &self,
        resource: &BTreeMap<String, String>,
        stmt_grantee: &Grantee,
        privilege: &str,
        object: GrantObjectKind,
    ) -> Option<Vec<String>> {
        let policy = self.policy_by_resource(resource).await.ok()??;
        let labels: Vec<String> = policy
            .get("policyLabels")
            .and_then(serde_json::Value::as_array)?
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect();
        // The identifiers come back out of the resource map so this does not need
        // them threaded in: they are exactly what the plan put there.
        let catalog = resource.get("catalog").map(String::as_str).unwrap_or_default();
        let namespace = resource.get("namespace").map(String::as_str);
        let table = resource.get("table").map(String::as_str);

        let mine = grant_label_for(stmt_grantee, object, privilege);
        if !labels.iter().any(|l| l == &mine) {
            // This grant was never labelled (written before labels existed, or
            // the label write failed). No provenance to reason from.
            return None;
        }
        let mut keep: Vec<String> = Vec::new();
        for label in labels.iter().filter(|l| *l != &mine) {
            let Some((_, name, other_priv)) = parse_grant_label(label) else {
                warn!(%label, "unparseable provenance label ignored");
                continue;
            };
            if name != stmt_grantee.name() {
                continue; // another grantee's grant; their items are separate
            }
            // What that OTHER privilege confers at THIS resource. Planned, not
            // looked up in a table, so held-back sets stay consistent with what
            // the grant actually wrote.
            // `other_priv` is already the PROFILE name (the label records it), so
            // it is planned as a table object: a view privilege is its own name and
            // needs no further translation.
            match deepest_policy(
                GrantObjectKind::Table,
                &other_priv,
                &self.realm,
                catalog,
                namespace,
                table,
            ) {
                Ok(p) => keep.extend(p.access_types),
                Err(e) => {
                    // A label naming a privilege the profile no longer plans. Skip
                    // it rather than guess: under-revoking is worse than the
                    // cascade, so the caller falls back to revoking verbatim.
                    warn!(%label, error = %e, "provenance label does not plan; ignored");
                    continue;
                }
            }
        }
        keep.sort();
        keep.dedup();
        Some(keep)
    }

    /// Drop this statement's provenance label. Called after a successful revoke.
    /// Every access type this grantee currently holds at `resource`, across all
    /// of the policy's items that name them.
    ///
    /// Read from Ranger rather than planned, so grants written before provenance
    /// labels existed, or written straight through the Ranger console, are still
    /// caught. `REVOKE ALL PRIVILEGES` that quietly skipped those would be the
    /// same class of defect as a `REVOKE SELECT` that leaves the row readable.
    async fn access_types_held(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
    ) -> Vec<String> {
        let Ok(Some(policy)) = self.policy_by_resource(resource).await else {
            return Vec::new();
        };
        let field = match grantee {
            Grantee::User(_) => "users",
            Grantee::Role(_) => "roles",
            Grantee::Group(_) => "groups",
        };
        let name = grantee.name();
        let mut held: Vec<String> = Vec::new();
        for item in policy
            .get("policyItems")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let is_theirs = item
                .get(field)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|names| {
                    names.iter().filter_map(serde_json::Value::as_str).any(|n| n == name)
                });
            if !is_theirs {
                continue;
            }
            for access in item
                .get("accesses")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(t) = access.get("type").and_then(serde_json::Value::as_str) {
                    held.push(t.to_string());
                }
            }
        }
        held.sort();
        held.dedup();
        held
    }

    /// Drop every provenance label belonging to `grantee` at `resource`.
    ///
    /// The per-privilege `remove_grant_label` cannot be looped here: the point of
    /// `REVOKE ALL PRIVILEGES` is not needing to know which privileges were
    /// granted in the first place.
    async fn remove_all_grant_labels(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
    ) {
        let Ok(Some(mut policy)) = self.policy_by_resource(resource).await else {
            return;
        };
        let Some(labels) = policy.get("policyLabels").and_then(serde_json::Value::as_array) else {
            return;
        };
        let name = grantee.name();
        let kept: Vec<String> = labels
            .iter()
            .filter_map(serde_json::Value::as_str)
            .filter(|l| match parse_grant_label(l) {
                Some((_, label_name, _)) => label_name != name,
                None => true, // unparseable: leave it rather than lose information
            })
            .map(str::to_string)
            .collect();
        if kept.len() == labels.len() {
            return;
        }
        policy["policyLabels"] = serde_json::json!(kept);
        if let Some(id) = policy.get("id").and_then(serde_json::Value::as_i64) {
            if let Err(e) = self.put_policy(id, &policy).await {
                warn!(grantee = %name, error = %e, "could not clear provenance labels");
            }
        }
    }

    /// Drop every DENY item naming `grantee` at `resource`.
    ///
    /// `remove_deny_items` matches the deny item a specific privilege would have
    /// written, which needs that privilege planned. `ALL PRIVILEGES` has no plan
    /// at a table coordinate by design, and "everything" should not leave a
    /// denial behind anyway: a DENY that outlives the grant it mirrored is a
    /// one-way door, which is the reason REVOKE clears denies at all.
    ///
    /// The grantee's NAME is removed from each item rather than the item being
    /// dropped outright, because one item can name several principals and the
    /// others' denials are not ours to lift.
    async fn remove_all_deny_items(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
    ) -> sqe_core::Result<()> {
        let Some(mut policy) = self.policy_by_resource(resource).await? else {
            return Ok(());
        };
        let field = match grantee {
            Grantee::User(_) => "users",
            Grantee::Role(_) => "roles",
            Grantee::Group(_) => "groups",
        };
        let name = grantee.name();
        let Some(items) = policy.get("denyPolicyItems").and_then(serde_json::Value::as_array)
        else {
            return Ok(());
        };
        let mut changed = false;
        let mut kept: Vec<serde_json::Value> = Vec::new();
        for item in items {
            let mut item = item.clone();
            let names: Vec<String> = item
                .get(field)
                .and_then(serde_json::Value::as_array)
                .map(|a| {
                    a.iter().filter_map(serde_json::Value::as_str).map(str::to_string).collect()
                })
                .unwrap_or_default();
            if !names.iter().any(|n| n == name) {
                kept.push(item);
                continue;
            }
            changed = true;
            let remaining: Vec<String> = names.into_iter().filter(|n| n != name).collect();
            let others_named = ["users", "roles", "groups"].iter().any(|f| {
                *f != field
                    && item
                        .get(*f)
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|a| !a.is_empty())
            });
            if remaining.is_empty() && !others_named {
                continue; // nothing left to deny: drop the item
            }
            item[field] = serde_json::json!(remaining);
            kept.push(item);
        }
        if !changed {
            return Ok(());
        }
        policy["denyPolicyItems"] = serde_json::json!(kept);
        if let Some(id) = policy.get("id").and_then(serde_json::Value::as_i64) {
            self.put_policy(id, &policy).await?;
        }
        Ok(())
    }

    /// `REVOKE ALL PRIVILEGES ON <object> FROM <grantee>`: leave the grantee
    /// holding nothing at that exact coordinate.
    ///
    /// Deliberately asymmetric with `GRANT ALL`, which still binds no deeper than
    /// the catalog. Granting "everything" at a coordinate needs a definition of
    /// everything, and getting that wrong once wrote a CATALOG-WIDE policy from a
    /// single-table grant. Revoking everything needs no such definition: it only
    /// removes, so it cannot widen access, and "afterwards they hold nothing here"
    /// is unambiguous at any level. Unity Catalog offers the same statement, and
    /// it is the one an operator reaches for during an incident.
    ///
    /// This exists because closing a gate otherwise required knowing the
    /// privilege implication graph: `REVOKE SELECT` leaves a grantee reading
    /// through a surviving INSERT, since a writer must hold the metadata reads
    /// that authorize a table load.
    async fn revoke_all(&self, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        // The coordinate is planned with a privilege that reaches the requested
        // level, purely to build the same resource map a normal revoke would use.
        // Its access types are then replaced wholesale, so which privilege is
        // borrowed here cannot affect what is revoked.
        let coordinate_privilege = match stmt.object {
            GrantObjectKind::View => "SELECT VIEW",
            GrantObjectKind::Table => "SELECT",
        };
        let mut body = self.build_grant_revoke_for(
            coordinate_privilege,
            stmt.object,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        body.grantor = required_grantor(stmt.grantor.as_deref(), "revoke")?.to_string();

        let held = self.access_types_held(&body.resource, &stmt.grantee).await;
        if held.is_empty() {
            // Nothing allowed here. Still clear provenance and any DENY, so the
            // statement is idempotent and a denial cannot outlive the grant.
            self.remove_all_grant_labels(&body.resource, &stmt.grantee).await;
            return self.remove_all_deny_items(&body.resource, &stmt.grantee).await;
        }
        debug!(
            grantee = %stmt.grantee.name(),
            access_types = held.len(),
            "REVOKE ALL PRIVILEGES: removing every access type held at this resource"
        );
        body.access_types = held;
        self.post_grant_revoke_with_privilege("revoke", "ALL PRIVILEGES", &body)
            .await?;
        self.remove_all_grant_labels(&body.resource, &stmt.grantee).await;
        self.remove_all_deny_items(&body.resource, &stmt.grantee).await
    }

    async fn remove_grant_label(
        &self,
        resource: &BTreeMap<String, String>,
        grantee: &Grantee,
        object: GrantObjectKind,
        privilege: &str,
    ) {
        let label = grant_label_for(grantee, object, privilege);
        let Ok(Some(mut policy)) = self.policy_by_resource(resource).await else {
            return;
        };
        let Some(labels) = policy.get("policyLabels").and_then(serde_json::Value::as_array) else {
            return;
        };
        let kept: Vec<String> = labels
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|l| *l != label.as_str())
            .map(str::to_string)
            .collect();
        if kept.len() == labels.len() {
            return;
        }
        policy["policyLabels"] = serde_json::json!(kept);
        if let Some(id) = policy.get("id").and_then(serde_json::Value::as_i64) {
            if let Err(e) = self.put_policy(id, &policy).await {
                warn!(%label, error = %e, "could not remove provenance label");
            }
        }
    }

    /// Undo the levels of a plan that already landed, after a later one failed.
    ///
    /// Innermost first, mirroring the forward order, so the intermediate states are
    /// the harmless ones. Returns whether every rollback succeeded; the caller
    /// reports either way rather than implying a clean failure, because a
    /// compensation that itself fails leaves discovery the operator did not ask
    /// for.
    async fn compensate(&self, written: &[GrantRevokeRequest], stmt: &GrantStatement) -> bool {
        let mut all_ok = true;
        for body in written.iter().rev() {
            if let Err(e) = self
                .post_grant_revoke_with_privilege("revoke", &stmt.privilege, body)
                .await
            {
                warn!(
                    grantee = %stmt.grantee.name(),
                    error = %e,
                    "could not roll back a partially written grant plan"
                );
                all_ok = false;
            }
        }
        all_ok
    }

    /// Fetch all policies for this service from Ranger Admin.
    /// One policy WITH its provenance, from the per-policy endpoint.
    ///
    /// Best effort by design: `SHOW GRANTS` answering with empty audit columns is
    /// far better than it failing, so every error path returns `None` and leaves
    /// the columns blank. The grants themselves already came from the list call.
    async fn fetch_policy_provenance(&self, name: &str) -> Option<RangerPolicy> {
        let url = format!(
            "{}/service/public/v2/api/service/{}/policy/{}",
            self.admin_url, self.service_name, name
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            debug!(
                policy = %name,
                status = %resp.status(),
                "policy provenance unavailable; SHOW GRANTS will leave granted_by/granted_at empty"
            );
            return None;
        }
        resp.json::<RangerPolicy>().await.ok()
    }

    /// Copy `createdBy` / `createTime` onto the policies at `targets`.
    ///
    /// Ranger's policy LIST endpoint omits both fields entirely (verified against
    /// Ranger 2.8: the keys are absent, not null), while the per-policy endpoint
    /// carries them. `SHOW GRANTS` therefore advertised two columns it could never
    /// fill. One request per policy is acceptable for an interactive admin
    /// statement but not unbounded, hence `targets` (only policies the caller's
    /// filter can return rows from) and `PROVENANCE_DETAIL_LIMIT`.
    async fn fill_provenance(&self, policies: &mut [RangerPolicy], targets: &[usize]) {
        if targets.len() > PROVENANCE_DETAIL_LIMIT {
            warn!(
                matched = targets.len(),
                limit = PROVENANCE_DETAIL_LIMIT,
                "too many policies match to fetch provenance for all of them; \
                 granted_by/granted_at will be empty beyond the limit"
            );
        }
        for &i in targets.iter().take(PROVENANCE_DETAIL_LIMIT) {
            let Some(name) = policies[i].name.clone() else { continue };
            if let Some(detail) = self.fetch_policy_provenance(&name).await {
                policies[i].created_by = detail.created_by;
                policies[i].create_time = detail.create_time;
            }
        }
    }

    async fn fetch_policies(&self) -> sqe_core::Result<Vec<RangerPolicy>> {
        // Public v2 endpoint returns a bare JSON array of policies. (The
        // /service/plugins/policies/... endpoint wraps them in a paginated
        // object, which does not match RangerPolicy deserialization.)
        let url = format!(
            "{}/service/public/v2/api/policy?serviceName={}",
            self.admin_url, self.service_name
        );
        let resp = self
            .client
            .get(&url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .map_err(|e| sqe_core::SqeError::Execution(format!("Ranger policy fetch failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            warn!(http_status = %status, ranger_body = %text, "Ranger policy fetch failed");
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger policy fetch failed (HTTP {status})"
            )));
        }
        resp.json().await.map_err(|e| {
            sqe_core::SqeError::Execution(format!("Ranger policy parse failed: {e}"))
        })
    }
}

/// Best-effort local evaluation of GrantEntry rows: deny-overrides-allow for a
/// given user (+roles), access type, and resource. This mirrors Ranger's deny
/// precedence but does NOT account for tag policies, conditions, or wildcard
/// resource matching beyond exact match. The authoritative decision is Polaris
/// enforcement; this is for `CHECK ACCESS` introspection only.
pub fn evaluate_access(
    entries: &[GrantEntry],
    user: &str,
    roles: &[String],
    access_type: &str,
    resource: &str,
) -> AccessCheckResult {
    let principal_matches = |e: &GrantEntry| -> bool {
        (e.grantee_type == "USER" && e.grantee_name == user)
            || (e.grantee_type == "ROLE" && roles.iter().any(|r| r == &e.grantee_name))
    };
    let relevant = |e: &&GrantEntry| {
        e.privilege == access_type && e.resource == resource && principal_matches(e)
    };

    if entries.iter().filter(|e| e.effect == "DENY").any(|e| relevant(&e)) {
        return AccessCheckResult {
            allowed: false,
            reason: Some(format!("Denied by a DENY policy on {resource}")),
        };
    }
    if let Some(e) = entries.iter().filter(|e| e.effect == "ALLOW").find(|e| relevant(e)) {
        return AccessCheckResult {
            allowed: true,
            reason: Some(format!("Allowed via {} '{}'", e.grantee_type, e.grantee_name)),
        };
    }
    AccessCheckResult {
        allowed: false,
        reason: Some(format!("No matching grant for {user} {access_type} on {resource}")),
    }
}

#[async_trait]
impl GrantBackend for RangerGrantBackend {
    async fn grant(&self, _token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let plan = self.build_grant_plan(
            &stmt.privilege,
            stmt.object,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        // Outermost first; the level the statement NAMES is last.
        let last = plan.len() - 1;
        // What has already landed, for compensation below.
        let mut written: Vec<GrantRevokeRequest> = Vec::new();

        for (i, mut body) in plan.into_iter().enumerate() {
            let is_primary = i == last;
            // Ranger authorizes against `grantor`, so this is the authority
            // check, not just an audit field. It applies to the traversal levels
            // too: an operator who may not grant on the namespace must not acquire
            // that authority by naming a table underneath it.
            body.grantor = required_grantor(stmt.grantor.as_deref(), "grant")?.to_string();
            if is_primary {
                // WITH GRANT OPTION becomes delegateAdmin, on the named object
                // only: a grantee must not gain authority to re-grant traversal.
                body.delegate_admin = stmt.with_grant_option;
            }
            // Skip a traversal level the grantee already holds at that exact
            // resource. Ranger MERGES access types, so re-POSTing a set already
            // present changes nothing -- but the POST is still authorized, and
            // delegate admin does NOT cascade upward (verified against Ranger 2.8:
            // a grantor holding it on `cat.ns.tbl` is refused 403 on `cat` and on
            // `cat.ns`). So without this, the first call of every delegated table
            // grant fails on a write that would have been a no-op, and
            // `WITH GRANT OPTION` confers nothing usable.
            //
            // Never skip the level the statement NAMES: that one may add access
            // types, or delegate admin, to what is already there.
            if !is_primary {
                if let Some(held) = self.held_access_types_at(&body.resource, &stmt.grantee).await {
                    if body.access_types.iter().all(|a| held.contains(a)) {
                        debug!(
                            level = level_name(&body.resource),
                            grantee = stmt.grantee.name(),
                            "traversal level already held; skipping a no-op grant"
                        );
                        // Deliberately NOT recorded in `written`: compensation must
                        // not roll back access that was there before this statement.
                        continue;
                    }
                }
            }
            if let Err(e) = self
                .post_grant_revoke_with_privilege("grant", &stmt.privilege, &body)
                .await
            {
                // Which level failed, when it is not the one the statement named.
                // "Ranger grant failed (HTTP 403)" on a statement naming a table
                // sends the reader to the table's policy, which is not where the
                // problem is. Appended to the same message rather than wrapped in a
                // second error, so the Display prefix appears once.
                let level_hint = if is_primary {
                    String::new()
                } else {
                    format!(
                        " (this is the {} level of the plan, not the object the \
                         statement names: reaching a table needs catalog and namespace \
                         visibility as well. Delegate admin does not cascade upward in \
                         Ranger, so holding it on the table does not confer it here. An \
                         admin seeding discovery for '{}' once -- GRANT USAGE ON DATABASE, \
                         GRANT USAGE ON SCHEMA -- makes this level already-held, and it \
                         is then skipped)",
                        level_name(&body.resource),
                        stmt.grantee.name(),
                    )
                };
                // COMPENSATE. Ranger has no transaction across these calls, so a
                // failure here leaves a partial plan, and half a plan is worse
                // than none: the traversal levels alone confer discovery the
                // operator never asked to grant, and they are invisible in
                // `SHOW GRANTS` output that shows no privilege on the object.
                //
                // Best-effort, and reported either way. Rolling back a grant that
                // may be shared with another statement is itself a judgement call,
                // so the message always states what happened rather than implying
                // a clean failure.
                let rolled_back = self.compensate(&written, stmt).await;
                // Reuse the inner message rather than its Display: both are
                // `SqeError::Execution`, so `{e}` here would print "Query execution
                // error:" twice in the one message operators are asked to read.
                let base = match &e {
                    sqe_core::SqeError::Execution(msg) => msg.clone(),
                    other => other.to_string(),
                };
                return Err(sqe_core::SqeError::Execution(format!(
                    "{base}{level_hint}{}",
                    match (written.is_empty(), rolled_back) {
                        (true, _) => String::new(),
                        (false, true) => format!(
                            " ({} outer level(s) of this grant had already been written \
                             and were rolled back, so no partial grant remains)",
                            written.len()
                        ),
                        (false, false) => format!(
                            " (WARNING: {} outer level(s) had already been written and \
                             could NOT be rolled back; '{}' may retain catalog or \
                             namespace discovery. Re-run the statement, or revoke \
                             USAGE explicitly)",
                            written.len(),
                            stmt.grantee.name()
                        ),
                    }
                )));
            }
            if is_primary {
                // Record WHICH statement contributed these access types, so a
                // later REVOKE of a different privilege on the same resource
                // does not take them away. Best-effort: the grant succeeded.
                //
                // DEEPEST LEVEL ONLY. The traversal policies are shared with every
                // other grant anyone holds in that catalog, so stamping them with
                // one grantee's privilege would misrepresent shared plumbing as
                // privately owned and invite a later revoke to release traversal
                // another grant still depends on. The platform's provenance module
                // takes the same position.
                self.add_grant_label(&body.resource, &stmt.grantee, stmt.object, &stmt.privilege)
                    .await;
            }
            written.push(body);
        }
        Ok(())
    }

    async fn revoke(&self, _token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        // `REVOKE ALL PRIVILEGES` is not a privilege expansion, it is "this
        // grantee holds nothing here afterwards". See `revoke_all`.
        if is_all_privileges(&stmt.privilege) {
            return self.revoke_all(stmt).await;
        }
        let mut body = self.build_grant_revoke_for(
            &stmt.privilege,
            stmt.object,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        body.grantor = required_grantor(stmt.grantor.as_deref(), "revoke")?.to_string();

        // Narrow the revoke to the access types no OTHER privilege this grantee
        // holds on this resource still needs. Ranger allows one policy per
        // resource, so `GRANT SELECT` + `GRANT INSERT` share one item and
        // WRITE_ACCESS is a strict superset of READ_ACCESS: revoking INSERT
        // verbatim used to leave the grantee with nothing at all.
        //
        // With no provenance to reason from we revoke exactly what was asked,
        // which is the previous behaviour: a cascade is wrong, but inventing a
        // narrower revoke from a guess would silently leave access in place.
        if let Some(keep) = self
            .retained_access_types(&body.resource, &stmt.grantee, &stmt.privilege, stmt.object)
            .await
        {
            let before = body.access_types.len();
            body.access_types.retain(|t| !keep.contains(t));
            if body.access_types.len() != before {
                debug!(
                    privilege = %stmt.privilege,
                    grantee = %stmt.grantee.name(),
                    held_back = before - body.access_types.len(),
                    "revoke narrowed: access types still required by another privilege"
                );
            }
            if body.access_types.is_empty() {
                // Everything this privilege confers is also owed to another one.
                // Drop only the provenance, so a later revoke of that other
                // privilege releases the access types.
                self.remove_grant_label(&body.resource, &stmt.grantee, stmt.object, &stmt.privilege)
                    .await;
                return self.remove_deny_items(stmt).await;
            }
        }

        self.post_grant_revoke_with_privilege("revoke", &stmt.privilege, &body)
            .await?;
        self.remove_grant_label(&body.resource, &stmt.grantee, stmt.object, &stmt.privilege)
            .await;
        // REVOKE also clears a matching DENY, matching Unity Catalog, where
        // REVOKE removes the grant whether it was an allow or a deny.
        //
        // Without this, DENY is a one-way door: the grant endpoint only touches
        // allow items, so undoing a denial would need Ranger console access. That
        // is not an acceptable operational story for a statement SQE offers.
        self.remove_deny_items(stmt).await
    }

    /// DENY goes through the POLICY api, not grant/revoke.
    ///
    /// Ranger's `/services/grant` endpoint writes allow items only; there is no
    /// field on `GrantRevokeRequest` for a deny. Deny lives as
    /// `denyPolicyItems` on a policy, so this finds or creates one policy per
    /// resource, named deterministically, and merges the deny item into it.
    ///
    /// One policy per resource, rather than appending to whatever policy already
    /// covers the resource: touching an operator's hand-written policy to add a
    /// deny would be a surprising side effect, and a deterministic name keeps the
    /// statement idempotent.
    ///
    /// CAVEAT, deliberate and documented: the policy API authorizes the
    /// authenticated REST user, not a `grantor` field, so unlike GRANT this is
    /// NOT resource-scoped to the caller. The `[auth] admin_roles` gate is the
    /// only check. Ranger offers no grantor-scoped deny.
    async fn deny(&self, _token: &str, stmt: &GrantStatement) -> sqe_core::Result<()> {
        let catalog = stmt.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Ranger DENY requires a catalog (use catalog.namespace.table)".into(),
            )
        })?;
        validate_identifier(catalog, "catalog")?;
        if let Some(ns) = stmt.namespace.as_deref() {
            validate_identifier(ns, "namespace")?;
        }
        if let Some(t) = stmt.table.as_deref() {
            validate_identifier(t, "table")?;
        }
        validate_identifier(stmt.grantee.name(), "grantee")?;
        // DENY writes the NAMED level only, like REVOKE. Denying the traversal
        // levels would hide every object under the namespace rather than the one
        // named, and `deepest_policy` also carries the scope guard, so GRANT,
        // REVOKE and DENY agree on what a statement's scope means. A widened DENY
        // over-restricts rather than over-grants, which is the safer direction and
        // just as surprising.
        let planned = deepest_policy(
            stmt.object,
            &stmt.privilege,
            &self.realm,
            catalog,
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
        )?;
        let (resource, access_types) = (planned.resource, planned.access_types);
        // Deterministic, resource-derived name so a repeated DENY updates the
        // same policy instead of piling up duplicates.
        let name = format!(
            "sqe-deny-{}",
            resource
                .iter()
                .filter(|(k, _)| k.as_str() != "root")
                .map(|(_, v)| v.as_str())
                .collect::<Vec<_>>()
                .join("-")
        );

        let deny_item = build_deny_item(&stmt.grantee, &access_types);

        let existing = self.policy_by_resource(&resource).await?;
        match existing {
            Some(mut policy) => {
                let items = policy
                    .get_mut("denyPolicyItems")
                    .and_then(|v| v.as_array_mut());
                match items {
                    Some(arr) => {
                        // Compare SEMANTICALLY, not by JSON equality. Ranger
                        // normalises a stored item (it fills in `users: []`,
                        // `groups: []`, `conditions: []`, `delegateAdmin`), so a
                        // freshly built item never equals the stored form and an
                        // exact comparison appended a duplicate on every rerun.
                        if !arr.iter().any(|e| deny_items_equivalent(e, &deny_item)) {
                            arr.push(deny_item);
                        }
                    }
                    None => {
                        policy["denyPolicyItems"] = serde_json::json!([deny_item]);
                    }
                }
                let id = policy
                    .get("id")
                    .and_then(serde_json::Value::as_i64)
                    .ok_or_else(|| {
                        sqe_core::SqeError::Execution("Ranger policy has no id".into())
                    })?;
                self.put_policy(id, &policy).await
            }
            None => {
                let body = serde_json::json!({
                    "service": self.service_name,
                    "name": name,
                    "policyType": 0,
                    "isEnabled": true,
                    "resources": resource
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::json!({"values": [v]})))
                        .collect::<serde_json::Map<_, _>>(),
                    "policyItems": [],
                    "denyPolicyItems": [deny_item],
                });
                self.post_policy(&body).await
            }
        }
    }

    async fn show_grants(
        &self,
        _token: &str,
        filter: &GrantFilter,
    ) -> sqe_core::Result<Vec<GrantEntry>> {
        let mut policies = self.fetch_policies().await?;

        // Provenance costs one request per policy, so ask only about the policies
        // this filter can actually return rows from. Every entry a policy produces
        // carries that policy's resource, so filtering at policy level here is
        // equivalent to the entry-level filter applied below.
        let prefix = match filter {
            GrantFilter::OnResource { catalog, namespace, table } => {
                let mut parts = Vec::new();
                if let Some(c) = catalog { parts.push(c.clone()); }
                if let Some(n) = namespace { parts.push(n.clone()); }
                if let Some(t) = table { parts.push(t.clone()); }
                Some(parts.join("."))
            }
            GrantFilter::ToGrantee(_) => None,
        };
        let targets: Vec<usize> = policies
            .iter()
            .enumerate()
            .filter(|(_, p)| match filter {
                GrantFilter::ToGrantee(g) => policy_names_grantee(p, g),
                GrantFilter::OnResource { .. } => resource_matches_prefix(
                    &format_policy_resource(&p.resources),
                    prefix.as_deref().unwrap_or_default(),
                ),
            })
            .map(|(i, _)| i)
            .collect();
        self.fill_provenance(&mut policies, &targets).await;

        let all = policies_to_entries(&policies);
        let filtered = match filter {
            GrantFilter::ToGrantee(g) => {
                all.into_iter().filter(|e| entry_matches_grantee(e, g)).collect()
            }
            GrantFilter::OnResource { .. } => {
                let prefix = prefix.unwrap_or_default();
                all.into_iter()
                    .filter(|e| resource_matches_prefix(&e.resource, &prefix))
                    .collect()
            }
        };
        Ok(filtered)
    }

    async fn show_effective(&self, _token: &str, user: &str) -> sqe_core::Result<Vec<GrantEntry>> {
        // Best-effort: return policies naming this user directly. Role-derived
        // grants are not expanded here (Ranger resolves roles at enforcement).
        let mut policies = self.fetch_policies().await?;
        let targets: Vec<usize> = policies
            .iter()
            .enumerate()
            .filter(|(_, p)| policy_names_user(p, user))
            .map(|(i, _)| i)
            .collect();
        self.fill_provenance(&mut policies, &targets).await;
        let all = policies_to_entries(&policies);
        Ok(all
            .into_iter()
            .filter(|e| e.grantee_type == "USER" && e.grantee_name == user)
            .collect())
    }

    async fn check_access(
        &self,
        _token: &str,
        check: &AccessCheck,
    ) -> sqe_core::Result<AccessCheckResult> {
        let catalog = check.catalog.as_deref().ok_or_else(|| {
            sqe_core::SqeError::Execution(
                "Ranger check_access requires a catalog; use catalog.namespace.table".into(),
            )
        })?;
        validate_identifier(catalog, "catalog")?;

        // The privilege's SEED, not the first of its expanded set: the seed is the
        // access type that defines it. Alphabetical order on the expansion would
        // report INSERT as `table-data-read`, i.e. a write privilege as a read.
        let canonical = profile().canonical_privilege(&check.privilege);
        let primary = profile()
            .deepest_seeds(&canonical)
            .and_then(|s| s.first())
            .map(String::as_str)
            .unwrap_or("");
        let mut parts = vec![catalog.to_string()];
        if let Some(n) = &check.namespace { parts.push(n.clone()); }
        if let Some(t) = &check.table { parts.push(t.clone()); }
        let resource = parts.join(".");

        let policies = self.fetch_policies().await?;
        let entries = policies_to_entries(&policies);

        // Resolve the TARGET user's roles, not the caller's. CHECK ACCESS asks
        // "can alice read this", so alice's membership decides it.
        //
        // This used to pass an empty role list, with a comment claiming roles
        // were unknown at this layer. They are not: Ranger serves them. The
        // result was that CHECK ACCESS answered "false" for access that plainly
        // worked, since role grants are the normal way to grant. Worse than a
        // missing feature, because the answer looked authoritative: an auditor
        // reading it concluded a table was closed while the user was reading it.
        //
        // A role lookup failure must not turn into a confident "no". Report the
        // degradation instead, so the caller can tell "not granted" from
        // "could not tell".
        let roles = match self.fetch_roles().await {
            Ok(rs) => roles_for_user(&rs, &check.user),
            Err(e) => {
                warn!(error = %e, user = %check.user,
                      "Ranger role lookup failed; CHECK ACCESS sees direct user grants only");
                let mut result =
                    evaluate_access(&entries, &check.user, &[], primary, &resource);
                if !result.allowed {
                    result.reason = Some(format!(
                        "No matching direct grant for {} {} on {}. Role membership \
                         could NOT be checked (Ranger role lookup failed), so a grant \
                         held through a role would not show here.",
                        check.user, primary, resource
                    ));
                }
                return Ok(result);
            }
        };

        Ok(evaluate_access(&entries, &check.user, &roles, primary, &resource))
    }

    fn backend_name(&self) -> &str {
        "ranger"
    }

    /// Ranger authorizes `grant` and `revoke` against the `grantor` field, per
    /// resource and per access type. Verified against 2.8: a grantor with no
    /// `delegateAdmin` on the resource is refused 403 "User doesn't have necessary
    /// permission to grant access" even when the HTTP call authenticates as the
    /// Ranger admin, and a grantor holding it for `table-data-read` is still
    /// refused when the request names `table-data-write`.
    ///
    /// Note what this does NOT cover: `deny` goes through the policy API, which
    /// authorizes the REST user and takes no grantor. `handle_deny` keeps its own
    /// admin gate for exactly that reason.
    fn enforces_grantor_authority(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_revoke_request_serializes_with_ranger_field_names() {
        let mut resource = BTreeMap::new();
        resource.insert("catalog".to_string(), "wh".to_string());
        let req = GrantRevokeRequest {
            grantor: "admin".into(),
            resource,
            users: vec!["alice".into()],
            groups: vec![],
            roles: vec![],
            access_types: vec!["table-data-read".into()],
            delegate_admin: false,
            enable_audit: true,
            replace_existing_permissions: false,
            is_recursive: false,
        };
        let j = serde_json::to_value(&req).unwrap();
        assert_eq!(j["grantor"], "admin");
        assert_eq!(j["accessTypes"], serde_json::json!(["table-data-read"]));
        assert_eq!(j["delegateAdmin"], false);
        assert_eq!(j["enableAudit"], true);
        assert_eq!(j["replaceExistingPermissions"], false);
        assert_eq!(j["isRecursive"], false);
        assert_eq!(j["users"], serde_json::json!(["alice"]));
        // empty grantee sets are omitted
        assert!(j.get("groups").is_none());
        assert!(j.get("roles").is_none());
    }

    // ── Task 4: constructor + grantee split + URL guard ──────────────

    fn test_backend() -> RangerGrantBackend {
        RangerGrantBackend::new(
            "http://ranger:6080/",
            "polaris",
            "admin",
            "admin-pw",
            "POLARIS",
            30,
            false,
        )
        .unwrap()
    }

    #[test]
    fn constructor_trims_trailing_slash_and_sets_name() {
        let b = test_backend();
        assert_eq!(b.admin_url, "http://ranger:6080");
        assert_eq!(b.service_name, "polaris");
        assert_eq!(b.backend_name(), "ranger");
    }

    #[test]
    fn grantee_to_user_role_fields() {
        assert_eq!(
            grantee_to_fields(&Grantee::User("alice".into())).unwrap(),
            (vec!["alice".to_string()], vec![])
        );
        assert_eq!(
            grantee_to_fields(&Grantee::Role("analyst".into())).unwrap(),
            (vec![], vec!["analyst".to_string()])
        );
    }

    #[test]
    fn a_group_grantee_becomes_a_ranger_role_of_the_same_name() {
        // Not the `groups` field. The control plane materialises every Keycloak
        // group as a Ranger ROLE of the identical name, with no name transform, so a
        // group grant and the same-named role grant must be the same write. The old
        // behaviour refused GROUP outright citing Ranger usersync, which this
        // deployment does not use.
        let (users, roles) = grantee_to_fields(&Grantee::Group("ws-sales-members".into()))
            .expect("a group grantee is supported");
        assert!(users.is_empty());
        assert_eq!(roles, vec!["ws-sales-members".to_string()]);
        assert_eq!(
            grantee_to_fields(&Grantee::Group("g".into())).unwrap(),
            grantee_to_fields(&Grantee::Role("g".into())).unwrap(),
            "a group and a role of the same name are the same Ranger write"
        );
    }

    #[test]
    fn build_grant_revoke_select_to_role() {
        let b = test_backend();
        let body = b
            .build_grant_revoke("SELECT", Some("wh"), Some("sales"), Some("orders"),
                &Grantee::Role("analyst".into()))
            .unwrap();
        assert_eq!(body.access_types.first().map(String::as_str), Some("table-data-read"));
        assert!(body.access_types.contains(&"table-properties-read".to_string()));
        assert_eq!(body.roles, vec!["analyst".to_string()]);
        assert!(body.users.is_empty());
        assert_eq!(body.resource.get("table").map(String::as_str), Some("orders"));
        assert_eq!(body.resource.get("root").map(String::as_str), Some("POLARIS"));
    }

    /// The grantor field is the AUTHORITY check, not decoration.
    ///
    /// Verified against a live Ranger 2.8: a POST to
    /// `/service/plugins/services/grant/{service}` carrying `grantor: "dave"`
    /// is refused with HTTP 403 "User doesn't have necessary permission to grant
    /// access" EVEN THOUGH the request authenticates with admin REST
    /// credentials. Ranger authorizes the named grantor, so sending the real
    /// caller is what makes grant authority resource-scoped.
    ///
    /// This test pins the wiring: the caller's name reaches the request body,
    /// and WITH GRANT OPTION becomes delegateAdmin. If the grantor silently fell
    /// back to the admin user, every caller would inherit admin's authority.
    /// A view grant uses the TABLE resource slot with VIEW access types.
    ///
    /// Verified against a live Polaris 1.6: granting `view-properties-read` +
    /// `view-list` on `{catalog, namespace, table: <view name>}` is what lets a
    /// grantee load the view. There is no `view` resource level in the
    /// servicedef, so the shape is a table's and only the access types differ.
    /// Deny items must dedupe semantically, or a repeated DENY grows the policy
    /// without bound. Ranger echoes a stored item with every optional field
    /// filled in, so byte equality never holds. Observed: two identical deny
    /// items after running the same DENY twice.
    #[test]
    fn deny_items_dedupe_ignoring_ranger_normalisation() {
        let fresh = serde_json::json!({
            "roles": ["analyst"],
            "accesses": [
                {"type": "table-data-read", "isAllowed": true},
                {"type": "table-list", "isAllowed": true}
            ]
        });
        // The same item as Ranger stores it.
        let stored = serde_json::json!({
            "users": [],
            "groups": [],
            "roles": ["analyst"],
            "conditions": [],
            "delegateAdmin": false,
            "accesses": [
                {"type": "table-list", "isAllowed": true},
                {"type": "table-data-read", "isAllowed": true}
            ]
        });
        assert_ne!(fresh, stored, "byte equality must not hold, else the test is moot");
        assert!(
            deny_items_equivalent(&fresh, &stored),
            "normalisation and access-type order must not defeat dedup"
        );

        let other_role = serde_json::json!({
            "roles": ["engineer"],
            "accesses": [{"type": "table-data-read", "isAllowed": true}]
        });
        assert!(!deny_items_equivalent(&fresh, &other_role), "a different grantee is a different item");

        let fewer = serde_json::json!({
            "roles": ["analyst"],
            "accesses": [{"type": "table-data-read", "isAllowed": true}]
        });
        assert!(!deny_items_equivalent(&fresh, &fewer), "a different access set is a different item");
    }

    #[test]
    fn a_table_grant_writes_v4s_three_level_plan() {
        // The shape is copied from grant-profile.json v4's SELECT entry:
        //   catalog:[namespace-list] | namespace:[namespace-properties-read]
        //   | table:[table-data-read ...]
        // SQE must write the same policies as the control plane, or a SQL grant
        // and the equivalent API call disagree about what a grant means.
        let b = test_backend();
        let plan = b
            .build_grant_plan(
                "SELECT",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::User("dave".into()),
            )
            .expect("build plan");
        assert_eq!(plan.len(), 3, "catalog, namespace, then the table itself");

        // OUTERMOST FIRST: a partial failure must leave the inert half.
        let cat = &plan[0];
        assert_eq!(cat.resource.get("catalog").map(String::as_str), Some("wh"));
        assert_eq!(cat.resource.get("namespace"), None);
        assert_eq!(cat.resource.get("table"), None);
        assert_eq!(cat.access_types, vec!["namespace-list".to_string()]);

        let ns = &plan[1];
        assert_eq!(ns.resource.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(ns.resource.get("table"), None);
        // v4's expansion, not just the seed: `namespace-properties-read` implies
        // `namespace-list`, so the namespace policy carries both. Scoped to THIS
        // namespace, which is not the catalog-wide enumeration the catalog level
        // grants.
        assert_eq!(
            ns.access_types,
            vec![
                "namespace-list".to_string(),
                "namespace-properties-read".to_string()
            ]
        );

        let tbl = &plan[2];
        assert_eq!(tbl.resource.get("table").map(String::as_str), Some("orders"));
        assert!(tbl.access_types.contains(&"table-data-read".to_string()));
    }
    #[test]
    fn the_auto_added_ancestor_never_carries_grant_option() {
        let b = test_backend();
        let plan = b
            .build_grant_plan(
                "SELECT",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::User("dave".into()),
            )
            .expect("build plan");
        // `grant()` sets delegate_admin on the primary from WITH GRANT OPTION and
        // leaves the ancestor alone, so the ancestor must be built false: the
        // grantee must not gain authority to re-grant namespace visibility.
        assert!(!plan[0].delegate_admin, "catalog discovery must not be re-grantable");
        assert!(!plan[1].delegate_admin, "namespace visibility must not be re-grantable");
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn only_a_catalog_bound_privilege_stands_alone() {
        let b = test_backend();
        // v4 gives catalog discovery to everything that reaches INTO a catalog,
        // including namespace-level privileges, so these are two-level plans.
        for (priv_, ns, tbl) in [("USAGE", Some("sales"), None), ("CREATE TABLE", Some("sales"), None)] {
            let plan = b
                .build_grant_plan(priv_, GrantObjectKind::Table, Some("wh"), ns, tbl, &Grantee::User("dave".into()))
                .unwrap_or_else(|e| panic!("build plan for {priv_}: {e}"));
            assert_eq!(plan.len(), 2, "{priv_}: catalog discovery plus its own level");
            assert_eq!(plan[0].access_types, vec!["namespace-list".to_string()]);
            assert_eq!(plan[0].resource.get("namespace"), None);
        }
        // MANAGE / ALL already bind at the catalog level and carry
        // catalog-content-manage, so there is nothing above them to add.
        for priv_ in ["ALL PRIVILEGES", "CREATE SCHEMA"] {
            let plan = b
                .build_grant_plan(priv_, GrantObjectKind::Table, Some("wh"), None, None, &Grantee::User("dave".into()))
                .unwrap_or_else(|e| panic!("build plan for {priv_}: {e}"));
            assert_eq!(plan.len(), 1, "{priv_} binds at the catalog level already");
        }
    }
    #[test]
    fn a_wildcard_table_grant_also_gets_the_namespace() {
        // `GRANT ... ON ALL TABLES IN SCHEMA cat.ns` resolves to table `"*"` at the
        // table level, so it picks up the ancestor too. Intended: those tables need
        // the namespace visible exactly as a single-table grant does, and the
        // ancestor names the REAL namespace, not a wildcard.
        let b = test_backend();
        let plan = b
            .build_grant_plan(
                "SELECT",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("*"),
                &Grantee::Role("analyst".into()),
            )
            .expect("build plan");
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[1].resource.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(plan[1].resource.get("table"), None);
        assert_eq!(plan[2].resource.get("table").map(String::as_str), Some("*"));
    }

    #[test]
    fn multi_level_plans_did_not_reopen_the_scope_widening_hole() {
        // `ALL` binds to the CATALOG level. Naming a table must still be refused
        // rather than quietly widened -- the Vec return is about ancestors, not
        // about relaxing where a privilege binds.
        let b = test_backend();
        let err = b
            .build_grant_plan(
                "ALL PRIVILEGES",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::User("dave".into()),
            )
            .expect_err("a catalog-level privilege named against a table must fail");
        assert!(
            err.to_string().contains("wider than the object named"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deny_stays_a_single_level_and_never_auto_denies_a_namespace() {
        // `deny()` builds its own resource map and does NOT go through
        // `build_grant_plan`. Asserted rather than left structural: auto-denying
        // `namespace-properties-read` at the namespace level would hide EVERY
        // table in the namespace, so a DENY on one table would lock the grantee
        // out of all of them.
        let src = include_str!("ranger.rs");
        let deny_body = src
            .split("async fn deny(")
            .nth(1)
            .expect("deny() present");
        let deny_body = &deny_body[..deny_body.find("\n    }").unwrap_or(deny_body.len())];
        // Guard against a vacuous extraction: if the slice above ever stops
        // finding the real body, the two negative assertions below would pass on
        // an empty string and this test would silently stop testing anything.
        assert!(
            deny_body.contains("denyPolicyItems") && deny_body.contains("build_deny_item"),
            "the deny() body was not extracted; the assertions below would be vacuous"
        );
        assert!(
            !deny_body.contains("build_grant_plan"),
            "deny() must stay single-level: a namespace-level deny would hide \
             every table under the namespace, not just the one named"
        );
        assert!(
            !deny_body.contains("NAMESPACE_VISIBILITY_ACCESS"),
            "deny() must not touch the traversal access type"
        );
    }

    #[test]
    fn view_grant_uses_view_access_types_on_the_table_slot() {
        let b = test_backend();
        let body = b
            .build_grant_revoke_for(
                "SELECT",
                GrantObjectKind::View,
                Some("wh"),
                Some("sales"),
                Some("v_orders"),
                &Grantee::Role("analyst".into()),
            )
            .expect("build view grant");
        assert_eq!(
            body.resource.get("table").map(String::as_str),
            Some("v_orders"),
            "the view name goes in the table slot; there is no view resource level"
        );
        assert!(body.access_types.contains(&"view-properties-read".to_string()));
        assert!(body.access_types.contains(&"view-list".to_string()));
        assert!(
            !body.access_types.contains(&"table-data-read".to_string()),
            "a view grant must NOT confer table data access: SQE expands the view \
             and the reader needs its own grant on the base table"
        );

        // The same privilege on a TABLE is unchanged.
        let tbl = b
            .build_grant_revoke_for(
                "SELECT",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::Role("analyst".into()),
            )
            .expect("build table grant");
        assert!(tbl.access_types.contains(&"table-data-read".to_string()));
        assert!(!tbl.access_types.contains(&"view-properties-read".to_string()));
    }

    #[test]
    fn all_privileges_on_a_table_is_refused_rather_than_widened_to_the_catalog() {
        // The bug this guards: ALL binds to the catalog level, so
        // build_resource_map drops the namespace and table and the write lands
        // as catalog-wide `catalog-content-manage`. The statement names one
        // table and reports success, and alice ends up able to manage every
        // table in `wh`. Silent scope widening on a GRANT is the worst
        // direction for it to fail in.
        let b = test_backend();
        let err = b
            .build_grant_revoke_for(
                "ALL PRIVILEGES",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::User("alice".into()),
            )
            .expect_err("ALL on a table must not silently become a catalog grant");
        let msg = err.to_string();
        assert!(
            msg.contains("catalog level"),
            "the error must name the level the privilege binds to: {msg}"
        );
        assert!(
            msg.contains("'wh'"),
            "the error must name the scope that WOULD have been written: {msg}"
        );

        // Named at the level it binds to, the same privilege still works.
        let ok = b
            .build_grant_revoke_for(
                "ALL PRIVILEGES",
                GrantObjectKind::Table,
                Some("wh"),
                None,
                None,
                &Grantee::User("alice".into()),
            )
            .expect("ALL against the catalog itself is a legitimate grant");
        assert!(ok.access_types.contains(&"catalog-content-manage".to_string()));
        assert!(!ok.resource.contains_key("namespace"));
    }

    #[test]
    fn namespace_privileges_named_against_a_table_are_refused() {
        // Same defect class, different level: USAGE binds to the namespace, so
        // naming a table drops it and writes a namespace-wide policy. The guard
        // is general rather than an ALL special case because this path widens
        // too, just less spectacularly.
        let b = test_backend();
        let err = b
            .build_grant_revoke_for(
                "USAGE",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                Some("orders"),
                &Grantee::User("alice".into()),
            )
            .expect_err("USAGE on a table must not silently become a namespace grant");
        assert!(
            err.to_string().contains("wh.sales"),
            "the error must name the namespace that would have been written: {err}"
        );

        // CREATE SCHEMA binds to the catalog, so naming a namespace widens it.
        let err = b
            .build_grant_revoke_for(
                "CREATE SCHEMA",
                GrantObjectKind::Table,
                Some("wh"),
                Some("sales"),
                None,
                &Grantee::User("alice".into()),
            )
            .expect_err("CREATE SCHEMA on a namespace must not become a catalog grant");
        assert!(err.to_string().contains("catalog level"), "{err}");
    }

    #[test]
    fn table_level_privileges_are_untouched_by_the_scope_guard() {
        // The guard must not fire on the common path. A table-level privilege
        // named against a table, a namespace-level privilege named against a
        // namespace, and a wildcard ON ALL TABLES grant all stay legal.
        let b = test_backend();
        for (privilege, ns, tbl) in [
            ("SELECT", Some("sales"), Some("orders")),
            ("INSERT", Some("sales"), Some("orders")),
            ("SELECT", Some("sales"), Some("*")),
            ("CREATE TABLE", Some("sales"), None),
            ("USAGE", Some("sales"), None),
            ("CREATE SCHEMA", None, None),
        ] {
            assert!(
                b.build_grant_revoke_for(
                    privilege,
                    GrantObjectKind::Table,
                    Some("wh"),
                    ns,
                    tbl,
                    &Grantee::User("alice".into()),
                )
                .is_ok(),
                "GRANT {privilege} on {ns:?}.{tbl:?} is correctly scoped and must be allowed"
            );
        }
    }

    #[test]
    fn grantor_and_delegate_admin_reach_the_request_body() {
        let store = test_backend();
        let stmt = GrantStatement {
            privilege: "SELECT".into(),
            catalog: Some("wh".into()),
            namespace: Some("sales".into()),
            table: Some("orders".into()),
            grantee: Grantee::Role("analyst".into()),
            grantor: Some("carol".into()),
            with_grant_option: true,
            object: GrantObjectKind::Table,
        };
        let mut body = store
            .build_grant_revoke(
                &stmt.privilege,
                stmt.catalog.as_deref(),
                stmt.namespace.as_deref(),
                stmt.table.as_deref(),
                &stmt.grantee,
            )
            .expect("build");
        // Mirrors what `grant()` does with the statement.
        if let Some(g) = stmt.grantor.as_deref() {
            body.grantor = g.to_string();
        }
        body.delegate_admin = stmt.with_grant_option;

        assert_eq!(
            body.grantor, "carol",
            "the authenticated caller must be the grantor, not the service identity"
        );
        assert!(
            body.delegate_admin,
            "WITH GRANT OPTION must map to delegateAdmin or authority can never be delegated"
        );

        // And the default stays the configured admin user, so callers with no
        // session (tests, tooling) keep working.
        let plain = store
            .build_grant_revoke(
                "SELECT",
                Some("wh"),
                None,
                None,
                &Grantee::User("bob".into()),
            )
            .expect("build");
        assert_eq!(plain.grantor, "admin");
        assert!(!plain.delegate_admin, "delegateAdmin must default to false");
    }

    #[test]
    fn build_grant_revoke_requires_catalog() {
        let b = test_backend();
        let err = b
            .build_grant_revoke("SELECT", None, None, None, &Grantee::User("a".into()))
            .unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::Execution(_)));
    }

    #[test]
    fn build_grant_revoke_rejects_bad_identifier() {
        let b = test_backend();
        let err = b
            .build_grant_revoke("SELECT", Some("wh/../x"), None, None, &Grantee::User("a".into()))
            .unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::Execution(_)));
    }

    // ── Task 5: policy parsing ────────────────────────────────────────

    #[test]
    fn parse_policies_into_grant_entries() {
        // Minimal Ranger policy JSON: one allow item granting table-data-read
        // to role "analyst" on wh.sales.orders.
        let json = r#"[
          {
            "name": "p1",
            "resources": {
              "catalog": {"values": ["wh"]},
              "namespace": {"values": ["sales"]},
              "table": {"values": ["orders"]}
            },
            "policyItems": [
              {"users": [], "groups": [], "roles": ["analyst"],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ],
            "denyPolicyItems": [
              {"users": ["mallory"], "groups": [], "roles": [],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
          }
        ]"#;
        let policies: Vec<RangerPolicy> = serde_json::from_str(json).unwrap();
        let entries = policies_to_entries(&policies);
        // one allow (analyst) + one deny (mallory)
        assert_eq!(entries.len(), 2);
        let allow = entries.iter().find(|e| e.effect == "ALLOW").unwrap();
        assert_eq!(allow.privilege, "table-data-read");
        assert_eq!(allow.grantee_type, "ROLE");
        assert_eq!(allow.grantee_name, "analyst");
        assert_eq!(allow.resource, "wh.sales.orders");
        let deny = entries.iter().find(|e| e.effect == "DENY").unwrap();
        assert_eq!(deny.grantee_type, "USER");
        assert_eq!(deny.grantee_name, "mallory");
    }

    /// The list endpoint omits provenance, so entries built from it must show the
    /// audit columns as empty rather than inventing a value. This is the state
    /// `SHOW GRANTS` was permanently stuck in before `fill_provenance` existed.
    #[test]
    fn entries_from_the_list_endpoint_have_empty_provenance() {
        let json = r#"[
          {
            "name": "grant-1786526606036",
            "resources": {"catalog": {"values": ["wh"]}},
            "policyItems": [
              {"users": ["alice"], "roles": [],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
          }
        ]"#;
        let policies: Vec<RangerPolicy> = serde_json::from_str(json).unwrap();
        let entries = policies_to_entries(&policies);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].granted_by, None);
        assert_eq!(entries[0].granted_at, None);
    }

    /// The per-policy endpoint carries `createdBy` and `createTime`, and both must
    /// reach every entry the policy produces, allow and deny alike. The values are
    /// a real Ranger 2.8 response observed on the quickstart stack.
    #[test]
    fn entries_carry_provenance_from_the_detail_endpoint() {
        let json = r#"[
          {
            "name": "grant-1786526606036",
            "createdBy": "carol sqe",
            "createTime": 1786526606039,
            "resources": {
              "catalog": {"values": ["sales_wh"]},
              "namespace": {"values": ["acparity"]}
            },
            "policyItems": [
              {"users": [], "roles": ["analyst"],
               "accesses": [{"type": "view-properties-read", "isAllowed": true}]}
            ],
            "denyPolicyItems": [
              {"users": ["mallory"], "roles": [],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
          }
        ]"#;
        let policies: Vec<RangerPolicy> = serde_json::from_str(json).unwrap();
        let entries = policies_to_entries(&policies);
        assert_eq!(entries.len(), 2);
        for e in &entries {
            assert_eq!(e.granted_by.as_deref(), Some("carol sqe"), "on {e:?}");
            assert_eq!(
                e.granted_at.as_deref(),
                Some("2026-08-12T09:23:26Z"),
                "on {e:?}"
            );
        }
    }

    #[test]
    fn grant_time_renders_as_utc_rfc3339() {
        assert_eq!(
            format_grant_time(1786526606039).as_deref(),
            Some("2026-08-12T09:23:26Z")
        );
        assert_eq!(format_grant_time(0).as_deref(), Some("1970-01-01T00:00:00Z"));
    }

    /// An unrepresentable instant must leave the column empty. An audit column that
    /// is blank is honest; one showing a wrong date is not.
    #[test]
    fn grant_time_out_of_range_is_none() {
        assert_eq!(format_grant_time(i64::MAX), None);
        assert_eq!(format_grant_time(i64::MIN), None);
    }

    /// `fill_provenance` is aimed with these, so a policy they miss silently loses
    /// its audit columns while still returning a row.
    #[test]
    fn policy_level_predicates_match_what_the_entry_filter_returns() {
        let json = r#"[
          {
            "name": "p1",
            "resources": {"catalog": {"values": ["wh"]}},
            "policyItems": [
              {"users": ["alice"], "roles": ["analyst"],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ],
            "denyPolicyItems": [
              {"users": ["mallory"], "roles": [],
               "accesses": [{"type": "table-data-read", "isAllowed": true}]}
            ]
          }
        ]"#;
        let p = &serde_json::from_str::<Vec<RangerPolicy>>(json).unwrap()[0];

        assert!(policy_names_grantee(p, &Grantee::Role("analyst".into())));
        assert!(policy_names_grantee(p, &Grantee::User("alice".into())));
        // A deny item still produces a row, so it still needs provenance.
        assert!(policy_names_grantee(p, &Grantee::User("mallory".into())));
        assert!(!policy_names_grantee(p, &Grantee::Role("engineer".into())));
        assert!(!policy_names_grantee(p, &Grantee::User("bob".into())));
        // Type matters: alice is a user here, not a role.
        assert!(!policy_names_grantee(p, &Grantee::Role("alice".into())));
        // RangerPolicyItem carries no groups, so a group can never produce a row.
        assert!(!policy_names_grantee(p, &Grantee::Group("analyst".into())));

        assert!(policy_names_user(p, "alice"));
        assert!(policy_names_user(p, "mallory"));
        assert!(!policy_names_user(p, "analyst"));

        // Every grantee the policy-level predicate accepts must be a grantee the
        // entry-level filter also accepts, or provenance is fetched for nothing.
        let entries = policies_to_entries(std::slice::from_ref(p));
        for g in [
            Grantee::Role("analyst".into()),
            Grantee::User("alice".into()),
            Grantee::User("mallory".into()),
        ] {
            assert!(
                entries.iter().any(|e| entry_matches_grantee(e, &g)),
                "policy-level predicate accepted {g:?} but no entry matches it"
            );
        }
    }

    #[test]
    fn entry_matches_grantee_filter() {
        let e = GrantEntry {
            privilege: "table-data-read".into(),
            resource: "wh".into(),
            grantee_type: "ROLE".into(),
            grantee_name: "analyst".into(),
            effect: "ALLOW".into(),
            granted_by: None,
            granted_at: None,
        };
        assert!(entry_matches_grantee(&e, &Grantee::Role("analyst".into())));
        assert!(!entry_matches_grantee(&e, &Grantee::Role("other".into())));
        assert!(!entry_matches_grantee(&e, &Grantee::User("analyst".into())));
    }

    #[test]
    fn resource_prefix_matches_on_dot_boundary() {
        // SHOW GRANTS ON CATALOG "wh" must match the catalog itself and
        // anything nested under it.
        assert!(resource_matches_prefix("wh", "wh"));
        assert!(resource_matches_prefix("wh.sales.orders", "wh"));
        // It must NOT match sibling catalogs that merely share the prefix bytes.
        assert!(!resource_matches_prefix("wharf.ns.t", "wh"));
        assert!(!resource_matches_prefix("wholesale", "wh"));
        // Deeper prefixes behave the same.
        assert!(resource_matches_prefix("wh.sales.orders", "wh.sales"));
        assert!(!resource_matches_prefix("wh.salesforce", "wh.sales"));
        // Empty prefix is "no filter".
        assert!(resource_matches_prefix("anything", ""));
    }

    // ── Task 6: check_access evaluator ───────────────────────────────

    #[test]
    fn check_match_allows_when_user_has_access() {
        let entries = vec![
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "USER".into(), grantee_name: "alice".into(), effect: "ALLOW".into(),
                granted_by: None, granted_at: None },
        ];
        let r = evaluate_access(&entries, "alice", &[], "table-data-read", "wh.sales.orders");
        assert!(r.allowed);
    }

    #[test]
    fn check_match_deny_overrides_allow() {
        let entries = vec![
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "ROLE".into(), grantee_name: "analyst".into(), effect: "ALLOW".into(),
                granted_by: None, granted_at: None },
            GrantEntry { privilege: "table-data-read".into(), resource: "wh.sales.orders".into(),
                grantee_type: "USER".into(), grantee_name: "alice".into(), effect: "DENY".into(),
                granted_by: None, granted_at: None },
        ];
        let r = evaluate_access(&entries, "alice", &["analyst".into()], "table-data-read", "wh.sales.orders");
        assert!(!r.allowed);
        assert!(r.reason.as_deref().unwrap_or("").to_lowercase().contains("deny"));
    }

    #[test]
    fn a_provenance_label_round_trips() {
        let l = grant_label(&Grantee::User("dave".into()), "select");
        assert_eq!(l, "chm:USER:dave:SELECT", "privilege is normalised upward");
        assert_eq!(
            parse_grant_label(&l),
            Some(("USER".into(), "dave".into(), "SELECT".into()))
        );
        assert_eq!(
            parse_grant_label(&grant_label(&Grantee::Role("analyst".into()), "INSERT")),
            Some(("ROLE".into(), "analyst".into(), "INSERT".into()))
        );
    }

    #[test]
    fn the_label_format_mirrors_the_platforms_byte_for_byte() {
        // SQE and the data-platform control plane write to the SAME Ranger
        // `polaris` service and both read these labels to decide what a REVOKE
        // must hold back. A private prefix makes each blind to the other and both
        // fall back to the cascade, which is the bug this mechanism exists to
        // prevent -- SQE would strip a grant the platform made, and vice versa.
        //
        // These literals are copied from
        // data-platform/backend/data_platform/access/provenance.py.
        assert_eq!(
            grant_label(&Grantee::User("alice".into()), "SELECT"),
            "chm:USER:alice:SELECT"
        );
        assert_eq!(
            grant_label(&Grantee::Role("ws-sales-members".into()), "MODIFY"),
            "chm:ROLE:ws-sales-members:MODIFY"
        );
    }

    #[test]
    fn a_group_grantee_is_labelled_role() {
        // The platform materialises every Keycloak group as a Ranger role of the
        // identical name, so there is no GROUP provenance type. Labelling a group
        // grant "GROUP" would hide it from the platform's parser (its
        // _GRANTEE_TYPES is {USER, ROLE}) and from ours.
        assert_eq!(
            grant_label(&Grantee::Group("ws-sales-members".into()), "SELECT"),
            "chm:ROLE:ws-sales-members:SELECT",
            "a group is a Ranger role by the same name"
        );
        assert_eq!(
            grant_label(&Grantee::Group("g".into()), "SELECT"),
            grant_label(&Grantee::Role("g".into()), "SELECT"),
            "a group and a role of the same name must share provenance"
        );
    }

    #[test]
    fn a_label_naming_an_unmapped_privilege_is_dropped() {
        // The grant path deliberately lets an operator name a native Polaris
        // access type directly. Accepting that back on the READ path would let a
        // forged or hand-edited label push an arbitrary string through
        // map_sql_to_ranger_access_for's pass-through arm, so revoke would hold
        // back an access type nobody granted -- an under-revoke, which is worse
        // than the cascade. Fails closed to "no provenance" instead.
        assert_eq!(parse_grant_label("chm:USER:dave:TABLE-SNAPSHOT-ADD"), None);
        assert_eq!(parse_grant_label("chm:USER:dave:NONSENSE"), None);
        // Every privilege the PROFILE plans still round-trips, so the guard is not
        // simply rejecting everything.
        for p in profile().known_privileges() {
            let l = grant_label(&Grantee::User("dave".into()), &p);
            assert!(
                parse_grant_label(&l).is_some(),
                "profile privilege {p} must round-trip through its label"
            );
        }
    }

    #[test]
    fn a_grantee_name_containing_a_colon_still_parses() {
        // Ranger accepts names with colons, and the label format uses colons as
        // separators. The type comes off the front and the privilege off the
        // back; everything between is the name. Getting this wrong would mean
        // attributing one grantee's access types to another.
        let g = Grantee::User("realm:dave".into());
        let l = grant_label(&g, "SELECT");
        assert_eq!(l, "chm:USER:realm:dave:SELECT");
        assert_eq!(
            parse_grant_label(&l),
            Some(("USER".into(), "realm:dave".into(), "SELECT".into()))
        );
    }

    #[test]
    fn a_malformed_label_is_rejected_not_guessed() {
        // Labels are editable strings in the Ranger console. A mis-parse would
        // hold back access types the grantee is not owed, which reads as a
        // successful revoke that did nothing.
        for bad in [
            "chm:USER:dave",           // no privilege
            "chm:WIZARD:dave:SELECT",  // unknown grantee kind
            "other:USER:dave:SELECT",  // not ours
            "sqe:USER::SELECT",        // empty name
            "sqe:USER:dave:",          // empty privilege
            "",
        ] {
            assert!(
                parse_grant_label(bad).is_none(),
                "{bad:?} must not parse into provenance"
            );
        }
    }

    #[test]
    fn check_match_denies_when_no_grant() {
        let r = evaluate_access(&[], "alice", &[], "table-data-read", "wh.sales.orders");
        assert!(!r.allowed);
    }

    /// Ranger's `/service/public/v2/api/roles` shape, trimmed to what
    /// `roles_for_user` reads.
    fn role(name: &str, users: &[&str], nested: &[&str]) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "users": users.iter().map(|u| serde_json::json!({"name": u})).collect::<Vec<_>>(),
            "groups": [],
            "roles": nested.iter().map(|r| serde_json::json!({"name": r})).collect::<Vec<_>>(),
        })
    }

    #[test]
    fn a_users_direct_roles_are_resolved() {
        // The bug: CHECK ACCESS passed an empty role list, so it answered
        // "false" for alice while alice was reading the table through the
        // analyst role. Confirmed live against Polaris 1.7 + Ranger 2.8:
        // SHOW GRANTS listed `table-data-read ROLE analyst`, alice returned 3
        // rows, and CHECK ACCESS said no.
        let roles = vec![
            role("analyst", &["alice", "bob"], &[]),
            role("engineer", &["bob"], &[]),
            role("sqe_admin", &["carol"], &[]),
        ];
        assert_eq!(roles_for_user(&roles, "alice"), vec!["analyst"]);
        assert_eq!(roles_for_user(&roles, "bob"), vec!["analyst", "engineer"]);
        assert!(
            roles_for_user(&roles, "dave").is_empty(),
            "a user in no role holds no role"
        );
    }

    #[test]
    fn nested_role_membership_is_transitive() {
        // A role that lists another role as a member confers it. Without this,
        // a grant made to the outer role is invisible to CHECK ACCESS even
        // though Ranger enforces it.
        let roles = vec![
            role("analyst", &["alice"], &[]),
            role("senior_analyst", &[], &["analyst"]),
            role("all_staff", &[], &["senior_analyst"]),
            role("unrelated", &["bob"], &[]),
        ];
        assert_eq!(
            roles_for_user(&roles, "alice"),
            vec!["all_staff", "analyst", "senior_analyst"],
            "membership follows nested roles all the way up"
        );
    }

    #[test]
    fn a_role_cycle_terminates_instead_of_hanging() {
        // Ranger does not stop an operator creating a cycle. A naive walk would
        // spin forever and hang the CHECK ACCESS request rather than answer it,
        // which is a worse failure than a wrong answer.
        let roles = vec![
            role("a", &["alice"], &["b"]),
            role("b", &[], &["a"]),
        ];
        let held = roles_for_user(&roles, "alice");
        assert_eq!(held, vec!["a", "b"]);
    }

    #[test]
    fn a_role_grant_is_visible_once_the_users_roles_are_resolved() {
        // End to end over the two functions: the grant is to the ROLE, the
        // question is about the USER, and the answer must be yes.
        let entries = vec![GrantEntry {
            privilege: "table-data-read".into(),
            resource: "wh.sales.orders".into(),
            grantee_type: "ROLE".into(),
            grantee_name: "analyst".into(),
            effect: "ALLOW".into(),
            granted_by: None,
            granted_at: None,
        }];
        let roles = vec![role("analyst", &["alice"], &[])];

        let held = roles_for_user(&roles, "alice");
        let r = evaluate_access(&entries, "alice", &held, "table-data-read", "wh.sales.orders");
        assert!(r.allowed, "alice holds analyst, and analyst holds the grant");
        assert!(
            r.reason.as_deref().unwrap_or("").contains("analyst"),
            "the reason should name the role the access came through: {:?}",
            r.reason
        );

        // The negative control: a user in no role still gets no.
        let none = roles_for_user(&roles, "dave");
        assert!(!evaluate_access(&entries, "dave", &none, "table-data-read", "wh.sales.orders").allowed);
    }
}
