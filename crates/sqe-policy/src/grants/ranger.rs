//! RangerGrantBackend — translates GRANT/REVOKE/SHOW GRANTS into Apache Ranger
//! Admin REST calls. Enforcement is delegated to Polaris 1.5's embedded Ranger
//! authorizer; this backend only writes/reads Ranger policies.
//!
//! Ranger service-def: `polaris`. Resource hierarchy: root -> catalog ->
//! namespace -> table. Access types are Polaris-native hyphenated names.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::{
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantObjectKind,
    GrantStatement, Grantee, RevokeStatement,
};

/// Which resource levels a privilege applies to. Determines which keys go into
/// the Ranger resource map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLevel {
    Catalog,
    Namespace,
    Table,
}

/// A SQL read (SELECT) through SQE loads the table then reads data files. The
/// Polaris embedded Ranger authorizer does NOT honor service-def impliedGrants,
/// so each required access type is listed explicitly.
const READ_ACCESS: &[&str] = &["table-data-read", "table-properties-read", "table-list"];

/// A SQL write (INSERT) loads the table and commits a new snapshot, which fans
/// out into many fine-grained Polaris operations. This is the explicit
/// equivalent of `table-data-write`'s impliedGrants (not auto-applied).
const WRITE_ACCESS: &[&str] = &[
    "table-data-write",
    "table-data-read",
    "table-properties-read",
    "table-properties-write",
    "table-properties-set",
    "table-properties-remove",
    "table-uuid-assign",
    "table-format-version-upgrade",
    "table-schema-add",
    "table-schema-set-current",
    "table-sort-order-add",
    "table-sort-order-set-default",
    "table-snapshot-add",
    "table-snapshots-remove",
    "table-snapshot-ref-set",
    "table-snapshot-ref-remove",
    "table-location-set",
    "table-statistics-set",
    "table-statistics-remove",
    "table-partition-spec-add",
    "table-partition-specs-remove",
    "table-structure-manage",
    "table-list",
];

/// Reading a VIEW through SQE loads the view metadata and then plans its SQL.
/// Polaris checks `view-properties-read` for the load and `view-list` for
/// discovery. Verified live: these on `{catalog, namespace, table: <view>}` are
/// what let a grantee load the view.
///
/// NOTE: this does NOT confer access to the view's base tables. SQE expands the
/// view SQL and plans against the underlying tables, so the reader needs its own
/// grant there. A view is therefore not a privilege boundary the way a Snowflake
/// secure view is; see design-notes/ranger-access-control.md.
const VIEW_READ_ACCESS: &[&str] = &["view-properties-read", "view-list"];

fn to_vec(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

/// Map a SQL privilege to the Ranger access type(s) it requires and the resource
/// level it binds to. A single SQL privilege expands to every Polaris access
/// type the corresponding operations check (impliedGrants are not honored).
/// Unknown privileges pass through lowercased so callers can use native Ranger
/// access-type names directly.
pub fn map_sql_to_ranger_access(sql_priv: &str) -> (Vec<String>, ResourceLevel) {
    map_sql_to_ranger_access_for(GrantObjectKind::Table, sql_priv)
}

/// `map_sql_to_ranger_access`, aware of whether the object is a table or a view.
///
/// A view uses the same resource shape as a table (its name goes in the `table`
/// slot) and differs only in the access types Polaris checks.
pub fn map_sql_to_ranger_access_for(
    object: GrantObjectKind,
    sql_priv: &str,
) -> (Vec<String>, ResourceLevel) {
    if object == GrantObjectKind::View {
        return match sql_priv.to_uppercase().as_str() {
            "SELECT" => (to_vec(VIEW_READ_ACCESS), ResourceLevel::Table),
            "DROP" => (to_vec(&["view-drop"]), ResourceLevel::Table),
            "CREATE VIEW" => (to_vec(&["view-create"]), ResourceLevel::Namespace),
            "ALL" | "ALL PRIVILEGES" => {
                (to_vec(&["view-metadata-full"]), ResourceLevel::Table)
            }
            // Unknown privileges pass through as raw Ranger access types, same
            // as the table path, so an operator can name a Polaris access type
            // directly.
            other => (vec![other.to_lowercase()], ResourceLevel::Table),
        };
    }
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => (to_vec(READ_ACCESS), ResourceLevel::Table),
        // Every data mutation resolves to the same Polaris access-type set.
        // UPDATE and DELETE in SQE are copy-on-write or merge-on-read: both load
        // the table, write new data files and commit a snapshot, which is
        // exactly what INSERT does, so there is no narrower set to give them.
        // MODIFY is the Databricks spelling and covers INSERT/UPDATE/DELETE.
        //
        // Before this, these fell through to the pass-through arm and were sent
        // as a literal access type ("update"), which the servicedef does not
        // declare, so Ranger answered a bare HTTP 400.
        "INSERT" | "UPDATE" | "DELETE" | "MODIFY" => {
            (to_vec(WRITE_ACCESS), ResourceLevel::Table)
        }
        "DROP" => (to_vec(&["table-drop"]), ResourceLevel::Table),
        "CREATE TABLE" => (to_vec(&["table-create"]), ResourceLevel::Namespace),
        "USAGE" => (
            to_vec(&["namespace-list", "namespace-properties-read"]),
            ResourceLevel::Namespace,
        ),
        "DROP SCHEMA" => (to_vec(&["namespace-drop"]), ResourceLevel::Namespace),
        "CREATE SCHEMA" | "CREATE" => (to_vec(&["namespace-create"]), ResourceLevel::Catalog),
        "ALL" | "ALL PRIVILEGES" => {
            (to_vec(&["catalog-content-manage"]), ResourceLevel::Catalog)
        }
        // Deliberate escape hatch: an unrecognised privilege is passed through
        // lowercased so an operator can name a Polaris access type directly
        // (e.g. `GRANT "table-snapshot-add" ON ...`). Ranger rejects anything the
        // servicedef does not declare, and `post_grant_revoke` turns that 400
        // into a message naming the privilege.
        other => (vec![other.to_lowercase()], ResourceLevel::Table),
    }
}

/// Privileges with an explicit mapping, for error messages.
pub const MAPPED_PRIVILEGES: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MODIFY",
    "DROP",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE SCHEMA",
    "DROP SCHEMA",
    "USAGE",
    "ALL PRIVILEGES",
];

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

/// Build the Ranger resource map for a given level. Includes `root` only when
/// `realm` is non-empty.
pub fn build_resource_map(
    realm: &str,
    catalog: &str,
    namespace: Option<&str>,
    table: Option<&str>,
    level: ResourceLevel,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if !realm.is_empty() {
        m.insert("root".to_string(), realm.to_string());
    }
    m.insert("catalog".to_string(), catalog.to_string());
    if matches!(level, ResourceLevel::Namespace | ResourceLevel::Table) {
        if let Some(ns) = namespace {
            m.insert("namespace".to_string(), ns.to_string());
        }
    }
    if matches!(level, ResourceLevel::Table) {
        if let Some(t) = table {
            m.insert("table".to_string(), t.to_string());
        }
    }
    m
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
fn grantee_to_fields(grantee: &Grantee) -> sqe_core::Result<(Vec<String>, Vec<String>)> {
    match grantee {
        Grantee::User(n) => Ok((vec![n.clone()], vec![])),
        Grantee::Role(n) => Ok((vec![], vec![n.clone()])),
        Grantee::Group(_) => Err(sqe_core::SqeError::NotImplemented(
            "Ranger backend supports USER and ROLE grantees only; GROUP requires Ranger usersync"
                .into(),
        )),
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

    fn build_grant_revoke_for(
        &self,
        privilege: &str,
        object: GrantObjectKind,
        catalog: Option<&str>,
        namespace: Option<&str>,
        table: Option<&str>,
        grantee: &Grantee,
    ) -> sqe_core::Result<GrantRevokeRequest> {
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

        let (access, level) = map_sql_to_ranger_access_for(object, privilege);
        let resource = build_resource_map(&self.realm, catalog, namespace, table, level);
        let (users, roles) = grantee_to_fields(grantee)?;

        Ok(GrantRevokeRequest {
            grantor: self.admin_user.clone(),
            resource,
            users,
            groups: vec![],
            roles,
            access_types: access,
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
        let (access_types, level) =
            map_sql_to_ranger_access_for(stmt.object, &stmt.privilege);
        let Some(catalog) = stmt.catalog.as_deref() else {
            return Ok(());
        };
        let resource = build_resource_map(
            &self.realm,
            catalog,
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            level,
        );
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
                (400, Some(p)) if !MAPPED_PRIVILEGES.contains(&p.to_uppercase().as_str()) => {
                    format!(
                        ". Privilege '{p}' has no mapping and was sent to Ranger as the \
                         access type '{}', which the service definition does not declare. \
                         Mapped privileges: {}. A native Polaris access type may also be \
                         named directly.",
                        p.to_lowercase(),
                        MAPPED_PRIVILEGES.join(", ")
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

#[derive(Debug, Deserialize)]
pub struct RangerPolicy {
    #[serde(default)]
    resources: BTreeMap<String, RangerResourceValues>,
    #[serde(default, rename = "policyItems")]
    policy_items: Vec<RangerPolicyItem>,
    #[serde(default, rename = "denyPolicyItems")]
    deny_policy_items: Vec<RangerPolicyItem>,
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
                            granted_by: None,
                            granted_at: None,
                        });
                    }
                    for r in &item.roles {
                        out.push(GrantEntry {
                            privilege: access.access_type.clone(),
                            resource: resource.clone(),
                            grantee_type: "ROLE".into(),
                            grantee_name: r.clone(),
                            effect: effect.into(),
                            granted_by: None,
                            granted_at: None,
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

/// Does an entry's grantee match the requested grantee (type + name)?
pub fn entry_matches_grantee(entry: &GrantEntry, grantee: &Grantee) -> bool {
    let want_type = match grantee {
        Grantee::User(_) => "USER",
        Grantee::Role(_) => "ROLE",
        Grantee::Group(_) => "GROUP",
    };
    entry.grantee_type == want_type && entry.grantee_name == grantee.name()
}

impl RangerGrantBackend {
    /// Fetch all policies for this service from Ranger Admin.
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
        let mut body = self.build_grant_revoke_for(
            &stmt.privilege,
            stmt.object,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        // Ranger authorizes against `grantor`, so this is the authority check,
        // not just an audit field. WITH GRANT OPTION becomes delegateAdmin.
        if let Some(g) = stmt.grantor.as_deref() {
            validate_identifier(g, "grantor")?;
            body.grantor = g.to_string();
        }
        body.delegate_admin = stmt.with_grant_option;
        self.post_grant_revoke_with_privilege("grant", &stmt.privilege, &body).await
    }

    async fn revoke(&self, _token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let mut body = self.build_grant_revoke_for(
            &stmt.privilege,
            stmt.object,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        if let Some(g) = stmt.grantor.as_deref() {
            validate_identifier(g, "grantor")?;
            body.grantor = g.to_string();
        }
        self.post_grant_revoke_with_privilege("revoke", &stmt.privilege, &body)
            .await?;
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
        let (access_types, level) =
            map_sql_to_ranger_access_for(stmt.object, &stmt.privilege);
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

        let resource = build_resource_map(
            &self.realm,
            catalog,
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            level,
        );
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
        let policies = self.fetch_policies().await?;
        let all = policies_to_entries(&policies);
        let filtered = match filter {
            GrantFilter::ToGrantee(g) => {
                all.into_iter().filter(|e| entry_matches_grantee(e, g)).collect()
            }
            GrantFilter::OnResource { catalog, namespace, table } => {
                let mut prefix = Vec::new();
                if let Some(c) = catalog { prefix.push(c.clone()); }
                if let Some(n) = namespace { prefix.push(n.clone()); }
                if let Some(t) = table { prefix.push(t.clone()); }
                let prefix = prefix.join(".");
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
        let policies = self.fetch_policies().await?;
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

        let (access, _) = map_sql_to_ranger_access(&check.privilege);
        // The privilege maps to a set; the first entry is the primary access
        // type that defines the privilege (e.g. SELECT -> table-data-read).
        let primary = access.first().map(String::as_str).unwrap_or("");
        let mut parts = vec![catalog.to_string()];
        if let Some(n) = &check.namespace { parts.push(n.clone()); }
        if let Some(t) = &check.table { parts.push(t.clone()); }
        let resource = parts.join(".");

        let policies = self.fetch_policies().await?;
        let entries = policies_to_entries(&policies);
        // Roles unknown at this layer; match on the user dimension only. Role
        // grants are surfaced via SHOW GRANTS and enforced by Polaris.
        Ok(evaluate_access(&entries, &check.user, &[], primary, &resource))
    }

    fn backend_name(&self) -> &str {
        "ranger"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_maps_to_table_data_read() {
        let (a, lvl) = map_sql_to_ranger_access("SELECT");
        // SELECT expands to the full read set; table-data-read is the primary.
        assert!(a.contains(&"table-data-read".to_string()));
        assert!(a.contains(&"table-properties-read".to_string()));
        assert_eq!(a.first().map(String::as_str), Some("table-data-read"));
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn insert_maps_to_table_data_write_with_commit_grants() {
        let (a, lvl) = map_sql_to_ranger_access("insert");
        // INSERT expands to write + the snapshot/schema commit grants, since
        // the embedded authorizer does not honor impliedGrants.
        assert_eq!(a.first().map(String::as_str), Some("table-data-write"));
        assert!(a.contains(&"table-snapshot-add".to_string()));
        assert!(a.contains(&"table-schema-add".to_string()));
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn create_table_is_namespace_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE TABLE");
        assert_eq!(a, vec!["table-create".to_string()]);
        assert_eq!(lvl, ResourceLevel::Namespace);
    }

    #[test]
    fn create_schema_is_catalog_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE SCHEMA");
        assert_eq!(a, vec!["namespace-create".to_string()]);
        assert_eq!(lvl, ResourceLevel::Catalog);
    }

    #[test]
    fn unknown_passes_through_lowercased() {
        let (a, lvl) = map_sql_to_ranger_access("table-metadata-full");
        assert_eq!(a, vec!["table-metadata-full".to_string()]);
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn resource_map_table_level_full_path() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), Some("orders"), ResourceLevel::Table);
        assert_eq!(m.get("root").map(String::as_str), Some("POLARIS"));
        assert_eq!(m.get("catalog").map(String::as_str), Some("wh"));
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(m.get("table").map(String::as_str), Some("orders"));
    }

    #[test]
    fn build_resource_map_future_tables_emits_table_wildcard() {
        // A FUTURE grant arrives as table = Some("*"). At table level the
        // resource map must carry an explicit "table": "*" so Ranger applies
        // the policy to every (existing and future) table in the namespace.
        let m = build_resource_map(
            "",
            "sales_wh",
            Some("sales"),
            Some("*"),
            ResourceLevel::Table,
        );
        assert_eq!(m.get("catalog").map(String::as_str), Some("sales_wh"));
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
        assert_eq!(m.get("table").map(String::as_str), Some("*"));
    }

    #[test]
    fn resource_map_namespace_level_omits_table() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), Some("orders"), ResourceLevel::Namespace);
        assert!(!m.contains_key("table"));
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
    }

    #[test]
    fn resource_map_catalog_level_only_catalog() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), None, ResourceLevel::Catalog);
        assert!(!m.contains_key("namespace"));
        assert_eq!(m.get("catalog").map(String::as_str), Some("wh"));
    }

    #[test]
    fn resource_map_empty_realm_omits_root() {
        let m = build_resource_map("", "wh", None, None, ResourceLevel::Catalog);
        assert!(!m.contains_key("root"));
    }

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
    fn grantee_group_is_rejected() {
        let err = grantee_to_fields(&Grantee::Group("sg".into())).unwrap_err();
        assert!(matches!(err, sqe_core::SqeError::NotImplemented(_)));
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
    fn check_match_denies_when_no_grant() {
        let r = evaluate_access(&[], "alice", &[], "table-data-read", "wh.sales.orders");
        assert!(!r.allowed);
    }
}
