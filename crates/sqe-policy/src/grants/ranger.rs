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
    AccessCheck, AccessCheckResult, GrantBackend, GrantEntry, GrantFilter, GrantStatement,
    Grantee, RevokeStatement,
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

fn to_vec(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

/// Map a SQL privilege to the Ranger access type(s) it requires and the resource
/// level it binds to. A single SQL privilege expands to every Polaris access
/// type the corresponding operations check (impliedGrants are not honored).
/// Unknown privileges pass through lowercased so callers can use native Ranger
/// access-type names directly.
pub fn map_sql_to_ranger_access(sql_priv: &str) -> (Vec<String>, ResourceLevel) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => (to_vec(READ_ACCESS), ResourceLevel::Table),
        "INSERT" => (to_vec(WRITE_ACCESS), ResourceLevel::Table),
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
        other => (vec![other.to_lowercase()], ResourceLevel::Table),
    }
}

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
    fn build_grant_revoke(
        &self,
        privilege: &str,
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

        let (access, level) = map_sql_to_ranger_access(privilege);
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
    async fn post_grant_revoke(&self, op: &str, body: &GrantRevokeRequest) -> sqe_core::Result<()> {
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
            return Err(sqe_core::SqeError::Execution(format!(
                "Ranger {op} failed (HTTP {status})"
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
        let body = self.build_grant_revoke(
            &stmt.privilege,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        self.post_grant_revoke("grant", &body).await
    }

    async fn revoke(&self, _token: &str, stmt: &RevokeStatement) -> sqe_core::Result<()> {
        let body = self.build_grant_revoke(
            &stmt.privilege,
            stmt.catalog.as_deref(),
            stmt.namespace.as_deref(),
            stmt.table.as_deref(),
            &stmt.grantee,
        )?;
        self.post_grant_revoke("revoke", &body).await
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
