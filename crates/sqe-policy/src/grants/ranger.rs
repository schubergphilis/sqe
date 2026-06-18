//! RangerGrantBackend — translates GRANT/REVOKE/SHOW GRANTS into Apache Ranger
//! Admin REST calls. Enforcement is delegated to Polaris 1.5's embedded Ranger
//! authorizer; this backend only writes/reads Ranger policies.
//!
//! Ranger service-def: `polaris`. Resource hierarchy: root -> catalog ->
//! namespace -> table. Access types are Polaris-native hyphenated names.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Which resource levels a privilege applies to. Determines which keys go into
/// the Ranger resource map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceLevel {
    Catalog,
    Namespace,
    Table,
}

/// Map a SQL privilege to a Ranger access type and the resource level it binds
/// to. Unknown privileges pass through lowercased (lets callers use native
/// Ranger access-type names directly).
pub fn map_sql_to_ranger_access(sql_priv: &str) -> (String, ResourceLevel) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => ("table-data-read".into(), ResourceLevel::Table),
        "INSERT" => ("table-data-write".into(), ResourceLevel::Table),
        "DROP" => ("table-drop".into(), ResourceLevel::Table),
        "CREATE TABLE" => ("table-create".into(), ResourceLevel::Namespace),
        "USAGE" => ("namespace-list".into(), ResourceLevel::Namespace),
        "DROP SCHEMA" => ("namespace-drop".into(), ResourceLevel::Namespace),
        "CREATE SCHEMA" | "CREATE" => ("namespace-create".into(), ResourceLevel::Catalog),
        "ALL" | "ALL PRIVILEGES" => ("catalog-content-manage".into(), ResourceLevel::Catalog),
        other => (other.to_lowercase(), ResourceLevel::Table),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_maps_to_table_data_read() {
        let (a, lvl) = map_sql_to_ranger_access("SELECT");
        assert_eq!(a, "table-data-read");
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn insert_maps_to_table_data_write() {
        let (a, lvl) = map_sql_to_ranger_access("insert");
        assert_eq!(a, "table-data-write");
        assert_eq!(lvl, ResourceLevel::Table);
    }

    #[test]
    fn create_table_is_namespace_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE TABLE");
        assert_eq!(a, "table-create");
        assert_eq!(lvl, ResourceLevel::Namespace);
    }

    #[test]
    fn create_schema_is_catalog_level() {
        let (a, lvl) = map_sql_to_ranger_access("CREATE SCHEMA");
        assert_eq!(a, "namespace-create");
        assert_eq!(lvl, ResourceLevel::Catalog);
    }

    #[test]
    fn unknown_passes_through_lowercased() {
        let (a, lvl) = map_sql_to_ranger_access("table-metadata-full");
        assert_eq!(a, "table-metadata-full");
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
        assert!(m.get("table").is_none());
        assert_eq!(m.get("namespace").map(String::as_str), Some("sales"));
    }

    #[test]
    fn resource_map_catalog_level_only_catalog() {
        let m = build_resource_map("POLARIS", "wh", Some("sales"), None, ResourceLevel::Catalog);
        assert!(m.get("namespace").is_none());
        assert_eq!(m.get("catalog").map(String::as_str), Some("wh"));
    }

    #[test]
    fn resource_map_empty_realm_omits_root() {
        let m = build_resource_map("", "wh", None, None, ResourceLevel::Catalog);
        assert!(m.get("root").is_none());
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
}
