//! RangerGrantBackend — translates GRANT/REVOKE/SHOW GRANTS into Apache Ranger
//! Admin REST calls. Enforcement is delegated to Polaris 1.5's embedded Ranger
//! authorizer; this backend only writes/reads Ranger policies.
//!
//! Ranger service-def: `polaris`. Resource hierarchy: root -> catalog ->
//! namespace -> table. Access types are Polaris-native hyphenated names.

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
}
