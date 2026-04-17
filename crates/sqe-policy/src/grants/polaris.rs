//! PolarisGrantBackend — calls the Polaris Management REST API.

// ── Privilege mapping (SQL -> Polaris) ────────────────────────────────────────

/// Map a SQL privilege string to the Polaris privilege name and resource type.
///
/// Returns `(polaris_privilege, resource_type)`. If the input already looks
/// like a Polaris native name (all-caps with underscores), pass it through.
pub fn map_sql_to_polaris_privilege(sql_priv: &str) -> (String, &'static str) {
    match sql_priv.to_uppercase().as_str() {
        "SELECT" => ("TABLE_READ_DATA".into(), "table"),
        "INSERT" => ("TABLE_WRITE_DATA".into(), "table"),
        "CREATE TABLE" => ("TABLE_CREATE".into(), "namespace"),
        "DROP" => ("TABLE_DROP".into(), "table"),
        "ALL" | "ALL PRIVILEGES" => ("CATALOG_MANAGE_CONTENT".into(), "catalog"),
        "USAGE" => ("NAMESPACE_LIST".into(), "namespace"),
        "CREATE SCHEMA" | "CREATE" => ("NAMESPACE_CREATE".into(), "catalog"),
        "DROP SCHEMA" => ("NAMESPACE_DROP".into(), "namespace"),
        _ => {
            // Pass-through: send unrecognized privileges verbatim.
            // Polaris native names (ALL_CAPS_WITH_UNDERSCORES) go through as-is.
            (sql_priv.to_uppercase(), "table")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_select_to_table_read_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("SELECT");
        assert_eq!(priv_name, "TABLE_READ_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_insert_to_table_write_data() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("INSERT");
        assert_eq!(priv_name, "TABLE_WRITE_DATA");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_all_to_catalog_manage_content() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("ALL");
        assert_eq!(priv_name, "CATALOG_MANAGE_CONTENT");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn map_usage_to_namespace_list() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("USAGE");
        assert_eq!(priv_name, "NAMESPACE_LIST");
        assert_eq!(res_type, "namespace");
    }

    #[test]
    fn map_create_schema_to_namespace_create() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("CREATE SCHEMA");
        assert_eq!(priv_name, "NAMESPACE_CREATE");
        assert_eq!(res_type, "catalog");
    }

    #[test]
    fn passthrough_polaris_native_privilege() {
        let (priv_name, res_type) = map_sql_to_polaris_privilege("TABLE_WRITE_PROPERTIES");
        assert_eq!(priv_name, "TABLE_WRITE_PROPERTIES");
        assert_eq!(res_type, "table");
    }

    #[test]
    fn map_is_case_insensitive() {
        let (priv_name, _) = map_sql_to_polaris_privilege("select");
        assert_eq!(priv_name.as_str(), "TABLE_READ_DATA");
    }
}
