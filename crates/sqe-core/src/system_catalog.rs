//! Which part of the catalog name `system` SQE serves itself (#901).
//!
//! Under `system`, only the schemas in [`RESERVED_SYSTEM_SCHEMAS`] are SQE's
//! virtual ones. Every other schema belongs to a Polaris catalog named
//! `system` when one exists, and is governed like any catalog. Every place
//! that decides "is this a virtual `system` relation" (catalog routing, the
//! policy rewriter, the result cache, the Polaris load check, MCP and the
//! Trino layer) asks [`is_virtual_system_ref`], so routing and policy can
//! never disagree.

/// The catalog name SQE's virtual provider is registered under.
pub const SYSTEM_CATALOG: &str = "system";

/// DataFusion's own in-memory catalog.
pub const DATAFUSION_CATALOG: &str = "datafusion";

/// Schemas of `system` that SQE serves. A Polaris namespace of one of these
/// names in a Polaris catalog `system` is never reachable.
pub const RESERVED_SYSTEM_SCHEMAS: [&str; 4] =
    ["runtime", "metadata", "jdbc", "information_schema"];

/// True when `schema` is one of [`RESERVED_SYSTEM_SCHEMAS`] (ASCII case-
/// insensitive). Folding only widens the reserved set, so a schema routed
/// here never reaches Polaris; the virtual provider answers it or nothing does.
pub fn is_reserved_system_schema(schema: &str) -> bool {
    RESERVED_SYSTEM_SCHEMAS
        .iter()
        .any(|r| r.eq_ignore_ascii_case(schema))
}

/// True when `(catalog, schema)` names one of SQE's virtual `system` schemas.
///
/// The catalog must be exactly `system`: that is the name the composite
/// provider is registered under, and DataFusion resolves a quoted `"SYSTEM"`
/// to a different catalog. Matching it case-insensitively would mark a real
/// table in such a catalog as virtual and skip its policy.
pub fn is_virtual_system_ref(catalog: &str, schema: &str) -> bool {
    catalog == SYSTEM_CATALOG && is_reserved_system_schema(schema)
}

/// True when `name` may not be used for a configured, attached or Hive
/// catalog: `system` (SQE wraps a Polaris catalog of that name, nothing else)
/// and `datafusion`. ASCII case-insensitive, so a quoted `"SYSTEM"` cannot
/// stand in for the reserved name either.
pub fn is_reserved_catalog_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(SYSTEM_CATALOG) || name.eq_ignore_ascii_case(DATAFUSION_CATALOG)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_schemas_fold_case() {
        for s in [
            "runtime",
            "RUNTIME",
            "Metadata",
            "jdbc",
            "information_schema",
        ] {
            assert!(is_reserved_system_schema(s), "{s}");
        }
        for s in ["access", "query", "lakeflow", "runtimes", ""] {
            assert!(!is_reserved_system_schema(s), "{s}");
        }
    }

    #[test]
    fn virtual_ref_needs_the_exact_catalog() {
        assert!(is_virtual_system_ref("system", "runtime"));
        assert!(is_virtual_system_ref("system", "JDBC"));
        assert!(!is_virtual_system_ref("system", "access"));
        assert!(!is_virtual_system_ref("SYSTEM", "runtime"));
        assert!(!is_virtual_system_ref("iceberg", "runtime"));
    }

    #[test]
    fn reserved_catalog_names_fold_case() {
        for n in ["system", "SYSTEM", "datafusion", "DataFusion"] {
            assert!(is_reserved_catalog_name(n), "{n}");
        }
        assert!(!is_reserved_catalog_name("iceberg"));
        assert!(!is_reserved_catalog_name("systems"));
    }
}
