//! Identifier helpers shared by the write, DDL and query paths.
//!
//! Pure functions over `sqlparser::ast::ObjectName` and `iceberg::TableIdent`.
//! They used to live in `catalog_ops.rs`, which made the write handler depend
//! on the DDL handler for five stateless helpers and blocked moving the write
//! path into its own crate (`docs/internal/plans/2026-09-07-coordinator-crate-split.md`,
//! MR A). Nothing here touches catalog state.

use iceberg::{NamespaceIdent, TableIdent};
use sqlparser::ast::ObjectName;

use sqe_core::{Session, SqeError, SqeErrorCode};

/// Fold a column identifier the way the query side does: unquoted names fold
/// to lowercase, quoted names are preserved. DataFusion's identifier
/// normalization lowercases ASCII letters only (`ÉTAT` resolves as `État`),
/// so this does too: a column stored from unquoted DDL resolves against the
/// same unquoted name in a query, and a double-quoted column keeps its case
/// and must be quoted to reference (#337).
pub fn fold_unquoted_ident(ident: &sqlparser::ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Parse a sqlparser `ObjectName` into an iceberg `TableIdent`.
///
/// Returning a `TableIdent` end-to-end removes the chance of an
/// argument-order swap at the `TableIdent::new(namespace, name)` call sites.
///
/// - 1 part  -> namespace = "default", table = name
/// - 2 parts -> namespace = parts[0], table = parts[1]
/// - 3 parts -> ignore catalog prefix, namespace = parts[1], table = parts[2]
pub fn parse_table_ref(name: &ObjectName) -> sqe_core::Result<TableIdent> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|ident| ident.value.clone())
        .collect();

    match parts.len() {
        1 => Ok(TableIdent::new(
            NamespaceIdent::new("default".to_string()),
            parts[0].clone(),
        )),
        2 => Ok(TableIdent::new(
            NamespaceIdent::new(parts[0].clone()),
            parts[1].clone(),
        )),
        3 => Ok(TableIdent::new(
            NamespaceIdent::new(parts[1].clone()),
            parts[2].clone(),
        )),
        n => Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("Invalid table reference with {n} parts: {name}"),
        )),
    }
}

/// Resolve a table reference to a `TableIdent`, honoring the session schema
/// (`X-Trino-Schema`) for an unqualified 1-part name.
///
/// `parse_table_ref` defaults an unqualified name to the `default` namespace,
/// but the read path (and Trino) resolve `t` against the session's default
/// schema. The write/DDL handlers must match that so a client on
/// `X-Trino-Schema: tpch_demo` targets `tpch_demo.t`, not `default.t` (#357).
/// A 2- or 3-part name already names its namespace and is returned unchanged;
/// when the session schema is unset or empty we fall back to `default`.
pub fn resolve_table_ident(
    name: &ObjectName,
    session: &Session,
) -> sqe_core::Result<TableIdent> {
    let ident = parse_table_ref(name)?;
    let is_unqualified = name.0.iter().filter_map(|p| p.as_ident()).count() == 1;
    if is_unqualified {
        if let Some(schema) = session.default_schema.as_deref().filter(|s| !s.is_empty()) {
            return Ok(TableIdent::new(
                NamespaceIdent::new(schema.to_string()),
                ident.name().to_string(),
            ));
        }
    }
    Ok(ident)
}

/// Extract the catalog qualifier from a 3-part table reference
/// (`catalog.namespace.table`). Returns `None` for 1- or 2-part names, which
/// resolve in the session's default warehouse as before.
///
/// `parse_table_ref` deliberately drops the catalog prefix (the iceberg
/// `TableIdent` is namespace+table only); the write path uses this to resolve
/// the *target* catalog instead of the configured default warehouse. Mirrors
/// the read path's `sqe_sql::extract_catalog_qualifiers` access (`.value`, no
/// quote/case normalisation) so reads and writes resolve the same catalog name.
pub fn catalog_qualifier(name: &ObjectName) -> Option<String> {
    leading_qualifier(name, 3)
}

/// Extract the catalog qualifier from a 2-part *schema* reference
/// (`catalog.schema`). Returns `None` for a 1-part bare schema (resolves in the
/// default warehouse). This is the schema-DDL analogue of [`catalog_qualifier`]:
/// `CREATE/DROP SCHEMA catalog.schema` is two parts (catalog + namespace),
/// whereas a table reference is three (catalog + namespace + table).
pub fn schema_catalog_qualifier(name: &ObjectName) -> Option<String> {
    leading_qualifier(name, 2)
}

/// First identifier of `name` when it has exactly `parts` identifiers.
fn leading_qualifier(name: &ObjectName, parts: usize) -> Option<String> {
    let idents: Vec<&str> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident())
        .map(|ident| ident.value.as_str())
        .collect();
    (idents.len() == parts).then(|| idents[0].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Ident;

    #[test]
    fn fold_unquoted_ident_folds_ascii_letters_only() {
        // `SELECT ÉTAT` resolves `État`, so that is the name to store.
        assert_eq!(fold_unquoted_ident(&Ident::new("ÉTAT")), "État");
        assert_eq!(fold_unquoted_ident(&Ident::new("STRASSE_Ä")), "strasse_Ä");
    }

    proptest::proptest! {
        /// The stored name is the name a query resolves: DataFusion's own
        /// identifier normalizer is the oracle, quoted or not.
        #[test]
        fn fold_unquoted_ident_matches_datafusion(
            value in "\\PC{0,12}",
            quote in proptest::option::of(proptest::sample::select(vec!['"', '`'])),
        ) {
            let ident = match quote {
                Some(q) => Ident::with_quote(q, value),
                None => Ident::new(value),
            };
            let query_side =
                datafusion::sql::planner::IdentNormalizer::new(true).normalize(ident.clone());
            proptest::prop_assert_eq!(fold_unquoted_ident(&ident), query_side);
        }
    }

    #[test]
    fn fold_unquoted_ident_folds_unquoted_preserves_quoted() {
        // Unquoted -> lowercase (matches Trino + DataFusion query-side folding).
        assert_eq!(
            fold_unquoted_ident(&Ident::new("testInteger")),
            "testinteger"
        );
        assert_eq!(fold_unquoted_ident(&Ident::new("ALREADYUP")), "alreadyup");
        // Already lowercase is unchanged.
        assert_eq!(fold_unquoted_ident(&Ident::new("plain")), "plain");
        // Double-quoted -> case preserved.
        assert_eq!(
            fold_unquoted_ident(&Ident::with_quote('"', "KeepMe")),
            "KeepMe"
        );
    }

    #[test]
    fn test_parse_table_ref_one_part() {
        let name = ObjectName::from(vec![Ident::new("my_table")]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("default".to_string())
        );
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_two_parts() {
        let name = ObjectName::from(vec![Ident::new("my_schema"), Ident::new("my_table")]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("my_schema".to_string())
        );
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_three_parts() {
        let name = ObjectName::from(vec![
            Ident::new("my_catalog"),
            Ident::new("my_schema"),
            Ident::new("my_table"),
        ]);
        let ident = parse_table_ref(&name).unwrap();
        assert_eq!(
            ident.namespace(),
            &NamespaceIdent::new("my_schema".to_string())
        );
        assert_eq!(ident.name(), "my_table");
    }

    #[test]
    fn test_parse_table_ref_empty_is_error() {
        let name = ObjectName::from(vec![] as Vec<Ident>);
        assert!(parse_table_ref(&name).is_err());
    }

    #[test]
    fn test_parse_table_ref_four_parts_is_error() {
        let name = ObjectName::from(vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ]);
        assert!(parse_table_ref(&name).is_err());
    }

    #[test]
    fn test_catalog_qualifier_one_part_is_none() {
        let name = ObjectName::from(vec![Ident::new("my_table")]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    #[test]
    fn test_catalog_qualifier_two_parts_is_none() {
        let name = ObjectName::from(vec![Ident::new("my_schema"), Ident::new("my_table")]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    #[test]
    fn test_catalog_qualifier_three_parts_returns_catalog() {
        let name = ObjectName::from(vec![
            Ident::new("team_a_data"),
            Ident::new("public"),
            Ident::new("events"),
        ]);
        assert_eq!(catalog_qualifier(&name), Some("team_a_data".to_string()));
    }

    #[test]
    fn test_catalog_qualifier_four_parts_is_none() {
        let name = ObjectName::from(vec![
            Ident::new("a"),
            Ident::new("b"),
            Ident::new("c"),
            Ident::new("d"),
        ]);
        assert_eq!(catalog_qualifier(&name), None);
    }

    // ─── schema_catalog_qualifier: 2-part catalog.schema ──────────────
    #[test]
    fn test_schema_catalog_qualifier_one_part_is_none() {
        let name = ObjectName::from(vec![Ident::new("dev_raw")]);
        assert_eq!(schema_catalog_qualifier(&name), None);
    }

    #[test]
    fn test_schema_catalog_qualifier_two_parts_returns_catalog() {
        let name = ObjectName::from(vec![Ident::new("ws_team_a"), Ident::new("dev_raw")]);
        assert_eq!(
            schema_catalog_qualifier(&name),
            Some("ws_team_a".to_string())
        );
    }

    #[test]
    fn test_schema_catalog_qualifier_three_parts_is_none() {
        let name = ObjectName::from(vec![Ident::new("a"), Ident::new("b"), Ident::new("c")]);
        assert_eq!(schema_catalog_qualifier(&name), None);
    }
}
