//! Catalog backend registry.
//!
//! SQE ships with the Iceberg REST catalog as the default backend. Phase A of
//! the `iceberg-matrix-parity` change adds feature-gated modules for Glue, HMS,
//! JDBC (SQLite/PostgreSQL), and a storage-only Hadoop backend. Each module
//! exposes a small, testable surface; the full `iceberg::Catalog` trait is
//! implemented either natively (Hadoop, SQL/SQLite) or by delegating to the
//! upstream `iceberg-catalog-*` workspace crates once the vendored fork
//! rebases onto a release that exports them.
//!
//! The module tree is gated per backend so that a minimal REST-only build does
//! not link unused dependencies.

#[cfg(feature = "glue")]
pub mod glue;

#[cfg(feature = "hms")]
pub mod hms;

#[cfg(feature = "sql")]
pub mod sql;

#[cfg(feature = "hadoop")]
pub mod hadoop;

/// Identifier used by the catalog registry to select a backend.
///
/// The variants match what users put into `catalog.type` in `sqe.toml`:
///
/// ```toml
/// [catalog]
/// type = "rest"     # default
/// type = "glue"
/// type = "hms"
/// type = "jdbc"     # maps to BackendKind::Sql
/// type = "hadoop"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    Rest,
    #[cfg(feature = "glue")]
    Glue,
    #[cfg(feature = "hms")]
    Hms,
    #[cfg(feature = "sql")]
    Sql,
    #[cfg(feature = "hadoop")]
    Hadoop,
}

impl BackendKind {
    /// Parse a catalog-type string from config into a backend identifier.
    ///
    /// Kept as an inherent method (not `std::str::FromStr`) because the return
    /// type is `Option<Self>`, not `Result<Self, Err>`, and the input is the
    /// full set of supported config strings rather than a strict round-trip.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "rest" => Some(Self::Rest),
            #[cfg(feature = "glue")]
            "glue" | "aws-glue" | "aws_glue" => Some(Self::Glue),
            #[cfg(feature = "hms")]
            "hms" | "hive" | "hive-metastore" => Some(Self::Hms),
            #[cfg(feature = "sql")]
            "sql" | "jdbc" => Some(Self::Sql),
            #[cfg(feature = "hadoop")]
            "hadoop" | "filesystem" => Some(Self::Hadoop),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_parses_from_string() {
        assert_eq!(BackendKind::from_str("rest"), Some(BackendKind::Rest));
        assert_eq!(BackendKind::from_str("REST"), Some(BackendKind::Rest));
    }

    #[test]
    fn unknown_backend_returns_none() {
        assert!(BackendKind::from_str("snowflake-native").is_none());
    }

    #[cfg(feature = "sql")]
    #[test]
    fn sql_and_jdbc_are_aliases() {
        assert_eq!(BackendKind::from_str("sql"), Some(BackendKind::Sql));
        assert_eq!(BackendKind::from_str("jdbc"), Some(BackendKind::Sql));
    }

    #[cfg(feature = "hadoop")]
    #[test]
    fn hadoop_parses() {
        assert_eq!(BackendKind::from_str("hadoop"), Some(BackendKind::Hadoop));
        assert_eq!(
            BackendKind::from_str("filesystem"),
            Some(BackendKind::Hadoop)
        );
    }
}
