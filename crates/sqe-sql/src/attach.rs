//! AST types for the ATTACH/DETACH/SECRET SQL extensions.
//!
//! These statements let operators register Iceberg catalogs and credentials
//! at runtime, mirroring DuckDB's `ATTACH` and `CREATE SECRET` ergonomics.
//! See `docs/superpowers/specs/2026-05-09-attach-catalog-and-secrets-design.md`.

use std::collections::BTreeMap;

/// `ATTACH '<location>' AS <name> (TYPE <kind>, <option> = <value>, ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachStatement {
    pub name: String,
    pub location: String,
    pub kind: CatalogKind,
    pub options: BTreeMap<String, OptionValue>,
}

/// `DETACH <name>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetachStatement {
    pub name: String,
}

/// `CREATE SECRET <name> (TYPE <kind>, <option> = <value>, ...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecretStatement {
    pub name: String,
    pub kind: SecretKind,
    pub options: BTreeMap<String, OptionValue>,
}

/// `DROP SECRET <name>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSecretStatement {
    pub name: String,
}

/// Kinds of Iceberg catalog backends that can be attached at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogKind {
    IcebergRest,
    Glue,
    S3Tables,
    Hms,
    Jdbc,
    Sqlite,
    Hadoop,
}

impl CatalogKind {
    /// Parse a case-insensitive backend keyword. Returns `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "iceberg_rest" | "rest" => Some(Self::IcebergRest),
            "glue" => Some(Self::Glue),
            "s3tables" => Some(Self::S3Tables),
            "hms" | "hive" => Some(Self::Hms),
            "jdbc" => Some(Self::Jdbc),
            "sqlite" => Some(Self::Sqlite),
            "hadoop" => Some(Self::Hadoop),
            _ => None,
        }
    }

    /// Stable lowercase label for metrics and audit logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::IcebergRest => "iceberg_rest",
            Self::Glue => "glue",
            Self::S3Tables => "s3tables",
            Self::Hms => "hms",
            Self::Jdbc => "jdbc",
            Self::Sqlite => "sqlite",
            Self::Hadoop => "hadoop",
        }
    }
}

/// Kinds of credential bundles supported by `CREATE SECRET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    Aws,
    Bearer,
    Basic,
}

impl SecretKind {
    /// Parse a case-insensitive secret kind. Returns `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "aws" => Some(Self::Aws),
            "bearer" => Some(Self::Bearer),
            "basic" => Some(Self::Basic),
            _ => None,
        }
    }

    /// Stable lowercase label for metrics and audit logs.
    pub fn name(self) -> &'static str {
        match self {
            Self::Aws => "aws",
            Self::Bearer => "bearer",
            Self::Basic => "basic",
        }
    }
}

/// Right-hand value of an `ATTACH`/`CREATE SECRET` option.
///
/// String literals carry concrete values. Bare identifiers reference a named
/// secret in the in-memory secret store and are resolved at attach time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    /// Single-quoted string literal: `WAREHOUSE 'my_wh'`.
    String(String),
    /// Unquoted identifier referencing a secret name: `SECRET aws_prod`.
    SecretRef(String),
}

impl OptionValue {
    /// Returns the inner string value if this is `Self::String`, else `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the secret name if this is `Self::SecretRef`, else `None`.
    pub fn as_secret_ref(&self) -> Option<&str> {
        match self {
            Self::SecretRef(s) => Some(s),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_kind_parses_all_variants() {
        assert_eq!(CatalogKind::parse("iceberg_rest"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("rest"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("glue"), Some(CatalogKind::Glue));
        assert_eq!(CatalogKind::parse("s3tables"), Some(CatalogKind::S3Tables));
        assert_eq!(CatalogKind::parse("hms"), Some(CatalogKind::Hms));
        assert_eq!(CatalogKind::parse("hive"), Some(CatalogKind::Hms));
        assert_eq!(CatalogKind::parse("jdbc"), Some(CatalogKind::Jdbc));
        assert_eq!(CatalogKind::parse("sqlite"), Some(CatalogKind::Sqlite));
        assert_eq!(CatalogKind::parse("hadoop"), Some(CatalogKind::Hadoop));
    }

    #[test]
    fn catalog_kind_parse_is_case_insensitive() {
        assert_eq!(CatalogKind::parse("ICEBERG_REST"), Some(CatalogKind::IcebergRest));
        assert_eq!(CatalogKind::parse("Glue"), Some(CatalogKind::Glue));
        assert_eq!(CatalogKind::parse("HaDoOp"), Some(CatalogKind::Hadoop));
    }

    #[test]
    fn catalog_kind_parse_rejects_unknown() {
        assert_eq!(CatalogKind::parse("postgres"), None);
        assert_eq!(CatalogKind::parse(""), None);
        assert_eq!(CatalogKind::parse("not_a_backend"), None);
    }

    #[test]
    fn catalog_kind_name_round_trip() {
        for k in [
            CatalogKind::IcebergRest,
            CatalogKind::Glue,
            CatalogKind::S3Tables,
            CatalogKind::Hms,
            CatalogKind::Jdbc,
            CatalogKind::Sqlite,
            CatalogKind::Hadoop,
        ] {
            // Every canonical name must round-trip through parse().
            assert_eq!(CatalogKind::parse(k.name()), Some(k), "round-trip failed for {k:?}");
        }
    }

    #[test]
    fn secret_kind_parses_all_variants() {
        assert_eq!(SecretKind::parse("aws"), Some(SecretKind::Aws));
        assert_eq!(SecretKind::parse("bearer"), Some(SecretKind::Bearer));
        assert_eq!(SecretKind::parse("basic"), Some(SecretKind::Basic));
    }

    #[test]
    fn secret_kind_parse_is_case_insensitive() {
        assert_eq!(SecretKind::parse("AWS"), Some(SecretKind::Aws));
        assert_eq!(SecretKind::parse("Bearer"), Some(SecretKind::Bearer));
        assert_eq!(SecretKind::parse("BASIC"), Some(SecretKind::Basic));
    }

    #[test]
    fn secret_kind_parse_rejects_unknown() {
        assert_eq!(SecretKind::parse("oauth"), None);
        assert_eq!(SecretKind::parse(""), None);
    }

    #[test]
    fn secret_kind_name_round_trip() {
        for k in [SecretKind::Aws, SecretKind::Bearer, SecretKind::Basic] {
            assert_eq!(SecretKind::parse(k.name()), Some(k));
        }
    }

    #[test]
    fn option_value_as_str_discriminates() {
        let v = OptionValue::String("hello".to_string());
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.as_secret_ref(), None);
    }

    #[test]
    fn option_value_as_secret_ref_discriminates() {
        let v = OptionValue::SecretRef("aws_prod".to_string());
        assert_eq!(v.as_secret_ref(), Some("aws_prod"));
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn ast_structs_are_constructable() {
        // Smoke test to confirm the public API shape compiles.
        let attach = AttachStatement {
            name: "polaris".to_string(),
            location: "https://polaris.example.com/api/catalog".to_string(),
            kind: CatalogKind::IcebergRest,
            options: BTreeMap::new(),
        };
        assert_eq!(attach.kind, CatalogKind::IcebergRest);

        let detach = DetachStatement { name: "polaris".to_string() };
        assert_eq!(detach.name, "polaris");

        let create = CreateSecretStatement {
            name: "aws_prod".to_string(),
            kind: SecretKind::Aws,
            options: BTreeMap::new(),
        };
        assert_eq!(create.kind, SecretKind::Aws);

        let drop_s = DropSecretStatement { name: "aws_prod".to_string() };
        assert_eq!(drop_s.name, "aws_prod");
    }
}
