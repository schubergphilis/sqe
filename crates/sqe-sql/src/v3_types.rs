//! Iceberg V3 type support for the SQL surface.
//!
//! Surfaces two V3 primitives through DDL parsing:
//!
//! - Nanosecond timestamps. `TIMESTAMP_NS(9)` and `TIMESTAMPTZ_NS(9)` arrive
//!   as `DataType::Custom` from sqlparser 0.54. The precision digit is
//!   cosmetic per the SQL spec convention: the type itself implies 1 ns.
//! - Column defaults. `DEFAULT <literal>` on a column is extracted from a
//!   sqlparser `Expr` into a simple literal kind that the caller can lift
//!   into `iceberg::spec::Literal`.
//!
//! The module stays free of `iceberg` types so that the conversion layer
//! lives in `sqe-coordinator`, where the rest of the Arrow + Iceberg
//! plumbing already sits.

use sqlparser::ast::{DataType as SqlType, Expr, TimezoneInfo, Value, ValueWithSpan};

/// Nanosecond timestamp flavour selected by the DDL parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NsTimestamp {
    /// `TIMESTAMP_NS` — without time zone.
    WithoutTz,
    /// `TIMESTAMPTZ_NS` — with time zone (UTC).
    WithTz,
}

/// Detect if a sqlparser `DataType` names a nanosecond timestamp.
///
/// sqlparser 0.54 has no `TIMESTAMP_NS` keyword, so the type lands as
/// `DataType::Custom(ObjectName, modifiers)`. The modifier carries the
/// cosmetic precision digit which we deliberately ignore.
pub fn detect_ns_timestamp(sql_type: &SqlType) -> Option<NsTimestamp> {
    let SqlType::Custom(object_name, _modifiers) = sql_type else {
        return None;
    };
    if object_name.0.len() != 1 {
        return None;
    }
    let name = object_name.0[0].as_ident()?.value.to_ascii_uppercase();
    match name.as_str() {
        "TIMESTAMP_NS" => Some(NsTimestamp::WithoutTz),
        "TIMESTAMPTZ_NS" => Some(NsTimestamp::WithTz),
        _ => None,
    }
}

/// True when a sqlparser `DataType` names the Iceberg V3 `VARIANT` type.
///
/// sqlparser has no `VARIANT` keyword, so it arrives as `DataType::Custom`.
pub fn is_variant_type(sql_type: &SqlType) -> bool {
    let SqlType::Custom(object_name, modifiers) = sql_type else {
        return false;
    };
    object_name.0.len() == 1
        && modifiers.is_empty()
        && object_name.0[0]
            .as_ident()
            .is_some_and(|i| i.value.eq_ignore_ascii_case("VARIANT"))
}

/// The Iceberg type string of a `GEOMETRY` / `GEOGRAPHY` column type, e.g.
/// `geometry(EPSG:3857)` or `geography(OGC:CRS84, spherical)`; `None` for
/// any other type (#863).
///
/// The CRS is an SRID number or a quoted CRS string. SRIDs map as Spark
/// writes them: 4326 is Iceberg's default `OGC:CRS84`, 0 is `SRID:0`, any
/// other n is `EPSG:n`. A geography takes an optional second argument, the
/// edge interpolation algorithm (default `spherical`). The caller parses the
/// string as an Iceberg type, which rejects a bad CRS, algorithm or arity.
pub fn geospatial_type(sql_type: &SqlType) -> Option<String> {
    let SqlType::Custom(object_name, modifiers) = sql_type else {
        return None;
    };
    if object_name.0.len() != 1 {
        return None;
    }
    let name = object_name.0[0].as_ident()?.value.to_ascii_lowercase();
    if name != "geometry" && name != "geography" {
        return None;
    }
    let crs = |m: &String| match m.as_str() {
        "4326" => "OGC:CRS84".to_string(),
        "0" => "SRID:0".to_string(),
        n if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) => format!("EPSG:{n}"),
        other => other.to_string(),
    };
    let mut params: Vec<String> = modifiers.iter().take(1).map(crs).collect();
    params.extend(modifiers.iter().skip(1).cloned());
    if params.is_empty() {
        return Some(name);
    }
    Some(format!("{name}({})", params.join(", ")))
}

/// True when a sqlparser `DataType` triggers format-version 3.
///
/// Nanosecond timestamps, `VARIANT`, `GEOMETRY` and `GEOGRAPHY` are V3-only.
/// `Unknown` is handled at a different layer.
pub fn is_v3_only_type(sql_type: &SqlType) -> bool {
    detect_ns_timestamp(sql_type).is_some()
        || is_variant_type(sql_type)
        || geospatial_type(sql_type).is_some()
}

/// Literal kinds we accept as column defaults.
///
/// Kept small on purpose: the Iceberg default is a stored value, not an
/// expression. Function calls, arithmetic, and correlated refs must fail
/// with a clear error.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultLiteral {
    /// Signed integer (i64 covers BIGINT and narrower).
    Int(i64),
    /// Double-precision float.
    Float(f64),
    /// Boolean literal.
    Bool(bool),
    /// UTF-8 string literal.
    String(String),
    /// Hex byte-string literal (`X'0102'`), for BINARY and FIXED columns.
    Bytes(Vec<u8>),
    /// Explicit SQL NULL.
    Null,
}

/// Error returned when a DEFAULT expression is not a supported literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultError {
    /// Human-readable message. Names the accepted forms.
    pub message: String,
}

impl std::fmt::Display for DefaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DefaultError {}

/// Convert a sqlparser column-default `Expr` into a literal kind.
///
/// Accepts integer, float, string, `X'hex'`, boolean, typed cast of the same, and
/// `NULL`. Anything else (function calls like `current_timestamp()`,
/// arithmetic, correlated subqueries) fails with `DefaultError` naming
/// the accepted forms.
pub fn extract_default_literal(expr: &Expr) -> Result<DefaultLiteral, DefaultError> {
    match expr {
        Expr::Value(value) => value_to_literal(&value.value),
        Expr::UnaryOp { op, expr } => {
            // Allow `-42` and `-3.14` as signed literals.
            use sqlparser::ast::UnaryOperator;
            match op {
                UnaryOperator::Minus => match expr.as_ref() {
                    Expr::Value(ValueWithSpan {
                        value: Value::Number(s, _),
                        ..
                    }) => {
                        if let Ok(i) = s.parse::<i64>() {
                            Ok(DefaultLiteral::Int(-i))
                        } else if let Ok(f) = s.parse::<f64>() {
                            Ok(DefaultLiteral::Float(-f))
                        } else {
                            Err(reject("unary minus on a non-numeric literal"))
                        }
                    }
                    _ => Err(reject("unary operator applied to a non-literal")),
                },
                UnaryOperator::Plus => match expr.as_ref() {
                    Expr::Value(v) => value_to_literal(&v.value),
                    _ => Err(reject("unary operator applied to a non-literal")),
                },
                _ => Err(reject("unsupported unary operator in DEFAULT")),
            }
        }
        Expr::Cast {
            expr: inner,
            data_type: _,
            format: _,
            kind: _,
            array: _,
        } => {
            // Typed casts such as `CAST(1 AS BIGINT)` are allowed. The
            // target data type is already known from the column, so we
            // just keep the inner literal and let the storage layer
            // coerce.
            extract_default_literal(inner)
        }
        Expr::Nested(inner) => extract_default_literal(inner),
        Expr::Function(func) => {
            let name = func
                .name
                .0
                .iter()
                .filter_map(|p| p.as_ident())
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            Err(reject(&format!(
                "function call `{name}` is not a supported DEFAULT"
            )))
        }
        _ => Err(reject("expression is not a simple literal")),
    }
}

fn value_to_literal(value: &Value) -> Result<DefaultLiteral, DefaultError> {
    match value {
        Value::Number(s, _) => {
            if let Ok(i) = s.parse::<i64>() {
                Ok(DefaultLiteral::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok(DefaultLiteral::Float(f))
            } else {
                Err(reject(&format!("could not parse number literal `{s}`")))
            }
        }
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::NationalStringLiteral(s)
        | Value::EscapedStringLiteral(s)
        | Value::DollarQuotedString(sqlparser::ast::DollarQuotedString { value: s, .. }) => {
            Ok(DefaultLiteral::String(s.clone()))
        }
        Value::HexStringLiteral(hex) => decode_hex(hex)
            .map(DefaultLiteral::Bytes)
            .ok_or_else(|| reject(&format!("`X'{hex}'` is not an even number of hex digits"))),
        Value::Boolean(b) => Ok(DefaultLiteral::Bool(*b)),
        Value::Null => Ok(DefaultLiteral::Null),
        _ => Err(reject(&format!("literal kind `{value}` is not supported"))),
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

fn reject(detail: &str) -> DefaultError {
    DefaultError {
        message: format!(
            "unsupported DEFAULT expression: {detail}. Accepted forms: integer, \
             float, string, X'hex', boolean, NULL, or a typed cast of the same"
        ),
    }
}

/// True when the SQL data type represents a UTC-tagged timestamp.
///
/// Helpful callers that need to distinguish `TimezoneInfo::Tz`
/// (TIMESTAMPTZ shorthand) from `TimezoneInfo::None`.
pub fn is_tz_variant(info: &TimezoneInfo) -> bool {
    matches!(info, TimezoneInfo::Tz | TimezoneInfo::WithTimeZone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_create(sql: &str) -> sqlparser::ast::Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("sql parses")
            .into_iter()
            .next()
            .expect("at least one statement")
    }

    #[test]
    fn detects_timestamp_ns() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMP_NS(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(
            detect_ns_timestamp(&col.data_type),
            Some(NsTimestamp::WithoutTz)
        );
    }

    #[test]
    fn detects_timestamptz_ns() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMPTZ_NS(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(
            detect_ns_timestamp(&col.data_type),
            Some(NsTimestamp::WithTz)
        );
    }

    #[test]
    fn ns_detection_is_case_insensitive() {
        let stmt = parse_create("CREATE TABLE t (ts timestamp_ns(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(
            detect_ns_timestamp(&col.data_type),
            Some(NsTimestamp::WithoutTz)
        );
    }

    #[test]
    fn plain_timestamp_is_not_ns() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMP(6))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(detect_ns_timestamp(&col.data_type), None);
    }

    #[test]
    fn ns_triggers_v3() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMP_NS(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        assert!(is_v3_only_type(&ct.columns[0].data_type));
    }

    #[test]
    fn geospatial_types_map_to_iceberg_type_strings() {
        let cases = [
            ("GEOMETRY", Some("geometry")),
            ("geometry(4326)", Some("geometry(OGC:CRS84)")),
            ("GEOMETRY(0)", Some("geometry(SRID:0)")),
            ("GEOMETRY(3857)", Some("geometry(EPSG:3857)")),
            ("GEOMETRY('EPSG:28992')", Some("geometry(EPSG:28992)")),
            ("GEOGRAPHY", Some("geography")),
            ("Geography(4326)", Some("geography(OGC:CRS84)")),
            (
                "GEOGRAPHY('OGC:CRS84', karney)",
                Some("geography(OGC:CRS84, karney)"),
            ),
            ("BYTEA", None),
            ("VARIANT", None),
        ];
        for (decl, want) in cases {
            let stmt = parse_create(&format!("CREATE TABLE t (g {decl})"));
            let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
                panic!("expected CreateTable");
            };
            let ty = &ct.columns[0].data_type;
            assert_eq!(geospatial_type(ty).as_deref(), want, "{decl}");
            let v3 = want.is_some() || decl == "VARIANT";
            assert_eq!(is_v3_only_type(ty), v3, "{decl}");
        }
    }

    #[test]
    fn extracts_string_default() {
        let stmt = parse_create("CREATE TABLE t (id BIGINT, status STRING DEFAULT 'pending')");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let status = &ct.columns[1];
        let default = status
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::String("pending".to_string())
        );
    }

    #[test]
    fn extracts_integer_default() {
        let stmt = parse_create("CREATE TABLE t (count BIGINT DEFAULT 42)");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::Int(42)
        );
    }

    #[test]
    fn extracts_negative_integer_default() {
        let stmt = parse_create("CREATE TABLE t (count BIGINT DEFAULT -7)");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::Int(-7)
        );
    }

    #[test]
    fn extracts_boolean_default() {
        let stmt = parse_create("CREATE TABLE t (flag BOOLEAN DEFAULT TRUE)");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::Bool(true)
        );
    }

    #[test]
    fn extracts_cast_default() {
        let stmt = parse_create("CREATE TABLE t (x INT DEFAULT CAST(1 AS INT))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::Int(1)
        );
    }

    #[test]
    fn rejects_function_default() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        let err = extract_default_literal(default).unwrap_err();
        assert!(
            err.message.contains("current_timestamp"),
            "error should name the rejected function, got: {}",
            err.message
        );
        assert!(
            err.message.contains("Accepted forms"),
            "error should list accepted forms, got: {}",
            err.message
        );
    }

    #[test]
    fn null_default_is_accepted() {
        let stmt = parse_create("CREATE TABLE t (x STRING DEFAULT NULL)");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        let default = col
            .options
            .iter()
            .find_map(|o| match &o.option {
                sqlparser::ast::ColumnOption::Default(e) => Some(e),
                _ => None,
            })
            .expect("DEFAULT parsed");
        assert_eq!(
            extract_default_literal(default).unwrap(),
            DefaultLiteral::Null
        );
    }
}
