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

/// True when a sqlparser `DataType` triggers format-version 3.
///
/// Only nanosecond timestamps are V3-only in the current SQL surface.
/// `Unknown` and variant types are handled at a different layer.
pub fn is_v3_only_type(sql_type: &SqlType) -> bool {
    detect_ns_timestamp(sql_type).is_some()
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
/// Accepts integer, float, string, boolean, typed cast of the same, and
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
        Value::Boolean(b) => Ok(DefaultLiteral::Bool(*b)),
        Value::Null => Ok(DefaultLiteral::Null),
        _ => Err(reject(&format!("literal kind `{value}` is not supported"))),
    }
}

fn reject(detail: &str) -> DefaultError {
    DefaultError {
        message: format!(
            "unsupported DEFAULT expression: {detail}. Accepted forms: integer, \
             float, string, boolean, NULL, or a typed cast of the same"
        ),
    }
}

/// True when the SQL data type represents a UTC-tagged timestamp.
///
/// Helpful callers that need to distinguish `TimezoneInfo::Tz`
/// (TIMESTAMPTZ shorthand) from `TimezoneInfo::None`.
pub fn is_tz_variant(info: &TimezoneInfo) -> bool {
    matches!(
        info,
        TimezoneInfo::Tz | TimezoneInfo::WithTimeZone
    )
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
        assert_eq!(detect_ns_timestamp(&col.data_type), Some(NsTimestamp::WithoutTz));
    }

    #[test]
    fn detects_timestamptz_ns() {
        let stmt = parse_create("CREATE TABLE t (ts TIMESTAMPTZ_NS(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(detect_ns_timestamp(&col.data_type), Some(NsTimestamp::WithTz));
    }

    #[test]
    fn ns_detection_is_case_insensitive() {
        let stmt = parse_create("CREATE TABLE t (ts timestamp_ns(9))");
        let sqlparser::ast::Statement::CreateTable(ct) = stmt else {
            panic!("expected CreateTable");
        };
        let col = &ct.columns[0];
        assert_eq!(detect_ns_timestamp(&col.data_type), Some(NsTimestamp::WithoutTz));
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
        assert_eq!(extract_default_literal(default).unwrap(), DefaultLiteral::Int(42));
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
        assert_eq!(extract_default_literal(default).unwrap(), DefaultLiteral::Int(-7));
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
        assert_eq!(extract_default_literal(default).unwrap(), DefaultLiteral::Bool(true));
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
        assert_eq!(extract_default_literal(default).unwrap(), DefaultLiteral::Int(1));
    }

    #[test]
    fn rejects_function_default() {
        let stmt =
            parse_create("CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())");
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
        assert_eq!(extract_default_literal(default).unwrap(), DefaultLiteral::Null);
    }
}
