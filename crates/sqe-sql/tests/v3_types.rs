//! Public-API integration tests for v3_types.
//!
//! Exercises the parser-level surface that CREATE TABLE and ALTER TABLE
//! ADD COLUMN consume. Keeps these tests out of the integration-test
//! harness that needs a running catalog.

use sqe_sql::{
    detect_ns_timestamp, extract_default_literal, is_v3_only_type, DefaultLiteral, NsTimestamp,
};
use sqlparser::ast::{ColumnOption, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql)
        .expect("sql parses")
        .into_iter()
        .next()
        .expect("one statement")
}

fn column_default<'a>(stmt: &'a Statement, col_name: &str) -> Option<&'a sqlparser::ast::Expr> {
    let Statement::CreateTable(ct) = stmt else {
        return None;
    };
    let col = ct.columns.iter().find(|c| c.name.value == col_name)?;
    col.options.iter().find_map(|o| match &o.option {
        ColumnOption::Default(e) => Some(e),
        _ => None,
    })
}

fn column_type<'a>(stmt: &'a Statement, col_name: &str) -> &'a sqlparser::ast::DataType {
    let Statement::CreateTable(ct) = stmt else {
        panic!("expected CreateTable");
    };
    let col = ct
        .columns
        .iter()
        .find(|c| c.name.value == col_name)
        .expect("column exists");
    &col.data_type
}

// ---------------------------------------------------------------------------
// Nanosecond timestamp parsing
// ---------------------------------------------------------------------------

#[test]
fn timestamp_ns_is_detected_in_create_table() {
    let stmt = parse("CREATE TABLE events (ts TIMESTAMP_NS(9))");
    let ty = column_type(&stmt, "ts");
    assert_eq!(detect_ns_timestamp(ty), Some(NsTimestamp::WithoutTz));
    assert!(is_v3_only_type(ty));
}

#[test]
fn timestamptz_ns_is_detected_in_create_table() {
    let stmt = parse("CREATE TABLE events (ts TIMESTAMPTZ_NS(9))");
    let ty = column_type(&stmt, "ts");
    assert_eq!(detect_ns_timestamp(ty), Some(NsTimestamp::WithTz));
    assert!(is_v3_only_type(ty));
}

#[test]
fn plain_timestamp_does_not_trigger_v3() {
    let stmt = parse("CREATE TABLE t (ts TIMESTAMP(6))");
    let ty = column_type(&stmt, "ts");
    assert_eq!(detect_ns_timestamp(ty), None);
    assert!(!is_v3_only_type(ty));
}

// ---------------------------------------------------------------------------
// DEFAULT literal extraction
// ---------------------------------------------------------------------------

#[test]
fn string_default_is_extracted() {
    let stmt = parse("CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')");
    let default = column_default(&stmt, "status").expect("DEFAULT present");
    assert_eq!(
        extract_default_literal(default).unwrap(),
        DefaultLiteral::String("pending".to_string())
    );
}

#[test]
fn integer_default_is_extracted() {
    let stmt = parse("CREATE TABLE t (count BIGINT DEFAULT 100)");
    let default = column_default(&stmt, "count").expect("DEFAULT present");
    assert_eq!(
        extract_default_literal(default).unwrap(),
        DefaultLiteral::Int(100)
    );
}

#[test]
fn boolean_default_is_extracted() {
    let stmt = parse("CREATE TABLE t (flag BOOLEAN DEFAULT FALSE)");
    let default = column_default(&stmt, "flag").expect("DEFAULT present");
    assert_eq!(
        extract_default_literal(default).unwrap(),
        DefaultLiteral::Bool(false)
    );
}

#[test]
fn function_default_is_rejected_with_clear_error() {
    let stmt = parse("CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())");
    let default = column_default(&stmt, "ts").expect("DEFAULT present");
    let err = extract_default_literal(default).unwrap_err();
    // Error must name the rejected function and list accepted forms.
    assert!(
        err.message.contains("current_timestamp"),
        "got: {}",
        err.message
    );
    assert!(
        err.message.contains("Accepted forms"),
        "got: {}",
        err.message
    );
    assert!(err.message.contains("integer"), "got: {}", err.message);
    assert!(err.message.contains("string"), "got: {}", err.message);
    assert!(err.message.contains("boolean"), "got: {}", err.message);
}

#[test]
fn alter_table_add_column_with_default_parses() {
    let stmt = parse("ALTER TABLE orders ADD COLUMN region STRING DEFAULT 'unknown'");
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected AlterTable");
    };
    let op = &alter.operations[0];
    let sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } = op else {
        panic!("expected AddColumn");
    };
    let default = column_def
        .options
        .iter()
        .find_map(|o| match &o.option {
            ColumnOption::Default(e) => Some(e),
            _ => None,
        })
        .expect("DEFAULT present");
    assert_eq!(
        extract_default_literal(default).unwrap(),
        DefaultLiteral::String("unknown".to_string())
    );
}
