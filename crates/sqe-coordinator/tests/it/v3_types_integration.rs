//! Integration-level coverage for V3 types (nanosec timestamps + DEFAULTs).
//!
//! These tests exercise the public SQL-to-Iceberg-schema wiring without
//! requiring a running Polaris catalog. Round-trip INSERT/SELECT and
//! ALTER TABLE retroactive fill scenarios sit under `scripts/integration-test.sh`
//! (requires the quickstart stack) and are covered by the spec scenarios.

use iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, Type};
use sqe_sql::{extract_default_literal, is_v3_only_type, DefaultLiteral};
use sqlparser::ast::{ColumnOption, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn parse(sql: &str) -> sqlparser::ast::CreateTable {
    let stmt = Parser::parse_sql(&GenericDialect {}, sql)
        .expect("sql parses")
        .into_iter()
        .next()
        .expect("one statement");
    match stmt {
        Statement::CreateTable(ct) => ct,
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

#[test]
fn nanosec_timestamp_round_trip_through_parser_to_iceberg_type() {
    // CREATE TABLE with TIMESTAMP_NS(9) must produce an Iceberg TimestampNs.
    let ct = parse("CREATE TABLE events (ts TIMESTAMP_NS(9))");
    assert!(is_v3_only_type(&ct.columns[0].data_type));

    // Route through the write_handler conversion (re-exported for tests).
    let arrow_type =
        sqe_coordinator::__test_support::sql_type_to_arrow_public(&ct.columns[0].data_type)
            .unwrap();
    let iceberg_type = iceberg::arrow::arrow_type_to_type(&arrow_type).unwrap();
    assert!(matches!(
        iceberg_type,
        Type::Primitive(PrimitiveType::TimestampNs)
    ));
}

#[test]
fn timestamptz_ns_round_trip_through_parser_to_iceberg_type() {
    let ct = parse("CREATE TABLE events (utcts TIMESTAMPTZ_NS(9))");
    assert!(is_v3_only_type(&ct.columns[0].data_type));
    let arrow_type =
        sqe_coordinator::__test_support::sql_type_to_arrow_public(&ct.columns[0].data_type)
            .unwrap();
    let iceberg_type = iceberg::arrow::arrow_type_to_type(&arrow_type).unwrap();
    assert!(matches!(
        iceberg_type,
        Type::Primitive(PrimitiveType::TimestamptzNs)
    ));
}

#[test]
fn default_string_lands_on_nestedfield() {
    let ct = parse("CREATE TABLE orders (id BIGINT, status STRING DEFAULT 'pending')");
    let schema = sqe_coordinator::__test_support::build_iceberg_schema_with_defaults(&ct)
        .expect("schema builds");
    let fields: Vec<_> = schema.as_struct().fields().to_vec();
    let status = fields.iter().find(|f| f.name == "status").unwrap();
    assert!(
        matches!(
            status.write_default.as_ref(),
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
        ),
        "status.write_default = {:?}",
        status.write_default
    );
    assert!(
        matches!(
            status.initial_default.as_ref(),
            Some(Literal::Primitive(PrimitiveLiteral::String(s))) if s == "pending"
        ),
        "status.initial_default = {:?}",
        status.initial_default
    );
}

#[test]
fn format_version_gates_to_v3_only_when_needed() {
    // V2-only table: no nanosec, no defaults.
    let v2 = parse("CREATE TABLE t (id BIGINT, name STRING)");
    assert!(!sqe_coordinator::__test_support::needs_v3(&v2).unwrap());

    // Nanosec timestamp: V3.
    let v3_ns = parse("CREATE TABLE t (ts TIMESTAMP_NS(9))");
    assert!(sqe_coordinator::__test_support::needs_v3(&v3_ns).unwrap());

    // DEFAULT literal: V3.
    let v3_default = parse("CREATE TABLE t (id BIGINT, status STRING DEFAULT 'pending')");
    assert!(sqe_coordinator::__test_support::needs_v3(&v3_default).unwrap());
}

#[test]
fn alter_table_add_column_default_extraction() {
    // Parser-level confirmation that ALTER TABLE carries the DEFAULT
    // through to the coordinator.
    let stmt = Parser::parse_sql(
        &GenericDialect {},
        "ALTER TABLE orders ADD COLUMN region STRING DEFAULT 'unknown'",
    )
    .unwrap()
    .remove(0);
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected AlterTable");
    };
    let op = &alter.operations[0];
    let sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } = op else {
        panic!("expected AddColumn");
    };
    let expr = column_def
        .options
        .iter()
        .find_map(|o| match &o.option {
            ColumnOption::Default(e) => Some(e),
            _ => None,
        })
        .expect("DEFAULT present");
    assert_eq!(
        extract_default_literal(expr).unwrap(),
        DefaultLiteral::String("unknown".to_string())
    );
}

#[test]
fn unsupported_default_surfaces_error_through_coordinator() {
    let ct = parse("CREATE TABLE t (ts TIMESTAMP DEFAULT current_timestamp())");
    let err = sqe_coordinator::__test_support::build_iceberg_schema_with_defaults(&ct)
        .expect_err("function default must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("current_timestamp"),
        "error should name rejected function: {msg}"
    );
    assert!(
        msg.contains("Accepted forms"),
        "error should list accepted forms: {msg}"
    );
}
