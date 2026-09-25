//! The compare verdict logic, exercised from outside the crate (review 706:
//! `comparison::classify::classify_status` used to be unreachable from
//! `tests/`). Every arm of `classify_status` is pinned here.

use sqe_bench::comparison::classify::{canonical_rows, classify_status, is_dml, ExpectedRows};
use sqe_bench::report::CompareStatusReport;
use std::collections::HashMap;

fn ok() -> Option<String> {
    None
}
fn err() -> Option<String> {
    Some("boom".to_string())
}

#[test]
fn both_succeed_same_rows_is_match() {
    assert!(matches!(
        classify_status(&ok(), &ok(), 5, 5, None),
        CompareStatusReport::Match
    ));
    // canonical is irrelevant once rows are non-zero and equal
    assert!(matches!(
        classify_status(&ok(), &ok(), 5, 5, Some(0)),
        CompareStatusReport::Match
    ));
}

#[test]
fn both_empty_without_canonical_is_vacuous() {
    assert!(matches!(
        classify_status(&ok(), &ok(), 0, 0, None),
        CompareStatusReport::Vacuous
    ));
}

#[test]
fn both_empty_with_canonical_zero_is_expected_empty() {
    assert!(matches!(
        classify_status(&ok(), &ok(), 0, 0, Some(0)),
        CompareStatusReport::ExpectedEmpty
    ));
}

#[test]
fn both_empty_with_canonical_nonzero_is_vacuous_bug() {
    assert!(matches!(
        classify_status(&ok(), &ok(), 0, 0, Some(12)),
        CompareStatusReport::VacuousBug
    ));
}

#[test]
fn row_diff_where_sqe_matches_canonical_is_dialect_diff() {
    assert!(matches!(
        classify_status(&ok(), &ok(), 6, 1, Some(6)),
        CompareStatusReport::DialectDiff
    ));
}

#[test]
fn row_diff_otherwise_stays_row_diff() {
    // no canonical
    assert!(matches!(
        classify_status(&ok(), &ok(), 6, 1, None),
        CompareStatusReport::RowDiff
    ));
    // canonical present but SQE is also off
    assert!(matches!(
        classify_status(&ok(), &ok(), 5, 1, Some(6)),
        CompareStatusReport::RowDiff
    ));
    // Trino matches canonical, SQE does not: SQE is the outlier, not a pass
    assert!(matches!(
        classify_status(&ok(), &ok(), 1, 6, Some(6)),
        CompareStatusReport::RowDiff
    ));
}

#[test]
fn engine_failures_map_to_their_arms() {
    assert!(matches!(
        classify_status(&err(), &ok(), 0, 0, Some(0)),
        CompareStatusReport::SqeFailed
    ));
    assert!(matches!(
        classify_status(&ok(), &err(), 0, 0, Some(7)),
        CompareStatusReport::TrinoFailed
    ));
    assert!(matches!(
        classify_status(&err(), &err(), 0, 0, None),
        CompareStatusReport::BothFailed
    ));
    // A failure with a stray row count on the other side is still a failure.
    assert!(matches!(
        classify_status(&err(), &ok(), 0, 9, None),
        CompareStatusReport::SqeFailed
    ));
}

#[test]
fn canonical_rows_resolves_scale_keys() {
    let mut manifest: ExpectedRows = HashMap::new();
    let mut tpch = HashMap::new();
    let mut q01 = HashMap::new();
    q01.insert("sf1_official_rows".to_string(), 4i64);
    q01.insert("sf10_official_rows".to_string(), 4i64);
    tpch.insert("q01".to_string(), q01);
    manifest.insert("tpch".to_string(), tpch);

    assert_eq!(canonical_rows(Some(&manifest), "tpch", "q01", 1.0), Some(4));
    assert_eq!(
        canonical_rows(Some(&manifest), "tpch", "q01", 10.0),
        Some(4)
    );
    assert_eq!(canonical_rows(Some(&manifest), "tpch", "q01", 0.01), None);
    assert_eq!(canonical_rows(Some(&manifest), "tpch", "q02", 1.0), None);
    assert_eq!(canonical_rows(None, "tpch", "q01", 1.0), None);
}

#[test]
fn dml_detection_sees_through_header_comments() {
    assert!(is_dml("UPDATE t SET a = 1"));
    assert!(is_dml(
        "-- name: x\n-- timeout: 5s\nDELETE FROM t WHERE 1=1"
    ));
    assert!(is_dml("  insert into t values (1)"));
    assert!(is_dml(
        "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE"
    ));
    assert!(!is_dml("SELECT * FROM t"));
    assert!(!is_dml("-- UPDATE in a comment only\nSELECT 1"));
}
