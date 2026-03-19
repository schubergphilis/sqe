//! File-driven SQL compatibility tests.
//! Each `.sql` file under `tests/sql/` contains test blocks in rgsql-inspired format:
//!
//! ```text
//! --- test_name
//! SQL statement here (single statement, may span lines);
//! --- expect
//! col1 | col2
//! val1 | val2
//! ```
//!
//! Blocks start with `--- name` lines.  The `--- expect` pseudo-name marks the
//! expected-output section for the preceding SQL block.  Values are compared
//! using the same `fmt_val` formatting used by the integration tests (NULL for
//! nulls, 2 decimal places for floats, etc.).
//!
//! Run with: ./scripts/integration-test.sh

mod common;

use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SqlBlock {
    name: String,
    sql: String,
    /// Expected rows: each row is a vec of column strings.
    /// If None, only the row count (derived from `expected_rows`) is checked.
    expected: Option<Vec<Vec<String>>>,
    /// Row count check (number of data rows in `--- expect` section, excluding header).
    expected_rows: usize,
}

fn parse_sql_file(content: &str) -> Vec<SqlBlock> {
    let mut blocks: Vec<SqlBlock> = Vec::new();

    // State machine
    let mut current_name: Option<String> = None;
    let mut current_sql_lines: Vec<String> = Vec::new();
    let mut in_expect = false;
    let mut expect_header: Option<Vec<String>> = None;
    let mut expect_rows: Vec<Vec<String>> = Vec::new();

    let finalize = |blocks: &mut Vec<SqlBlock>,
                    name: Option<String>,
                    sql_lines: &mut Vec<String>,
                    expect_header: &mut Option<Vec<String>>,
                    expect_rows: &mut Vec<Vec<String>>| {
        let name = match name {
            Some(n) => n,
            None => return,
        };
        let sql = sql_lines.join("\n").trim().to_string();
        if sql.is_empty() {
            return;
        }
        let row_count = expect_rows.len();
        let expected = if expect_header.is_some() || !expect_rows.is_empty() {
            let mut all: Vec<Vec<String>> = Vec::new();
            if let Some(h) = expect_header.take() {
                all.push(h);
            }
            all.extend(std::mem::take(expect_rows));
            Some(all)
        } else {
            *expect_header = None;
            None
        };
        blocks.push(SqlBlock {
            name,
            sql,
            expected,
            expected_rows: row_count,
        });
        sql_lines.clear();
    };

    for raw_line in content.lines() {
        let line = raw_line.trim_end();

        if let Some(rest) = line.strip_prefix("---") {
            let tag = rest.trim();

            if tag.eq_ignore_ascii_case("expect") {
                // Start of expected output for the current block
                in_expect = true;
                expect_header = None;
                expect_rows.clear();
            } else {
                // New test block — finalize the previous one
                finalize(
                    &mut blocks,
                    current_name.take(),
                    &mut current_sql_lines,
                    &mut expect_header,
                    &mut expect_rows,
                );
                in_expect = false;
                current_name = Some(tag.to_string());
            }
            continue;
        }

        // Skip blank lines and comments outside of expect sections
        if in_expect {
            if line.is_empty() {
                continue;
            }
            let cols: Vec<String> = line.split('|').map(|s| s.trim().to_string()).collect();
            if expect_header.is_none() {
                // First non-blank line in expect section is the header
                expect_header = Some(cols);
            } else {
                expect_rows.push(cols);
            }
        } else if current_name.is_some() {
            current_sql_lines.push(line.to_string());
        }
    }

    // Finalize last block
    finalize(
        &mut blocks,
        current_name,
        &mut current_sql_lines,
        &mut expect_header,
        &mut expect_rows,
    );

    blocks
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

fn sql_dir() -> PathBuf {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    // CARGO_MANIFEST_DIR = crates/sqe-coordinator
    // SQL files live in crates/sqe-coordinator/tests/sql/
    PathBuf::from(manifest_dir).join("tests").join("sql")
}

async fn run_sql_file(filename: &str) {
    let path = sql_dir().join(filename);
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Cannot read {}: {e}", path.display()));

    let blocks = parse_sql_file(&content);
    assert!(!blocks.is_empty(), "No test blocks found in {filename}");

    let (session, handler) = common::setup_handler().await;
    let mut failed = 0usize;

    for block in &blocks {
        let result = handler.execute(&session, &block.sql).await;

        match result {
            Err(e) => {
                eprintln!("[FAIL] {filename}::{} — query error: {e}", block.name);
                eprintln!("  SQL: {}", block.sql);
                failed += 1;
            }
            Ok(batches) => {
                common::print_results(&format!("{filename}::{}", block.name), &block.sql, &batches);

                let actual_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                // Check row count
                if block.expected_rows > 0 && actual_rows != block.expected_rows {
                    eprintln!(
                        "[FAIL] {filename}::{} — expected {} rows, got {}",
                        block.name, block.expected_rows, actual_rows
                    );
                    failed += 1;
                    continue;
                }

                // Check values if expected rows were provided
                if let Some(expected) = &block.expected {
                    // expected[0] is the header (column names), expected[1..] are data rows
                    let data_rows = if expected.len() > 1 { &expected[1..] } else { &[] };

                    if actual_rows != data_rows.len() {
                        eprintln!(
                            "[FAIL] {filename}::{} — expected {} data rows, got {}",
                            block.name,
                            data_rows.len(),
                            actual_rows
                        );
                        failed += 1;
                        continue;
                    }

                    let mut row_idx = 0usize;
                    'outer: for batch in &batches {
                        for batch_row in 0..batch.num_rows() {
                            if row_idx >= data_rows.len() {
                                break 'outer;
                            }
                            let expected_cols = &data_rows[row_idx];
                            let actual_cols: Vec<String> = batch
                                .columns()
                                .iter()
                                .map(|c| common::fmt_val(c.as_ref(), batch_row))
                                .collect();

                            if actual_cols != *expected_cols {
                                eprintln!(
                                    "[FAIL] {filename}::{} row {} — expected {:?}, got {:?}",
                                    block.name,
                                    row_idx,
                                    expected_cols,
                                    actual_cols
                                );
                                failed += 1;
                            }
                            row_idx += 1;
                        }
                    }
                }
            }
        }
    }

    assert_eq!(
        failed,
        0,
        "{failed} test block(s) failed in {filename} — see stderr for details"
    );
}

// ---------------------------------------------------------------------------
// One test function per SQL file
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires: ./scripts/integration-test.sh
async fn test_sql_compat_01_basic_select() {
    run_sql_file("01_basic_select.sql").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_sql_compat_02_null_handling() {
    run_sql_file("02_null_handling.sql").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_sql_compat_03_cte_queries() {
    run_sql_file("03_cte_queries.sql").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_sql_compat_04_string_functions() {
    run_sql_file("04_string_functions.sql").await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_sql_compat_05_aggregations() {
    run_sql_file("05_aggregations.sql").await;
}
