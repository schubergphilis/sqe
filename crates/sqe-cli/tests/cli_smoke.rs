//! Binary-level smoke tests for the `sqe-cli` executable.
//!
//! These exercise the actual compiled binary via `assert_cmd`. The library
//! tests in `src/` cover module behaviour; this file pins flag parsing,
//! exit codes, stderr formatting, mutually-exclusive flag rejection, and
//! the happy path of `--embedded --memory -e <sql>`.

use assert_cmd::Command;
use predicates::prelude::*;

fn bin() -> Command {
    Command::cargo_bin("sqe-cli").expect("sqe-cli binary builds")
}

#[test]
fn help_flag_exits_zero_and_prints_usage() {
    bin()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("sqe-cli"))
        .stdout(predicate::str::contains("Usage:"));
}

#[test]
fn version_flag_exits_zero_and_prints_version() {
    bin()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("sqe-cli"));
}

#[test]
fn unknown_flag_exits_two() {
    // Clap returns exit code 2 for argument parsing errors.
    bin()
        .arg("--definitely-not-a-flag")
        .assert()
        .code(2)
        .stderr(
            predicate::str::contains("unexpected argument").or(predicate::str::contains("error:")),
        );
}

#[test]
fn invalid_protocol_value_exits_two() {
    bin()
        .args(["--protocol", "carrier-pigeon"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid value").or(predicate::str::contains("error:")));
}

#[test]
fn catalog_spec_missing_equals_rejected() {
    // No `=` separator: `parse_catalog_spec` returns Err, clap surfaces it.
    bin()
        .args(["--embedded", "--catalog", "noequals"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("expected NAME=PATH"));
}

#[test]
fn catalog_spec_empty_name_rejected() {
    bin()
        .args(["--embedded", "--catalog", "=/tmp/x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("catalog name is empty"));
}

#[test]
fn catalog_spec_with_dot_in_name_rejected() {
    bin()
        .args(["--embedded", "--catalog", "prod.eu=/tmp/x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot contain"));
}

#[test]
fn memory_and_warehouse_are_mutually_exclusive() {
    // Clap enforces `conflicts_with_all` and returns exit code 2.
    bin()
        .args(["--embedded", "--memory", "--warehouse", "/tmp/sqe-wh"])
        .assert()
        .code(2)
        .stderr(
            predicate::str::contains("cannot be used with").or(predicate::str::contains("error:")),
        );
}

#[test]
fn embedded_memory_runs_select_one() {
    // Happy path: in-memory embedded engine runs `SELECT 1` end-to-end and
    // exits zero. The banner goes to stderr; the table goes to stdout.
    bin()
        .args(["--embedded", "--memory", "-e", "SELECT 1 AS one"])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success()
        .stderr(predicate::str::contains("embedded engine"))
        .stdout(predicate::str::contains("one").and(predicate::str::contains("1")));
}

#[test]
fn embedded_memory_syntax_error_exits_nonzero() {
    // Bad SQL must propagate a non-zero exit from main.
    bin()
        .args(["--embedded", "--memory", "-e", "SELEC"])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure();
}

#[test]
fn embedded_warehouse_delete_update_merge() {
    // DELETE, UPDATE and MERGE run through the server's write handlers and
    // commit to the warehouse: a second process reads the result back.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("dml.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, v VARCHAR);\n\
         INSERT INTO iceberg.s.t VALUES (1, 'a'), (2, 'b'), (3, 'c');\n\
         DELETE FROM iceberg.s.t WHERE id = 1;\n\
         UPDATE iceberg.s.t SET v = 'z' WHERE id = 2;\n\
         MERGE INTO iceberg.s.t t USING (SELECT * FROM (VALUES (3, 'm'), (4, 'n')) AS s(id, v)) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT string_agg(CAST(id AS VARCHAR) || v, ',' ORDER BY id) AS rows FROM iceberg.s.t",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("2z,3m,4n"), "unexpected rows: {stdout}");
}
