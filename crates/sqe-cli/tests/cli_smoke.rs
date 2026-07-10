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
        .stderr(predicate::str::contains("unexpected argument").or(
            predicate::str::contains("error:"),
        ));
}

#[test]
fn invalid_protocol_value_exits_two() {
    bin()
        .args(["--protocol", "carrier-pigeon"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid value").or(
            predicate::str::contains("error:"),
        ));
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
        .stderr(predicate::str::contains("cannot be used with").or(
            predicate::str::contains("error:"),
        ));
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
