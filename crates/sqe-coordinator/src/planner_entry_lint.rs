//! Lint: every planner entry point prepares the file table functions.
//!
//! `read_parquet` / `read_csv` / `read_json` build their providers ahead of
//! planning, per statement (`sqe_catalog::file_tvf_prepare`). A path that
//! calls `ctx.sql` or `create_logical_plan` directly plans without them and
//! fails with "the table function was not prepared before planning", and it
//! did, intermittently, for CTAS under parallel dbt threads. This scans the
//! non-test source of every crate that plans SQL and fails on a direct call.

use std::path::{Path, PathBuf};

/// Crates whose production code plans SQL.
const CRATES: [&str; 7] = [
    "sqe-coordinator",
    "sqe-write",
    "sqe-cli",
    "sqe-server",
    "sqe-catalog",
    "sqe-mcp",
    "sqe-trino-compat",
];

/// Where the direct calls belong: the wrappers themselves.
const ALLOWED: [&str; 1] = ["sqe-catalog/src/file_tvf_prepare.rs"];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Direct planner calls in `source` outside its test module: `.sql(<args>)`
/// or `.create_logical_plan(<args>)` with a non-empty argument list.
fn direct_planner_calls(source: &str) -> Vec<(usize, String)> {
    let production = source.split("#[cfg(test)]").next().unwrap_or(source);
    let mut found = Vec::new();
    for (index, line) in production.lines().enumerate() {
        let code = line.trim_start();
        if code.starts_with("//") {
            continue;
        }
        for needle in [".sql(", ".create_logical_plan("] {
            let mut from = 0;
            while let Some(at) = code[from..].find(needle) {
                let after = &code[from + at + needle.len()..];
                if !after.trim_start().starts_with(')') {
                    found.push((index + 1, code.to_string()));
                }
                from += at + needle.len();
            }
        }
    }
    found
}

#[test]
fn every_planner_entry_point_goes_through_plan_sql() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut offenders = Vec::new();
    for krate in CRATES {
        let mut files = Vec::new();
        rust_files(&crates_dir.join(krate).join("src"), &mut files);
        for file in files {
            let relative = file
                .strip_prefix(&crates_dir)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if ALLOWED.contains(&relative.as_str())
                || name.ends_with("_test.rs")
                || name == "tests.rs"
                || relative.contains("/tests/")
            {
                continue;
            }
            let source = std::fs::read_to_string(&file).expect("read source");
            for (line, code) in direct_planner_calls(&source) {
                offenders.push(format!("{relative}:{line}: {code}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "plan through sqe_catalog::file_tvf_prepare::plan_sql / create_logical_plan, \
         not ctx.sql / state.create_logical_plan directly:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_lint_sees_direct_calls_and_ignores_tests_and_comments() {
    let source = "fn a() { let df = ctx.sql(&q).await; }\n\
                  // ctx.sql(x) in a comment\n\
                  fn b() { normalized.sql() }\n\
                  fn c() { state\n    .create_logical_plan(sql) }\n\
                  #[cfg(test)]\nmod tests { fn t() { ctx.sql(\"SELECT 1\"); } }\n";
    let found = direct_planner_calls(source);
    assert_eq!(found.iter().map(|(l, _)| *l).collect::<Vec<_>>(), vec![1, 5]);
}
