//! `cargo xtask test-groups`: the `#[ignore]` vocabulary (finding 807).
//!
//! 278 tests in this workspace are `#[ignore]`d because they need a stack that
//! plain `cargo test` does not have: Polaris + RustFS, the two-worker overlay,
//! Ranger + Keycloak, Spark, Postgres, AWS credentials. Before this command the
//! reason string was the only record of which stack, 166 of them had none, and
//! the suites could only be run all-or-nothing with `--ignored`.
//!
//! The rule this enforces: every `#[ignore]` carries a reason whose first
//! token names its group. `needs:<stack>` for environment, `bug:#<n>` for a
//! known defect with an issue, `bench` for informational benchmarks, `deferred`
//! for stretch assertions, `release-only` for tests that only make sense in an
//! optimised build. Free text may follow after `;`.
//!
//! ```text
//! #[ignore = "needs:polaris"]
//! #[ignore = "needs:ranger; run scripts/access-control-test.sh"]
//! #[ignore = "bug:#630; `)` inside a string literal defeats split_top_level_as"]
//! ```
//!
//! Subcommands of one command:
//!   cargo xtask test-groups                 table of groups with counts
//!   cargo xtask test-groups --check         non-zero exit on a bare or unknown reason (CI)
//!   cargo xtask test-groups --names polaris test names in a group, one per line
//!                                            (what scripts/integration-test.sh passes
//!                                            to `cargo test -- --exact`)
//!   cargo xtask test-groups --json          machine-readable inventory

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::Serialize;

/// Stacks a test may declare. Adding one here is the whole procedure; the
/// checker rejects anything else so a typo cannot create a group of one.
pub const STACKS: &[(&str, &str)] = &[
    ("polaris", "docker-compose.test.yml: Polaris (in-memory) + RustFS; `make test-integration`"),
    ("polaris+workers", "test.yml + docker-compose.distributed.yml (two `sqe-server --mode worker` processes); `make test-distributed`"),
    ("ranger", "quickstart/polaris-ranger-keycloak; `scripts/access-control-test.sh`"),
    ("ranger+spark", "quickstart/polaris-ranger-keycloak plus Spark; `scripts/spark-access-control-test.sh`"),
    ("spark", "docker-compose.spark.yml + iceberg-spark-runtime"),
    ("trino", "docker-compose.trino.yml + trino-iceberg-connector"),
    ("postgres", "docker-compose.test.yml `postgres` service (JDBC catalog)"),
    ("hms", "docker-compose HMS (Hive Metastore Thrift) stack"),
    ("nessie", "docker-compose Nessie stack"),
    ("unity", "Unity Catalog, docker-compose stack or M2M credentials"),
    ("minio", "MinIO-backed warehouse, SQE_TEST_MINIO_URL"),
    ("aws", "live AWS credentials: Glue catalog, S3 bucket or S3 Tables"),
];

/// Non-environment reasons. `bug:#<n>` is matched by prefix.
pub const OTHER_TOKENS: &[(&str, &str)] = &[
    ("bug:#", "known defect, issue number follows; enable when fixed"),
    ("bench", "informational benchmark, not a gate"),
    ("deferred", "stretch assertion kept for later"),
    ("release-only", "only meaningful in an optimised build"),
];

#[derive(Parser)]
pub struct Args {
    /// Exit non-zero on a bare `#[ignore]` or a reason outside the vocabulary.
    #[arg(long)]
    check: bool,
    /// Print the test function names in one group (e.g. `polaris`), one per line.
    #[arg(long, value_name = "GROUP")]
    names: Option<String>,
    /// Print the inventory as JSON.
    #[arg(long)]
    json: bool,
    /// Workspace root (defaults to the current directory).
    #[arg(long, default_value = ".")]
    root: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct Entry {
    pub file: String,
    pub line: usize,
    /// The function name.
    pub test: String,
    /// What libtest calls it: `module::fn` for a file that is a module of the
    /// `tests/it/main.rs` harness, the bare function name for a standalone
    /// test binary. This is what `--names` prints and what `--exact` matches.
    pub name: String,
    pub group: String,
    pub reason: String,
}

#[derive(Debug, Default, Serialize)]
pub struct Inventory {
    pub entries: Vec<Entry>,
    pub problems: Vec<String>,
}

pub fn run(args: Args) -> Result<()> {
    let inv = scan(&args.root)?;

    if let Some(group) = &args.names {
        if !STACKS.iter().any(|(s, _)| s == group) && group != "bench" && group != "deferred" {
            bail!("unknown group `{group}`; known: {}", known_groups().join(", "));
        }
        for e in inv.entries.iter().filter(|e| &e.group == group) {
            println!("{}", e.name);
        }
        return finish(&inv, args.check);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&inv)?);
        return finish(&inv, args.check);
    }

    let mut by_group: BTreeMap<&str, Vec<&Entry>> = BTreeMap::new();
    for e in &inv.entries {
        by_group.entry(e.group.as_str()).or_default().push(e);
    }
    println!("{:<16} {:>5}  how to run", "group", "tests");
    for (group, entries) in &by_group {
        let how = STACKS
            .iter()
            .find(|(k, _)| k == group)
            .or_else(|| OTHER_TOKENS.iter().find(|(k, _)| k.trim_end_matches(":#") == *group))
            .map(|(_, d)| *d)
            .unwrap_or("");
        println!("{:<16} {:>5}  {}", group, entries.len(), how);
    }
    println!("{:<16} {:>5}", "total", inv.entries.len());
    finish(&inv, args.check)
}

fn finish(inv: &Inventory, check: bool) -> Result<()> {
    if inv.problems.is_empty() {
        return Ok(());
    }
    eprintln!();
    eprintln!("{} `#[ignore]` attribute(s) outside the vocabulary:", inv.problems.len());
    for p in &inv.problems {
        eprintln!("  {p}");
    }
    eprintln!(
        "\nEvery #[ignore] needs a reason whose first token is one of: {}.\n\
         Free text may follow after `;`. See xtask/src/test_groups.rs.",
        known_groups().join(", ")
    );
    if check {
        bail!("test-groups --check failed");
    }
    Ok(())
}

fn known_groups() -> Vec<String> {
    STACKS
        .iter()
        .map(|(s, _)| format!("needs:{s}"))
        .chain(OTHER_TOKENS.iter().map(|(t, _)| t.to_string()))
        .collect()
}

/// Walk `crates/*/{src,tests}` and `xtask/src` for `#[ignore ...]` attributes,
/// including the `#[cfg_attr(<pred>, ignore ...)]` form.
pub fn scan(root: &Path) -> Result<Inventory> {
    let mut inv = Inventory::default();
    let mut files = Vec::new();
    for crate_dir in read_dir_sorted(&root.join("crates"))? {
        for sub in ["src", "tests"] {
            collect_rs(&crate_dir.join(sub), &mut files)?;
        }
    }
    collect_rs(&root.join("xtask").join("src"), &mut files)?;

    for file in files {
        let text = fs::read_to_string(&file).with_context(|| format!("read {}", file.display()))?;
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .to_string_lossy()
            .into_owned();
        let lines: Vec<&str> = text.lines().collect();
        for (idx, line) in lines.iter().enumerate() {
            if !line.trim_start().starts_with("#[") || !line.contains("ignore") {
                continue;
            }
            // A reason string may continue onto following lines (a `\` line
            // break inside the literal); join until the attribute closes.
            let mut joined = line.to_string();
            let mut extra = 0;
            while !joined.contains(']') && extra < 6 && idx + 1 + extra < lines.len() {
                joined.push(' ');
                joined.push_str(lines[idx + 1 + extra].trim());
                extra += 1;
            }
            let Some(attr) = parse_ignore_attr(&joined) else { continue };
            let test = lines[idx + 1 + extra..]
                .iter()
                .take(8)
                .find_map(|l| fn_name(l))
                .unwrap_or_default()
                .to_string();
            let where_ = format!("{rel}:{}", idx + 1);
            match attr {
                None => inv.problems.push(format!("{where_}: bare #[ignore] ({test})")),
                Some(reason) => match classify(&reason) {
                    Some(group) => inv.entries.push(Entry {
                        file: rel.clone(),
                        line: idx + 1,
                        name: libtest_name(&file, &test),
                        test,
                        group,
                        reason,
                    }),
                    None => inv
                        .problems
                        .push(format!("{where_}: reason `{reason}` starts with no known token ({test})")),
                },
            }
        }
    }
    Ok(inv)
}

/// `Some(None)` for a bare ignore, `Some(Some(reason))` for one with a reason,
/// `None` when the line is not an ignore attribute at all. Only attribute
/// lines count; `#[ignore]` inside a comment or a string is not one. A
/// trailing `// comment` after the closing bracket is allowed.
fn parse_ignore_attr(line: &str) -> Option<Option<String>> {
    let t = line.trim_start();
    if !t.starts_with("#[") {
        return None;
    }
    let body = t.strip_prefix("#[")?;
    // The closing bracket is the last `]` before any trailing `//` comment;
    // a `]` inside the reason string is not one, so find the `"]` pair first.
    let end = match body.find("\"]") {
        Some(q) => q + 1,
        None => body.find(']')?,
    };
    let inner = &body[..end];
    // #[cfg_attr(pred, ignore ...)] -> "ignore ..."
    let inner = if let Some(rest) = inner.strip_prefix("cfg_attr(") {
        let rest = rest.strip_suffix(')')?;
        let (_, after_comma) = rest.split_once(',')?;
        after_comma.trim()
    } else {
        inner.trim()
    };
    if inner == "ignore" {
        return Some(None);
    }
    let rest = inner.strip_prefix("ignore")?.trim_start();
    let rest = rest.strip_prefix('=')?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.rfind('"')?;
    Some(Some(rest[..end].to_string()))
}

/// The group a reason belongs to: the text before the first `;`, which must be
/// `needs:<stack>` for a known stack or one of the other tokens.
fn classify(reason: &str) -> Option<String> {
    let head = reason.split(';').next().unwrap_or("").trim();
    if let Some(stack) = head.strip_prefix("needs:") {
        return STACKS
            .iter()
            .find(|(s, _)| *s == stack)
            .map(|(s, _)| s.to_string());
    }
    if head.starts_with("bug:#") && head["bug:#".len()..].chars().all(|c| c.is_ascii_digit()) && head.len() > 5 {
        return Some("bug".to_string());
    }
    OTHER_TOKENS
        .iter()
        .skip(1)
        .find(|(t, _)| *t == head)
        .map(|(t, _)| t.to_string())
}

/// `module::fn` when `file` is a module of a `tests/<harness>/main.rs`
/// binary (the coordinator's `tests/it/`), otherwise the bare function name.
/// Unit tests inside `src/` are not run through the groups, so their nested
/// module paths are not modelled.
fn libtest_name(file: &Path, test: &str) -> String {
    let parent = file.parent();
    let is_harness_module = parent
        .and_then(|p| p.parent())
        .is_some_and(|pp| pp.file_name().is_some_and(|n| n == "tests"))
        && parent.is_some_and(|p| p.join("main.rs").is_file())
        && file.file_name().is_some_and(|n| n != "main.rs");
    match (is_harness_module, file.file_stem().and_then(|s| s.to_str())) {
        (true, Some(stem)) => format!("{stem}::{test}"),
        _ => test.to_string(),
    }
}

fn fn_name(line: &str) -> Option<&str> {
    let t = line.trim_start();
    let t = t.strip_prefix("pub ").unwrap_or(t);
    let t = t.strip_prefix("async ").unwrap_or(t);
    let rest = t.strip_prefix("fn ")?;
    let end = rest.find(|c: char| !(c.is_alphanumeric() || c == '_'))?;
    Some(&rest[..end])
}

fn read_dir_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    out.sort();
    Ok(out)
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            collect_rs(&p, out)?;
        } else if p.extension().is_some_and(|e| e == "rs") {
            out.push(p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_three_attribute_shapes() {
        assert_eq!(parse_ignore_attr("    #[ignore]"), Some(None));
        assert_eq!(
            parse_ignore_attr(r#"#[ignore = "needs:polaris; run with --nocapture"]"#),
            Some(Some("needs:polaris; run with --nocapture".to_string()))
        );
        assert_eq!(
            parse_ignore_attr(r#"#[cfg_attr(debug_assertions, ignore = "release-only")]"#),
            Some(Some("release-only".to_string()))
        );
        assert_eq!(parse_ignore_attr("#[cfg_attr(debug_assertions, ignore)]"), Some(None));
        assert_eq!(parse_ignore_attr("//! was an `#[ignore]` test"), None);
        assert_eq!(parse_ignore_attr("#[tokio::test]"), None);
        assert_eq!(
            parse_ignore_attr(r#"#[ignore = "needs:polaris"] // Requires: docker compose up"#),
            Some(Some("needs:polaris".to_string()))
        );
        assert_eq!(parse_ignore_attr("#[ignore] // stack"), Some(None));
    }

    #[test]
    fn classifies_by_leading_token_only() {
        assert_eq!(classify("needs:polaris"), Some("polaris".into()));
        assert_eq!(classify("needs:ranger+spark; run scripts/x.sh"), Some("ranger+spark".into()));
        assert_eq!(classify("bug:#630; parser"), Some("bug".into()));
        assert_eq!(classify("bench; informational"), Some("bench".into()));
        assert_eq!(classify("needs:mars"), None);
        assert_eq!(classify("needs docker-compose.test.yml + Polaris"), None);
        assert_eq!(classify("bug:#"), None);
    }

    #[test]
    fn finds_the_test_name_after_the_attribute() {
        assert_eq!(fn_name("async fn polaris_auto_lazy_hit() {"), Some("polaris_auto_lazy_hit"));
        assert_eq!(fn_name("    pub fn x_1(a: u8)"), Some("x_1"));
        assert_eq!(fn_name("    #[tokio::test]"), None);
    }
}
