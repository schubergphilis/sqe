//! `cargo xtask budgets`: size and structure budgets for the workspace.
//!
//! Build time and crate size here were brought down several times and grew
//! back each time, because nothing held the result: `sqe-coordinator` was 35K
//! lines in June 2026 and, counting `sqe-write` that was split out of it, 93K
//! in November; three source files passed 12K lines,
//! and a dependency edge that had been cut on purpose could return in any
//! merge request without anyone being asked. This command turns those numbers
//! into budgets that live in `.config/budgets.json` and are checked on every
//! merge request without compiling anything.
//!
//! What is budgeted:
//!
//! - lines per source file under `crates/*/src` (a default ceiling, plus one
//!   recorded ceiling per file that is already over it);
//! - lines per crate (`src/` only);
//! - the first-party dependencies of every crate. An edit to a crate rebuilds
//!   every crate that reaches it, so each new edge lengthens somebody's
//!   rebuild; the edges are an exact list;
//! - packages in `Cargo.lock`, and crate names locked at more than one version.
//!
//! The rule that makes it a ratchet: this tool only ever lowers a budget.
//! `--update` tightens ceilings to just above the current numbers and drops edges that no
//! longer exist. Raising a ceiling, adding an edge or adding a crate is a hand
//! edit of `.config/budgets.json`, so it shows up as its own line in review.
//! A line budget with too much slack fails the check as well, so a file that
//! shrinks cannot quietly grow back to its old size. The `Cargo.lock` budgets
//! are upper bounds only.
//!
//! These budgets bound size and coupling. They do not measure compile time:
//! that is dominated by trait-solving cost, which `scripts/compile-profile.sh`
//! measures per crate.
//!
//! ```text
//! cargo xtask budgets            report: every crate, files over the default
//! cargo xtask budgets --check    non-zero exit on any breach (CI, pre-push)
//! cargo xtask budgets --update   tighten budgets to just above the current numbers
//! cargo xtask budgets --json     the report as JSON
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};

const BUDGET_FILE: &str = ".config/budgets.json";

/// Ceilings are recorded on these steps: the next boundary above the actual
/// value plus one step of room. See [`ceiling`] and [`has_slack`].
const FILE_STEP: usize = 250;
const CRATE_STEP: usize = 1000;
const LOCK_STEP: usize = 25;
const DUPLICATE_STEP: usize = 5;

#[derive(Parser)]
pub struct Args {
    /// Exit non-zero when a budget is exceeded, missing or has slack.
    #[arg(long)]
    check: bool,
    /// Lower budgets to the current numbers. Never raises one.
    #[arg(long, conflicts_with = "check")]
    update: bool,
    /// Print the report as JSON.
    #[arg(long)]
    json: bool,
    /// Workspace root (defaults to the current directory).
    #[arg(long, default_value = ".")]
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budgets {
    /// Ceiling for any source file that has no entry in `files`.
    pub file_lines_default: usize,
    /// Files already over the default, with their own ceiling.
    pub files: BTreeMap<String, usize>,
    pub crates: BTreeMap<String, CrateBudget>,
    pub lock: LockBudget,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrateBudget {
    /// Ceiling for the lines under the crate's `src/`.
    pub lines: usize,
    /// The crate's first-party dependencies (normal and build), exactly.
    pub deps: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LockBudget {
    pub packages: usize,
    /// Crate names that `Cargo.lock` holds at more than one version.
    pub duplicate_names: usize,
}

/// What the tree looks like now.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Actual {
    /// Repo-relative path -> lines, for every `.rs` under `crates/*/src`.
    pub files: BTreeMap<String, usize>,
    pub crates: BTreeMap<String, ActualCrate>,
    pub lock_packages: usize,
    pub lock_duplicate_names: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ActualCrate {
    pub lines: usize,
    pub deps: BTreeSet<String>,
}

#[derive(Serialize)]
struct Report<'a> {
    actual: &'a Actual,
    budgets: &'a Budgets,
    /// Crate -> how many first-party crates rebuild when it changes.
    rebuilds: BTreeMap<String, usize>,
    problems: &'a [String],
}

pub fn run(args: Args) -> Result<()> {
    let path = args.root.join(BUDGET_FILE);
    let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut budgets: Budgets =
        serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    let actual = measure(&args.root)?;

    if args.update {
        let before = budgets.clone();
        tighten(&mut budgets, &actual);
        if budgets != before {
            fs::write(&path, serde_json::to_string_pretty(&budgets)? + "\n")?;
            println!("tightened {BUDGET_FILE}");
        }
    }

    let problems = evaluate(&budgets, &actual);
    if args.json {
        let report = Report {
            actual: &actual,
            budgets: &budgets,
            rebuilds: rebuild_fan_out(&actual),
            problems: &problems,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if !args.check {
        print_report(&budgets, &actual);
    } else if problems.is_empty() {
        println!(
            "==> budgets ok ({} crates, {} files over the default, {} locked packages)",
            actual.crates.len(),
            budgets.files.len(),
            actual.lock_packages
        );
    }

    if problems.is_empty() {
        return Ok(());
    }
    eprintln!();
    eprintln!("{} budget problem(s):", problems.len());
    for p in &problems {
        eprintln!("  {p}");
    }
    eprintln!(
        "\nBudgets live in {BUDGET_FILE}. `cargo xtask budgets --update` lowers them to just\n\
         above the current numbers and drops removed edges. Raising a ceiling or adding an edge or a\n\
         crate is a hand edit there: say in the merge request why the growth is worth it.\n\
         See xtask/src/budgets.rs."
    );
    if args.check || args.update {
        bail!("budgets check failed");
    }
    Ok(())
}

fn round_up(n: usize, step: usize) -> usize {
    n.div_ceil(step) * step
}

/// The ceiling `--update` records: the next step boundary plus one whole step,
/// so there is always between one and two steps of room. A ceiling that sat
/// right on the current number would fail on the next bug fix, and every
/// merge request touching that crate would then edit the same line.
fn ceiling(actual: usize, step: usize) -> usize {
    round_up(actual, step) + step
}

/// A file with its own ceiling gives the entry up once it is a whole step
/// under the default. A file that hovers around the default keeps its entry,
/// so a small cleanup does not fail the check and the next fix does not need
/// a hand edit to put the entry back.
fn clear_of_default(lines: usize, default: usize) -> bool {
    lines + FILE_STEP <= default
}

/// A ceiling is slack when it sits two steps or more above what `--update`
/// would record.
fn has_slack(actual: usize, limit: usize, step: usize) -> bool {
    limit >= ceiling(actual, step) + 2 * step
}

/// Every way the tree is outside its budgets, as one line each.
pub fn evaluate(b: &Budgets, a: &Actual) -> Vec<String> {
    let mut out = Vec::new();

    for (file, &lines) in &a.files {
        match b.files.get(file) {
            Some(&limit) if lines > limit => {
                out.push(format!(
                    "{file}: {lines} lines, over its ceiling of {limit}"
                ));
            }
            Some(_) => {}
            None if lines > b.file_lines_default => out.push(format!(
                "{file}: {lines} lines, over the {} default for a source file; split it",
                b.file_lines_default
            )),
            None => {}
        }
    }
    for (file, &limit) in &b.files {
        match a.files.get(file) {
            None => out.push(format!("{file}: has a ceiling but no longer exists; --update")),
            Some(&lines) if clear_of_default(lines, b.file_lines_default) => out.push(format!(
                "{file}: {lines} lines, back under the {} default; --update drops its entry",
                b.file_lines_default
            )),
            Some(&lines) if has_slack(lines, limit, FILE_STEP) => out.push(format!(
                "{file}: {lines} lines against a ceiling of {limit}; --update locks in the reduction"
            )),
            Some(_) => {}
        }
    }

    for (name, c) in &a.crates {
        let Some(cb) = b.crates.get(name) else {
            out.push(format!(
                "{name}: crate has no budget; add it with a line ceiling and its first-party deps"
            ));
            continue;
        };
        if c.lines > cb.lines {
            out.push(format!(
                "{name}: {} lines, over its ceiling of {}",
                c.lines, cb.lines
            ));
        } else if has_slack(c.lines, cb.lines, CRATE_STEP) {
            out.push(format!(
                "{name}: {} lines against a ceiling of {}; --update locks in the reduction",
                c.lines, cb.lines
            ));
        }
        for dep in c.deps.difference(&cb.deps) {
            out.push(format!(
                "{name} -> {dep}: new first-party dependency; an edit to {dep} now rebuilds {name} \
                 and everything above it"
            ));
        }
        for dep in cb.deps.difference(&c.deps) {
            out.push(format!(
                "{name} -> {dep}: edge is gone; --update drops it from the budget"
            ));
        }
    }
    for name in b.crates.keys().filter(|n| !a.crates.contains_key(*n)) {
        out.push(format!(
            "{name}: has a budget but is not a workspace crate; --update"
        ));
    }

    // Upper bound only. A dependency-update bot removes packages as often as
    // it adds them and cannot run `--update`, so slack here is not a failure.
    if a.lock_packages > b.lock.packages {
        out.push(format!(
            "{} packages in Cargo.lock, over the ceiling of {}",
            a.lock_packages, b.lock.packages
        ));
    }
    if a.lock_duplicate_names > b.lock.duplicate_names {
        out.push(format!(
            "{} crate names locked at more than one version, over the ceiling of {}",
            a.lock_duplicate_names, b.lock.duplicate_names
        ));
    }
    out
}

/// Lower every budget to just above the current numbers. Never raises a ceiling, adds an
/// edge or adds a crate: those stay failures until someone edits the file.
pub fn tighten(b: &mut Budgets, a: &Actual) {
    let default = b.file_lines_default;
    b.files.retain(|file, limit| match a.files.get(file) {
        Some(&lines) if !clear_of_default(lines, default) => {
            *limit = (*limit).min(ceiling(lines, FILE_STEP));
            true
        }
        _ => false,
    });
    b.crates.retain(|name, cb| match a.crates.get(name) {
        Some(c) => {
            cb.lines = cb.lines.min(ceiling(c.lines, CRATE_STEP));
            cb.deps.retain(|d| c.deps.contains(d));
            true
        }
        None => false,
    });
    b.lock.packages = b.lock.packages.min(ceiling(a.lock_packages, LOCK_STEP));
    b.lock.duplicate_names = b
        .lock
        .duplicate_names
        .min(ceiling(a.lock_duplicate_names, DUPLICATE_STEP));
}

/// For each crate, the number of first-party crates that depend on it directly
/// or through others: what an edit to it sends back through the compiler.
pub fn rebuild_fan_out(a: &Actual) -> BTreeMap<String, usize> {
    let mut reach: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for name in a.crates.keys() {
        let mut seen = BTreeSet::new();
        let mut stack = vec![name.as_str()];
        while let Some(n) = stack.pop() {
            for dep in a.crates.get(n).into_iter().flat_map(|c| &c.deps) {
                if seen.insert(dep.as_str()) {
                    stack.push(dep.as_str());
                }
            }
        }
        reach.insert(name, seen);
    }
    a.crates
        .keys()
        .map(|n| {
            (
                n.clone(),
                reach
                    .values()
                    .filter(|deps| deps.contains(n.as_str()))
                    .count(),
            )
        })
        .collect()
}

fn print_report(b: &Budgets, a: &Actual) {
    let fan_out = rebuild_fan_out(a);
    println!(
        "{:<22} {:>7} {:>8} {:>5}  rebuilds on edit",
        "crate", "lines", "ceiling", "deps"
    );
    let mut crates: Vec<_> = a.crates.iter().collect();
    crates.sort_by_key(|x| std::cmp::Reverse(x.1.lines));
    for (name, c) in crates {
        let limit = b
            .crates
            .get(name)
            .map_or("-".to_string(), |cb| cb.lines.to_string());
        println!(
            "{:<22} {:>7} {:>8} {:>5}  {}",
            name,
            c.lines,
            limit,
            c.deps.len(),
            fan_out.get(name).copied().unwrap_or(0)
        );
    }

    println!("\nfiles over the {}-line default:", b.file_lines_default);
    let mut files: Vec<_> = a
        .files
        .iter()
        .filter(|(_, &lines)| lines > b.file_lines_default)
        .collect();
    files.sort_by(|x, y| y.1.cmp(x.1));
    for (file, lines) in files {
        let limit = b.files.get(file).map_or("-".to_string(), |l| l.to_string());
        println!("  {lines:>6} / {limit:<6} {file}");
    }

    println!(
        "\nCargo.lock: {} packages (ceiling {}), {} names at more than one version (ceiling {})",
        a.lock_packages, b.lock.packages, a.lock_duplicate_names, b.lock.duplicate_names
    );
}

// ---- measuring the tree -----------------------------------------------------

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    name: String,
    manifest_path: PathBuf,
    dependencies: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    /// `null` for a normal dependency, `"dev"` or `"build"` otherwise.
    kind: Option<String>,
}

fn measure(root: &Path) -> Result<Actual> {
    let root = root
        .canonicalize()
        .with_context(|| format!("resolve {}", root.display()))?;
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(&root)
        .output()
        .context("run cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let metadata: Metadata =
        serde_json::from_slice(&output.stdout).context("parse cargo metadata")?;

    // Budgeted crates are the ones under crates/, minus the generated
    // workspace-hack that every crate depends on by design.
    let crates_dir = root.join("crates");
    let members: Vec<&Package> = metadata
        .packages
        .iter()
        .filter(|p| p.manifest_path.starts_with(&crates_dir) && p.name != "workspace-hack")
        .collect();
    let names: BTreeSet<&str> = members.iter().map(|p| p.name.as_str()).collect();

    let mut actual = Actual::default();
    for p in &members {
        let src = p
            .manifest_path
            .parent()
            .context("manifest has no parent")?
            .join("src");
        let mut files = Vec::new();
        collect_rs(&src, &mut files)?;
        let mut lines = 0;
        for f in files {
            let n = count_lines(&fs::read(&f).with_context(|| format!("read {}", f.display()))?);
            lines += n;
            let rel = f
                .strip_prefix(&root)
                .unwrap_or(&f)
                .to_string_lossy()
                .replace('\\', "/");
            actual.files.insert(rel, n);
        }
        let deps = p
            .dependencies
            .iter()
            .filter(|d| d.kind.as_deref() != Some("dev") && names.contains(d.name.as_str()))
            .map(|d| d.name.clone())
            .collect();
        actual
            .crates
            .insert(p.name.clone(), ActualCrate { lines, deps });
    }

    let lock = fs::read_to_string(root.join("Cargo.lock")).context("read Cargo.lock")?;
    (actual.lock_packages, actual.lock_duplicate_names) = lock_counts(&lock);
    Ok(actual)
}

fn count_lines(bytes: &[u8]) -> usize {
    let newlines = bytes.iter().filter(|&&b| b == b'\n').count();
    // A last line without a newline is still a line.
    newlines + usize::from(bytes.last().is_some_and(|&b| b != b'\n'))
}

/// (packages, crate names that appear at more than one version).
fn lock_counts(lock: &str) -> (usize, usize) {
    let mut versions: BTreeMap<&str, usize> = BTreeMap::new();
    let mut in_package = false;
    for line in lock.lines() {
        if line == "[[package]]" {
            in_package = true;
        } else if in_package {
            if let Some(name) = line
                .strip_prefix("name = \"")
                .and_then(|s| s.strip_suffix('"'))
            {
                *versions.entry(name).or_default() += 1;
            }
            in_package = false;
        }
    }
    (
        versions.values().sum(),
        versions.values().filter(|&&n| n > 1).count(),
    )
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let p = entry?.path();
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

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// core <- sql <- top, with one 4,100-line file in `top`.
    fn tree() -> (Budgets, Actual) {
        let mut actual = Actual {
            lock_packages: 95,
            lock_duplicate_names: 3,
            ..Default::default()
        };
        for (name, lines, deps) in [
            ("core", 900, set(&[])),
            ("sql", 1800, set(&["core"])),
            ("top", 4600, set(&["core", "sql"])),
        ] {
            actual
                .crates
                .insert(name.into(), ActualCrate { lines, deps });
        }
        actual.files.insert("crates/top/src/big.rs".into(), 4100);
        actual.files.insert("crates/top/src/lib.rs".into(), 500);

        let budgets = Budgets {
            file_lines_default: 3000,
            files: [("crates/top/src/big.rs".to_string(), 4500)].into(),
            crates: actual
                .crates
                .iter()
                .map(|(n, c)| {
                    (
                        n.clone(),
                        CrateBudget {
                            lines: ceiling(c.lines, CRATE_STEP),
                            deps: c.deps.clone(),
                        },
                    )
                })
                .collect(),
            lock: LockBudget {
                packages: 100,
                duplicate_names: 5,
            },
        };
        (budgets, actual)
    }

    #[test]
    fn a_tree_inside_its_budgets_has_no_problems() {
        let (b, a) = tree();
        assert_eq!(evaluate(&b, &a), Vec::<String>::new());
    }

    #[test]
    fn growth_past_a_ceiling_is_reported_per_kind() {
        let (b, mut a) = tree();
        a.files.insert("crates/top/src/big.rs".into(), 4501);
        a.files.insert("crates/top/src/lib.rs".into(), 3001);
        a.crates.get_mut("sql").unwrap().lines = 3001;
        a.lock_packages = 101;
        a.lock_duplicate_names = 6;
        let problems = evaluate(&b, &a);
        assert_eq!(problems.len(), 5, "{problems:#?}");
        assert!(problems[0].contains("big.rs: 4501 lines, over its ceiling of 4500"));
        assert!(problems[1].contains("lib.rs: 3001 lines, over the 3000 default"));
        assert!(problems[2].contains("sql: 3001 lines, over its ceiling of 3000"));
        assert!(problems[3].contains("101 packages in Cargo.lock"));
        assert!(problems[4].contains("6 crate names locked at more than one version"));
    }

    #[test]
    fn a_new_edge_and_a_new_crate_need_a_hand_edit() {
        let (b, mut a) = tree();
        a.crates.get_mut("core").unwrap().deps.insert("sql".into());
        a.crates.insert(
            "extra".into(),
            ActualCrate {
                lines: 10,
                deps: set(&[]),
            },
        );
        let problems = evaluate(&b, &a);
        assert_eq!(problems.len(), 2, "{problems:#?}");
        assert!(problems[0].starts_with("core -> sql: new first-party dependency"));
        assert!(problems[1].starts_with("extra: crate has no budget"));

        // --update must not make either of them pass.
        let mut tightened = b.clone();
        tighten(&mut tightened, &a);
        assert_eq!(tightened, b);
        assert_eq!(evaluate(&tightened, &a).len(), 2);
    }

    #[test]
    fn a_reduction_must_be_locked_in_and_update_does_it() {
        let (mut b, mut a) = tree();
        a.files.insert("crates/top/src/big.rs".into(), 3600); // ceiling 4500
        a.crates.get_mut("top").unwrap().lines = 2900; // ceiling 6000
        a.crates.get_mut("top").unwrap().deps.remove("sql");
        a.lock_packages = 40; // ceiling 100
        let problems = evaluate(&b, &a);
        // The file, the crate and the edge. A smaller Cargo.lock is not a failure.
        assert_eq!(problems.len(), 3, "{problems:#?}");
        assert!(
            problems.iter().all(|p| p.contains("--update")),
            "{problems:#?}"
        );

        tighten(&mut b, &a);
        assert_eq!(b.files["crates/top/src/big.rs"], 4000);
        assert_eq!(b.crates["top"].lines, 4000);
        assert_eq!(b.crates["top"].deps, set(&["core"]));
        assert_eq!(b.lock.packages, 75);
        assert_eq!(evaluate(&b, &a), Vec::<String>::new());
    }

    #[test]
    fn update_leaves_at_least_one_step_of_room() {
        for lines in [3001, 3249, 3250, 3251] {
            let room = ceiling(lines, FILE_STEP) - lines;
            assert!(
                (FILE_STEP..2 * FILE_STEP).contains(&room),
                "{lines}: {room}"
            );
        }
    }

    #[test]
    fn a_file_back_under_the_default_loses_its_entry() {
        let (mut b, mut a) = tree();
        // Just under the default the entry stays, with a ceiling to match.
        a.files.insert("crates/top/src/big.rs".into(), 2800);
        assert!(evaluate(&b, &a)[0].contains("--update locks in the reduction"));
        tighten(&mut b, &a);
        assert_eq!(b.files["crates/top/src/big.rs"], 3250);
        assert_eq!(evaluate(&b, &a), Vec::<String>::new());

        // A whole step under it, the entry goes.
        a.files.insert("crates/top/src/big.rs".into(), 2750);
        assert!(evaluate(&b, &a)[0].contains("back under the 3000 default"));
        tighten(&mut b, &a);
        assert!(b.files.is_empty());

        // A deleted file and a deleted crate go the same way.
        let (mut b, mut a) = tree();
        a.files.remove("crates/top/src/big.rs");
        a.crates.remove("top");
        assert_eq!(evaluate(&b, &a).len(), 2);
        tighten(&mut b, &a);
        assert_eq!(evaluate(&b, &a), Vec::<String>::new());
    }

    #[test]
    fn small_headroom_is_not_slack() {
        // --update records 4500 for 4100 lines; two steps above that is slack.
        assert!(!has_slack(4100, 4500, FILE_STEP));
        assert!(!has_slack(4100, 4750, FILE_STEP));
        assert!(has_slack(4100, 5000, FILE_STEP));
        assert_eq!(round_up(4000, FILE_STEP), 4000);
        assert_eq!(round_up(4001, FILE_STEP), 4250);
    }

    #[test]
    fn rebuild_fan_out_counts_transitive_dependents() {
        let (_, a) = tree();
        let fan_out = rebuild_fan_out(&a);
        assert_eq!(fan_out["core"], 2);
        assert_eq!(fan_out["sql"], 1);
        assert_eq!(fan_out["top"], 0);
    }

    #[test]
    fn lock_counts_packages_and_duplicated_names() {
        let lock = "version = 4\n\n[[package]]\nname = \"a\"\nversion = \"1.0.0\"\n\n\
                    [[package]]\nname = \"a\"\nversion = \"2.0.0\"\ndependencies = [\n \"b\",\n]\n\n\
                    [[package]]\nname = \"b\"\nversion = \"0.1.0\"\n";
        assert_eq!(lock_counts(lock), (3, 1));
    }

    #[test]
    fn a_last_line_without_a_newline_counts() {
        assert_eq!(count_lines(b""), 0);
        assert_eq!(count_lines(b"a\nb\n"), 2);
        assert_eq!(count_lines(b"a\nb"), 2);
    }
}
