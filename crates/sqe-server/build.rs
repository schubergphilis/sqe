//! Records the git commit and build profile into the binary (issue #669),
//! so the startup log and `/api/v1/status/detail` can say which code and which
//! optimisation level is running. Falls back to `unknown` outside a git
//! checkout (release tarballs, vendored builds) rather than failing the build.

use std::process::Command;

fn main() {
    let sha = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SQE_GIT_SHA={sha}");

    // PROFILE is "debug" or "release" as cargo sees it; custom profiles
    // inheriting from release report "release".
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=SQE_BUILD_PROFILE={profile}");

    // Re-run when HEAD moves so the sha does not go stale across commits,
    // without re-running on every source change. Only paths that exist are
    // watched: cargo treats a missing `rerun-if-changed` path as always
    // changed and would recompile this crate on every build. A branch ref is
    // missing from the obvious place in two common cases: a linked worktree
    // keeps refs in the main repository's git dir (`--git-path` resolves
    // that), and `git pack-refs` moves a ref into `packed-refs`. The reflog
    // covers the commit that turns a packed ref back into a loose one.
    watch(git_path("HEAD"));
    watch(git_path("logs/HEAD"));
    let reference = git(&["symbolic-ref", "-q", "HEAD"]);
    let loose = reference.as_deref().and_then(git_path);
    if loose
        .as_deref()
        .is_some_and(|p| std::path::Path::new(p).exists())
    {
        watch(loose);
    } else {
        watch(git_path("packed-refs"));
    }
}

fn watch(path: Option<String>) {
    if let Some(path) = path.filter(|p| std::path::Path::new(p).exists()) {
        println!("cargo:rerun-if-changed={path}");
    }
}

/// Where git keeps `name` for this checkout, absolute.
fn git_path(name: &str) -> Option<String> {
    git(&["rev-parse", "--path-format=absolute", "--git-path", name])
}

fn git(args: &[&str]) -> Option<String> {
    Command::new("git")
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
