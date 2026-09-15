//! Records the git commit and build profile into the binary (issue #669),
//! so the startup log and `/api/v1/status` can say which code and which
//! optimisation level is running. Falls back to `unknown` outside a git
//! checkout (release tarballs, vendored builds) rather than failing the build.

use std::process::Command;

fn main() {
    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=SQE_GIT_SHA={sha}");

    // PROFILE is "debug" or "release" as cargo sees it; custom profiles
    // inheriting from release report "release".
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=SQE_BUILD_PROFILE={profile}");

    // Re-run when HEAD moves so the sha does not go stale across commits,
    // without re-running on every source change.
    if let Some(git_dir) = git_dir() {
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        if let Ok(head) = std::fs::read_to_string(format!("{git_dir}/HEAD")) {
            if let Some(reference) = head.trim().strip_prefix("ref: ") {
                println!("cargo:rerun-if-changed={git_dir}/{reference}");
            }
        }
    }
}

fn git_dir() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
