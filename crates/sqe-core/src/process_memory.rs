//! Portable process / container memory helpers.
//!
//! Linux containers expose limits via cgroup (`/sys/fs/cgroup`, `/proc`).
//! macOS and Windows do not have an equivalent fail-closed container limit
//! API that we can trust without extra native deps, so
//! [`enforced_memory_limit_bytes`] returns `None` there and callers skip
//! headroom fail-closed checks.
//!
//! RSS is best-effort on all platforms; returns `None` when unavailable.

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

/// Best-effort process resident set size in bytes.
///
/// | OS      | Source                                      |
/// |---------|---------------------------------------------|
/// | Linux   | `/proc/self/status` `VmRSS`                 |
/// | macOS   | `ps -o rss=` (kilobytes)                    |
/// | Windows | `GetProcessMemoryInfo` WorkingSetSize       |
/// | other   | `None`                                      |
pub fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        return process_rss_linux();
    }
    #[cfg(target_os = "macos")]
    {
        return process_rss_macos();
    }
    #[cfg(target_os = "windows")]
    {
        return process_rss_windows();
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        None
    }
}

/// Container- or OS-enforced memory limit in bytes, when known.
///
/// On Linux this is the effective cgroup memory max (v1 or v2), resolving
/// the process's own cgroup path when present. Unlimited / absent limits
/// yield `None`.
///
/// On macOS and Windows there is no portable container limit in-tree;
/// returns `None` so headroom validation becomes a no-op (log and continue).
pub fn enforced_memory_limit_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        return cgroup_memory_max_linux();
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Human-readable description of how the limit was discovered (for logs).
pub fn enforced_memory_limit_source() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "linux-cgroup"
    }
    #[cfg(target_os = "macos")]
    {
        "none-macos-no-cgroup"
    }
    #[cfg(target_os = "windows")]
    {
        "none-windows-no-cgroup"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        "none-unknown-os"
    }
}

// ─── Linux ───────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn process_rss_linux() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb.saturating_mul(1024))
}

/// Resolve the effective cgroup memory max for this process.
#[cfg(target_os = "linux")]
fn cgroup_memory_max_linux() -> Option<u64> {
    // Prefer the process's own cgroup path (handles container nesting).
    if let Some(v) = cgroup_v2_from_proc_self() {
        return parse_memory_limit_value(&v);
    }
    if let Some(v) = cgroup_v1_from_proc_self() {
        return parse_memory_limit_value(&v);
    }
    // Flat mounts (rare bare-metal / simple containers).
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(s) = fs::read_to_string(path) {
            if let Some(v) = parse_memory_limit_value(s.trim()) {
                return Some(v);
            }
        }
    }
    None
}

/// cgroup v2: `/proc/self/cgroup` line `0::/path` → `/sys/fs/cgroup{path}/memory.max`
#[cfg(target_os = "linux")]
fn cgroup_v2_from_proc_self() -> Option<String> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in cgroup.lines() {
        // Format: hierarchy-ID:controller-list:cgroup-path
        // v2 unified: "0::/user.slice/..."
        if let Some(rest) = line.strip_prefix("0::") {
            let rel = rest.trim();
            let path = if rel.is_empty() || rel == "/" {
                PathBuf::from("/sys/fs/cgroup/memory.max")
            } else {
                Path::new("/sys/fs/cgroup").join(rel.trim_start_matches('/')).join("memory.max")
            };
            if let Ok(s) = fs::read_to_string(&path) {
                return Some(s);
            }
        }
    }
    None
}

/// cgroup v1 memory controller path from `/proc/self/cgroup`.
#[cfg(target_os = "linux")]
fn cgroup_v1_from_proc_self() -> Option<String> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in cgroup.lines() {
        // e.g. "4:memory:/user.slice"
        let mut parts = line.splitn(3, ':');
        let _id = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        if !controllers.split(',').any(|c| c == "memory") {
            continue;
        }
        let full = Path::new("/sys/fs/cgroup/memory")
            .join(path.trim_start_matches('/'))
            .join("memory.limit_in_bytes");
        if let Ok(s) = fs::read_to_string(&full) {
            return Some(s);
        }
    }
    None
}

/// Parse a cgroup memory limit string (`"max"`, number, or huge sentinel).
fn parse_memory_limit_value(raw: &str) -> Option<u64> {
    let t = raw.trim();
    if t.is_empty() || t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    // Kernel uses ~2^63-1 style sentinels for "unlimited".
    if v == 0 || v > (1u64 << 60) {
        return None;
    }
    Some(v)
}

// ─── macOS ───────────────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn process_rss_macos() -> Option<u64> {
    // `ps` is always present on macOS; avoid extra native deps.
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?;
    let kb: u64 = s.trim().parse().ok()?;
    Some(kb.saturating_mul(1024))
}

// ─── Windows ─────────────────────────────────────────────────────────────────

#[cfg(target_os = "windows")]
fn process_rss_windows() -> Option<u64> {
    // Avoid hard-linking psapi/kernel32 helpers that differ across targets.
    // Working-set size is optional diagnostics only; headroom fail-closed is
    // Linux-cgroup only (see enforced_memory_limit_bytes).
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_limit_handles_max_and_sentinel() {
        assert_eq!(parse_memory_limit_value("max"), None);
        assert_eq!(parse_memory_limit_value(""), None);
        assert_eq!(parse_memory_limit_value("0"), None);
        assert_eq!(parse_memory_limit_value("1048576"), Some(1048576));
        assert_eq!(parse_memory_limit_value(&((1u64 << 62)).to_string()), None);
    }

    #[test]
    fn rss_does_not_panic_on_this_os() {
        // May be Some or None depending on environment; must not panic.
        let _ = process_rss_bytes();
    }

    #[test]
    fn enforced_limit_source_is_nonempty() {
        assert!(!enforced_memory_limit_source().is_empty());
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn non_linux_has_no_enforced_cgroup_limit() {
        // Fail-closed headroom is Linux-cgroup only; other OSes skip.
        assert!(enforced_memory_limit_bytes().is_none());
    }
}
