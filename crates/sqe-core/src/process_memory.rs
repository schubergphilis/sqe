//! Container / cgroup / OS-aware process memory helpers.
//!
//! Production headroom fail-closed checks need the **enforced** limit the
//! kernel (or container runtime) will actually kill us at — not host RAM from
//! `/proc/meminfo`, which is often the node total inside a pod.
//!
//! Detection layers (tightest first for enforcement):
//!
//! 1. **Linux cgroup** — nested walk of cgroup v2 `memory.max` / v1
//!    `memory.limit_in_bytes` for this process (Docker, containerd, CRI-O,
//!    Podman, Kubernetes, bare-metal slices).
//! 2. **Container signals** — `/.dockerenv`, `/run/.containerenv`, cgroup path
//!    markers, `KUBERNETES_SERVICE_HOST`, `container=` env (diagnostics only;
//!    do not invent a limit from these).
//! 3. **Host OS memory** — total / available for capacity logs and non-container
//!    deployments. Never used as a hard fail-closed limit by itself on Linux
//!    when a cgroup limit is present.
//!
//! | Platform | Enforced limit | RSS | Host total |
//! |----------|----------------|-----|------------|
//! | Linux    | cgroup v1/v2   | `/proc/self/status` | `/proc/meminfo` |
//! | macOS    | none (no cgroup) | `ps` | `sysctl hw.memsize` |
//! | Windows  | none (no Job Object probe) | none | none |
//!
//! ## Lifecycle (config vs kernel)
//!
//! - **`sqe.toml` / `worker.memory_limit`**: loaded once at process start. There
//!   is no hot-reload of the DataFusion pool, governor, or spill budgets. Change
//!   the file and **restart** the worker.
//! - **Kernel / cgroup limit**: can move under a live process (Kubernetes
//!   in-place resize, `systemd` property changes, nested limit rewrites). Each
//!   call to [`runtime_memory_info`] re-reads `/sys` and `/proc`. Use
//!   [`spawn_runtime_memory_watch`] to log when the live enforced limit drifts
//!   from the value seen at boot relative to the configured need.
//! - Mid-run we **do not** resize the memory pool automatically: in-flight
//!   grants and reservations would be inconsistent. Operators get a loud log
//!   and must restart (or raise the cgroup) when need no longer fits.

use std::fmt;
use std::time::Duration;

#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

// ─── Public snapshot types ───────────────────────────────────────────────────

/// Host operating system bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsKind {
    Linux,
    MacOs,
    Windows,
    Other,
}

impl OsKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::MacOs => "macos",
            Self::Windows => "windows",
            Self::Other => "other",
        }
    }
}

impl fmt::Display for OsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Best-effort container / isolation detection.
///
/// Used for logs and ops context. **Does not invent a memory limit** — only
/// the cgroup (or future platform Job Object) supplies enforcement numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerKind {
    /// No container markers observed.
    BareMetal,
    /// Kubernetes pod (env and/or cgroup path).
    Kubernetes,
    /// Docker (file or cgroup path).
    Docker,
    /// Podman / libpod.
    Podman,
    /// containerd / cri-containerd cgroup path.
    Containerd,
    /// LXC.
    Lxc,
    /// Generic: `container=` env or cgroup path looked container-like.
    Generic,
    /// Non-Linux: container isolation is not probeable the same way.
    NotApplicable,
}

impl ContainerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BareMetal => "bare-metal",
            Self::Kubernetes => "kubernetes",
            Self::Docker => "docker",
            Self::Podman => "podman",
            Self::Containerd => "containerd",
            Self::Lxc => "lxc",
            Self::Generic => "container",
            Self::NotApplicable => "n/a",
        }
    }

    pub fn is_container(self) -> bool {
        !matches!(self, Self::BareMetal | Self::NotApplicable)
    }
}

impl fmt::Display for ContainerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Which cgroup memory interface was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CgroupVersion {
    V1,
    V2,
    None,
}

impl CgroupVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::None => "none",
        }
    }
}

impl fmt::Display for CgroupVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cgroup memory view for this process (Linux). Empty on other OSes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CgroupMemoryInfo {
    pub version: CgroupVersion,
    /// Relative cgroup path from `/proc/self/cgroup` when known.
    pub path: Option<String>,
    /// Tightest finite memory limit after walking ancestors (bytes).
    pub memory_max_bytes: Option<u64>,
    /// Current cgroup memory usage when readable (bytes).
    pub memory_current_bytes: Option<u64>,
    /// Filesystem path of the limit file that supplied `memory_max_bytes`.
    pub limit_file: Option<String>,
}

impl CgroupMemoryInfo {
    fn empty() -> Self {
        Self {
            version: CgroupVersion::None,
            path: None,
            memory_max_bytes: None,
            memory_current_bytes: None,
            limit_file: None,
        }
    }
}

/// Host (or node) memory from OS interfaces — advisory, not fail-closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HostMemoryInfo {
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
}

/// Full runtime memory environment for logs and headroom decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMemoryInfo {
    pub os: OsKind,
    pub container: ContainerKind,
    pub cgroup: CgroupMemoryInfo,
    pub host: HostMemoryInfo,
    pub process_rss_bytes: Option<u64>,
    /// Hard limit used for fail-closed headroom (cgroup on Linux when set).
    pub enforced_memory_limit_bytes: Option<u64>,
    /// Stable short tag for metrics / logs (e.g. `linux-cgroup-v2`).
    pub enforced_memory_limit_source: &'static str,
}

impl RuntimeMemoryInfo {
    /// True when a kernel/container hard limit is known and usable for
    /// fail-closed validation.
    pub fn has_enforced_limit(&self) -> bool {
        self.enforced_memory_limit_bytes
            .is_some_and(|n| n > 0)
    }

    /// True when `configured_need` (typically `memory_limit + process_headroom`)
    /// exceeds the live enforced cgroup limit.
    pub fn configured_need_exceeds_enforced(&self, configured_need_bytes: u64) -> bool {
        match self.enforced_memory_limit_bytes {
            Some(enforced) if enforced > 0 => configured_need_bytes > enforced,
            _ => false,
        }
    }
}

/// Diff between two probes (cgroup/container/OS — not sqe.toml).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMemoryChange {
    pub enforced_limit_changed: bool,
    pub container_changed: bool,
    pub cgroup_path_changed: bool,
    pub previous_enforced_bytes: Option<u64>,
    pub current_enforced_bytes: Option<u64>,
    pub previous_source: &'static str,
    pub current_source: &'static str,
}

impl RuntimeMemoryChange {
    pub fn between(prev: &RuntimeMemoryInfo, next: &RuntimeMemoryInfo) -> Self {
        Self {
            enforced_limit_changed: prev.enforced_memory_limit_bytes
                != next.enforced_memory_limit_bytes
                || prev.enforced_memory_limit_source != next.enforced_memory_limit_source,
            container_changed: prev.container != next.container,
            cgroup_path_changed: prev.cgroup.path != next.cgroup.path,
            previous_enforced_bytes: prev.enforced_memory_limit_bytes,
            current_enforced_bytes: next.enforced_memory_limit_bytes,
            previous_source: prev.enforced_memory_limit_source,
            current_source: next.enforced_memory_limit_source,
        }
    }

    pub fn any(&self) -> bool {
        self.enforced_limit_changed || self.container_changed || self.cgroup_path_changed
    }
}

/// Default interval for [`spawn_runtime_memory_watch`].
pub const DEFAULT_RUNTIME_MEMORY_WATCH_INTERVAL: Duration = Duration::from_secs(30);

// ─── Public entry points ─────────────────────────────────────────────────────

/// Probe OS, container markers, cgroup, host memory, and process RSS.
///
/// Always re-reads kernel interfaces (no process-wide cache). Safe to call
/// from a background watch; cheap relative to query work (a few small files).
pub fn runtime_memory_info() -> RuntimeMemoryInfo {
    let os = current_os();
    let process_rss_bytes = process_rss_bytes();
    let (cgroup, container) = probe_linux_isolation();
    let host = host_memory_info();

    let enforced_memory_limit_bytes = cgroup.memory_max_bytes;
    let enforced_memory_limit_source = match (os, cgroup.version, enforced_memory_limit_bytes) {
        (OsKind::Linux, CgroupVersion::V2, Some(_)) => "linux-cgroup-v2",
        (OsKind::Linux, CgroupVersion::V1, Some(_)) => "linux-cgroup-v1",
        (OsKind::Linux, _, None) => "linux-no-cgroup-limit",
        (OsKind::MacOs, _, _) => "macos-no-cgroup",
        (OsKind::Windows, _, _) => "windows-no-cgroup",
        (OsKind::Other, _, _) => "unknown-os",
        // Limit present but version somehow None: still report linux.
        (OsKind::Linux, CgroupVersion::None, Some(_)) => "linux-cgroup",
    };

    // Prefer structured container detection; on non-Linux mark N/A.
    let container = match os {
        OsKind::Linux => container,
        _ => ContainerKind::NotApplicable,
    };

    RuntimeMemoryInfo {
        os,
        container,
        cgroup,
        host,
        process_rss_bytes,
        enforced_memory_limit_bytes,
        enforced_memory_limit_source,
    }
}

/// Best-effort process resident set size in bytes.
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

/// Container- or cgroup-enforced memory limit in bytes, when known.
///
/// Linux: tightest finite cgroup memory max for this process (nested walk).
/// Other OS: `None` — headroom fail-closed is a no-op.
pub fn enforced_memory_limit_bytes() -> Option<u64> {
    runtime_memory_info().enforced_memory_limit_bytes
}

/// Short source tag for the enforced limit (for logs / metrics labels).
pub fn enforced_memory_limit_source() -> &'static str {
    runtime_memory_info().enforced_memory_limit_source
}

/// Periodically re-probe container/cgroup/OS memory and log when the **live
/// kernel limit** moves relative to boot or no longer covers `configured_need_bytes`.
///
/// This does **not** reload `sqe.toml`. Pool size and governor budgets stay at
/// the values fixed when the process started. When the live cgroup shrinks
/// below the configured need, we log at error level; the operator must
/// restart with a lower `worker.memory_limit` or raise the cgroup.
///
/// Returns a [`JoinHandle`] so callers can abort on shutdown if desired.
/// Matches the worker heartbeat pattern (process-lifetime background task).
pub fn spawn_runtime_memory_watch(
    configured_need_bytes: u64,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let interval = if interval.is_zero() {
        DEFAULT_RUNTIME_MEMORY_WATCH_INTERVAL
    } else {
        interval
    };
    tokio::spawn(async move {
        let mut last = runtime_memory_info();
        tracing::info!(
            interval_secs = interval.as_secs(),
            configured_need_bytes,
            enforced_memory_limit_bytes = last.enforced_memory_limit_bytes.unwrap_or(0),
            enforced_source = last.enforced_memory_limit_source,
            os = %last.os,
            container = %last.container,
            "Watching live container/cgroup/OS memory (sqe.toml memory settings are not hot-reloaded)"
        );
        loop {
            tokio::time::sleep(interval).await;
            let now = runtime_memory_info();
            let change = RuntimeMemoryChange::between(&last, &now);

            if change.any() {
                tracing::warn!(
                    previous_enforced_bytes = change.previous_enforced_bytes.unwrap_or(0),
                    current_enforced_bytes = change.current_enforced_bytes.unwrap_or(0),
                    previous_source = change.previous_source,
                    current_source = change.current_source,
                    enforced_limit_changed = change.enforced_limit_changed,
                    container_changed = change.container_changed,
                    cgroup_path_changed = change.cgroup_path_changed,
                    container = %now.container,
                    cgroup_path = now.cgroup.path.as_deref().unwrap_or(""),
                    process_rss_bytes = now.process_rss_bytes.unwrap_or(0),
                    host_total_bytes = now.host.total_bytes.unwrap_or(0),
                    "Live runtime memory environment changed (cgroup/container/OS; not sqe.toml)"
                );
            }

            if now.configured_need_exceeds_enforced(configured_need_bytes) {
                let enforced = now.enforced_memory_limit_bytes.unwrap_or(0);
                tracing::error!(
                    configured_need_bytes,
                    enforced_memory_limit_bytes = enforced,
                    enforced_source = now.enforced_memory_limit_source,
                    os = %now.os,
                    container = %now.container,
                    cgroup_version = %now.cgroup.version,
                    "Configured memory_limit+headroom no longer fits live cgroup limit. \
                     Pool size is fixed until restart — lower worker.memory_limit and restart, \
                     or raise the container/cgroup memory limit."
                );
            } else if change.enforced_limit_changed {
                tracing::info!(
                    configured_need_bytes,
                    enforced_memory_limit_bytes = now.enforced_memory_limit_bytes.unwrap_or(0),
                    enforced_source = now.enforced_memory_limit_source,
                    "Live enforced memory limit still covers configured need"
                );
            }

            last = now;
        }
    })
}

fn current_os() -> OsKind {
    #[cfg(target_os = "linux")]
    {
        OsKind::Linux
    }
    #[cfg(target_os = "macos")]
    {
        OsKind::MacOs
    }
    #[cfg(target_os = "windows")]
    {
        OsKind::Windows
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        OsKind::Other
    }
}

// ─── Linux isolation (container + cgroup) ────────────────────────────────────

#[cfg(target_os = "linux")]
fn probe_linux_isolation() -> (CgroupMemoryInfo, ContainerKind) {
    let cgroup = probe_cgroup_memory();
    let container = detect_container_linux(cgroup.path.as_deref());
    (cgroup, container)
}

#[cfg(not(target_os = "linux"))]
fn probe_linux_isolation() -> (CgroupMemoryInfo, ContainerKind) {
    (CgroupMemoryInfo::empty(), ContainerKind::NotApplicable)
}

#[cfg(target_os = "linux")]
fn detect_container_linux(cgroup_path: Option<&str>) -> ContainerKind {
    // 1. Explicit env signals (Kubernetes injects this in every pod).
    if std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() {
        return ContainerKind::Kubernetes;
    }
    if let Ok(v) = std::env::var("container") {
        let v = v.to_ascii_lowercase();
        return match v.as_str() {
            "docker" => ContainerKind::Docker,
            "podman" => ContainerKind::Podman,
            "lxc" | "lxcfs" => ContainerKind::Lxc,
            "cri-containerd" | "containerd" => ContainerKind::Containerd,
            _ if !v.is_empty() => ContainerKind::Generic,
            _ => ContainerKind::BareMetal,
        };
    }

    // 2. Well-known marker files.
    if Path::new("/.dockerenv").exists() {
        return ContainerKind::Docker;
    }
    if Path::new("/run/.containerenv").exists() {
        return ContainerKind::Podman;
    }

    // 3. cgroup path markers used by runtimes / kubelet.
    if let Some(p) = cgroup_path {
        let lower = p.to_ascii_lowercase();
        if lower.contains("kubepods") || lower.contains("kubelet") {
            return ContainerKind::Kubernetes;
        }
        if lower.contains("docker") {
            return ContainerKind::Docker;
        }
        if lower.contains("podman") || lower.contains("libpod") {
            return ContainerKind::Podman;
        }
        if lower.contains("containerd") || lower.contains("cri-containerd") {
            return ContainerKind::Containerd;
        }
        if lower.contains("lxc") {
            return ContainerKind::Lxc;
        }
        // Generic containerd/docker style: .../system.slice/docker-*.scope or
        // long hex ids under cgroup paths are weak signals only.
        if lower.contains("/docker-") || lower.contains("crio-") {
            return ContainerKind::Generic;
        }
    }

    ContainerKind::BareMetal
}

#[cfg(target_os = "linux")]
fn probe_cgroup_memory() -> CgroupMemoryInfo {
    if let Some(info) = cgroup_v2_probe() {
        return info;
    }
    if let Some(info) = cgroup_v1_probe() {
        return info;
    }
    // Flat mounts without /proc/self/cgroup resolution.
    for (version, path) in [
        (CgroupVersion::V2, "/sys/fs/cgroup/memory.max"),
        (
            CgroupVersion::V1,
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ),
    ] {
        if let Ok(s) = fs::read_to_string(path) {
            if let Some(v) = parse_memory_limit_value(s.trim()) {
                let current = match version {
                    CgroupVersion::V2 => read_u64_file(Path::new("/sys/fs/cgroup/memory.current")),
                    CgroupVersion::V1 => {
                        read_u64_file(Path::new("/sys/fs/cgroup/memory/memory.usage_in_bytes"))
                    }
                    CgroupVersion::None => None,
                };
                return CgroupMemoryInfo {
                    version,
                    path: Some("/".into()),
                    memory_max_bytes: Some(v),
                    memory_current_bytes: current,
                    limit_file: Some(path.into()),
                };
            }
        }
    }
    CgroupMemoryInfo::empty()
}

/// cgroup v2: resolve process path, walk ancestors for tightest `memory.max`.
#[cfg(target_os = "linux")]
fn cgroup_v2_probe() -> Option<CgroupMemoryInfo> {
    let rel = cgroup_v2_rel_path()?;
    let (max, limit_file) = tightest_memory_max_v2(&rel)?;
    let current_path = cgroup_v2_file(&rel, "memory.current");
    let memory_current_bytes = read_u64_file(&current_path);
    Some(CgroupMemoryInfo {
        version: CgroupVersion::V2,
        path: Some(rel),
        memory_max_bytes: Some(max),
        memory_current_bytes,
        limit_file: Some(limit_file),
    })
}

#[cfg(target_os = "linux")]
fn cgroup_v2_rel_path() -> Option<String> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in cgroup.lines() {
        // Unified hierarchy: "0::/user.slice/..."
        if let Some(rest) = line.strip_prefix("0::") {
            return Some(normalize_cgroup_rel(rest.trim()));
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn cgroup_v2_file(rel: &str, name: &str) -> PathBuf {
    if rel.is_empty() || rel == "/" {
        Path::new("/sys/fs/cgroup").join(name)
    } else {
        Path::new("/sys/fs/cgroup")
            .join(rel.trim_start_matches('/'))
            .join(name)
    }
}

/// Walk from leaf cgroup to root; return min finite `memory.max` and its file.
#[cfg(target_os = "linux")]
fn tightest_memory_max_v2(rel: &str) -> Option<(u64, String)> {
    let mut best: Option<(u64, String)> = None;
    for ancestor in cgroup_ancestors(rel) {
        let file = cgroup_v2_file(&ancestor, "memory.max");
        if let Ok(s) = fs::read_to_string(&file) {
            if let Some(v) = parse_memory_limit_value(s.trim()) {
                best = match best {
                    Some((cur, f)) if cur <= v => Some((cur, f)),
                    _ => Some((v, file.display().to_string())),
                };
            }
        }
    }
    best
}

#[cfg(target_os = "linux")]
fn cgroup_v1_probe() -> Option<CgroupMemoryInfo> {
    let (rel, leaf_limit_path) = cgroup_v1_memory_paths()?;
    let (max, limit_file) = tightest_memory_max_v1(&rel).or_else(|| {
        let s = fs::read_to_string(&leaf_limit_path).ok()?;
        let v = parse_memory_limit_value(s.trim())?;
        Some((v, leaf_limit_path.display().to_string()))
    })?;
    let usage = Path::new("/sys/fs/cgroup/memory")
        .join(rel.trim_start_matches('/'))
        .join("memory.usage_in_bytes");
    Some(CgroupMemoryInfo {
        version: CgroupVersion::V1,
        path: Some(rel),
        memory_max_bytes: Some(max),
        memory_current_bytes: read_u64_file(&usage),
        limit_file: Some(limit_file),
    })
}

#[cfg(target_os = "linux")]
fn cgroup_v1_memory_paths() -> Option<(String, PathBuf)> {
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
        let rel = normalize_cgroup_rel(path);
        let full = Path::new("/sys/fs/cgroup/memory")
            .join(rel.trim_start_matches('/'))
            .join("memory.limit_in_bytes");
        return Some((rel, full));
    }
    None
}

#[cfg(target_os = "linux")]
fn tightest_memory_max_v1(rel: &str) -> Option<(u64, String)> {
    let mut best: Option<(u64, String)> = None;
    for ancestor in cgroup_ancestors(rel) {
        let file = Path::new("/sys/fs/cgroup/memory")
            .join(ancestor.trim_start_matches('/'))
            .join("memory.limit_in_bytes");
        if let Ok(s) = fs::read_to_string(&file) {
            if let Some(v) = parse_memory_limit_value(s.trim()) {
                best = match best {
                    Some((cur, f)) if cur <= v => Some((cur, f)),
                    _ => Some((v, file.display().to_string())),
                };
            }
        }
    }
    best
}

/// Ancestors from leaf to root, inclusive (`"/a/b"`, `"/a"`, `"/"`).
#[cfg(target_os = "linux")]
fn cgroup_ancestors(rel: &str) -> Vec<String> {
    let rel = normalize_cgroup_rel(rel);
    let mut out = Vec::new();
    let mut cur = rel;
    loop {
        out.push(cur.clone());
        if cur == "/" {
            break;
        }
        let path = Path::new(&cur);
        cur = path
            .parent()
            .map(|p| {
                let s = p.to_string_lossy();
                if s.is_empty() {
                    "/".into()
                } else {
                    s.into_owned()
                }
            })
            .unwrap_or_else(|| "/".into());
    }
    out
}

#[cfg(target_os = "linux")]
fn normalize_cgroup_rel(raw: &str) -> String {
    let t = raw.trim();
    if t.is_empty() || t == "/" {
        return "/".into();
    }
    if t.starts_with('/') {
        t.to_string()
    } else {
        format!("/{t}")
    }
}

#[cfg(target_os = "linux")]
fn read_u64_file(path: &Path) -> Option<u64> {
    let s = fs::read_to_string(path).ok()?;
    parse_memory_limit_value(s.trim()).or_else(|| s.trim().parse().ok())
}

#[cfg(target_os = "linux")]
fn process_rss_linux() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb.saturating_mul(1024))
}

/// Parse a cgroup memory limit string (`"max"`, number, or huge sentinel).
#[cfg(any(test, target_os = "linux"))]
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

// ─── Host OS memory (advisory) ───────────────────────────────────────────────

fn host_memory_info() -> HostMemoryInfo {
    #[cfg(target_os = "linux")]
    {
        return host_memory_linux();
    }
    #[cfg(target_os = "macos")]
    {
        return host_memory_macos();
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        HostMemoryInfo::default()
    }
}

#[cfg(target_os = "linux")]
fn host_memory_linux() -> HostMemoryInfo {
    // Note: inside many containers MemTotal is still the *node* total.
    // Prefer cgroup for enforcement; host figures are capacity / ops only.
    let Ok(meminfo) = fs::read_to_string("/proc/meminfo") else {
        return HostMemoryInfo::default();
    };
    let mut total = None;
    let mut available = None;
    for line in meminfo.lines() {
        if let Some(v) = parse_meminfo_kb(line, "MemTotal:") {
            total = Some(v.saturating_mul(1024));
        } else if let Some(v) = parse_meminfo_kb(line, "MemAvailable:") {
            available = Some(v.saturating_mul(1024));
        }
    }
    HostMemoryInfo {
        total_bytes: total,
        available_bytes: available,
    }
}

#[cfg(target_os = "linux")]
fn parse_meminfo_kb(line: &str, key: &str) -> Option<u64> {
    let rest = line.strip_prefix(key)?.trim();
    let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
    Some(kb)
}

#[cfg(target_os = "macos")]
fn host_memory_macos() -> HostMemoryInfo {
    // `sysctl -n hw.memsize` → total physical RAM in bytes.
    let total = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    HostMemoryInfo {
        total_bytes: total,
        // Available would need host_statistics64; skip without native deps.
        available_bytes: None,
    }
}

// ─── macOS / Windows RSS ─────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn process_rss_macos() -> Option<u64> {
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

#[cfg(target_os = "windows")]
fn process_rss_windows() -> Option<u64> {
    // No hard link to psapi; RSS is optional diagnostics. Enforced limits
    // require Job Object APIs we do not bind here.
    None
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_limit_handles_max_and_sentinel() {
        assert_eq!(parse_memory_limit_value("max"), None);
        assert_eq!(parse_memory_limit_value(""), None);
        assert_eq!(parse_memory_limit_value("0"), None);
        assert_eq!(parse_memory_limit_value("1048576"), Some(1048576));
        assert_eq!(
            parse_memory_limit_value(&((1u64 << 62)).to_string()),
            None
        );
    }

    #[test]
    fn runtime_snapshot_is_consistent() {
        let info = runtime_memory_info();
        assert!(!info.enforced_memory_limit_source.is_empty());
        assert_eq!(info.os, current_os());
        // Convenience wrappers match snapshot.
        assert_eq!(
            enforced_memory_limit_bytes(),
            info.enforced_memory_limit_bytes
        );
        assert_eq!(
            enforced_memory_limit_source(),
            info.enforced_memory_limit_source
        );
        if info.has_enforced_limit() {
            assert!(matches!(
                info.cgroup.version,
                CgroupVersion::V1 | CgroupVersion::V2
            ));
        }
    }

    #[test]
    fn rss_does_not_panic_on_this_os() {
        let _ = process_rss_bytes();
    }

    #[test]
    fn enforced_limit_source_is_nonempty() {
        assert!(!enforced_memory_limit_source().is_empty());
    }

    #[test]
    #[cfg(not(target_os = "linux"))]
    fn non_linux_has_no_enforced_cgroup_limit() {
        let info = runtime_memory_info();
        assert!(info.enforced_memory_limit_bytes.is_none());
        assert!(!info.has_enforced_limit());
        assert_eq!(info.container, ContainerKind::NotApplicable);
        assert_eq!(info.cgroup.version, CgroupVersion::None);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_exposes_host_total_when_sysctl_works() {
        let info = runtime_memory_info();
        assert_eq!(info.os, OsKind::MacOs);
        // Dev machines almost always have sysctl; treat as soft assert if Some.
        if let Some(total) = info.host.total_bytes {
            assert!(total > 0);
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn linux_cgroup_ancestors_include_root() {
        let a = cgroup_ancestors("/foo/bar");
        assert_eq!(a, vec!["/foo/bar".to_string(), "/foo".to_string(), "/".to_string()]);
        assert_eq!(cgroup_ancestors("/"), vec!["/".to_string()]);
        assert_eq!(cgroup_ancestors(""), vec!["/".to_string()]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn container_kind_from_cgroup_path_markers() {
        assert_eq!(
            detect_container_linux(Some(
                "/kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope"
            )),
            ContainerKind::Kubernetes
        );
        assert_eq!(
            detect_container_linux(Some("/docker/abc123")),
            ContainerKind::Docker
        );
        assert_eq!(
            detect_container_linux(Some("/machine.slice/libpod-xyz.scope")),
            ContainerKind::Podman
        );
        // Without env/files, empty path → bare metal (test env may still set
        // KUBERNETES_SERVICE_HOST — only assert marker paths above).
        let _ = detect_container_linux(Some("/user.slice"));
    }

    #[test]
    fn os_and_container_display() {
        assert_eq!(OsKind::Linux.as_str(), "linux");
        assert_eq!(ContainerKind::Kubernetes.as_str(), "kubernetes");
        assert!(ContainerKind::Docker.is_container());
        assert!(!ContainerKind::BareMetal.is_container());
    }

    #[test]
    fn runtime_memory_change_detects_enforced_drift() {
        let a = runtime_memory_info();
        let mut b = a.clone();
        assert!(!RuntimeMemoryChange::between(&a, &b).any());

        b.enforced_memory_limit_bytes = Some(
            b.enforced_memory_limit_bytes
                .unwrap_or(0)
                .saturating_add(1)
                .max(1),
        );
        let change = RuntimeMemoryChange::between(&a, &b);
        assert!(change.enforced_limit_changed);
        assert!(change.any());
    }

    #[test]
    fn configured_need_exceeds_only_when_enforced_known() {
        let mut info = runtime_memory_info();
        info.enforced_memory_limit_bytes = None;
        assert!(!info.configured_need_exceeds_enforced(u64::MAX));

        info.enforced_memory_limit_bytes = Some(1024);
        assert!(info.configured_need_exceeds_enforced(2048));
        assert!(!info.configured_need_exceeds_enforced(512));
        assert!(!info.configured_need_exceeds_enforced(1024));
    }
}
