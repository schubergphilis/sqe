//! Cap the number of glibc malloc arenas (#788).
//!
//! glibc gives each thread that contends on the main heap its own arena, up to
//! 8 x cores. Freed memory stays in the arena it came from, so a spilling
//! sort, aggregate and join running side by side leave gigabytes of
//! fragmented, unreturned heap spread over dozens of arenas. DataFusion's
//! memory pool never sees that memory: at SF100 a concurrent sort/agg/join run
//! held 4.6-10 GB in the pool while the process reached 25 GB anonymous RSS
//! and the kernel OOM-killed it. The same run with two arenas finished every
//! query with a peak RSS of 18.5 GB.
//!
//! The cap must be set before the runtime spawns threads, so `main` calls
//! [`apply`] first. An operator who sets `MALLOC_ARENA_MAX` keeps their value:
//! glibc reads it at startup and we leave it alone.

/// Arenas the process may use. Two is the value qualified at SF100 (#788).
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const ARENA_MAX: i32 = 2;

/// What [`apply`] did, logged once tracing is up.
#[cfg_attr(not(all(target_os = "linux", target_env = "gnu")), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArenaCap {
    /// `mallopt(M_ARENA_MAX)` succeeded with this value.
    Applied(i32),
    /// `MALLOC_ARENA_MAX` was set in the environment; glibc uses that.
    FromEnv,
    /// `mallopt` returned an error; glibc keeps its default.
    Failed,
    /// Not a glibc target: nothing to do.
    Unsupported,
}

static OUTCOME: std::sync::OnceLock<ArenaCap> = std::sync::OnceLock::new();

/// Cap glibc arenas unless the environment already does. Call before any
/// thread is spawned.
pub fn apply() -> ArenaCap {
    let cap = if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        ArenaCap::FromEnv
    } else {
        apply_inner()
    };
    let _ = OUTCOME.set(cap);
    cap
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn apply_inner() -> ArenaCap {
    // SAFETY: mallopt only adjusts allocator tunables and takes plain ints.
    // It runs in `main` before the runtime starts, with no other threads.
    let rc = unsafe { libc::mallopt(libc::M_ARENA_MAX, ARENA_MAX) };
    if rc == 1 {
        ArenaCap::Applied(ARENA_MAX)
    } else {
        ArenaCap::Failed
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn apply_inner() -> ArenaCap {
    ArenaCap::Unsupported
}

/// Log the outcome of [`apply`] once tracing is initialised.
pub fn log() {
    let Some(cap) = OUTCOME.get().copied() else {
        return;
    };
    match cap {
        ArenaCap::Applied(n) => tracing::info!(arenas = n, "glibc malloc arenas capped (#788)"),
        ArenaCap::FromEnv => tracing::info!(
            malloc_arena_max = ?std::env::var("MALLOC_ARENA_MAX").ok(),
            "glibc malloc arenas set by MALLOC_ARENA_MAX"
        ),
        ArenaCap::Failed => tracing::warn!(
            "mallopt(M_ARENA_MAX) failed; glibc keeps its default arena count, \
             which fragments heap under concurrent spilling queries (#788)"
        ),
        ArenaCap::Unsupported => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_reports_an_outcome() {
        // The test binary may already have threads, but mallopt itself is
        // safe to call; the point is that it compiles and does not fail on
        // the platforms we build.
        let cap = apply();
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        assert!(
            matches!(cap, ArenaCap::Applied(2) | ArenaCap::FromEnv),
            "{cap:?}"
        );
        #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
        assert!(
            matches!(cap, ArenaCap::Unsupported | ArenaCap::FromEnv),
            "{cap:?}"
        );
    }
}
