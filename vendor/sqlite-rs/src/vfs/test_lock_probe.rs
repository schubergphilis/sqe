// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Test-only: drives the `lock_probe` helper binary (`tests/helpers/
//! lock_probe.rs`) — a genuine second OS process — to observe `fcntl`
//! byte-range lock contention that a same-process re-lock could never see
//! (POSIX record locks are scoped to `(process, inode)`). Replaces the old
//! `fork`/`waitpid`/`_exit`-based test helpers (#66): a subprocess gives a
//! fresh address space, closer to a real second `sqlite3` process, without
//! any `unsafe`.
//!
//! Shared by `src/vfs/lock.rs` and `src/vfs/shm.rs`'s tests.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};

use crate::sys::fcntl::off_t;

/// Locates the `lock_probe` helper binary next to this test binary.
/// `CARGO_BIN_EXE_lock_probe` isn't available here — Cargo only sets it for
/// integration tests and benches, not a lib's own `#[cfg(test)]` unit
/// tests — but `cargo test` still builds `lock_probe` as a plain sibling
/// artifact at `target/<profile>/lock_probe`, one level up from this test
/// binary's `target/<profile>/deps/` directory.
#[allow(
    clippy::expect_used,
    reason = "test-only helper: a missing helper binary has no reasonable fallback"
)]
fn helper_path() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let deps_dir = exe.parent().expect("test binary has a parent dir");
    let profile_dir = if deps_dir.file_name().and_then(|n| n.to_str()) == Some("deps") {
        deps_dir.parent().expect("deps dir has a parent dir")
    } else {
        deps_dir
    };
    let path = profile_dir.join("lock_probe");
    assert!(
        path.exists(),
        "lock_probe helper binary not found at {path:?} — is it declared as a [[bin]] in Cargo.toml?"
    );
    path
}

/// Spawns the helper for a one-shot, non-blocking lock attempt on
/// `path`'s `start..start+len` byte range (`kind` is `"rdlock"` or
/// `"wrlock"`) and reports whether it succeeded.
#[allow(clippy::expect_used, reason = "test-only helper")]
pub(crate) fn lock_available(path: &Path, kind: &str, start: off_t, len: off_t) -> bool {
    let status = Command::new(helper_path())
        .args([
            "trylock",
            &path.display().to_string(),
            kind,
            &start.to_string(),
            &len.to_string(),
        ])
        .status()
        .expect("spawn lock_probe");
    status.success()
}

/// A lock claimed by a spawned `lock_probe` subprocess, held until
/// [`HeldLock::release`] hands it back.
pub(crate) struct HeldLock {
    child: Child,
    stdin: ChildStdin,
}

impl HeldLock {
    #[allow(
        clippy::expect_used,
        reason = "test-only helper: a fork/spawn failure has no reasonable fallback"
    )]
    fn spawn(path: &Path, kind: &str, start: off_t, len: off_t) -> Self {
        let mut child = Command::new(helper_path())
            .args([
                "holdlock",
                &path.display().to_string(),
                kind,
                &start.to_string(),
                &len.to_string(),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn lock_probe");
        let stdin = child.stdin.take().expect("child stdin");
        let stdout = child.stdout.take().expect("child stdout");

        let mut line = String::new();
        BufReader::new(stdout)
            .read_line(&mut line)
            .expect("read lock_probe ack");
        assert_eq!(line.trim(), "locked", "lock_probe failed to claim the lock");

        HeldLock { child, stdin }
    }

    #[allow(clippy::expect_used, reason = "test-only helper")]
    fn release(mut self) {
        self.stdin.write_all(b"\n").ok();
        self.child.wait().expect("wait for lock_probe to exit");
    }
}

/// Runs `during` while a subprocess holds a lock on `path`'s
/// `start..start+len` byte range (`kind` is `"rdlock"` or `"wrlock"`), then
/// releases it.
pub(crate) fn lock_held_by_subprocess<R>(
    path: &Path,
    kind: &str,
    start: off_t,
    len: off_t,
    during: impl FnOnce() -> R,
) -> R {
    let held = HeldLock::spawn(path, kind, start, len);
    let result = during();
    held.release();
    result
}

/// Spawns one subprocess per `(kind, start, len)` in `specs`, each holding
/// its lock until [`release_all`] is called. Generalizes
/// [`lock_held_by_subprocess`] to "every slot contended at once".
pub(crate) fn hold_multiple(path: &Path, specs: &[(&str, off_t, off_t)]) -> Vec<HeldLock> {
    specs
        .iter()
        .map(|&(kind, start, len)| HeldLock::spawn(path, kind, start, len))
        .collect()
}

pub(crate) fn release_all(held: Vec<HeldLock>) {
    for h in held {
        h.release();
    }
}
