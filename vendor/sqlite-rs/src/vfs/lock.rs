// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Journal-mode SHARED byte-range locking via `crate::sys::fcntl` (a
//! vendored safe wrapper over POSIX `fcntl(F_SETLK)`, #563 — previously
//! `nix::fcntl`). `src/vfs/` used to be the crate's sole
//! `#![allow(unsafe_code)]` carve-out (see the Makefile's `mvl-limit`
//! boundary-policy comment); with this module's `unsafe fcntl`/`mmap`/
//! `fork` calls replaced by safe `nix`/`std` APIs (#66), that carve-out
//! moved to `src/sys/` — see `.openspec/adr/0031-vendor-nix-subset.md`.
//!
//! Byte offsets verified against SQLite's own source (`os_unix.c`) by
//! spike 005 (`tests/spike/005_locking_interop/findings.md`) — not
//! re-derived here. Busy detection is mapped one layer up, in
//! `src/vfs/unix.rs`'s `to_lock_error`. The WAL `-shm` reader-mark protocol
//! is out of scope for this module; see #45.

use std::fs::File;
use std::io;

use crate::sys::fcntl::{fcntl_call, off_t, FcntlArg, F_RDLCK, F_UNLCK, F_WRLCK};

/// SQLite's `PENDING_BYTE` (`os_unix.c`): base of the reserved lock-byte
/// page.
const PENDING_BYTE: off_t = 0x40000000;
/// `RESERVED_BYTE` (`os_unix.c`): `PENDING_BYTE + 1`.
const RESERVED_BYTE: off_t = PENDING_BYTE + 1;
/// `SHARED_FIRST` (`os_unix.c`): first byte of the SHARED-lock range.
const SHARED_FIRST: off_t = PENDING_BYTE + 2;
/// `SHARED_SIZE` (`os_unix.c`): width of the SHARED-lock range.
const SHARED_SIZE: off_t = 510;

/// SQLite's 5-state journal-mode lock ladder (`os_unix.c`'s `unixLock`),
/// ordered `Unlocked < Shared < Reserved < Pending < Exclusive` so callers
/// can compare levels directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockLevel {
    /// No lock held.
    Unlocked,
    /// Read lock on the SHARED byte range — any number of readers may hold
    /// this concurrently.
    Shared,
    /// Write lock on `RESERVED_BYTE`, held alongside the SHARED read lock —
    /// signals "about to write" to other readers/writers without blocking
    /// them yet. At most one holder at a time.
    Reserved,
    /// Write lock on `PENDING_BYTE`, held alongside RESERVED — blocks any
    /// *new* SHARED lock attempt (existing readers may finish and drop
    /// out) while this writer waits for them to drain before going
    /// EXCLUSIVE.
    Pending,
    /// Write lock across the whole SHARED byte range (upgraded from the
    /// read lock), held alongside PENDING and RESERVED — exclusive access,
    /// blocks every other lock level.
    Exclusive,
}

/// A file's held lock, tracking its current [`LockLevel`] and transitioning
/// between levels via real `fcntl` byte-range locks — SQLite's journal-mode
/// lock ladder (`os_unix.c`'s `unixLock`/`unixUnlock`), byte-identical so it
/// interoperates with a live stock `sqlite3` process (validated by spike
/// 005, `tests/spike/005_locking_interop/findings.md`). All locks are
/// released when dropped.
pub struct FileLockState {
    file: File,
    level: LockLevel,
}

impl FileLockState {
    /// Wraps `file` with no lock held yet (`LockLevel::Unlocked`).
    pub fn new(file: File) -> Self {
        FileLockState {
            file,
            level: LockLevel::Unlocked,
        }
    }

    /// The lock level currently held.
    pub fn lock_state(&self) -> LockLevel {
        self.level
    }

    /// Non-blocking probe: whether some *other* process currently holds a
    /// write lock on `RESERVED_BYTE` — SQLite's `sqlite3OsCheckReservedLock`
    /// (`os_unix.c`). RESERVED is held for the whole `Reserved`/`Pending`/
    /// `Exclusive` span of the ladder (see this enum's doc comments), so
    /// this single byte-range probe catches all three. Uses `F_GETLK`
    /// (test-only, never blocks and never acquires anything) rather than
    /// `F_SETLK`, so it never disturbs a level this process already holds.
    pub fn check_reserved(&self) -> io::Result<bool> {
        check_reserved_lock(&self.file)
    }

    /// The underlying file, for callers that need to read/write the same
    /// fd this lock ladder tracks — never a second, independently-opened
    /// fd to the same path (POSIX `fcntl` locks are scoped to `(process,
    /// inode)`, not the open file description: closing *any* fd this
    /// process holds to the file drops the lock for all of them).
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// Whether the held level reserves the right to write (`Reserved`,
    /// `Pending`, or `Exclusive` — the levels past plain `Shared` reading).
    pub fn is_write_locked(&self) -> bool {
        self.level >= LockLevel::Reserved
    }

    /// Transitions to `target`, acquiring or releasing the intermediate
    /// byte ranges one step at a time so the level actually held always
    /// matches a real level in the ladder — never a half-acquired state.
    /// `Err` (lock contention or I/O failure) leaves `self.level` at the
    /// last level successfully reached.
    pub fn set_level(&mut self, target: LockLevel) -> io::Result<()> {
        while self.level < target {
            self.step_up()?;
        }
        while self.level > target {
            self.step_down()?;
        }
        Ok(())
    }

    fn step_up(&mut self) -> io::Result<()> {
        match self.level {
            LockLevel::Unlocked => {
                // `os_unix.c`'s `unixLock`: a plain fcntl read lock on
                // SHARED_FIRST wouldn't itself conflict with a writer's
                // PENDING_BYTE write lock (different byte ranges), so a
                // new reader must explicitly probe PENDING_BYTE first —
                // that's what actually stops a new SHARED lock from
                // starting once a writer is mid-ladder.
                fcntl_lock(&self.file, F_RDLCK, PENDING_BYTE, 1)?;
                fcntl_lock(&self.file, F_UNLCK, PENDING_BYTE, 1)?;
                fcntl_lock(&self.file, F_RDLCK, SHARED_FIRST, SHARED_SIZE)?;
                self.level = LockLevel::Shared;
            }
            LockLevel::Shared => {
                fcntl_lock(&self.file, F_WRLCK, RESERVED_BYTE, 1)?;
                self.level = LockLevel::Reserved;
            }
            LockLevel::Reserved => {
                fcntl_lock(&self.file, F_WRLCK, PENDING_BYTE, 1)?;
                self.level = LockLevel::Pending;
            }
            LockLevel::Pending => {
                fcntl_lock(&self.file, F_WRLCK, SHARED_FIRST, SHARED_SIZE)?;
                self.level = LockLevel::Exclusive;
            }
            LockLevel::Exclusive => {}
        }
        Ok(())
    }

    fn step_down(&mut self) -> io::Result<()> {
        match self.level {
            LockLevel::Exclusive => {
                // Downgrade the SHARED range back to a read lock rather
                // than dropping it — PENDING is still held below, so this
                // stays a real ladder level (Pending), not a gap.
                fcntl_lock(&self.file, F_RDLCK, SHARED_FIRST, SHARED_SIZE)?;
                self.level = LockLevel::Pending;
            }
            LockLevel::Pending => {
                fcntl_lock(&self.file, F_UNLCK, PENDING_BYTE, 1)?;
                self.level = LockLevel::Reserved;
            }
            LockLevel::Reserved => {
                fcntl_lock(&self.file, F_UNLCK, RESERVED_BYTE, 1)?;
                self.level = LockLevel::Shared;
            }
            LockLevel::Shared => {
                fcntl_lock(&self.file, F_UNLCK, SHARED_FIRST, SHARED_SIZE)?;
                self.level = LockLevel::Unlocked;
            }
            LockLevel::Unlocked => {}
        }
        Ok(())
    }
}

impl Drop for FileLockState {
    fn drop(&mut self) {
        // Best-effort: a `drop` can't propagate failure, and there's
        // nothing more to do about one anyway.
        self.set_level(LockLevel::Unlocked).ok();
    }
}

/// Whether some other process holds a write lock overlapping
/// `RESERVED_BYTE`, via `fcntl(F_GETLK)` — a query, never an acquisition.
/// If this process itself already holds the byte, `F_GETLK` reports
/// `F_UNLCK` (a process never conflicts with its own lock), which is
/// exactly the "am I clear to escalate" answer callers need.
/// `SEEK_SET`: shares the same numeric value (0) on macOS and Linux.
const SEEK_SET: i16 = 0;

fn check_reserved_lock(file: &File) -> io::Result<bool> {
    let mut fl = crate::sys::fcntl::flock {
        l_type: F_WRLCK,
        l_whence: SEEK_SET,
        l_start: RESERVED_BYTE,
        l_len: 1,
        l_pid: 0,
    };
    fcntl_call(file, FcntlArg::F_GETLK(&mut fl))?;
    Ok(fl.l_type != F_UNLCK)
}

/// Generic byte-range `fcntl(F_SETLK)` primitive — used both for the
/// journal-mode SHARED lock above and (via `pub(crate)`) for the WAL
/// `-shm` reader-mark lock bytes in `src/vfs/shm.rs`; the underlying
/// syscall is identical, only the byte offsets differ.
pub(crate) fn fcntl_lock(file: &File, kind: i16, start: off_t, len: off_t) -> io::Result<()> {
    let fl = crate::sys::fcntl::flock {
        l_type: kind,
        l_whence: SEEK_SET,
        l_start: start,
        l_len: len,
        l_pid: 0,
    };
    fcntl_call(file, FcntlArg::F_SETLK(&fl)).map(|_| ())
}

/// Test-only: whether a non-blocking EXCLUSIVE lock on `path`'s SHARED-lock
/// byte range would currently succeed, probed via a real second OS process
/// (`src/vfs/test_lock_probe.rs`) — needed by `src/pager/mod.rs`'s tests,
/// which observe `Pager::open`/`drop` lock state from outside this module.
#[cfg(test)]
pub(crate) fn exclusive_lock_available(path: &std::path::Path) -> bool {
    super::test_lock_probe::lock_available(path, "wrlock", SHARED_FIRST, SHARED_SIZE)
}

/// Test-only: `(start, len)` of the RESERVED byte, for cross-module tests
/// that simulate another process holding it via a real subprocess
/// (`src/pager.rs`'s hot-journal-vs-live-writer test, #359).
#[cfg(test)]
pub(crate) fn reserved_byte_range() -> (off_t, off_t) {
    (RESERVED_BYTE, 1)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::vfs::test_lock_probe::lock_held_by_subprocess;

    fn temp_file() -> (std::fs::File, PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("sqlite-rs-lock-test-{}-{n}", std::process::id()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        (file, path)
    }

    #[test]
    fn lock_shared_succeeds_on_a_fresh_file() {
        let (file, _path) = temp_file();
        let mut lock = FileLockState::new(file);
        assert!(lock.set_level(LockLevel::Shared).is_ok());
    }

    #[test]
    fn shared_lock_blocks_concurrent_exclusive_lock_until_dropped() {
        let (file, path) = temp_file();
        let mut lock = FileLockState::new(file);
        lock.set_level(LockLevel::Shared).unwrap();

        assert!(
            !exclusive_lock_available(&path),
            "a held SHARED lock must block a concurrent EXCLUSIVE lock"
        );

        drop(lock);

        assert!(
            exclusive_lock_available(&path),
            "dropping the lock must release the SHARED lock"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// A SHARED lock contending with a real EXCLUSIVE lock held by another
    /// OS process (not just this process re-locking, which `fcntl` never
    /// sees as contention) must surface as lock contention (`EAGAIN`/
    /// `EACCES`), which `src/vfs/unix.rs`'s `to_lock_error` turns into
    /// `VfsError::Locked` — 001-architecture Req-4's busy-detection
    /// scenario.
    #[test]
    fn lock_shared_fails_with_contention_errno_when_exclusively_held_elsewhere() {
        use crate::vfs::{UnixVfs, Vfs, VfsError};

        let (_file, path) = temp_file();

        let result = lock_held_by_subprocess(&path, "wrlock", SHARED_FIRST, SHARED_SIZE, || {
            let file = UnixVfs.open_read(&path).unwrap();
            file.lock_shared()
        });

        match result {
            Err(VfsError::Locked { .. }) => {}
            Err(other) => panic!("expected VfsError::Locked, got {other:?}"),
            Ok(_) => panic!("expected VfsError::Locked, got Ok"),
        }

        std::fs::remove_file(&path).unwrap();
    }

    fn reserved_lock_available(path: &std::path::Path) -> bool {
        crate::vfs::test_lock_probe::lock_available(path, "wrlock", RESERVED_BYTE, 1)
    }

    fn pending_lock_available(path: &std::path::Path) -> bool {
        crate::vfs::test_lock_probe::lock_available(path, "wrlock", PENDING_BYTE, 1)
    }

    fn shared_read_available(path: &std::path::Path) -> bool {
        crate::vfs::test_lock_probe::lock_available(path, "rdlock", SHARED_FIRST, SHARED_SIZE)
    }

    #[test]
    fn levels_climb_and_report_write_locked_from_reserved_up() {
        let (file, _path) = temp_file();
        let mut lock = FileLockState::new(file);

        assert_eq!(lock.lock_state(), LockLevel::Unlocked);
        assert!(!lock.is_write_locked());

        lock.set_level(LockLevel::Shared).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Shared);
        assert!(!lock.is_write_locked());

        lock.set_level(LockLevel::Reserved).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Reserved);
        assert!(lock.is_write_locked());

        lock.set_level(LockLevel::Pending).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Pending);
        assert!(lock.is_write_locked());

        lock.set_level(LockLevel::Exclusive).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Exclusive);
        assert!(lock.is_write_locked());
    }

    #[test]
    fn set_level_can_jump_straight_to_exclusive_and_back_to_unlocked() {
        let (file, _path) = temp_file();
        let mut lock = FileLockState::new(file);

        lock.set_level(LockLevel::Exclusive).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Exclusive);

        lock.set_level(LockLevel::Unlocked).unwrap();
        assert_eq!(lock.lock_state(), LockLevel::Unlocked);
    }

    #[test]
    fn reserved_blocks_a_second_reserved_but_not_new_shared_readers() {
        let (file, path) = temp_file();
        let mut lock = FileLockState::new(file);
        lock.set_level(LockLevel::Reserved).unwrap();

        assert!(
            !reserved_lock_available(&path),
            "RESERVED must be exclusive: at most one holder at a time"
        );
        assert!(
            shared_read_available(&path),
            "RESERVED must not block new SHARED readers"
        );

        drop(lock);
        assert!(reserved_lock_available(&path));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn pending_blocks_new_shared_readers() {
        // A raw fcntl read lock on the SHARED range is a different byte
        // range from PENDING_BYTE, so the OS alone would never stop a new
        // reader — SQLite's protection is a cooperating-process convention
        // (`unixLock`'s own PENDING_BYTE probe before taking SHARED),
        // which is exactly what `FileLockState::set_level` implements. So
        // this contends via a real subprocess holding PENDING_BYTE (POSIX
        // record locks don't contend with themselves within one process),
        // not the in-process `FileLockState` used elsewhere in this file.
        let (file, path) = temp_file();

        let (level_while_blocked, was_err, pending_still_exclusive) =
            lock_held_by_subprocess(&path, "wrlock", PENDING_BYTE, 1, || {
                let mut reader = FileLockState::new(file);
                let err = reader.set_level(LockLevel::Shared);
                (
                    reader.lock_state(),
                    err.is_err(),
                    !pending_lock_available(&path),
                )
            });
        assert!(was_err, "PENDING must block a new SHARED lock attempt");
        assert_eq!(level_while_blocked, LockLevel::Unlocked);
        assert!(
            pending_still_exclusive,
            "PENDING must be exclusive: at most one holder at a time"
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn exclusive_blocks_shared_readers() {
        let (file, path) = temp_file();
        let mut lock = FileLockState::new(file);
        lock.set_level(LockLevel::Exclusive).unwrap();

        assert!(
            !shared_read_available(&path),
            "EXCLUSIVE must block every other lock level, including SHARED reads"
        );

        drop(lock);
        assert!(shared_read_available(&path));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn downgrading_from_exclusive_to_shared_releases_write_locks_but_keeps_reading() {
        let (file, path) = temp_file();
        let mut lock = FileLockState::new(file);
        lock.set_level(LockLevel::Exclusive).unwrap();
        lock.set_level(LockLevel::Shared).unwrap();

        assert_eq!(lock.lock_state(), LockLevel::Shared);
        assert!(!lock.is_write_locked());
        assert!(
            reserved_lock_available(&path),
            "downgrading past RESERVED must release the RESERVED_BYTE write lock"
        );
        assert!(
            pending_lock_available(&path),
            "downgrading past PENDING must release the PENDING_BYTE write lock"
        );
        assert!(
            !exclusive_lock_available(&path),
            "the SHARED read lock itself must still block a concurrent EXCLUSIVE"
        );

        std::fs::remove_file(&path).unwrap();
    }
}
