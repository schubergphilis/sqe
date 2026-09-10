// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Virtual filesystem: the read path sqlite-rs uses to open and read
//! database files. Read-only for now (see issue #11) — the write path is
//! deliberately out of scope here. [`VfsFile::lock_shared`] (#50) acquires
//! the journal-mode SHARED lock a safe reader needs before serving pages,
//! surfacing lock contention as [`VfsError::Locked`] (#45). The WAL `-shm`
//! reader-mark protocol and the per-inode fd-cache for the
//! `close()`-drops-all-locks trap are further follow-up tracked in #45.
//!
//! This module is the designated `dyn` boundary (see the `mvl-limit`
//! Makefile target): everything above the VFS stays in the qualified
//! subset. It is no longer an `unsafe` boundary (#66) — `fcntl`/`-shm`
//! access here goes through safe `nix`/`std` APIs, and the crate is
//! `#![forbid(unsafe_code)]` with no local override anywhere.

pub(crate) mod lock;
mod memory;
mod page_source;
pub(crate) mod shm;
#[cfg(test)]
pub(crate) mod test_lock_probe;
mod unix;

pub use lock::{FileLockState, LockLevel};
pub use memory::MemoryVfs;
pub use page_source::{PageError, PageSource, VfsPageSource, WritablePageSource};
pub use unix::UnixVfs;

use std::path::{Path, PathBuf};

/// Failure opening, reading, or locking a database file through the VFS.
#[derive(Debug)]
pub enum VfsError {
    /// No file exists at the given path.
    NotFound {
        /// The path that had no file behind it.
        path: String,
    },

    /// The database is held by another connection's lock.
    Locked {
        /// The path of the locked database file.
        path: String,
    },

    /// The underlying OS file operation failed.
    Io {
        /// The path the failing operation targeted.
        path: String,
        /// The underlying OS error.
        source: std::io::Error,
    },
}

impl std::fmt::Display for VfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VfsError::NotFound { path } => write!(f, "file not found: {path}"),
            VfsError::Locked { path } => write!(f, "database is locked: {path}"),
            VfsError::Io { path, source } => write!(f, "I/O error on {path}: {source}"),
        }
    }
}

impl std::error::Error for VfsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VfsError::NotFound { .. } | VfsError::Locked { .. } => None,
            VfsError::Io { source, .. } => Some(source),
        }
    }
}

/// Shorthand for a [`VfsError`]-producing result.
pub type Result<T> = std::result::Result<T, VfsError>;

/// A source of database files, opened by path.
pub trait Vfs {
    /// Opens `path` for reading.
    fn open_read(&self, path: &Path) -> Result<Box<dyn VfsFile>>;

    /// Opens `path` for reading and writing (#166 pager write path). The
    /// file must already exist — creating new database files is out of
    /// scope here. Callers that only ever read a page (e.g. b-tree
    /// scans through [`VfsPageSource`]) keep using [`Vfs::open_read`] so a
    /// genuinely read-only filesystem is never asked for write access it
    /// doesn't need.
    fn open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>>;

    /// Whether `path` exists — used to detect sibling `-wal` / `-journal`
    /// files.
    fn exists(&self, path: &Path) -> Result<bool>;

    /// Opens `path` for reading and writing, creating it (empty) first if
    /// it doesn't already exist — used to create the `-journal` companion
    /// file on a transaction's first write (#172 rollback journal).
    fn create_or_open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>>;

    /// Removes `path` if it exists; a no-op (not an error) if it doesn't —
    /// used to delete the `-journal` file on commit (#172, DELETE mode).
    fn delete(&self, path: &Path) -> Result<()>;

    /// Claims a WAL reader-mark slot on `path`'s `-shm` companion file (if
    /// one exists) so a live checkpointer backs off rather than
    /// backfilling/truncating WAL frames this reader depends on (#45).
    /// Released when the returned [`FileLock`] drops. Default: a no-op
    /// (`Ok(None)`) — correct for backends with no real `-shm` file to
    /// coordinate through, e.g. [`MemoryVfs`].
    fn claim_wal_read_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let _ = path;
        Ok(None)
    }

    /// Claims the WAL checkpoint lock on `path`'s `-shm` companion file (if
    /// one exists), so a concurrent checkpoint attempt is refused rather
    /// than racing on the same backfill state (#386). Released when the
    /// returned [`FileLock`] drops. Default: a no-op (`Ok(None)`) —
    /// correct for backends with no real `-shm` file to coordinate through.
    fn claim_wal_checkpoint_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let _ = path;
        Ok(None)
    }

    /// The frame marks of readers currently pinned to this WAL generation
    /// (via `path`'s `-shm` companion file), bounding how far a PASSIVE
    /// checkpoint (#386) may safely backfill. Default: empty — no `-shm`
    /// to coordinate through, so nothing constrains the checkpoint.
    fn active_wal_reader_marks(&self, path: &Path) -> Result<Vec<u32>> {
        let _ = path;
        Ok(Vec::new())
    }

    /// Publishes `n_backfill` (how many leading WAL frames a checkpoint
    /// just copied into the main file) to `path`'s `-shm` companion file.
    /// Default: a no-op — nothing to publish without a real `-shm` file.
    fn publish_wal_backfill(&self, path: &Path, n_backfill: u32) -> Result<()> {
        let _ = (path, n_backfill);
        Ok(())
    }

    /// Reads back the `nBackfill` a prior checkpoint published (0 if none
    /// ever ran, or there's no real `-shm` file to read it from).
    fn read_wal_backfill(&self, path: &Path) -> Result<u32> {
        let _ = path;
        Ok(0)
    }

    /// Claims the WAL write lock on `path`'s `-shm` companion file (if one
    /// exists), so only one writer at a time appends frames or advances
    /// `mxFrame` (#389) — a second concurrent writer is refused rather
    /// than interleaving frames or racing the `mxFrame` publish. Released
    /// when the returned [`FileLock`] drops. Default: a no-op (`Ok(None)`)
    /// — correct for backends with no real `-shm` file to coordinate
    /// through, e.g. [`MemoryVfs`].
    fn claim_wal_write_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let _ = path;
        Ok(None)
    }

    /// Publishes a new `mxFrame` (the WAL's current end-of-valid-data, in
    /// frames) to `path`'s `-shm` companion file after a writer commits
    /// one or more frames (#389). Default: a no-op — nothing to publish
    /// without a real `-shm` file.
    fn publish_wal_mx_frame(&self, path: &Path, mx_frame: u32) -> Result<()> {
        let _ = (path, mx_frame);
        Ok(())
    }

    /// Opens a persistent handle to `path`'s `-shm` companion file, meant
    /// to be cached by the caller (`Pager`, #437) for its connection's
    /// whole lifetime rather than reopened on every commit. Unlike
    /// [`Vfs::claim_wal_write_lock`]/[`Vfs::publish_wal_mx_frame`], which
    /// each do their own fresh `open()` and only exist for a single call,
    /// this handle's own `claim_write_lock`/`release_write_lock`/
    /// `publish_mx_frame` methods reuse the same already-open fd across
    /// every subsequent commit — spike 011 (`tests/spike/011_wal_performance`)
    /// found repeated per-commit `open()`+`fstat` on `-shm` to be the
    /// dominant cost in the WAL write path. Default: `Ok(None)` — no real
    /// `-shm` file to cache a handle to (e.g. [`MemoryVfs`]); callers fall
    /// back to the per-call methods above in that case.
    fn open_wal_shm(&self, path: &Path) -> Result<Option<AnyWalShm>> {
        let _ = path;
        Ok(None)
    }
}

/// Builds the path of a companion file (e.g. `-wal`, `-journal`) by
/// appending `suffix` to `path`'s full name — never `.set_extension`, since
/// companion suffixes are appended after the existing `.db` extension, not
/// substituted for it (`test.db` + `-wal` = `test.db-wal`, not `test.wal`).
pub fn companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// A single file opened via [`Vfs::open_read`].
pub trait VfsFile {
    /// Reads into `buf` starting at `offset`, returning the number of bytes
    /// actually read (fewer than `buf.len()` at EOF).
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// The file's total size in bytes.
    fn size(&self) -> Result<u64>;

    /// Acquires a SHARED byte-range lock on the file's journal-mode lock
    /// bytes (`PENDING_BYTE+2` / `SHARED_SIZE`, matching SQLite's
    /// `os_unix.c`) so a concurrent writer can detect this reader per
    /// SQLite's rollback-journal lock ladder — validated to interop
    /// correctly with a live stock `sqlite3` process by spike 005
    /// (`tests/spike/005_locking_interop/findings.md`). Released when the
    /// returned guard is dropped.
    fn lock_shared(&self) -> Result<FileLock>;

    /// Writes `buf` at `offset`, extending the file if `offset + buf.len()`
    /// is past the current end (#166 pager write path).
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()>;

    /// Truncates (or, if `len` is past the current end, extends with
    /// zeros) the file to exactly `len` bytes — used by rollback-journal
    /// recovery to shrink the main file back to its pre-transaction page
    /// count after replaying journaled pages (#172).
    fn truncate(&self, len: u64) -> Result<()>;

    /// Flushes any buffered writes to durable storage.
    fn sync(&self) -> Result<()>;
}

/// A boxed [`VfsFile`], for callers outside `src/vfs/` that need to hold
/// a file handle across several calls without naming `dyn` themselves —
/// same pattern as [`FileLock`] below, one trait earlier. The
/// rollback-journal write path (#172, `src/pager.rs`/`src/pager/journal.rs`)
/// is the motivating caller: it opens a `-journal`/main-file handle once
/// and writes to it across several method calls.
pub struct AnyVfsFile(Box<dyn VfsFile>);

impl From<Box<dyn VfsFile>> for AnyVfsFile {
    fn from(file: Box<dyn VfsFile>) -> Self {
        AnyVfsFile(file)
    }
}

impl AnyVfsFile {
    /// Reads into `buf` starting at `offset` — see [`VfsFile::read_at`].
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.0.read_at(buf, offset)
    }

    /// The file's total size in bytes — see [`VfsFile::size`].
    pub fn size(&self) -> Result<u64> {
        self.0.size()
    }

    /// Writes `buf` at `offset` — see [`VfsFile::write_at`].
    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.0.write_at(buf, offset)
    }

    /// Truncates or extends the file to `len` bytes — see
    /// [`VfsFile::truncate`].
    pub fn truncate(&self, len: u64) -> Result<()> {
        self.0.truncate(len)
    }

    /// Flushes buffered writes — see [`VfsFile::sync`].
    pub fn sync(&self) -> Result<()> {
        self.0.sync()
    }

    /// Acquires a SHARED lock on the underlying file — see
    /// [`VfsFile::lock_shared`].
    pub fn lock_shared(&self) -> Result<FileLock> {
        self.0.lock_shared()
    }

    /// Unwraps back to the boxed [`VfsFile`] this was built from — for
    /// [`WritablePageSource::from_file`](crate::vfs::WritablePageSource::from_file),
    /// the one place outside `src/vfs/` allowed to hold the underlying
    /// `Box<dyn VfsFile>` directly (that module is `mvl-limit`-exempt;
    /// `AnyVfsFile`'s whole purpose elsewhere is letting callers avoid
    /// naming `dyn` themselves).
    pub(crate) fn into_inner(self) -> Box<dyn VfsFile> {
        self.0
    }
}

/// A boxed [`Vfs`], for a long-lived struct outside `src/vfs/` that needs
/// to hold "the `Vfs` it was opened with" without itself naming `dyn` or
/// becoming generic over `V: Vfs` (`Pager`, #172 — it creates/deletes the
/// `-journal` companion file from methods called well after `open`
/// returns, once the original `&V` borrow is long gone).
pub struct AnyVfs(Box<dyn Vfs>);

impl AnyVfs {
    /// Boxes `vfs`, erasing its concrete type.
    pub fn new<V: Vfs + 'static>(vfs: V) -> Self {
        AnyVfs(Box::new(vfs))
    }

    /// Whether `path` exists — see [`Vfs::exists`].
    pub fn exists(&self, path: &Path) -> Result<bool> {
        self.0.exists(path)
    }

    /// Opens `path` for reading — see [`Vfs::open_read`].
    pub fn open_read(&self, path: &Path) -> Result<AnyVfsFile> {
        self.0.open_read(path).map(AnyVfsFile::from)
    }

    /// Opens `path` for reading and writing — see [`Vfs::open_write`].
    pub fn open_write(&self, path: &Path) -> Result<AnyVfsFile> {
        self.0.open_write(path).map(AnyVfsFile::from)
    }

    /// Opens `path` for reading and writing, creating it if needed — see
    /// [`Vfs::create_or_open_write`].
    pub fn create_or_open_write(&self, path: &Path) -> Result<AnyVfsFile> {
        self.0.create_or_open_write(path).map(AnyVfsFile::from)
    }

    /// Removes `path` if it exists — see [`Vfs::delete`].
    pub fn delete(&self, path: &Path) -> Result<()> {
        self.0.delete(path)
    }

    /// Claims the WAL checkpoint lock on `path` — see
    /// [`Vfs::claim_wal_checkpoint_lock`].
    pub fn claim_wal_checkpoint_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        self.0.claim_wal_checkpoint_lock(path)
    }

    /// The frame marks of readers pinned to `path`'s WAL generation — see
    /// [`Vfs::active_wal_reader_marks`].
    pub fn active_wal_reader_marks(&self, path: &Path) -> Result<Vec<u32>> {
        self.0.active_wal_reader_marks(path)
    }

    /// Publishes `n_backfill` for `path` — see [`Vfs::publish_wal_backfill`].
    pub fn publish_wal_backfill(&self, path: &Path, n_backfill: u32) -> Result<()> {
        self.0.publish_wal_backfill(path, n_backfill)
    }

    /// Reads back the last published `nBackfill` for `path` — see
    /// [`Vfs::read_wal_backfill`].
    pub fn read_wal_backfill(&self, path: &Path) -> Result<u32> {
        self.0.read_wal_backfill(path)
    }

    /// Claims the WAL write lock on `path` — see
    /// [`Vfs::claim_wal_write_lock`].
    pub fn claim_wal_write_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        self.0.claim_wal_write_lock(path)
    }

    /// Publishes a new `mxFrame` for `path` — see
    /// [`Vfs::publish_wal_mx_frame`].
    pub fn publish_wal_mx_frame(&self, path: &Path, mx_frame: u32) -> Result<()> {
        self.0.publish_wal_mx_frame(path, mx_frame)
    }

    /// Opens a persistent `-shm` handle for `path` — see
    /// [`Vfs::open_wal_shm`].
    pub fn open_wal_shm(&self, path: &Path) -> Result<Option<AnyWalShm>> {
        self.0.open_wal_shm(path)
    }
}

/// A held file lock, released when dropped. Opaque on purpose: it hides
/// `dyn SharedLockGuard` behind a concrete type so callers outside
/// `src/vfs/` (e.g. [`crate::pager::Pager`]) never need to write `dyn`
/// themselves — this module is the qualified-subset gate's designated
/// `dyn` boundary (see the `mvl-limit` Makefile target).
pub struct FileLock(Box<dyn SharedLockGuard>);

impl std::fmt::Debug for FileLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileLock(..)")
    }
}

impl FileLock {
    /// Non-blocking probe: does some *other* process hold a RESERVED (or
    /// higher) lock on this file right now? Used by hot-journal recovery
    /// (`src/pager.rs::Pager::open`) to decide whether replaying a hot
    /// journal is safe — SQLite's `sqlite3OsCheckReservedLock`. Default
    /// `Ok(false)` for backends with no real concurrent process to
    /// coordinate with (e.g. [`MemoryVfs`]).
    pub(crate) fn check_reserved(&self) -> Result<bool> {
        self.0.check_reserved()
    }

    /// Escalates this held SHARED lock to EXCLUSIVE, stepping through
    /// RESERVED and PENDING on the way (see
    /// [`crate::vfs::lock::FileLockState::set_level`]) so a concurrent
    /// holder of any level in the ladder makes this call fail fast instead
    /// of silently interleaving. Two callers: `Pager::open`'s hot-journal
    /// recovery (`os_unix.c`'s `sqlite3PagerSharedLock` also takes
    /// EXCLUSIVE there, never lingering at RESERVED — a live RESERVED lock
    /// is how a second opener recognizes "someone already validated this
    /// journal and is rolling it back", so pausing here would let a racing
    /// opener wrongly conclude the database is safe to read while recovery
    /// is still in flight); and `Pager::flush`, which needs real mutual
    /// exclusion against another connection's own `flush`/recovery for the
    /// duration of its journal-write/page-write/journal-delete sequence.
    pub(crate) fn escalate_to_exclusive(&mut self) -> Result<()> {
        self.0.escalate_to_exclusive()
    }

    /// Reverses [`FileLock::escalate_to_exclusive`] once recovery or
    /// [`crate::pager::Pager::flush`] finishes, returning to the plain
    /// reader lock.
    pub(crate) fn de_escalate_to_shared(&mut self) -> Result<()> {
        self.0.de_escalate_to_shared()
    }

    /// Steps this held lock to exactly `level`, up or down the ladder as
    /// needed (see [`crate::vfs::lock::FileLockState::set_level`]).
    /// Generalizes [`FileLock::escalate_to_exclusive`]/
    /// [`FileLock::de_escalate_to_shared`] for callers that need an
    /// arbitrary target — `BEGIN IMMEDIATE`/`EXCLUSIVE` escalating to
    /// RESERVED/EXCLUSIVE at `BEGIN` time (#395), and `Pager::flush`
    /// releasing back to whatever level the transaction held before its
    /// own transient EXCLUSIVE escalation.
    pub(crate) fn set_level(&mut self, level: lock::LockLevel) -> Result<()> {
        self.0.set_level(level)
    }
}

/// Implemented next to each [`VfsFile`] backend (e.g. the Unix backend's
/// real `fcntl` lock, or a no-op for the in-memory backend).
trait SharedLockGuard {
    /// See [`FileLock::check_reserved`]. Default: no other process to
    /// contend with.
    fn check_reserved(&self) -> Result<bool> {
        Ok(false)
    }

    /// See [`FileLock::escalate_to_exclusive`]. Default: no-op.
    fn escalate_to_exclusive(&mut self) -> Result<()> {
        Ok(())
    }

    /// See [`FileLock::de_escalate_to_shared`]. Default: no-op.
    fn de_escalate_to_shared(&mut self) -> Result<()> {
        Ok(())
    }

    /// See [`FileLock::set_level`]. Default: no-op — backends with no real
    /// concurrent process to coordinate with (e.g. [`MemoryVfs`]) have
    /// nothing to step.
    fn set_level(&mut self, _level: lock::LockLevel) -> Result<()> {
        Ok(())
    }
}

/// A persistent handle to a `-shm` companion file, returned by
/// [`Vfs::open_wal_shm`] (#437) and cached by [`crate::pager::Pager`] for
/// its whole connection lifetime — see that method's doc comment for why.
/// Erases the concrete backend type behind a boxed trait object, same
/// pattern as [`FileLock`]/[`AnyVfsFile`] just above: this module is the
/// designated `dyn` boundary, so `Pager` never has to name `dyn` itself.
pub struct AnyWalShm(Box<dyn WalShm>);

impl std::fmt::Debug for AnyWalShm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AnyWalShm(..)")
    }
}

impl From<Box<dyn WalShm>> for AnyWalShm {
    fn from(handle: Box<dyn WalShm>) -> Self {
        AnyWalShm(handle)
    }
}

impl AnyWalShm {
    /// Claims the `WAL_WRITE_LOCK` byte on this handle's already-open fd
    /// (no fresh `open()`/`fstat`, unlike [`Vfs::claim_wal_write_lock`]).
    /// Pairs with [`AnyWalShm::release_write_lock`], which the caller must
    /// call even on an early `?` return — [`crate::pager::Pager`] does
    /// this via a small `Drop`-based guard, the same shape
    /// [`FileLock`]'s own callers already use elsewhere.
    pub(crate) fn claim_write_lock(&self) -> Result<()> {
        self.0.claim_write_lock()
    }

    /// Releases the `WAL_WRITE_LOCK` byte claimed by
    /// [`AnyWalShm::claim_write_lock`]. Idempotent-in-practice (unlocking
    /// an already-unlocked byte range succeeds as a no-op), but callers
    /// should still pair each claim with exactly one release.
    pub(crate) fn release_write_lock(&self) -> Result<()> {
        self.0.release_write_lock()
    }

    /// Publishes `mx_frame` through this same held handle's fd, instead
    /// of [`Vfs::publish_wal_mx_frame`] reopening `-shm` a second time.
    pub(crate) fn publish_mx_frame(&self, mx_frame: u32) -> Result<()> {
        self.0.publish_mx_frame(mx_frame)
    }
}

/// Implemented next to each [`VfsFile`] backend's real `-shm` handle
/// (e.g. the Unix backend's real `fcntl`-lockable fd) — see
/// [`Vfs::open_wal_shm`].
trait WalShm {
    /// See [`AnyWalShm::claim_write_lock`].
    fn claim_write_lock(&self) -> Result<()>;

    /// See [`AnyWalShm::release_write_lock`].
    fn release_write_lock(&self) -> Result<()>;

    /// See [`AnyWalShm::publish_mx_frame`].
    fn publish_mx_frame(&self, mx_frame: u32) -> Result<()>;
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::arithmetic_side_effects
)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The contract every `Vfs` implementation must satisfy — run against
    /// both `UnixVfs` and `MemoryVfs` below.
    fn run_contract(vfs: impl Vfs, present: &Path, absent: &Path, contents: &[u8]) {
        assert!(vfs.exists(present).unwrap());
        assert!(!vfs.exists(absent).unwrap());
        assert!(vfs.open_read(absent).is_err());

        let file = vfs.open_read(present).unwrap();
        assert_eq!(file.size().unwrap(), contents.len() as u64);

        let mut buf = vec![0u8; contents.len()];
        let n = file.read_at(&mut buf, 0).unwrap();
        assert_eq!(n, contents.len());
        assert_eq!(buf, contents);

        let mut mid = vec![0u8; 4];
        let n = file.read_at(&mut mid, 2).unwrap();
        assert_eq!(n, 4);
        assert_eq!(mid, contents[2..6]);

        let mut past_eof = vec![0u8; 4];
        let n = file
            .read_at(&mut past_eof, contents.len() as u64 + 10)
            .unwrap();
        assert_eq!(n, 0);

        drop(file.lock_shared().unwrap());
    }

    /// A write through [`Vfs::open_write`] must be visible to a fresh
    /// [`Vfs::open_read`] handle on the same path — the contract every
    /// backend's write path must satisfy.
    fn run_write_contract(vfs: impl Vfs, path: &Path) {
        let write_file = vfs.open_write(path).unwrap();
        write_file.write_at(b"WXYZ", 2).unwrap();
        write_file.sync().unwrap();

        let read_file = vfs.open_read(path).unwrap();
        let mut buf = vec![0u8; 6];
        read_file.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"heWXYZ");
    }

    #[test]
    fn memory_vfs_contract() {
        let mut vfs = MemoryVfs::new();
        let contents = b"hello sqlite-rs vfs contract".to_vec();
        vfs.insert("/present.db", contents.clone());
        vfs.insert("/writable.db", contents.clone());
        run_write_contract(vfs.clone(), Path::new("/writable.db"));
        run_contract(
            vfs,
            Path::new("/present.db"),
            Path::new("/absent.db"),
            &contents,
        );
    }

    #[test]
    fn companion_file_detection() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", b"main file".to_vec());
        vfs.insert("/test.db-wal", b"wal file".to_vec());
        vfs.insert("/test.db-journal", b"journal file".to_vec());

        assert!(vfs
            .exists(&companion_path(Path::new("/test.db"), "-wal"))
            .unwrap());
        assert!(vfs
            .exists(&companion_path(Path::new("/test.db"), "-journal"))
            .unwrap());
        assert!(!vfs
            .exists(&companion_path(Path::new("/other.db"), "-wal"))
            .unwrap());
    }

    #[test]
    fn unix_vfs_contract() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("sqlite-rs-vfs-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let present = dir.join("present.db");
        let absent = dir.join("absent.db");
        let contents = b"hello sqlite-rs vfs contract".to_vec();
        std::fs::write(&present, &contents).unwrap();
        let writable = dir.join("writable.db");
        std::fs::write(&writable, &contents).unwrap();
        run_write_contract(UnixVfs, &writable);

        run_contract(UnixVfs, &present, &absent, &contents);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
