// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Unix `Vfs` implementation, backed by `std::fs`.

use std::cell::RefCell;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::sys::fcntl::{EACCES, EAGAIN};

use super::{companion_path, lock, shm, FileLock, Result, SharedLockGuard, Vfs, VfsError, VfsFile};

/// Reads database files directly from the local filesystem via `std::fs`.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnixVfs;

impl Vfs for UnixVfs {
    fn open_read(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let file = File::open(path).map_err(|source| to_vfs_error(path, source))?;
        Ok(Box::new(UnixVfsFile::new(file, path)))
    }

    fn open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| to_vfs_error(path, source))?;
        Ok(Box::new(UnixVfsFile::new(file, path)))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        path.try_exists()
            .map_err(|source| to_vfs_error(path, source))
    }

    fn create_or_open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|source| to_vfs_error(path, source))?;
        Ok(Box::new(UnixVfsFile::new(file, path)))
    }

    fn delete(&self, path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(to_vfs_error(path, source)),
        }
    }

    fn claim_wal_read_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let shm_path = companion_path(path, "-shm");
        shm::claim_wal_read_lock(&shm_path)
            .map(|opt| opt.map(|guard| FileLock(Box::new(guard))))
            .map_err(|source| to_lock_error(&shm_path, source))
    }

    fn claim_wal_checkpoint_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(None);
        }
        shm::claim_wal_checkpoint_lock(&shm_path)
            .map(|guard| Some(FileLock(Box::new(guard))))
            .map_err(|source| to_lock_error(&shm_path, source))
    }

    fn active_wal_reader_marks(&self, path: &Path) -> Result<Vec<u32>> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(Vec::new());
        }
        shm::active_reader_marks(&shm_path).map_err(|source| to_vfs_error(&shm_path, source))
    }

    fn publish_wal_backfill(&self, path: &Path, n_backfill: u32) -> Result<()> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(());
        }
        shm::publish_backfill(&shm_path, n_backfill)
            .map_err(|source| to_vfs_error(&shm_path, source))
    }

    fn read_wal_backfill(&self, path: &Path) -> Result<u32> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(0);
        }
        shm::read_backfill(&shm_path).map_err(|source| to_vfs_error(&shm_path, source))
    }

    fn claim_wal_write_lock(&self, path: &Path) -> Result<Option<FileLock>> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(None);
        }
        shm::claim_wal_write_lock(&shm_path)
            .map(|guard| Some(FileLock(Box::new(guard))))
            .map_err(|source| to_lock_error(&shm_path, source))
    }

    fn publish_wal_mx_frame(&self, path: &Path, mx_frame: u32) -> Result<()> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(());
        }
        shm::publish_mx_frame(&shm_path, mx_frame).map_err(|source| to_vfs_error(&shm_path, source))
    }

    fn open_wal_shm(&self, path: &Path) -> Result<Option<crate::vfs::AnyWalShm>> {
        let shm_path = companion_path(path, "-shm");
        if !shm_path.exists() {
            return Ok(None);
        }
        shm::open_wal_shm(&shm_path)
            .map(|handle| {
                Some(crate::vfs::AnyWalShm::from(
                    Box::new(handle) as Box<dyn crate::vfs::WalShm>
                ))
            })
            .map_err(|source| to_vfs_error(&shm_path, source))
    }
}

/// A single fd, shared (via `Rc`) between this file's I/O and any
/// [`FileLock`] `lock_shared` hands out — never a second, independently-
/// opened fd to the same path. `Pager::open`'s hot-journal recovery reads,
/// writes, and locks the main database file through this one handle end to
/// end, sidestepping the "`close()` drops all `fcntl` locks on the inode"
/// trap (POSIX `fcntl` locks are scoped to `(process, inode)`, not the open
/// file description — see [`lock::FileLockState::file`]).
struct UnixVfsFile {
    lock: Rc<RefCell<lock::FileLockState>>,
    path: PathBuf,
}

impl UnixVfsFile {
    fn new(file: File, path: &Path) -> Self {
        UnixVfsFile {
            lock: Rc::new(RefCell::new(lock::FileLockState::new(file))),
            path: path.to_path_buf(),
        }
    }
}

impl VfsFile for UnixVfsFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.lock
            .borrow()
            .file()
            .read_at(buf, offset)
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    fn size(&self) -> Result<u64> {
        self.lock
            .borrow()
            .file()
            .metadata()
            .map(|m| m.len())
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    fn lock_shared(&self) -> Result<FileLock> {
        self.lock
            .borrow_mut()
            .set_level(lock::LockLevel::Shared)
            .map_err(|source| to_lock_error(&self.path, source))?;
        Ok(FileLock(Box::new(UnixLockGuard {
            lock: Rc::clone(&self.lock),
            path: self.path.clone(),
        })))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.lock
            .borrow()
            .file()
            .write_all_at(buf, offset)
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    fn truncate(&self, len: u64) -> Result<()> {
        self.lock
            .borrow()
            .file()
            .set_len(len)
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    // On macOS, `std::fs::File::sync_data` upgrades to
    // `fcntl(F_FULLFSYNC)` — a full flush past the drive's write cache —
    // rather than a plain `fsync()`. That's a much stronger (and ~80x
    // slower on this crate's own dev hardware, #652) guarantee than real
    // SQLite's own default on the same platform: `PRAGMA fullfsync`
    // governs exactly this and defaults to off, so `synchronous=FULL`
    // alone calls plain `fsync()` there. `crate::sys::fcntl::fsync`
    // matches that default; see its doc comment. Linux is unaffected —
    // `sync_data` already calls plain `fdatasync` there, matching
    // SQLite's own Linux default — so only the macOS path is routed
    // through the vendored wrapper.
    #[cfg(target_os = "macos")]
    fn sync(&self) -> Result<()> {
        crate::sys::fcntl::fsync(self.lock.borrow().file())
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    #[cfg(not(target_os = "macos"))]
    fn sync(&self) -> Result<()> {
        self.lock
            .borrow()
            .file()
            .sync_data()
            .map_err(|source| to_vfs_error(&self.path, source))
    }
}

/// Returned by [`UnixVfsFile::lock_shared`]: holds the fd's shared lock
/// ladder at `Shared` (or, briefly, `Exclusive` for hot-journal recovery —
/// [`FileLock::escalate_to_exclusive`]) until dropped.
struct UnixLockGuard {
    lock: Rc<RefCell<lock::FileLockState>>,
    path: PathBuf,
}

impl SharedLockGuard for UnixLockGuard {
    fn check_reserved(&self) -> Result<bool> {
        self.lock
            .borrow()
            .check_reserved()
            .map_err(|source| to_vfs_error(&self.path, source))
    }

    fn escalate_to_exclusive(&mut self) -> Result<()> {
        self.lock
            .borrow_mut()
            .set_level(lock::LockLevel::Exclusive)
            .map_err(|source| to_lock_error(&self.path, source))
    }

    fn de_escalate_to_shared(&mut self) -> Result<()> {
        self.lock
            .borrow_mut()
            .set_level(lock::LockLevel::Shared)
            .map_err(|source| to_lock_error(&self.path, source))
    }

    fn set_level(&mut self, level: lock::LockLevel) -> Result<()> {
        self.lock
            .borrow_mut()
            .set_level(level)
            .map_err(|source| to_lock_error(&self.path, source))
    }
}

impl Drop for UnixLockGuard {
    fn drop(&mut self) {
        // Best-effort, matching `FileLockState`'s own `Drop`: a `drop`
        // can't propagate failure, and there is nothing more to do about
        // one anyway. The fd stays open via `UnixVfsFile`'s own `Rc`
        // clone — only the lock level this guard represents is released.
        self.lock
            .borrow_mut()
            .set_level(lock::LockLevel::Unlocked)
            .ok();
    }
}

fn to_vfs_error(path: &Path, source: std::io::Error) -> VfsError {
    let path_str = path.display().to_string();
    if source.kind() == std::io::ErrorKind::NotFound {
        VfsError::NotFound { path: path_str }
    } else {
        VfsError::Io {
            path: path_str,
            source,
        }
    }
}

/// Like [`to_vfs_error`], but maps `fcntl(F_SETLK)`'s lock-contention errno
/// values (`EAGAIN`/`EACCES` — POSIX allows either, `fcntl(2)`) to
/// [`VfsError::Locked`] so callers can distinguish "another process holds
/// this lock" from an ordinary I/O failure.
fn to_lock_error(path: &Path, source: std::io::Error) -> VfsError {
    match source.raw_os_error() {
        Some(EAGAIN) | Some(EACCES) => VfsError::Locked {
            path: path.display().to_string(),
        },
        _ => to_vfs_error(path, source),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "sqlite_rs_unix_vfs_test_{}_{}_{}",
            std::process::id(),
            name,
            fastrand_stub()
        ));
        p
    }

    // Cheap unique suffix without pulling in a rand dependency.
    fn fastrand_stub() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    #[test]
    fn open_read_missing_file_is_not_found() {
        let vfs = UnixVfs;
        let path = tmp_path("missing_read");
        let err = vfs.open_read(&path).err().unwrap();
        assert!(matches!(err, VfsError::NotFound { .. }));
    }

    #[test]
    fn open_write_missing_file_is_not_found() {
        let vfs = UnixVfs;
        let path = tmp_path("missing_write");
        let err = vfs.open_write(&path).err().unwrap();
        assert!(matches!(err, VfsError::NotFound { .. }));
    }

    #[test]
    fn exists_reports_true_and_false() {
        let vfs = UnixVfs;
        let path = tmp_path("exists");
        assert!(!vfs.exists(&path).unwrap());
        std::fs::write(&path, b"hi").unwrap();
        assert!(vfs.exists(&path).unwrap());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn create_or_open_write_creates_then_reopens_without_truncating() {
        let vfs = UnixVfs;
        let path = tmp_path("create_or_open");
        std::fs::remove_file(&path).ok();

        let file = vfs.create_or_open_write(&path).unwrap();
        file.write_at(b"hello", 0).unwrap();
        drop(file);

        let file = vfs.create_or_open_write(&path).unwrap();
        assert_eq!(file.size().unwrap(), 5);
        let mut buf = [0u8; 5];
        file.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn delete_missing_file_is_ok() {
        let vfs = UnixVfs;
        let path = tmp_path("delete_missing");
        vfs.delete(&path).unwrap();
    }

    #[test]
    fn delete_existing_file_removes_it() {
        let vfs = UnixVfs;
        let path = tmp_path("delete_existing");
        std::fs::write(&path, b"x").unwrap();
        vfs.delete(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn wal_shm_helpers_return_none_or_empty_without_shm_file() {
        let vfs = UnixVfs;
        let path = tmp_path("no_shm.db");
        std::fs::remove_file(companion_path(&path, "-shm")).ok();

        assert!(vfs.claim_wal_checkpoint_lock(&path).unwrap().is_none());
        assert!(vfs.active_wal_reader_marks(&path).unwrap().is_empty());
        vfs.publish_wal_backfill(&path, 3).unwrap();
        assert_eq!(vfs.read_wal_backfill(&path).unwrap(), 0);
        assert!(vfs.claim_wal_write_lock(&path).unwrap().is_none());
        vfs.publish_wal_mx_frame(&path, 7).unwrap();
        assert!(vfs.open_wal_shm(&path).unwrap().is_none());
    }

    #[test]
    fn vfs_file_write_read_truncate_sync() {
        let vfs = UnixVfs;
        let path = tmp_path("rw");
        std::fs::remove_file(&path).ok();
        let file = vfs.create_or_open_write(&path).unwrap();

        file.write_at(b"abcdef", 0).unwrap();
        assert_eq!(file.size().unwrap(), 6);

        file.truncate(3).unwrap();
        assert_eq!(file.size().unwrap(), 3);

        file.sync().unwrap();

        let mut buf = [0u8; 3];
        file.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"abc");

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn lock_shared_guard_checks_reserved_and_escalates() {
        let vfs = UnixVfs;
        let path = tmp_path("lock");
        std::fs::remove_file(&path).ok();
        let file = vfs.create_or_open_write(&path).unwrap();

        let mut guard = file.lock_shared().unwrap();
        assert!(!guard.check_reserved().unwrap());
        guard.escalate_to_exclusive().unwrap();
        guard.de_escalate_to_shared().unwrap();
        guard.set_level(lock::LockLevel::Unlocked).unwrap();
        drop(guard);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn to_vfs_error_display_variants() {
        let path = Path::new("/some/path");
        let not_found = to_vfs_error(path, std::io::Error::from(std::io::ErrorKind::NotFound));
        assert!(matches!(not_found, VfsError::NotFound { .. }));

        let other = to_vfs_error(
            path,
            std::io::Error::from(std::io::ErrorKind::PermissionDenied),
        );
        assert!(matches!(other, VfsError::Io { .. }));
    }

    #[test]
    fn to_lock_error_maps_eagain_and_eacces_to_locked() {
        let path = Path::new("/some/path");

        let eagain = to_lock_error(path, std::io::Error::from_raw_os_error(EAGAIN));
        assert!(matches!(eagain, VfsError::Locked { .. }));

        let eacces = to_lock_error(path, std::io::Error::from_raw_os_error(EACCES));
        assert!(matches!(eacces, VfsError::Locked { .. }));

        let other = to_lock_error(path, std::io::Error::from(std::io::ErrorKind::NotFound));
        assert!(matches!(other, VfsError::NotFound { .. }));
    }
}
