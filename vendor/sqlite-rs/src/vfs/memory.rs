// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! In-memory `Vfs` implementation, for tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{FileLock, Result, SharedLockGuard, Vfs, VfsError, VfsFile};

type FileTable = Arc<Mutex<HashMap<PathBuf, Arc<Mutex<Vec<u8>>>>>>;

/// An in-memory [`Vfs`] backed by a path -> bytes map. Lets tests exercise
/// `Vfs`-consuming code without touching the real filesystem. The map
/// itself is `Arc<Mutex<..>>`-shared (not just each file's contents) so
/// that a [`Pager`](crate::pager::Pager), which stores its own `Clone` of
/// the `Vfs` it was opened with (#172 rollback journal — needed to create/
/// delete the `-journal` companion file after `open` returns), sees the
/// same file table as the original handle: a journal file created via
/// [`Vfs::create_or_open_write`] on the clone is visible to `exists`/
/// `open_read` on the original, matching a real filesystem's semantics.
#[derive(Debug, Default, Clone)]
pub struct MemoryVfs {
    files: FileTable,
    /// Total `VfsFile::sync` calls across every file handle this `Vfs`
    /// (or a clone of it) has opened — `Arc`-shared like `files`, so it
    /// stays visible from the original handle after `Pager::open`
    /// clones it. Exists purely so `PRAGMA synchronous` (#645) tests
    /// can assert *whether* a commit fsynced, since an in-memory
    /// backend has no real fsync effect to observe otherwise.
    sync_calls: Arc<AtomicUsize>,
}

impl MemoryVfs {
    /// Creates an empty in-memory filesystem with no registered files.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a file's contents under `path`.
    pub fn insert(&mut self, path: impl Into<PathBuf>, contents: Vec<u8>) {
        let mut files = match self.files.lock() {
            Ok(files) => files,
            Err(_) => return,
        };
        files.insert(path.into(), Arc::new(Mutex::new(contents)));
    }

    /// Total `VfsFile::sync` calls across every file this `Vfs` (or a
    /// clone of it) has opened so far — never reset, so callers
    /// snapshot the count before and after the operation under test and
    /// compare the delta (#645).
    pub fn sync_calls(&self) -> usize {
        self.sync_calls.load(Ordering::SeqCst)
    }

    fn handle(&self, path: &Path) -> Result<Arc<Mutex<Vec<u8>>>> {
        let files = self.files.lock().map_err(|_| poisoned(path))?;
        files.get(path).cloned().ok_or_else(|| VfsError::NotFound {
            path: path.display().to_string(),
        })
    }
}

impl Vfs for MemoryVfs {
    fn open_read(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        Ok(Box::new(MemoryVfsFile(
            self.handle(path)?,
            self.sync_calls.clone(),
        )))
    }

    fn open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        Ok(Box::new(MemoryVfsFile(
            self.handle(path)?,
            self.sync_calls.clone(),
        )))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        let files = self.files.lock().map_err(|_| poisoned(path))?;
        Ok(files.contains_key(path))
    }

    fn create_or_open_write(&self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let handle = {
            let mut files = self.files.lock().map_err(|_| poisoned(path))?;
            files
                .entry(path.to_path_buf())
                .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
                .clone()
        };
        Ok(Box::new(MemoryVfsFile(handle, self.sync_calls.clone())))
    }

    fn delete(&self, path: &Path) -> Result<()> {
        let mut files = self.files.lock().map_err(|_| poisoned(path))?;
        files.remove(path);
        Ok(())
    }
}

struct MemoryVfsFile(Arc<Mutex<Vec<u8>>>, Arc<AtomicUsize>);

/// The in-memory backend's `Mutex`es are only ever contended within a
/// single test process and never cross a panic boundary while held, so a
/// poisoned lock here indicates a bug in the test itself, not a condition
/// production code needs to recover from — surfaced as an ordinary I/O
/// error rather than a panic (`clippy::unwrap_used`/`panic` stay denied).
fn poisoned(path: &Path) -> VfsError {
    VfsError::Io {
        path: path.display().to_string(),
        source: std::io::Error::other("poisoned in-memory file lock"),
    }
}

impl VfsFile for MemoryVfsFile {
    #[allow(
        clippy::indexing_slicing,
        reason = "offset < data.len() is checked above; n = min(buf.len(), available.len()) is always in bounds on both sides"
    )]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let data = self.0.lock().map_err(|_| poisoned(Path::new("<memory>")))?;
        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }
        let available = &data[offset..];
        let n = buf.len().min(available.len());
        buf[..n].copy_from_slice(&available[..n]);
        Ok(n)
    }

    fn size(&self) -> Result<u64> {
        let data = self.0.lock().map_err(|_| poisoned(Path::new("<memory>")))?;
        Ok(data.len() as u64)
    }

    fn lock_shared(&self) -> Result<FileLock> {
        Ok(FileLock(Box::new(NoopLock)))
    }

    #[allow(
        clippy::indexing_slicing,
        reason = "offset..end is grown to end via resize just above, so it is always in bounds"
    )]
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        let mut data = self.0.lock().map_err(|_| poisoned(Path::new("<memory>")))?;
        let offset = offset as usize;
        let end = offset.saturating_add(buf.len());
        if data.len() < end {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(buf);
        Ok(())
    }

    fn truncate(&self, len: u64) -> Result<()> {
        let mut data = self.0.lock().map_err(|_| poisoned(Path::new("<memory>")))?;
        data.resize(len as usize, 0);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.1.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The in-memory backend has no real file descriptor to lock — a no-op
/// satisfies the [`VfsFile`] contract for tests exercising `Vfs`-generic
/// code that also locks.
struct NoopLock;

impl SharedLockGuard for NoopLock {}

/// Poisoning the file-table `Mutex` is only reachable via a genuine panic
/// while it's held, which black-box `tests/unit/vfs.rs` has no way to
/// trigger (the field is private) — hence these white-box tests live here
/// instead, exercising the `poisoned` error path per #224.
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    /// Poisons `vfs`'s shared file table by locking it on another thread
    /// and panicking while the guard is held.
    fn poison(vfs: &MemoryVfs) {
        let files = vfs.files.clone();
        std::thread::spawn(move || {
            let _guard = files.lock().unwrap();
            panic!("intentionally poisoning the lock for a test");
        })
        .join()
        .ok();
    }

    #[test]
    fn insert_is_a_noop_when_lock_poisoned() {
        let mut vfs = MemoryVfs::new();
        poison(&vfs);

        // Must not panic, and the insert is silently dropped.
        vfs.insert("/x", vec![1, 2, 3]);
        assert!(vfs.exists(Path::new("/x")).is_err());
    }

    #[test]
    fn handle_surfaces_io_error_when_lock_poisoned() {
        let vfs = MemoryVfs::new();
        poison(&vfs);

        let err = match vfs.open_read(Path::new("/x")) {
            Ok(_) => panic!("expected an error"),
            Err(e) => e,
        };
        match err {
            VfsError::Io { path, .. } => assert_eq!(path, "/x"),
            other => panic!("expected Io error, got {other:?}"),
        }
    }

    #[test]
    fn exists_surfaces_io_error_when_lock_poisoned() {
        let vfs = MemoryVfs::new();
        poison(&vfs);

        let err = vfs.exists(Path::new("/y")).unwrap_err();
        match err {
            VfsError::Io { path, .. } => assert_eq!(path, "/y"),
            other => panic!("expected Io error, got {other:?}"),
        }
    }
}
