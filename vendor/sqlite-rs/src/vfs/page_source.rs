// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Whole-page reads on top of [`VfsFile::read_at`], for the b-tree layer.
//!
//! [`PageSource`] is defined here (not in `src/btree/`) so the b-tree
//! module can depend on it generically without ever writing `dyn` itself —
//! `src/btree/` is not exempt from the `mvl-limit` gate; only this module
//! is (see the Makefile's qualified-subset gate comment).

use std::path::Path;
use std::rc::Rc;

use super::{AnyVfsFile, FileLock, Vfs, VfsError, VfsFile};

/// Failure reading or writing a whole page through a [`PageSource`].
#[derive(Debug)]
pub enum PageError {
    /// Page numbers are 1-based; page 0 was requested.
    InvalidPageNumber,

    /// A read returned fewer bytes than a full page.
    ShortRead {
        /// The page that came up short.
        page_num: u32,
        /// The page size that was expected.
        expected: usize,
        /// The number of bytes actually read.
        got: usize,
    },

    /// [`WritablePageSource::write_page`] was given a buffer that isn't
    /// exactly one page long.
    WrongLength {
        /// The page that was being written.
        page_num: u32,
        /// The page size that was expected.
        expected: usize,
        /// The length of the buffer actually given.
        got: usize,
    },

    /// The underlying VFS operation failed.
    Vfs(VfsError),
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::InvalidPageNumber => write!(f, "invalid page number 0"),
            PageError::ShortRead {
                page_num,
                expected,
                got,
            } => write!(
                f,
                "short read on page {page_num}: expected {expected} bytes, got {got}"
            ),
            PageError::WrongLength {
                page_num,
                expected,
                got,
            } => write!(
                f,
                "wrong buffer length writing page {page_num}: expected {expected} bytes, got {got}"
            ),
            PageError::Vfs(source) => std::fmt::Display::fmt(source, f),
        }
    }
}

impl std::error::Error for PageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PageError::Vfs(source) => Some(source),
            PageError::InvalidPageNumber
            | PageError::ShortRead { .. }
            | PageError::WrongLength { .. } => None,
        }
    }
}

impl From<VfsError> for PageError {
    fn from(source: VfsError) -> Self {
        PageError::Vfs(source)
    }
}

/// Reads page `page_num` directly into `buf`, which must already be
/// exactly `page_size` bytes long — shared between [`read_page_at`] (fresh
/// buffer, the common case) and [`WritablePageSource::read_page_into`]
/// (#509: an already-allocated buffer, typically an evicted page's own
/// uniquely-owned `Rc<[u8]>` recycled in place by `Pager`'s page cache,
/// see its own doc). No zero-fill happens here or is needed: `buf`'s
/// existing bytes (zeros for a brand new allocation, stale page data for
/// a recycled one) are fully overwritten by this read on success, and an
/// error (including a short read) is propagated without the caller
/// treating `buf` as valid.
fn read_page_at_into(
    file: &dyn VfsFile,
    page_size: u32,
    page_num: u32,
    buf: &mut [u8],
) -> Result<(), PageError> {
    if page_num == 0 {
        return Err(PageError::InvalidPageNumber);
    }
    debug_assert_eq!(buf.len(), page_size as usize);
    // page_num >= 1 here (checked above) and page_size is a validated
    // power of two in [512, 65536] (header.rs), so this product stays
    // far below u64::MAX; saturating_* just avoids asserting that by
    // inspection.
    let offset = (page_num as u64)
        .saturating_sub(1)
        .saturating_mul(page_size as u64);
    let n = file.read_at(buf, offset)?;
    if n != buf.len() {
        return Err(PageError::ShortRead {
            page_num,
            expected: buf.len(),
            got: n,
        });
    }
    Ok(())
}

/// Reads page `page_num` from `file` into a freshly allocated buffer,
/// shared between [`VfsPageSource`] and [`WritablePageSource`].
fn read_page_at(file: &dyn VfsFile, page_size: u32, page_num: u32) -> Result<Rc<[u8]>, PageError> {
    let mut buf = vec![0u8; page_size as usize];
    read_page_at_into(file, page_size, page_num, &mut buf)?;
    Ok(Rc::from(buf))
}

/// A source of whole database pages, numbered from 1. Returns [`Rc<[u8]>`]
/// rather than `Vec<u8>` so a cache hit (the common case once a page is
/// warm — see `Pager`'s `page_cache`) is a refcount bump, not a copy; the
/// b-tree read path (`src/btree.rs`'s `reassemble_payload`) leans on this
/// to avoid a per-row `Vec` allocation for the non-overflow case (#467).
pub trait PageSource {
    /// Reads page `page_num` (1-based) and returns exactly `page_size`
    /// bytes. `page_num == 0` or a short read is `Err`.
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError>;
}

/// A [`PageSource`] backed by a [`VfsFile`] opened through a [`Vfs`].
pub struct VfsPageSource {
    file: Box<dyn VfsFile>,
    page_size: u32,
}

impl VfsPageSource {
    /// Opens `path` for reading through `vfs`.
    pub fn open(vfs: &dyn Vfs, path: &Path, page_size: u32) -> Result<Self, VfsError> {
        let file = vfs.open_read(path)?;
        Ok(VfsPageSource { file, page_size })
    }

    /// Acquires a SHARED lock on the underlying file — see
    /// [`VfsFile::lock_shared`].
    pub fn lock_shared(&self) -> Result<FileLock, VfsError> {
        self.file.lock_shared()
    }
}

impl PageSource for VfsPageSource {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        read_page_at(self.file.as_ref(), self.page_size, page_num)
    }
}

/// A [`PageSource`] backed by a read-write [`VfsFile`] opened through
/// [`Vfs::open_write`], adding [`WritablePageSource::write_page`] and
/// [`WritablePageSource::sync`] on top of the same single file handle used
/// for reads (#166 pager write path). Using one handle for both directions
/// — rather than a second fd opened alongside a read-only [`VfsPageSource`]
/// — sidesteps the documented "`close()` drops all `fcntl` locks on the
/// inode" trap (`src/pager.rs`'s module doc, #45): [`Pager`](crate::pager::Pager)
/// never opens a second fd to the same path, so there is nothing whose drop
/// could silently release a lock acquired through this one.
pub struct WritablePageSource {
    file: Box<dyn VfsFile>,
    page_size: u32,
}

impl WritablePageSource {
    /// Opens `path` for reading and writing through `vfs`.
    pub fn open(vfs: &dyn Vfs, path: &Path, page_size: u32) -> Result<Self, VfsError> {
        let file = vfs.open_write(path)?;
        Ok(WritablePageSource { file, page_size })
    }

    /// Wraps an already-opened file handle rather than opening a fresh one
    /// — for `Pager::open`'s hot-journal recovery (#359), which must probe
    /// and escalate the lock on, then read/write/truncate, the *same* fd
    /// used for every page access afterward. A second independently-opened
    /// fd to the same path would reintroduce the "`close()` drops all
    /// `fcntl` locks on the inode" trap this struct's own doc comment
    /// above already commits to avoiding.
    pub fn from_file(file: AnyVfsFile, page_size: u32) -> Self {
        WritablePageSource {
            file: file.into_inner(),
            page_size,
        }
    }

    /// Acquires a SHARED lock on the underlying file — see
    /// [`VfsFile::lock_shared`].
    pub fn lock_shared(&self) -> Result<FileLock, VfsError> {
        self.file.lock_shared()
    }

    /// Writes exactly `page_size` bytes of `bytes` as page `page_num`
    /// (1-based). `page_num == 0` or a wrong-length buffer is `Err`.
    pub fn write_page(&self, page_num: u32, bytes: &[u8]) -> Result<(), PageError> {
        if page_num == 0 {
            return Err(PageError::InvalidPageNumber);
        }
        if bytes.len() != self.page_size as usize {
            return Err(PageError::WrongLength {
                page_num,
                expected: self.page_size as usize,
                got: bytes.len(),
            });
        }
        let offset = (page_num as u64)
            .saturating_sub(1)
            .saturating_mul(self.page_size as u64);
        self.file.write_at(bytes, offset)?;
        Ok(())
    }

    /// Flushes all writes made via [`WritablePageSource::write_page`] to
    /// durable storage.
    pub fn sync(&self) -> Result<(), VfsError> {
        self.file.sync()
    }

    /// Reads page `page_num` into `buf` in place (#509) rather than
    /// allocating a fresh, zero-filled buffer — `buf` must already be
    /// exactly `page_size` bytes long. `Pager`'s page-cache eviction uses
    /// this to recycle an evicted page's own uniquely-owned `Rc<[u8]>`
    /// (via `Rc::get_mut`) for the newly missed page instead of paying a
    /// fresh allocation and zero-fill on every cache miss once the cache
    /// is warm (steady-state: most misses are evictions, not first
    /// touches).
    pub(crate) fn read_page_into(&self, page_num: u32, buf: &mut [u8]) -> Result<(), PageError> {
        read_page_at_into(self.file.as_ref(), self.page_size, page_num, buf)
    }
}

impl PageSource for WritablePageSource {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        read_page_at(self.file.as_ref(), self.page_size, page_num)
    }
}

/// Lets the VDBE (`src/vdbe/cursor.rs`) share one page source across
/// several `TableCursor`s (one per open `OpenRead` cursor slot) without
/// cloning the underlying file handle — `Rc` is cheap to clone, and the
/// VM is single-threaded, so this never needs to be `Send`/`Sync`.
impl PageSource for std::rc::Rc<dyn PageSource> {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        (**self).read_page(page_num)
    }
}

/// Lets a `TableCursor` borrow a page source by shared reference instead
/// of consuming it — needed by schema write helpers (`src/btree/master.rs`,
/// #193) that scan a table (e.g. `sqlite_master`) through `&Pager` while
/// still holding the same `Pager` for a later mutable write.
// If `PageSource` grows a second method, forward it here too — the
// compiler won't warn about a missing forward on a trait with only one
// method.
impl<T: PageSource + ?Sized> PageSource for &T {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        (**self).read_page(page_num)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::vfs::MemoryVfs;
    use std::path::Path;

    #[test]
    fn page_zero_is_rejected() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![0u8; 16]);
        let source = VfsPageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        assert!(matches!(
            source.read_page(0),
            Err(PageError::InvalidPageNumber)
        ));
    }

    #[test]
    fn short_file_reports_short_read() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![0u8; 8]);
        let source = VfsPageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        match source.read_page(1) {
            Err(PageError::ShortRead {
                page_num,
                expected,
                got,
            }) => {
                assert_eq!(page_num, 1);
                assert_eq!(expected, 16);
                assert_eq!(got, 8);
            }
            other => panic!("expected ShortRead, got {other:?}"),
        }
    }

    #[test]
    fn display_and_source_for_each_variant() {
        assert_eq!(
            PageError::InvalidPageNumber.to_string(),
            "invalid page number 0"
        );
        let short = PageError::ShortRead {
            page_num: 3,
            expected: 16,
            got: 4,
        };
        assert_eq!(
            short.to_string(),
            "short read on page 3: expected 16 bytes, got 4"
        );
        let wrong = PageError::WrongLength {
            page_num: 2,
            expected: 16,
            got: 8,
        };
        assert_eq!(
            wrong.to_string(),
            "wrong buffer length writing page 2: expected 16 bytes, got 8"
        );

        use std::error::Error;
        assert!(PageError::InvalidPageNumber.source().is_none());
        assert!(short.source().is_none());
        assert!(wrong.source().is_none());

        let vfs = MemoryVfs::new();
        let missing = match vfs.open_read(Path::new("/missing")) {
            Err(e) => e,
            Ok(_) => panic!("expected open_read to fail for a missing file"),
        };
        let page_err: PageError = missing.into();
        assert!(matches!(page_err, PageError::Vfs(_)));
        assert!(page_err.source().is_some());
        assert_eq!(page_err.to_string(), "file not found: /missing");
    }

    #[test]
    fn writable_page_source_round_trip() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![0u8; 16]);
        let source = WritablePageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        source.write_page(1, &[7u8; 16]).unwrap();
        source.sync().unwrap();
        let page = source.read_page(1).unwrap();
        assert_eq!(&*page, &[7u8; 16][..]);
        drop(source.lock_shared().unwrap());
    }

    #[test]
    fn writable_page_source_rejects_page_zero() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![0u8; 16]);
        let source = WritablePageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        assert!(matches!(
            source.write_page(0, &[0u8; 16]),
            Err(PageError::InvalidPageNumber)
        ));
    }

    #[test]
    fn writable_page_source_rejects_wrong_length() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![0u8; 16]);
        let source = WritablePageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        match source.write_page(1, &[0u8; 8]) {
            Err(PageError::WrongLength {
                page_num,
                expected,
                got,
            }) => {
                assert_eq!(page_num, 1);
                assert_eq!(expected, 16);
                assert_eq!(got, 8);
            }
            other => panic!("expected WrongLength, got {other:?}"),
        }
    }

    #[test]
    fn writable_page_source_from_file_and_read_into() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![9u8; 16]);
        let any_vfs = crate::vfs::AnyVfs::new(vfs);
        let file = any_vfs.open_write(Path::new("/db")).unwrap();
        let source = WritablePageSource::from_file(file, 16);
        let mut buf = vec![0u8; 16];
        source.read_page_into(1, &mut buf).unwrap();
        assert_eq!(buf, vec![9u8; 16]);
    }

    #[test]
    fn rc_and_ref_page_source_forward() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/db", vec![5u8; 16]);
        let source = VfsPageSource::open(&vfs, Path::new("/db"), 16).unwrap();
        let rc: Rc<dyn PageSource> = Rc::new(source);
        assert_eq!(&*rc.read_page(1).unwrap(), &[5u8; 16][..]);

        let by_ref: &dyn PageSource = &*rc;
        assert_eq!(&*by_ref.read_page(1).unwrap(), &[5u8; 16][..]);
    }
}
