// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! WAL-mode `-shm` reader-mark protocol: claims a `WAL_READ_LOCK` slot and
//! publishes the frame count a reader is pinned to (`aReadMark`), so a live
//! checkpointer backs off rather than backfilling/truncating WAL frames a
//! concurrent reader still depends on. Byte offsets and the wal-index
//! header layout verified against SQLite's own source (`os_unix.c`,
//! `wal.c`) by spike 005 (`tests/spike/005_locking_interop/findings.md`,
//! `src/wal_shm.rs`) — not re-derived here; experiment 4 there validated
//! this exact protocol against a live stock `sqlite3` checkpointer.
//!
//! wal-index (`-shm`) header layout (`WalIndexHdr` + `WalCkptInfo`,
//! `wal.c`):
//! ```text
//! offset  field
//! 0       WalIndexHdr copy 1 (48 bytes) — mxFrame at +16
//! 48      WalIndexHdr copy 2 (identical layout)
//! 96      WalCkptInfo.nBackfill (u32)
//! 100     WalCkptInfo.aReadMark[5] (u32 x5)
//! 120     WalCkptInfo.aLock[8]        <- UNIX_SHM_BASE
//! 128     WalCkptInfo.nBackfillAttempted (u32)
//! ```
//!
//! No longer `mmap`s the `-shm` file (#66): every field access here is a
//! `pread`/`pwrite` (`std::os::unix::fs::FileExt`) at these same fixed
//! offsets. Coherence with a concurrent `sqlite3` process's own `MAP_SHARED`
//! mapping of this file relies on the OS's unified page cache keeping
//! buffered file I/O and `mmap`'d access to the same file coherent — true
//! on Linux and macOS, sqlite-rs's supported platforms. A bonus of this
//! approach over `mmap`: a `-shm` file truncated out from under a reader
//! now yields a structured `Err` from the read, not an uncatchable
//! `SIGBUS`. This was #54's Option C (see `.openspec/adr/0001-shm-access-pread-not-mmap.md`)
//! — the `SIGBUS` exposure this module used to carry as known residual risk
//! is eliminated, not merely narrowed. `validate_shm_len` also bounds the
//! file above `MAX_SHM_LEN`, so an oversized `-shm` (sparse or otherwise) is
//! rejected before any offset into it is trusted.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::sys::fcntl::{off_t, EACCES, EAGAIN, F_RDLCK, F_UNLCK, F_WRLCK, O_NOFOLLOW};

use super::lock::fcntl_lock;
use super::{SharedLockGuard, VfsError};

const MX_FRAME_OFFSET: u64 = 16;
const READ_MARK_BASE_OFFSET: u64 = 100;
/// `aReadMark` slot value meaning "no reader has claimed this slot in
/// the current WAL generation" — every slot starts here on a freshly
/// created `-shm` file ([`fresh_shm_bytes`]).
const READ_MARK_UNUSED: u32 = 0xFFFF_FFFF;

/// Minimum `-shm` file length for a valid wal-index header: through the
/// end of `aReadMark[5]` at offset 100..120.
const MIN_SHM_LEN: u64 = READ_MARK_BASE_OFFSET + 20;

/// SQLite grows `-shm` in fixed 32KB regions (`os_unix.c`'s
/// `SHM_REGION_SIZE`, guarded by a cap of 8 regions in the same file) and
/// never needs more than a handful of them for the wal-index header plus
/// lock bytes. A `-shm` file far above that is not something a cooperating
/// writer produces — it is either corrupt or hostile, and #54 requires
/// rejecting it outright rather than trusting the filesystem-reported
/// length unconditionally.
const SHM_REGION_SIZE: u64 = 32 * 1024;
const MAX_SHM_LEN: u64 = SHM_REGION_SIZE * 8;

/// SQLite's `UNIX_SHM_BASE` (`os_unix.c`): base of the `-shm` lock-byte
/// range.
const UNIX_SHM_BASE: off_t = 120;

/// `slot` ranges 1..=4 — slot 0 is reserved (always considered "in use" by
/// SQLite's own protocol) and is never claimed by a reader here, matching
/// spike 005 experiment 4.
pub(crate) fn wal_read_lock_byte(slot: usize) -> off_t {
    UNIX_SHM_BASE
        .saturating_add(3)
        .saturating_add(slot as off_t)
}

/// SQLite's `WAL_WRITE_LOCK` (`wal.c`): guards `mxFrame`/the `-wal` file's
/// own tail against a second concurrent writer (#389) — exactly one
/// connection may hold this at a time, matching WAL's single-writer
/// invariant. `wal.c`'s lock layout, order 0..7: write, ckpt, recover,
/// read(0..4) — this is byte 0, i.e. `UNIX_SHM_BASE` itself.
const WAL_WRITE_LOCK_BYTE: off_t = UNIX_SHM_BASE;

/// SQLite's `WAL_CKPT_LOCK` (`wal.c`): guards a PASSIVE checkpoint (#386)
/// while it reads `aReadMark`/writes `nBackfill`, so two concurrent
/// checkpoint attempts don't race on the same backfill state. A single
/// byte at a fixed offset from `UNIX_SHM_BASE`, same as the reader-mark
/// bytes above (`wal.c`'s lock layout, order 0..7: write, ckpt, recover,
/// read(0..4)).
const WAL_CKPT_LOCK_BYTE: off_t = UNIX_SHM_BASE.saturating_add(1);

/// A held `WAL_WRITE_LOCK`, releasing on drop — taken by a writer (#389)
/// before appending frames/advancing `mxFrame`, so a second concurrent
/// writer is refused rather than interleaving frames or racing the
/// `mxFrame` publish. Same shape as [`WalCheckpointLock`] just below.
#[derive(Debug)]
pub struct WalWriteLock {
    file: Arc<File>,
}

impl SharedLockGuard for WalWriteLock {}

impl Drop for WalWriteLock {
    fn drop(&mut self) {
        fcntl_lock(&self.file, F_UNLCK, WAL_WRITE_LOCK_BYTE, 1).ok();
    }
}

pub(crate) fn claim_wal_write_lock(shm_path: &Path) -> io::Result<WalWriteLock> {
    let file = open_shm_shared(shm_path)?;
    validate_shm_len(&file)?;
    fcntl_lock(&file, F_WRLCK, WAL_WRITE_LOCK_BYTE, 1)?;
    Ok(WalWriteLock { file })
}

/// A persistent `-shm` fd for [`super::Vfs::open_wal_shm`] (#437): opened
/// and validated once, then reused across every subsequent
/// `claim_write_lock`/`release_write_lock`/`publish_mx_frame` call
/// instead of [`claim_wal_write_lock`]/[`publish_mx_frame`] each doing
/// their own fresh `open()` per call. Unlike [`WalWriteLock`], holding
/// this does not itself mean the write lock is held — `claim_write_lock`/
/// `release_write_lock` toggle the same `fcntl` byte on this one fd
/// explicitly, once per commit, instead of the lock's lifetime being
/// tied to a value's `Drop`.
pub(crate) struct UnixWalShm {
    file: Arc<File>,
    path: PathBuf,
}

fn to_shm_vfs_error(path: &Path, source: io::Error) -> VfsError {
    VfsError::Io {
        path: path.display().to_string(),
        source,
    }
}

/// Like [`to_shm_vfs_error`], but maps `fcntl(F_SETLK)`'s lock-contention
/// errno values to [`VfsError::Locked`] — mirrors `unix.rs`'s private
/// `to_lock_error` (duplicated here rather than shared across modules,
/// since both are a handful of lines tied to their own module's error
/// paths).
fn to_shm_lock_error(path: &Path, source: io::Error) -> VfsError {
    match source.raw_os_error() {
        Some(EAGAIN) | Some(EACCES) => VfsError::Locked {
            path: path.display().to_string(),
        },
        _ => to_shm_vfs_error(path, source),
    }
}

impl super::WalShm for UnixWalShm {
    fn claim_write_lock(&self) -> super::Result<()> {
        fcntl_lock(&self.file, F_WRLCK, WAL_WRITE_LOCK_BYTE, 1)
            .map_err(|source| to_shm_lock_error(&self.path, source))
    }

    fn release_write_lock(&self) -> super::Result<()> {
        fcntl_lock(&self.file, F_UNLCK, WAL_WRITE_LOCK_BYTE, 1)
            .map_err(|source| to_shm_vfs_error(&self.path, source))
    }

    fn publish_mx_frame(&self, mx_frame: u32) -> super::Result<()> {
        write_u32_at(&self.file, MX_FRAME_OFFSET, mx_frame)
            .map_err(|source| to_shm_vfs_error(&self.path, source))
    }
}

pub(crate) fn open_wal_shm(shm_path: &Path) -> io::Result<UnixWalShm> {
    let file = open_shm_shared(shm_path)?;
    validate_shm_len(&file)?;
    Ok(UnixWalShm {
        file,
        path: shm_path.to_path_buf(),
    })
}

/// Every live `-shm` fd this process holds, keyed by path (#491). POSIX
/// `fcntl` record locks are scoped to `(process, inode)`, not to a file
/// descriptor: closing *any* fd this process holds to a file releases
/// *every* lock the process holds on that inode, even ones taken through a
/// different fd on the same path. Before [`open_shm_shared`], each
/// guard/helper below opened its own independent `File` — so two guards
/// alive at once on the same `-shm` path (e.g. a `Pager`'s own long-lived
/// `WalReadLock` plus its lazily-opened `UnixWalShm`, or a second
/// `Pager`/a free-standing `checkpoint_passive` call in the same process)
/// could have one's `Drop`-triggered `close()` silently release the
/// other's still-needed lock. `open_shm_shared` makes every caller in
/// this module reuse the one fd already open for a path instead, via a
/// `Weak` entry that's only ever upgraded — it actually closes only once
/// the last `Arc<File>` referencing it drops, at which point (by
/// construction) nothing in this process needs a lock on that inode
/// anymore.
///
/// `Arc`/`Mutex` rather than this crate's more common single-threaded
/// `Rc`/`RefCell` (e.g. `UnixVfsFile`'s own per-path fd sharing in
/// `unix.rs`): a plain module-level `static` must be `Sync`, which an
/// `Rc`-based type never is regardless of contents — `thread_local!` would
/// avoid that, but its macro invocation falls outside `src/vfs/shm.rs`'s
/// qualified-subset allowlist (`make check-mvl-limit`, issue #23), so a real
/// `static` it is. Real cross-thread contention never happens in
/// practice (this crate has no threads), so the `Mutex` here is `Sync`-
/// satisfying scaffolding, not a concurrency mechanism in active use.
static SHM_FILES: OnceLock<Mutex<HashMap<PathBuf, Weak<File>>>> = OnceLock::new();

fn shm_files() -> &'static Mutex<HashMap<PathBuf, Weak<File>>> {
    SHM_FILES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns the one shared `-shm` fd this process holds open for
/// `shm_path`, opening a fresh one only if none is currently live (#491).
/// Always opened read+write with `O_NOFOLLOW`, so a symlink planted at
/// that path (e.g. in a shared or world-writable directory) can't
/// redirect a lock claim or a raw `nBackfill` write onto an arbitrary file
/// this process happens to have write access to. Every `-shm` open in
/// this module goes through here rather than a bare `OpenOptions::open`.
fn open_shm_shared(shm_path: &Path) -> io::Result<Arc<File>> {
    let mut files = shm_files()
        .lock()
        .map_err(|_| io::Error::other("poisoned -shm fd registry"))?;
    if let Some(file) = files.get(shm_path).and_then(Weak::upgrade) {
        return Ok(file);
    }
    let file = Arc::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_NOFOLLOW)
            .open(shm_path)?,
    );
    files.insert(shm_path.to_path_buf(), Arc::downgrade(&file));
    Ok(file)
}

/// The byte layout of a fresh, valid `-shm` file for a brand-new WAL
/// generation (#388, `PRAGMA journal_mode=WAL`): one `SHM_REGION_SIZE`
/// region — the same fixed size this module's own tests already use
/// for a minimal valid `-shm` — with every `aReadMark` slot set to
/// [`READ_MARK_UNUSED`] (no reader has claimed one in this WAL
/// generation yet) and everything else (mxFrame, nBackfill, lock bytes)
/// zeroed, matching a brand-new WAL with no frames written.
///
/// Returns bytes rather than writing a file directly, unlike this
/// module's other functions: `claim_wal_read_lock`/
/// `claim_wal_checkpoint_lock`/etc. are inherently real-file-only
/// (`std::fs`/`fcntl` record locks have no `MemoryVfs` equivalent — a
/// known, already-accepted limitation, see `checkpoint.rs`'s tests),
/// but the `-shm` file's *content* has no such restriction and must be
/// creatable uniformly on every backend. The caller
/// (`crate::pager::Pager::set_journal_mode`) writes these bytes through
/// the abstract `Vfs` trait instead.
pub(crate) fn fresh_shm_bytes() -> Vec<u8> {
    let mut bytes = vec![0u8; SHM_REGION_SIZE as usize];
    for slot in 0..5usize {
        let off = read_mark_offset(slot) as usize;
        if let Some(dest) = bytes.get_mut(off..off.saturating_add(4)) {
            dest.copy_from_slice(&READ_MARK_UNUSED.to_ne_bytes());
        }
    }
    bytes
}

/// `WalCkptInfo.nBackfill` (`wal.c`): how many leading frames a checkpoint
/// has already copied into the main database file.
const N_BACKFILL_OFFSET: u64 = 96;

/// A held `WAL_CKPT_LOCK`, releasing on drop — taken by a checkpointer
/// (#386) before reading `aReadMark`/writing `nBackfill` so a concurrent
/// checkpoint attempt is refused rather than racing on the same backfill
/// state.
#[derive(Debug)]
pub struct WalCheckpointLock {
    file: Arc<File>,
}

impl SharedLockGuard for WalCheckpointLock {}

impl Drop for WalCheckpointLock {
    fn drop(&mut self) {
        fcntl_lock(&self.file, F_UNLCK, WAL_CKPT_LOCK_BYTE, 1).ok();
    }
}

pub(crate) fn claim_wal_checkpoint_lock(shm_path: &Path) -> io::Result<WalCheckpointLock> {
    let file = open_shm_shared(shm_path)?;
    validate_shm_len(&file)?;
    fcntl_lock(&file, F_WRLCK, WAL_CKPT_LOCK_BYTE, 1)?;
    Ok(WalCheckpointLock { file })
}

/// The frame marks of readers currently pinned to this WAL generation —
/// probed by attempting a transient exclusive claim on each of the 4
/// reader-mark lock bytes: a slot that's still `EAGAIN`/`EACCES` has a live
/// reader, whose published `aReadMark` bounds how far a PASSIVE checkpoint
/// (#386) may safely backfill. A slot whose lock is free is skipped (no
/// reader; its stale mark, if any, is not a constraint) — mirrors
/// `claim_wal_read_lock`'s "the lock decides occupancy, not the mark
/// value" rule.
pub(crate) fn active_reader_marks(shm_path: &Path) -> io::Result<Vec<u32>> {
    let file = open_shm_shared(shm_path)?;
    validate_shm_len(&file)?;

    let mut marks = Vec::new();
    for slot in 1..=4usize {
        let byte = wal_read_lock_byte(slot);
        match fcntl_lock(&file, F_WRLCK, byte, 1) {
            Ok(()) => {
                fcntl_lock(&file, F_UNLCK, byte, 1)?;
            }
            Err(e) if matches!(e.raw_os_error(), Some(EAGAIN) | Some(EACCES)) => {
                marks.push(read_mark(&file, slot)?);
            }
            Err(e) => return Err(e),
        }
    }
    Ok(marks)
}

/// Reads `nBackfill`: how many leading frames the last checkpoint already
/// copied to the main file.
pub(crate) fn read_backfill(shm_path: &Path) -> io::Result<u32> {
    let file = open_shm_shared(shm_path)?;
    read_u32_at(&file, N_BACKFILL_OFFSET)
}

/// Publishes a new `nBackfill` after a PASSIVE checkpoint (#386) copies
/// frames `1..=nBackfill` into the main database file.
pub(crate) fn publish_backfill(shm_path: &Path, n_backfill: u32) -> io::Result<()> {
    let file = open_shm_shared(shm_path)?;
    write_u32_at(&file, N_BACKFILL_OFFSET, n_backfill)
}

/// Publishes a new `mxFrame` (the WAL's current end-of-valid-data, in
/// frames) after a writer (#389) appends and commits one or more frames —
/// so a reader that claims a fresh reader-mark slot afterward
/// ([`claim_wal_read_lock`]'s `mx_frame(&file)` read) sees the up-to-date
/// value rather than whatever the WAL generation started at.
pub(crate) fn publish_mx_frame(shm_path: &Path, mx_frame: u32) -> io::Result<()> {
    let file = open_shm_shared(shm_path)?;
    write_u32_at(&file, MX_FRAME_OFFSET, mx_frame)
}

fn read_mark_offset(slot: usize) -> u64 {
    READ_MARK_BASE_OFFSET.saturating_add((slot as u64).saturating_mul(4))
}

fn read_u32_at(file: &File, offset: u64) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact_at(&mut buf, offset)?;
    Ok(u32::from_ne_bytes(buf))
}

fn write_u32_at(file: &File, offset: u64, value: u32) -> io::Result<()> {
    file.write_all_at(&value.to_ne_bytes(), offset)
}

fn mx_frame(file: &File) -> io::Result<u32> {
    read_u32_at(file, MX_FRAME_OFFSET)
}

fn set_read_mark(file: &File, slot: usize, value: u32) -> io::Result<()> {
    write_u32_at(file, read_mark_offset(slot), value)
}

/// Reads back a published mark — used by [`active_reader_marks`] once a
/// slot's lock has confirmed it's actually held, and in tests to verify
/// `set_read_mark`. Never used to determine slot occupancy on its own:
/// that's the lock, not the mark value (see `claim_wal_read_lock`'s doc
/// comment) — a stale mark left behind by a released slot is meaningless
/// until the lock says the slot is live again.
fn read_mark(file: &File, slot: usize) -> io::Result<u32> {
    read_u32_at(file, read_mark_offset(slot))
}

/// Validates that `file` is at least long enough to hold a full wal-index
/// header — the `-shm` equivalent of `ShmMap::open`'s old length check,
/// still needed because a crash-truncated or half-written `-shm` file must
/// be rejected with a structured `Err`, not read out of bounds.
fn validate_shm_len(file: &File) -> io::Result<()> {
    let len = file.metadata()?.len();
    if len < MIN_SHM_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "-shm file too short for a wal-index header",
        ));
    }
    if len > MAX_SHM_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "-shm file larger than any size a cooperating writer produces",
        ));
    }
    Ok(())
}

/// A claimed WAL reader-mark slot, released (lock dropped, mark left in
/// place) when this guard drops. Holds the `-shm` file open for its own
/// lifetime so the fd used to take the lock is never closed out from
/// under it — POSIX drops all `fcntl` record locks on `close()`.
#[derive(Debug)]
pub struct WalReadLock {
    file: Arc<File>,
    slot: usize,
}

impl SharedLockGuard for WalReadLock {}

impl Drop for WalReadLock {
    fn drop(&mut self) {
        // Best-effort: `drop` can't propagate a failure, and there is
        // nothing more this crate can do about one anyway.
        fcntl_lock(&self.file, F_UNLCK, wal_read_lock_byte(self.slot), 1).ok();
    }
}

/// Claims a WAL reader-mark slot on `shm_path` (SQLite's `<db>-shm`
/// companion file) at the WAL's current `mxFrame`, so a live checkpointer
/// backing off on this slot's lock never backfills past the frame count
/// this reader is relying on. Returns `Ok(None)` if `shm_path` doesn't
/// exist — no live WAL writer has ever opened this database, so there is
/// no checkpointer to coordinate with and nothing to lock. The existence
/// check is folded into the `open` call itself (rather than a separate
/// `try_exists`) so there's no TOCTOU window between checking and
/// opening for something else to replace the path with.
///
/// Tries each of the 4 reader slots (1..=4; slot 0 is reserved, matching
/// SQLite's own protocol) in order. A slot's lock, not its stale
/// `aReadMark` value, is what determines whether it's free — the mark of
/// a slot whose lock was already released is left in place (no reader
/// resets it on drop), so it can't be used to tell "free" from "held".
/// `mxFrame` is read fresh after each successful exclusive claim, not
/// once up front — a concurrent writer could otherwise advance it in the
/// gap between reading it and acquiring the lock, publishing a stale
/// mark. `Err` on the first non-contention `fcntl` failure, or if every
/// slot is genuinely contended (`EAGAIN`/`EACCES`).
pub(crate) fn claim_wal_read_lock(shm_path: &Path) -> io::Result<Option<WalReadLock>> {
    let file = match open_shm_shared(shm_path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    validate_shm_len(&file)?;

    let mut last_err = None;
    for slot in 1..=4usize {
        let byte = wal_read_lock_byte(slot);
        // Briefly exclusive, only long enough to publish this slot's mark
        // before downgrading to the SHARED lock held for the guard's
        // lifetime — matches SQLite's own claim sequence (spike 005 exp 4).
        match fcntl_lock(&file, F_WRLCK, byte, 1) {
            Ok(()) => {
                set_read_mark(&file, slot, mx_frame(&file)?)?;
                fcntl_lock(&file, F_RDLCK, byte, 1)?;
                return Ok(Some(WalReadLock { file, slot }));
            }
            Err(e) if matches!(e.raw_os_error(), Some(EAGAIN) | Some(EACCES)) => {
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| io::Error::other("no WAL read-lock slot available")))
}

/// Test-only: whether `slot` on `shm_path` is currently free, probed via a
/// real second OS process (`src/vfs/test_lock_probe.rs`) — for tests
/// outside this module (e.g. `src/pager/mod.rs`) that need to observe
/// reader-mark lock state.
#[cfg(test)]
pub(crate) fn slot_is_free_test_only(shm_path: &Path, slot: usize) -> bool {
    super::test_lock_probe::lock_available(shm_path, "wrlock", wal_read_lock_byte(slot), 1)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::panic
)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::vfs::test_lock_probe::{hold_multiple, release_all};

    /// Builds a minimal, valid-enough `-shm` file: a zeroed wal-index
    /// header with `mxFrame` set and every `aReadMark` slot unused.
    fn temp_shm(mx_frame: u32) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("sqlite-rs-shm-test-{}-{n}-shm", std::process::id()));
        let mut bytes = vec![0u8; 32768];
        bytes[MX_FRAME_OFFSET as usize..MX_FRAME_OFFSET as usize + 4]
            .copy_from_slice(&mx_frame.to_ne_bytes());
        for slot in 0..5 {
            let off = READ_MARK_BASE_OFFSET as usize + slot * 4;
            bytes[off..off + 4].copy_from_slice(&READ_MARK_UNUSED.to_ne_bytes());
        }
        let mut file = File::create(&path).unwrap();
        file.write_all(&bytes).unwrap();
        path
    }

    #[test]
    fn missing_shm_file_yields_no_lock() {
        let path = std::env::temp_dir().join("sqlite-rs-shm-test-missing-shm");
        assert!(claim_wal_read_lock(&path).unwrap().is_none());
    }

    #[test]
    fn claims_a_slot_and_publishes_mx_frame() {
        let path = temp_shm(42);

        let guard = claim_wal_read_lock(&path).unwrap().expect("shm exists");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert_eq!(read_mark(&file, guard.slot).unwrap(), 42);

        drop(guard);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn released_lock_can_be_reclaimed() {
        let path = temp_shm(7);

        let guard = claim_wal_read_lock(&path).unwrap().unwrap();
        let slot = guard.slot;
        drop(guard);

        // The released slot's lock is available again, so a later claim
        // that tries slots in the same order (1..=4) reclaims it, even
        // though its stale `aReadMark` value (left in place, not reset by
        // `drop`) doesn't reflect that.
        let guard2 = claim_wal_read_lock(&path).unwrap().unwrap();
        assert_eq!(guard2.slot, slot, "the now-free slot should be reclaimed");

        std::fs::remove_file(&path).unwrap();
    }

    /// A slot whose lock is held by another process must be skipped in
    /// favor of the next free one — POSIX record locks never conflict
    /// with a further request from the *same* process (they're scoped to
    /// (process, inode)), so a real second process is required to prove
    /// this.
    #[test]
    fn contended_slot_is_skipped_for_the_next_free_one() {
        let path = temp_shm(5);
        let held = hold_multiple(&path, &[("rdlock", wal_read_lock_byte(1), 1)]);

        let guard = claim_wal_read_lock(&path).unwrap().unwrap();
        assert_eq!(guard.slot, 2, "slot 1 is held, so slot 2 must be claimed");

        release_all(held);
        drop(guard);
        std::fs::remove_file(&path).unwrap();
    }

    /// When every reader slot is genuinely contended, `claim_wal_read_lock`
    /// must return `Err`, not silently succeed or panic — the failure mode
    /// a `Pager::open` caller needs to distinguish from success.
    #[test]
    fn all_slots_contended_returns_err() {
        let path = temp_shm(9);
        let held = hold_multiple(
            &path,
            &[1, 2, 3, 4].map(|slot| ("rdlock", wal_read_lock_byte(slot), 1)),
        );

        let result = claim_wal_read_lock(&path);
        assert!(result.is_err(), "expected Err, got {result:?}");

        release_all(held);
        std::fs::remove_file(&path).unwrap();
    }

    /// A `-shm` file shorter than the wal-index header must be rejected,
    /// not read out-of-bounds or panic — a realistic input for a
    /// crash-truncated or half-written `-shm` file. Since `-shm` access is
    /// now `pread`/`pwrite` rather than `mmap` (#66), this is also the
    /// scenario that used to risk `SIGBUS`: a truncated file now yields
    /// this structured `Err` instead.
    #[test]
    fn truncated_shm_file_is_rejected() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sqlite-rs-shm-test-{}-{n}-truncated-shm",
            std::process::id()
        ));
        std::fs::write(&path, vec![0u8; 32]).unwrap();

        let result = claim_wal_read_lock(&path);
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
            "expected InvalidData, got {result:?}"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// A `-shm` file far larger than any size a cooperating writer produces
    /// (SQLite grows it in fixed 32KB regions, capped at 8) must be
    /// rejected rather than trusted wholesale — the upper-bound half of
    /// #54's hardening, mirroring `truncated_shm_file_is_rejected`'s
    /// lower-bound check.
    ///
    /// **Tests:** `src/vfs/shm.rs::tests::oversized_shm_file_is_rejected`
    #[test]
    fn oversized_shm_file_is_rejected() {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "sqlite-rs-shm-test-{}-{n}-oversized-shm",
            std::process::id()
        ));
        let file = File::create(&path).unwrap();
        file.set_len(MAX_SHM_LEN + 1).unwrap();
        drop(file);

        let result = claim_wal_read_lock(&path);
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
            "expected InvalidData, got {result:?}"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// `UnixVfs::claim_wal_read_lock` (the trait-level entry point
    /// `Pager::open` actually calls) must surface lock contention as
    /// `VfsError::Locked`, not just this module's lower-level `io::Error`
    /// — the busy-detection contract applies to the WAL reader-mark path
    /// too, not only the main-db SHARED lock.
    #[test]
    fn unix_vfs_surfaces_locked_error_when_all_slots_contended() {
        use crate::vfs::{companion_path, UnixVfs, Vfs, VfsError};

        // `UnixVfs::claim_wal_read_lock` takes the *main db* path and
        // derives `<db>-shm` itself — so the shm file must live at
        // `db_path` + "-shm", not at a path already ending in "-shm".
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = std::env::temp_dir().join(format!(
            "sqlite-rs-shm-test-{}-{n}-unixvfs.db",
            std::process::id()
        ));
        let shm_path = companion_path(&db_path, "-shm");
        std::fs::rename(temp_shm(3), &shm_path).unwrap();

        let held = hold_multiple(
            &shm_path,
            &[1, 2, 3, 4].map(|slot| ("rdlock", wal_read_lock_byte(slot), 1)),
        );

        let result = UnixVfs.claim_wal_read_lock(&db_path);
        match result {
            Err(VfsError::Locked { .. }) => {}
            other => panic!("expected VfsError::Locked, got {other:?}"),
        }

        release_all(held);
        std::fs::remove_file(&shm_path).unwrap();
    }

    #[test]
    fn write_lock_is_exclusive_to_a_single_writer() {
        let path = temp_shm(0);

        let held = hold_multiple(&path, &[("wrlock", WAL_WRITE_LOCK_BYTE, 1)]);
        let result = claim_wal_write_lock(&path);
        assert!(result.is_err(), "expected Err, got {result:?}");
        release_all(held);

        let guard = claim_wal_write_lock(&path).unwrap();
        drop(guard);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn mx_frame_round_trips() {
        let path = temp_shm(0);
        publish_mx_frame(&path, 7).unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert_eq!(mx_frame(&file).unwrap(), 7);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn checkpoint_lock_is_exclusive_to_a_single_checkpointer() {
        let path = temp_shm(0);

        let held = hold_multiple(&path, &[("wrlock", WAL_CKPT_LOCK_BYTE, 1)]);
        let result = claim_wal_checkpoint_lock(&path);
        assert!(result.is_err(), "expected Err, got {result:?}");
        release_all(held);

        let guard = claim_wal_checkpoint_lock(&path).unwrap();
        drop(guard);
        std::fs::remove_file(&path).unwrap();
    }

    /// #491: two different lock guards on the same `-shm` path, held
    /// concurrently *within this one process*, must not interfere via the
    /// fd each opens. Before `open_shm_shared`, each guard opened its own
    /// independent `File` — dropping one closed its own fd, and per
    /// POSIX, closing *any* fd this process holds to a file releases
    /// *every* `fcntl` lock the process holds on that inode, including a
    /// still-live sibling guard's lock taken through a different fd. A
    /// real second process (`slot_is_free_test_only`, matching this
    /// module's other lock-contention tests) is required to observe this:
    /// POSIX locks never conflict with a further request from the same
    /// process, so only an external probe can tell "genuinely still
    /// locked" from "silently released".
    #[test]
    fn dropping_one_guard_does_not_release_a_different_guards_lock() {
        let path = temp_shm(0);

        let read_guard = claim_wal_read_lock(&path).unwrap().unwrap();
        let slot = read_guard.slot;
        let ckpt_guard = claim_wal_checkpoint_lock(&path).unwrap();

        assert!(
            !slot_is_free_test_only(&path, slot),
            "the read lock must be held right after both guards are claimed"
        );

        // Dropping the *other* guard must not touch the read lock's byte
        // range, even though (pre-#491) it shared the same underlying
        // `-shm` inode via an independent fd.
        drop(ckpt_guard);
        assert!(
            !slot_is_free_test_only(&path, slot),
            "dropping a different guard on the same -shm file must not release this one's lock"
        );

        drop(read_guard);
        assert!(
            slot_is_free_test_only(&path, slot),
            "the read lock must actually release once its own guard drops"
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn active_reader_marks_reports_only_locked_slots() {
        let path = temp_shm(0);
        // Slot 1 has a live reader at mark 5; slot 2's lock is free (its
        // leftover mark of 9 must NOT be reported — occupancy is the lock,
        // not the mark, per this module's central invariant).
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        set_read_mark(&file, 1, 5).unwrap();
        set_read_mark(&file, 2, 9).unwrap();
        let held = hold_multiple(&path, &[("rdlock", wal_read_lock_byte(1), 1)]);

        let marks = active_reader_marks(&path).unwrap();
        assert_eq!(marks, vec![5]);

        release_all(held);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn active_reader_marks_is_empty_when_no_readers() {
        let path = temp_shm(0);
        assert!(active_reader_marks(&path).unwrap().is_empty());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn backfill_round_trips() {
        let path = temp_shm(0);
        assert_eq!(read_backfill(&path).unwrap(), 0);
        publish_backfill(&path, 3).unwrap();
        assert_eq!(read_backfill(&path).unwrap(), 3);
        std::fs::remove_file(&path).unwrap();
    }
}
