// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! The page-access layer between the [`Vfs`] and the b-tree cursor. See
//! `.openspec/specs/007-pager/spec.md` for the requirements this
//! implements.
//!
//! [`Pager`] implements [`PageSource`] directly, so `TableCursor<Pager>` /
//! `IndexCursor<Pager>` behave exactly like `TableCursor<VfsPageSource>` on
//! every already-covered fixture (007-pager Requirement 1's "zero behavior
//! change" scenario) — `Pager::open` only adds a check that runs once,
//! before any page is read.
//!
//! Freelist / pointer-map pages need no special handling here: the b-tree
//! cursor only ever visits pages reachable by following explicit child
//! pointers out of a b-tree page, and freelist/pointer-map pages are never
//! part of that structure (they're addressed only by a raw sequential scan,
//! which this read path never performs) — see `autovacuum_fixture_reads_identically`.
//!
//! `Pager::open` acquires a journal-mode SHARED lock (#50) and, if a
//! `-shm` file is present, a WAL reader-mark lock (#45), before serving
//! any page — both released when the `Pager` drops. Spike 005 (#8,
//! closed) validated that this obligation is real and that byte-identical
//! `fcntl` locks interoperate correctly with a live stock `sqlite3`
//! process, including its checkpointer backing off on a held reader-mark.
//! Lock contention on either surfaces as [`crate::vfs::VfsError::Locked`].
//! The per-inode fd-cache for the `close()`-drops-all-locks trap remains
//! deferred (#45) — nothing here yet opens two fds to the same path, so
//! there is no bug for it to fix; see #45 for when that changes.

pub mod checkpoint;
mod error;
pub mod freelist;
pub mod journal;
pub mod wal;

pub use error::PagerError;
pub use freelist::TrunkPage;

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::header::{JournalMode, SynchronousMode};
use crate::vfs::{
    companion_path, AnyVfs, AnyVfsFile, AnyWalShm, FileLock, PageError, PageSource, Vfs, VfsError,
    WritablePageSource,
};
use journal::{JournalError, JournalWriter};

/// The 8-byte magic that opens a valid rollback-journal header (SQLite
/// file-format reference, "The Rollback Journal"). A `-journal` file with
/// different leading bytes (e.g. all zero, from `PRAGMA
/// journal_mode=PERSIST`'s post-commit zeroing, or a short/empty file) is
/// not hot — it is safe to open the main file alongside it.
const JOURNAL_MAGIC: [u8; 8] = [0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7];

/// [`PageCache`]'s bound, on the order of SQLite's own `cache_size` pragma
/// default (#320) — a deliberate, named constant rather than a silent
/// hard-coded number, matching #269's `MAX_EPHEMERAL_ROWS` precedent.
const DEFAULT_PAGE_CACHE_CAPACITY: usize = 2000;

/// A small, bounded, hand-rolled LRU over physical page bytes (#320),
/// keyed by page number. Deliberately hand-rolled rather than a
/// dependency (`hashlink`'s `LruCache` is only a transitive *dev*
/// dependency today, via `rusqlite`, not vetted for `src/`) — the logic
/// is small enough not to justify promoting one through `cargo vet`.
///
/// Only ever holds pages that came from [`Pager`]'s own `source.read_page`
/// call — never a `dirty`/WAL-overlay page, both of which are already
/// correct and disjoint from the physical file's own pages (see
/// [`Pager::read_page`]/[`Pager::get_page_mut`]).
///
/// Recency is tracked via a monotonic tick stamped on each entry, rather
/// than a `Vec`/`VecDeque` reordered on every touch: a b-tree's root/
/// interior pages are read on nearly every cursor seek, so a cache hit
/// (the overwhelmingly common case once warm) must be O(1) — an
/// `O(capacity)` `retain`-based reorder on every hit was tried first and
/// measurably *regressed* the `join` tier-1 benchmark (millions of
/// `SeekRowid` calls each re-touching the same handful of hot pages).
/// Eviction candidates are tracked in a `BinaryHeap` keyed by tick
/// (#509), so finding the smallest tick is `O(log capacity)` rather than
/// an `O(capacity)` scan of every entry: a full sequential scan over a
/// working set larger than the cache — every read is a miss that
/// immediately evicts, the opposite of the "rare once warm" case the
/// linear scan was tuned for — turned that scan into the dominant cost,
/// visible as time inside `Pager::read_page` in a `perf`/`sample`
/// profile of the `full_scan_1col`/`full_scan_3col` benches (#509). A
/// heap entry can go stale (the same page re-inserted or re-touched
/// after being pushed, so an older tick for it still sits in the heap);
/// `insert`'s eviction loop checks the popped entry's tick against the
/// page's current tick in `entries` and skips/discards anything that no
/// longer matches, rather than removing the stale entry from the heap
/// up front (removal from a `BinaryHeap` by key isn't `O(log n)`).
/// Multiplicative `u32` hasher (FxHash's mixing constant) for
/// [`PageCache::entries`] (#457) — page numbers are plain sequential
/// integers, not attacker-controlled input, so the DoS-resistance
/// `HashMap`'s default `SipHash` provides is wasted cost on the
/// cache's hot get/insert path. Hand-rolled rather than pulling in
/// `rustc-hash`/`ahash`/`fxhash`: ADR-0022 already rejected adding a
/// dependency for this cache (`hashlink`'s `LruCache`) on the grounds
/// that the logic involved is too small to justify `cargo vet`ing a
/// new crate, and the same reasoning applies here.
#[derive(Default)]
struct PageNumHasher(u64);

impl Hasher for PageNumHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }

    fn write_u32(&mut self, i: u32) {
        self.0 = (i as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

struct PageCache {
    capacity: usize,
    entries: HashMap<u32, (Rc<[u8]>, u64), BuildHasherDefault<PageNumHasher>>,
    /// Eviction candidates as `(tick, page_num)`, smallest tick first via
    /// `Reverse`. May contain stale entries for a page that was
    /// re-inserted/re-touched after being pushed here — see this
    /// struct's own doc for why those are checked-and-skipped in
    /// `insert` rather than removed up front.
    eviction_heap: BinaryHeap<Reverse<(u64, u32)>>,
    tick: u64,
}

impl PageCache {
    fn new(capacity: usize) -> Self {
        PageCache {
            capacity,
            entries: HashMap::default(),
            eviction_heap: BinaryHeap::new(),
            tick: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    fn get(&mut self, page_num: u32) -> Option<Rc<[u8]>> {
        let tick = self.next_tick();
        // Single hash lookup per hit (#588): touch the tick and clone the
        // `Rc` out of the same `get_mut` borrow, rather than re-looking
        // the key up again after the heap push.
        let entry = self.entries.get_mut(&page_num)?;
        entry.1 = tick;
        let page = Rc::clone(&entry.0);
        self.eviction_heap.push(Reverse((tick, page_num)));
        // A hit never evicts (only an over-capacity `insert` does), so a
        // hot working set that's touched over and over (e.g. `point_lookup`/
        // `join`'s repeated root/index-page hits) would otherwise grow
        // `eviction_heap` by one stale-or-not entry per touch forever.
        // Rebuilding it from `entries`' current ticks once it's grown well
        // past `capacity` bounds it to O(capacity) amortized instead.
        if self.eviction_heap.len() > self.capacity.saturating_mul(4).max(16) {
            self.compact_eviction_heap();
        }
        Some(page)
    }

    fn compact_eviction_heap(&mut self) {
        self.eviction_heap = self
            .entries
            .iter()
            .map(|(&page_num, &(_, tick))| Reverse((tick, page_num)))
            .collect();
    }

    fn insert(&mut self, page_num: u32, bytes: Rc<[u8]>) {
        let tick = self.next_tick();
        self.entries.insert(page_num, (bytes, tick));
        self.eviction_heap.push(Reverse((tick, page_num)));
        if self.entries.len() > self.capacity {
            self.evict_one();
        }
    }

    /// Pops the least-recently-used entry, if any, discarding stale heap
    /// candidates along the way (see this struct's own doc).
    fn evict_one(&mut self) -> Option<(u32, Rc<[u8]>)> {
        while let Some(Reverse((candidate_tick, candidate_page))) = self.eviction_heap.pop() {
            let is_current = self
                .entries
                .get(&candidate_page)
                .is_some_and(|&(_, current_tick)| current_tick == candidate_tick);
            if is_current {
                return self
                    .entries
                    .remove(&candidate_page)
                    .map(|(bytes, _)| (candidate_page, bytes));
            }
            // Stale heap entry (the page was re-inserted/re-touched after
            // this tick was pushed, or already evicted) — discard and try
            // the next candidate.
        }
        None
    }

    /// If inserting one more page would push the cache over capacity,
    /// evicts one entry now and hands back its buffer (#509) so the
    /// caller can try to recycle it in place (`Rc::get_mut`) for the
    /// incoming page instead of allocating and zero-filling a fresh one.
    /// A no-op (returns `None`) once the cache isn't yet full — the
    /// initial fill-up still pays one allocation per page, same as
    /// before.
    fn evict_one_if_full(&mut self) -> Option<Rc<[u8]>> {
        if self.entries.len() < self.capacity {
            return None;
        }
        self.evict_one().map(|(_, bytes)| bytes)
    }

    fn invalidate(&mut self, page_num: u32) {
        self.entries.remove(&page_num);
    }
}

/// A source of whole database pages, refusing to open a database with a
/// hot rollback journal rather than risk serving pre-rollback pages as
/// committed data (001-architecture Req-4's "hot journal is never ignored"
/// scenario), and transparently overlaying any committed WAL frames from
/// an adjacent `-wal` file (Req-4's "Read a database with uncheckpointed
/// WAL" scenario).
pub struct Pager {
    /// Escalated to `Exclusive` for the duration of each [`Pager::flush`]
    /// and released back to `Shared` immediately after (see `flush`'s doc
    /// comment); otherwise just held for its `Drop`, which releases the
    /// SHARED lock acquired in `open`. Declared before `source`: struct
    /// fields drop in declaration order, and the lock must be released
    /// while `source`'s file handle is still open — POSIX `close()`
    /// silently drops all `fcntl` locks on that fd, so unlocking a fd
    /// number the kernel may already have reused for something else would
    /// be a real bug.
    lock: FileLock,
    /// The lock level a `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE` has already
    /// escalated `lock` to for the current transaction (`Shared` — i.e.
    /// no escalation yet — for a bare/DEFERRED `BEGIN`, #395). `flush`
    /// consults this so its own transient EXCLUSIVE escalation
    /// de-escalates back to plain `Shared` (the transaction is over,
    /// commit succeeded) rather than assuming there was nothing to
    /// release; `rollback` and a no-op `flush` (nothing dirty) use it to
    /// release a BEGIN-time lock that was never touched by a write.
    tx_lock_level: crate::vfs::LockLevel,
    /// Held only for its `Drop`, which releases the WAL `-shm`
    /// reader-mark lock claimed in `open` (#45) — `None` when there is no
    /// `-shm` file to coordinate through (no live WAL writer has ever
    /// opened this database). Owns its own file handle (`WalReadLock` in
    /// `src/vfs/shm.rs`), so no interaction with `source`'s fd/drop
    /// ordering.
    #[allow(dead_code, reason = "held only for its Drop side effect")]
    wal_lock: Option<FileLock>,
    /// A cached persistent handle to the `-shm` companion file (#437),
    /// reused by every [`Pager::flush_wal_locked`] call across this
    /// connection's lifetime instead of reopening `-shm` on each commit —
    /// see [`crate::vfs::Vfs::open_wal_shm`]'s doc comment. Lazily opened
    /// on the first WAL commit (`None` for a connection that never writes
    /// in WAL mode); reset to `None` on any WAL/journal-mode switch, since
    /// the underlying `-shm` file is deleted (`switch_wal_to_journal`) or
    /// freshly recreated (`switch_journal_to_wal`) there.
    wal_shm: Option<AnyWalShm>,
    /// A cached [`wal::WalResumeHint`] (ADR-0027) letting
    /// [`Pager::flush_wal_locked`] skip `WalWriter::open_existing`'s
    /// read-and-rescan of the whole `-wal` file when it's still valid —
    /// populated after every successful `WalWriter::sync` in this
    /// connection's lifetime, `None` for a connection that hasn't
    /// committed in WAL mode yet. Reset to `None` at the same three
    /// sites `wal_shm` above is: `switch_wal_to_journal`,
    /// `switch_journal_to_wal`, `recreate_wal_locked` — each deletes or
    /// recreates the underlying `-wal` file, so a hint captured against
    /// the old file must never be handed to a writer opened against the
    /// new one. `open_existing` itself also falls back to a full rescan
    /// whenever the file's actual size doesn't match the hint (a
    /// concurrent external writer, or a torn file from a crash), so this
    /// cache never needs to be "perfectly" invalidated — only cheaply
    /// invalidated at the points where staleness is certain.
    wal_resume: Option<wal::WalResumeHint>,
    source: WritablePageSource,
    /// Committed WAL overlay pages, shared as `Rc<[u8]>` so a read hit
    /// hands out a refcount bump instead of copying `page_size` bytes
    /// per read (#588) — these are immutable once committed, so sharing
    /// is safe; a new commit replaces the entry wholesale.
    wal_pages: HashMap<u32, Rc<[u8]>>,
    /// Pages fetched via [`Pager::get_page_mut`] since the last
    /// [`Pager::flush`], keyed by page number (#166). Also consulted by
    /// [`Pager::read_page`] ahead of `wal_pages`/`source` so an
    /// unflushed write is visible to a subsequent read through the same
    /// `Pager`.
    dirty: HashMap<u32, Vec<u8>>,
    /// Caches physical pages read via `self.source.read_page` (#320) —
    /// never a `dirty`/WAL-overlay page. `RefCell`, not a plain field:
    /// [`PageSource::read_page`] takes `&self`, so populating/touching the
    /// LRU on a read needs interior mutability (same pattern ADR-0017
    /// already established for a writable `Pager` shared as
    /// `Rc<RefCell<Pager>>`).
    page_cache: RefCell<PageCache>,
    /// The page size this database was opened with (#167) — needed by
    /// [`Pager::allocate_page`] to size a freshly-extended page and by
    /// [`Pager::deallocate_page`] to compute a trunk page's leaf capacity,
    /// without re-deriving it from `source` on every call.
    page_size: u32,
    /// Its own `Clone` of the `Vfs` `open` was called with (#172) — needed
    /// to create/delete the `-journal` companion file, since
    /// [`WritablePageSource`] only exposes the one file handle it was
    /// opened with. Both concrete `Vfs` impls (`UnixVfs`, `MemoryVfs`) are
    /// cheap to clone (a marker struct / an `Arc`-backed table). Wrapped in
    /// [`AnyVfs`] rather than a bare `Box<dyn Vfs>` field so this file
    /// never has to write `dyn` itself — `src/pager/` is not exempt from
    /// the `mvl-limit` qualified-subset gate (this module's doc comment).
    vfs: AnyVfs,
    /// The main database file's own path, precomputed once in `open`
    /// (#388) — needed by [`Pager::set_journal_mode`] to derive the
    /// `-wal`/`-shm` companion paths and to call
    /// [`checkpoint::checkpoint_passive`], which (unlike every other
    /// helper here) takes the main db path itself, not a companion path.
    db_path: PathBuf,
    /// The `-journal` companion path, precomputed once in `open`.
    journal_path: PathBuf,
    /// The journal mode this database was opened with (page 1's
    /// write/read-version bytes, read once in `open`), kept in sync by
    /// [`Pager::set_journal_mode`] on every switch (#389) — [`Pager::flush`]
    /// consults this on every call to route between the rollback-journal
    /// and WAL write paths, so it's tracked here rather than re-read off
    /// page 1 on every flush.
    journal_mode: JournalMode,
    /// `PRAGMA synchronous` (#645) — defaults to `Full` on every fresh
    /// [`Pager::open`], same as stock SQLite; never read from or
    /// written to the database file. See [`SynchronousMode`].
    synchronous: SynchronousMode,
}

/// Byte offsets of the three header fields ([`crate::header::DatabaseHeader`])
/// that freelist allocate/deallocate mutate: page count (bytes 28-31),
/// freelist trunk page (32-35), freelist page count (36-39). Patched
/// in-place on page 1's raw buffer rather than round-tripping through a
/// full header serializer, since no such serializer exists yet (#167).
const PAGE_COUNT_OFFSET: usize = 28;
const FREELIST_TRUNK_PAGE_OFFSET: usize = 32;
const FREELIST_PAGE_COUNT_OFFSET: usize = 36;

fn read_be_u32(buf: &[u8], offset: usize) -> Result<u32, freelist::FreelistError> {
    let end = offset.saturating_add(4);
    let bytes: [u8; 4] = buf
        .get(offset..end)
        .ok_or(freelist::FreelistError::PageTooShort {
            offset,
            len: buf.len(),
        })?
        .try_into()
        .map_err(|_| freelist::FreelistError::PageTooShort {
            offset,
            len: buf.len(),
        })?;
    Ok(u32::from_be_bytes(bytes))
}

fn write_be_u32(buf: &mut [u8], offset: usize, value: u32) -> Result<(), freelist::FreelistError> {
    let end = offset.saturating_add(4);
    let len = buf.len();
    let slice = buf
        .get_mut(offset..end)
        .ok_or(freelist::FreelistError::PageTooShort { offset, len })?;
    slice.copy_from_slice(&value.to_be_bytes());
    Ok(())
}

impl Pager {
    /// Opens `path` (page size `page_size`) through `vfs`. If an adjacent
    /// `-journal` file has a valid rollback-journal header (a hot
    /// journal — a prior writer never committed or crashed mid-commit),
    /// its pages are replayed into the main file and the journal deleted
    /// (`recover_hot_journal`, #172) before opening proceeds — rather
    /// than V1's original refuse-and-explain (`PagerError::HotJournal`
    /// still exists for a journal whose own header/records don't parse,
    /// which recovery can't safely act on). Returns [`PagerError::Wal`]
    /// if an adjacent non-empty `-wal` file's header is malformed or
    /// declares a page size that doesn't match `page_size`.
    ///
    /// Recovery itself (#359) matches stock SQLite's `hasHotJournal`/
    /// `sqlite3PagerSharedLock` (`os_unix.c`/`pager.c`) rather than acting
    /// on the journal's magic alone: after taking the SHARED lock every
    /// open needs anyway, a non-blocking probe checks whether some other
    /// connection already holds RESERVED (or higher) on the main file —
    /// if so, that connection is either mid-transaction or already
    /// rolling this same journal back itself, so replaying it here too
    /// would race it, and `open` fails with [`VfsError::Locked`] instead.
    /// Otherwise the lock jumps straight from SHARED to EXCLUSIVE,
    /// deliberately skipping RESERVED (see
    /// [`FileLock::escalate_to_exclusive`]'s doc comment), and every read/
    /// write/truncate of both the probe and the replay itself goes
    /// through the one fd opened below — never a second, independently
    /// opened handle to the same path (the "`close()` drops all `fcntl`
    /// locks on the inode" trap `WritablePageSource::from_file` documents).
    pub fn open<V: Vfs + Clone + 'static>(
        vfs: &V,
        path: &Path,
        page_size: u32,
    ) -> Result<Self, PagerError> {
        let journal_path = companion_path(path, "-journal");
        let mut journal_is_hot = false;
        if vfs.exists(&journal_path)? {
            let journal = vfs.open_read(&journal_path)?;
            let mut magic = [0u8; JOURNAL_MAGIC.len()];
            let n = journal.read_at(&mut magic, 0)?;
            journal_is_hot = n == JOURNAL_MAGIC.len() && magic == JOURNAL_MAGIC;
        }

        let db_file: AnyVfsFile = vfs.open_write(path)?.into();
        let mut lock = db_file.lock_shared()?;

        if journal_is_hot {
            if lock.check_reserved()? {
                return Err(VfsError::Locked {
                    path: path.display().to_string(),
                }
                .into());
            }
            lock.escalate_to_exclusive()?;
            recover_hot_journal(vfs, &db_file, &journal_path)?;
            lock.de_escalate_to_shared()?;
        }

        // Claimed before reading WAL frames below, so a live checkpointer
        // that starts backfilling/truncating mid-read still backs off on
        // this reader's slot (#45) — pinning happens before, not after,
        // the read it protects.
        let wal_lock = vfs.claim_wal_read_lock(path)?;
        let wal_pages = read_wal_pages(vfs, path, page_size)?;

        let source = WritablePageSource::from_file(db_file, page_size);
        let journal_mode = journal_mode_from_page1(&source.read_page(1)?);
        Ok(Pager {
            lock,
            tx_lock_level: crate::vfs::LockLevel::Shared,
            wal_lock,
            wal_shm: None,
            wal_resume: None,
            source,
            wal_pages,
            dirty: HashMap::new(),
            page_cache: RefCell::new(PageCache::new(DEFAULT_PAGE_CACHE_CAPACITY)),
            page_size,
            vfs: AnyVfs::new(vfs.clone()),
            db_path: path.to_path_buf(),
            journal_path,
            journal_mode,
            synchronous: SynchronousMode::default(),
        })
    }

    /// Returns a mutable buffer for page `page_num` (1-based), reading it
    /// first if it isn't already dirty. Mutations are visible to
    /// subsequent [`PageSource::read_page`] calls on this same `Pager`
    /// immediately, but only reach disk once [`Pager::flush`] runs.
    pub fn get_page_mut(&mut self, page_num: u32) -> Result<&mut Vec<u8>, PagerError> {
        // Must happen before `dirty` shadows this page number (#320): once
        // a page is dirty, `read_page` never falls through to the cache
        // for it anyway, but a *stale* cached copy of its pre-write bytes
        // must not survive to be served after a later `flush`/reopen.
        self.page_cache.borrow_mut().invalidate(page_num);
        match self.dirty.entry(page_num) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let page = read_page(&self.wal_pages, &self.source, page_num)?;
                Ok(entry.insert(page))
            }
        }
    }

    /// Commits every dirty page: writes a rollback journal recording the
    /// on-disk pre-image of each page that existed before this
    /// transaction (statement atomicity, #172), syncs it, writes the
    /// dirty pages to the main file in ascending page-number order,
    /// syncs that, then deletes the journal (DELETE mode). Pages beyond
    /// the pre-transaction page count (freshly allocated by
    /// [`Pager::allocate_page`]) are never journaled — a crash before
    /// commit leaves them unreferenced by anything on disk, and
    /// `recover_hot_journal`'s truncate-to-`initial_page_count` step
    /// drops them.
    pub fn flush(&mut self) -> Result<(), PagerError> {
        if self.dirty.is_empty() {
            // Nothing to write, but a `BEGIN IMMEDIATE`/`EXCLUSIVE` may
            // still be holding RESERVED/EXCLUSIVE from `begin_immediate`/
            // `begin_exclusive` (#395) with no write ever happening before
            // this `COMMIT` — release it now, since the transaction is
            // ending either way.
            self.release_tx_lock()?;
            return Ok(());
        }

        // WAL mode (#389) never touches the rollback journal or the main
        // file at commit time, and deliberately never escalates `self.lock`
        // (the main file's SHARED lock) to EXCLUSIVE either — that
        // escalation exists only to serialize the journal-path's direct
        // writes into the main file, and would defeat the entire point of
        // WAL ("writers don't block readers": a concurrent reader only
        // needs its own SHARED lock plus its own read-mark slot, neither of
        // which `flush_wal_locked`'s `WAL_WRITE_LOCK` touches). Mutual
        // exclusion between writers is `WAL_WRITE_LOCK` instead.
        if self.journal_mode == JournalMode::Wal {
            let result = self.flush_wal_locked();
            self.release_tx_lock()?;
            return result;
        }

        // Escalate the SHARED lock every `Pager` already holds up to
        // EXCLUSIVE before touching the journal or the main file — without
        // this, two `Pager`s (or a `Pager` racing a live stock `sqlite3`
        // writer) could both pass `open`'s SHARED check and interleave
        // journal/page writes with no real mutual exclusion, corrupting
        // both. Mirrors `sqlite3PagerCommitPhaseOne`. A contended escalation
        // (some other connection is already RESERVED-or-higher) surfaces as
        // `VfsError::Locked` here, before any byte of this transaction is
        // journaled or written — `self.dirty` is left intact so the caller
        // can retry or roll back. Already at RESERVED/EXCLUSIVE from
        // `begin_immediate`/`begin_exclusive` just steps the remainder of
        // the ladder, not a double-escalation.
        self.lock.set_level(crate::vfs::LockLevel::Exclusive)?;
        let result = self.flush_locked();
        // Always attempt to release back to SHARED, even on failure, so a
        // mid-flush error doesn't leave this connection wedged at EXCLUSIVE
        // for the rest of its lifetime. The transaction is over either way
        // (commit succeeded, or the caller will roll back), so this always
        // goes all the way to `Shared`, not back to `tx_lock_level`.
        self.lock.set_level(crate::vfs::LockLevel::Shared)?;
        self.tx_lock_level = crate::vfs::LockLevel::Shared;
        result
    }

    /// Escalates the held lock to RESERVED, as stock SQLite's `BEGIN
    /// IMMEDIATE` does at `BEGIN` time rather than waiting for the first
    /// write (#395) — a concurrent writer is blocked immediately. Fails
    /// with [`VfsError::Locked`](crate::vfs::VfsError::Locked) if another
    /// connection already holds RESERVED or higher.
    pub fn begin_immediate(&mut self) -> Result<(), PagerError> {
        self.lock.set_level(crate::vfs::LockLevel::Reserved)?;
        self.tx_lock_level = crate::vfs::LockLevel::Reserved;
        Ok(())
    }

    /// Escalates the held lock all the way to EXCLUSIVE at `BEGIN` time, as
    /// stock SQLite's `BEGIN EXCLUSIVE` does (#395) — blocks every other
    /// reader and writer immediately, not just at `COMMIT`.
    pub fn begin_exclusive(&mut self) -> Result<(), PagerError> {
        self.lock.set_level(crate::vfs::LockLevel::Exclusive)?;
        self.tx_lock_level = crate::vfs::LockLevel::Exclusive;
        Ok(())
    }

    /// The lock level escalated to by `begin_immediate`/`begin_exclusive`
    /// for the current transaction, or `Shared` if neither has run since
    /// the last commit/rollback (or since `open`). Test-only: nothing in
    /// `src/` reads this — `flush`/`rollback` consult `self.tx_lock_level`
    /// directly — but `src/vdbe/control.rs`'s tests assert on it to
    /// verify `BEGIN IMMEDIATE`/`EXCLUSIVE` escalate correctly (#395).
    #[cfg(test)]
    pub(crate) fn tx_lock_level(&self) -> crate::vfs::LockLevel {
        self.tx_lock_level
    }

    /// Releases a lock level escalated by `begin_immediate`/
    /// `begin_exclusive` back to plain `Shared`, if one is held. A no-op
    /// for a DEFERRED/bare `BEGIN`, which never escalates at `BEGIN` time.
    fn release_tx_lock(&mut self) -> Result<(), PagerError> {
        if self.tx_lock_level > crate::vfs::LockLevel::Shared {
            self.lock.set_level(crate::vfs::LockLevel::Shared)?;
            self.tx_lock_level = crate::vfs::LockLevel::Shared;
        }
        Ok(())
    }

    fn flush_locked(&mut self) -> Result<(), PagerError> {
        let mut page_nums: Vec<u32> = self.dirty.keys().copied().collect();
        page_nums.sort_unstable();

        let initial_page_count = read_be_u32(&self.source.read_page(1)?, PAGE_COUNT_OFFSET)?;
        let to_journal: Vec<u32> = page_nums
            .iter()
            .copied()
            .filter(|&n| n <= initial_page_count)
            .collect();

        if !to_journal.is_empty() {
            let writer = JournalWriter::create(
                &self.vfs,
                &self.journal_path,
                self.page_size,
                self.page_size,
                initial_page_count,
                to_journal.len() as u32,
                random_nonce(),
            )
            .map_err(journal_to_pager_error)?;
            for (index, &page_num) in to_journal.iter().enumerate() {
                let original = self.source.read_page(page_num)?;
                writer
                    .write_record(index as u32, page_num, &original)
                    .map_err(journal_to_pager_error)?;
            }
            // `PRAGMA synchronous` (#645): the journal fsync is skipped
            // only at `Off` — `Normal` keeps it, since it's what lets a
            // crash mid-write-to-the-main-file recover via
            // `recover_hot_journal` at all (ADR-0036).
            if self.synchronous != SynchronousMode::Off {
                writer.sync().map_err(journal_to_pager_error)?;
            }
        }

        for page_num in page_nums {
            if let Some(bytes) = self.dirty.get(&page_num) {
                self.source.write_page(page_num, bytes)?;
            }
        }
        // `PRAGMA synchronous` (#645): the main-file fsync (the second
        // of the two-fsync rollback-journal commit protocol) is the one
        // `Normal` relaxes relative to `Full` (ADR-0036).
        if self.synchronous == SynchronousMode::Full {
            self.source.sync()?;
        }

        if !to_journal.is_empty() {
            self.vfs.delete(&self.journal_path)?;
        }
        self.dirty.clear();
        Ok(())
    }

    /// WAL-mode half of [`Pager::flush`] (#389): appends every dirty page
    /// as a WAL frame (in ascending page-number order, matching
    /// `flush_locked`'s own ordering), marks the last one as the commit
    /// frame, publishes the new `mxFrame`, and folds the newly-written
    /// pages into `self.wal_pages` so this same connection's own
    /// subsequent reads see them immediately — without re-claiming a new
    /// WAL reader-mark slot, since `self.wal_lock`'s slot is only this
    /// connection's *own* snapshot bookkeeping, not something a writer
    /// needs to touch to observe its own just-committed writes.
    ///
    /// Unlike the rollback-journal path, every dirty page is written here
    /// — not just the ones that pre-existed the transaction — because the
    /// main database file is never touched at commit time in WAL mode; a
    /// freshly allocated page has nowhere else to live until a later
    /// checkpoint backfills it.
    ///
    /// Claims [`crate::vfs::Vfs::claim_wal_write_lock`] for the duration,
    /// so a second concurrent writer is refused (surfaces as
    /// [`crate::vfs::VfsError::Locked`], converted to [`PagerError::Vfs`]
    /// the same way every other lock-contention path already converts)
    /// rather than interleaving frames or racing the `mxFrame` publish —
    /// this is WAL's only writer-side mutual exclusion; `self.lock` (the
    /// main file's SHARED lock) is deliberately left untouched, per
    /// `flush`'s own doc comment.
    fn flush_wal_locked(&mut self) -> Result<(), PagerError> {
        let mut page_nums: Vec<u32> = self.dirty.keys().copied().collect();
        page_nums.sort_unstable();

        let wal_path = companion_path(&self.db_path, "-wal");
        let to_pager_error = |source| PagerError::Wal {
            path: wal_path.display().to_string(),
            source,
        };

        // Lazily cache a persistent `-shm` handle (#437) so every commit
        // after the first reuses the same already-open fd for the write
        // lock and the `mxFrame` publish, instead of reopening `-shm`
        // twice per commit (spike 011's dominant cost). Falls back to the
        // old per-call `Vfs::claim_wal_write_lock` for backends with no
        // real `-shm` file to cache a handle to (e.g. `MemoryVfs`).
        if self.wal_shm.is_none() {
            self.wal_shm = self.vfs.open_wal_shm(&self.db_path)?;
        }
        let fallback_guard = if self.wal_shm.is_none() {
            self.vfs.claim_wal_write_lock(&self.db_path)?
        } else {
            None
        };
        if let Some(shm) = &self.wal_shm {
            shm.claim_write_lock()?;
        }

        let outcome = (|| -> Result<(), PagerError> {
            // The post-transaction page count: layered through
            // `self.dirty` (this transaction's own edit to page 1, if
            // any) then `self.wal_pages` (a prior WAL commit's value)
            // then `self.source` (the true pre-WAL original) — i.e.
            // exactly `PageSource::read_page`'s own precedence, since the
            // main file's own page-1 bytes go stale the moment the first
            // WAL frame is ever written and stay stale until a
            // checkpoint backfills them.
            let post_page_count = read_be_u32(&self.read_page(1)?, PAGE_COUNT_OFFSET)?;

            let mut writer = match wal::WalWriter::open_existing(
                &self.vfs,
                &wal_path,
                self.page_size,
                self.wal_resume.as_ref(),
            ) {
                Ok(writer) => writer,
                // The `-wal` this `Pager` believed was live has vanished —
                // e.g. a concurrent `sqlite3` connection auto-checkpointed
                // and deleted `-wal`/`-shm` on close (#422). `journal_mode`
                // still says `Wal` (this closure only ever runs from that
                // branch), so recover exactly as `switch_journal_to_wal`
                // creates one from scratch, rather than failing the commit.
                Err(wal::WalError::Vfs(VfsError::NotFound { .. })) => self.recreate_wal_locked()?,
                Err(source) => return Err(to_pager_error(source)),
            };

            let last_index = page_nums.len().saturating_sub(1);
            for (index, &page_num) in page_nums.iter().enumerate() {
                if let Some(bytes) = self.dirty.get(&page_num) {
                    let commit_db_size = if index == last_index {
                        post_page_count
                    } else {
                        0
                    };
                    writer
                        .append_frame(page_num, bytes, commit_db_size)
                        .map_err(to_pager_error)?;
                }
            }
            // `PRAGMA synchronous` (#645): `Full` fsyncs the WAL on every
            // commit; `Normal`/`Off` don't — matching stock SQLite's
            // documented WAL+NORMAL behavior of only syncing at
            // checkpoint boundaries (ADR-0036).
            if self.synchronous == SynchronousMode::Full {
                writer.sync().map_err(to_pager_error)?;
            }
            self.wal_resume = Some(writer.resume_hint());

            let new_mx_frame = writer.frame_count();
            match &self.wal_shm {
                Some(shm) => shm.publish_mx_frame(new_mx_frame)?,
                None => self.vfs.publish_wal_mx_frame(&self.db_path, new_mx_frame)?,
            }
            Ok(())
        })();

        // Always release the write lock, success or failure, before
        // propagating — mirrors the fallback path's own `Drop`-on-scope-
        // exit release, just explicit since the cached handle's lock
        // isn't tied to a value's lifetime.
        if let Some(shm) = &self.wal_shm {
            shm.release_write_lock().ok();
        }
        drop(fallback_guard);
        outcome?;

        for page_num in page_nums {
            if let Some(bytes) = self.dirty.remove(&page_num) {
                self.wal_pages.insert(page_num, Rc::from(bytes));
            }
        }
        self.dirty.clear();
        Ok(())
    }

    /// Discards every dirty page (#360's SQL-level `ROLLBACK`, as
    /// opposed to [`recover_hot_journal`]'s crash-recovery rollback):
    /// since writes only reach disk in [`Pager::flush`], undoing an
    /// in-progress transaction is just forgetting what
    /// [`Pager::get_page_mut`] buffered — nothing to journal, sync, or
    /// evict from `page_cache` (which never holds a dirty page's
    /// content in the first place, per its own doc comment). Also
    /// releases any RESERVED/EXCLUSIVE lock a `BEGIN IMMEDIATE`/
    /// `EXCLUSIVE` escalated to at `BEGIN` time (#395), since the
    /// transaction is ending here rather than at a later `flush`.
    pub fn rollback(&mut self) -> Result<(), PagerError> {
        self.dirty.clear();
        self.release_tx_lock()
    }

    /// Switches this database between rollback-journal (`Legacy`) and
    /// WAL journal modes (#388), matching `PRAGMA journal_mode =
    /// WAL|DELETE`. A request for the mode already active is a no-op.
    /// Independently enforces "no pending transaction" (`self.dirty`
    /// empty and no `BEGIN IMMEDIATE`/`EXCLUSIVE` lock still held)
    /// regardless of what the VDBE layer already checked
    /// (`Vm::autocommit`, see `src/vdbe/pragma.rs`) — this is a public
    /// API a caller could invoke directly, so it must be independently
    /// safe. The header-byte flip always hits disk immediately via
    /// `self.source`, never through `self.dirty`/`flush` — a mode
    /// switch is not part of any pending user transaction.
    pub fn set_journal_mode(&mut self, mode: JournalMode) -> Result<(), PagerError> {
        if !self.dirty.is_empty() || self.tx_lock_level > crate::vfs::LockLevel::Shared {
            return Err(PagerError::PendingTransaction);
        }

        if self.journal_mode == mode {
            return Ok(());
        }

        match mode {
            JournalMode::Wal => self.switch_journal_to_wal()?,
            JournalMode::Legacy => self.switch_wal_to_journal()?,
        }
        self.journal_mode = mode;

        // Re-read page 1 fresh rather than reusing any copy read before
        // the branch above: `switch_wal_to_journal`'s checkpoint may
        // have just rewritten page 1 itself directly (via a separate
        // `Vfs::open_write` handle, not `self.source`) if it happened
        // to hold a backfilled frame for page 1 — writing back a copy
        // read before that would silently discard the checkpoint's own
        // write.
        let mut page1 = self.source.read_page(1)?.to_vec();
        let version_bytes = if mode == JournalMode::Wal {
            [2, 2]
        } else {
            [1, 1]
        };
        patch_journal_mode_bytes(&mut page1, version_bytes)?;
        self.source.write_page(1, &page1)?;
        self.source.sync()?;
        self.page_cache.borrow_mut().invalidate(1);
        Ok(())
    }

    /// The active `PRAGMA synchronous` level (#645), consulted by
    /// [`Pager::flush_locked`]/[`Pager::flush_wal_locked`] to decide
    /// which commit-time fsyncs to skip.
    pub fn synchronous(&self) -> SynchronousMode {
        self.synchronous
    }

    /// Sets the active `PRAGMA synchronous` level (#645). Unlike
    /// [`Pager::set_journal_mode`], this has no on-disk representation
    /// to flip and no pending-transaction restriction — stock SQLite
    /// allows changing it at any time, including mid-transaction, since
    /// it only affects fsync behavior at the *next* commit.
    pub fn set_synchronous(&mut self, mode: SynchronousMode) {
        self.synchronous = mode;
    }

    /// Recovers [`Pager::flush_wal_locked`] from a `-wal` that vanished out
    /// from under it (#422) — e.g. a concurrent `sqlite3` connection
    /// auto-checkpointed and deleted `-wal`/`-shm` on close. Creates a
    /// fresh, zero-frame `-wal`/`-shm` pair exactly like
    /// [`Pager::switch_journal_to_wal`] does for a deliberate mode switch,
    /// and re-opens [`Pager::wal_shm`] against the new file so the rest of
    /// this flush's `publish_mx_frame` call targets it instead of the
    /// stale (already-`None`) handle.
    fn recreate_wal_locked(&mut self) -> Result<wal::WalWriter, PagerError> {
        let wal_path = companion_path(&self.db_path, "-wal");
        let shm_path = companion_path(&self.db_path, "-shm");
        let to_pager_error = |source| PagerError::Wal {
            path: wal_path.display().to_string(),
            source,
        };

        let salt1 = random_nonce();
        let salt2 = random_nonce() ^ 0x5A5A_5A5A;
        let header = wal::WalHeader::new(true, self.page_size, salt1, salt2, 1);
        let writer =
            wal::WalWriter::create(&self.vfs, &wal_path, header).map_err(to_pager_error)?;

        let shm_file = self.vfs.create_or_open_write(&shm_path)?;
        shm_file.write_at(&crate::vfs::shm::fresh_shm_bytes(), 0)?;
        shm_file.sync()?;

        self.wal_shm = self.vfs.open_wal_shm(&self.db_path)?;
        // The resume hint (ADR-0027), if any, described the now-deleted
        // generation of `-wal`; `flush_wal_locked` overwrites this with
        // the fresh writer's own hint once its commit succeeds, but
        // clear it here too so a failure before that point never leaves
        // a stale hint pointing at a generation this `Pager` just
        // discarded.
        self.wal_resume = None;
        Ok(writer)
    }

    /// Journal -> WAL half of [`Pager::set_journal_mode`]: creates a
    /// fresh, zero-frame `-wal` and a fresh `-shm`. The caller
    /// (`set_journal_mode`) patches page 1's version bytes afterward.
    fn switch_journal_to_wal(&mut self) -> Result<(), PagerError> {
        let wal_path = companion_path(&self.db_path, "-wal");
        let shm_path = companion_path(&self.db_path, "-shm");

        let to_pager_error = |source| PagerError::Wal {
            path: wal_path.display().to_string(),
            source,
        };
        // Two independent nonces, not a shared one — WAL frames validate
        // against *both* salts (`wal::committed_pages`), so a real
        // implementation must never let them collide by construction;
        // XORing with a fixed pattern keeps them apart even if the
        // nanosecond clock returns the same value twice in a row.
        let salt1 = random_nonce();
        let salt2 = random_nonce() ^ 0x5A5A_5A5A;
        let header = wal::WalHeader::new(true, self.page_size, salt1, salt2, 1);
        wal::WalWriter::create(&self.vfs, &wal_path, header).map_err(to_pager_error)?;

        // Written through the abstract `Vfs` trait, not the raw
        // `std::fs` helpers `src/vfs/shm.rs`'s locking functions use —
        // those are inherently real-file-only, but the `-shm` file's
        // *content* must be creatable uniformly on every backend
        // (`MemoryVfs` included, e.g. by this module's own unit tests).
        let shm_file = self.vfs.create_or_open_write(&shm_path)?;
        shm_file.write_at(&crate::vfs::shm::fresh_shm_bytes(), 0)?;
        shm_file.sync()?;

        // Drop any handle cached (#437) against a now-stale `-shm`
        // generation — `flush_wal_locked` reopens fresh against the file
        // just created above on its next call. Likewise drop any cached
        // resume hint (ADR-0027): it was captured against whatever
        // generation of `-wal` existed before this call, which no longer
        // exists.
        self.wal_shm = None;
        self.wal_resume = None;
        Ok(())
    }

    /// WAL -> Journal/DELETE half of [`Pager::set_journal_mode`]:
    /// checkpoints every WAL frame into the main file (looping
    /// [`checkpoint::checkpoint_passive`], which only makes one pass per
    /// call, until there is nothing left to backfill), then deletes the
    /// now-empty `-wal`/`-shm`. The caller (`set_journal_mode`) patches
    /// page 1's version bytes afterward — deliberately, since the
    /// checkpoint above may itself rewrite page 1 (if a backfilled frame
    /// targets it), so the version-byte patch must happen after, against
    /// a freshly re-read copy, not before. Drops this `Pager`'s own WAL
    /// reader-mark lock first (`self.wal_lock = None`) — otherwise it
    /// would itself count as a live reader a checkpoint has to respect,
    /// and (being this same connection, with nothing else ever releasing
    /// it) could never finish draining. Also clears `self.wal_pages`: it
    /// was populated once, at `open`, as a read overlay for exactly the
    /// WAL frames that (by construction) are now backfilled into
    /// `self.source` byte-for-byte — but it must not survive as a stale
    /// overlay shadowing a *later* write to the same page once this
    /// connection is back to writing the main file directly.
    fn switch_wal_to_journal(&mut self) -> Result<(), PagerError> {
        let wal_path = companion_path(&self.db_path, "-wal");
        let shm_path = companion_path(&self.db_path, "-shm");

        self.wal_lock = None;

        // A single-writer connection with no other live reader (the
        // common case here) always fully backfills within a handful of
        // passes; this bound just rules out spinning forever if that
        // assumption is ever wrong.
        const MAX_PASSES: u32 = 1000;
        let mut complete = false;
        for _ in 0..MAX_PASSES {
            let result = checkpoint::checkpoint_passive(&self.vfs, &self.db_path, self.page_size)?;
            if result.checkpoint_complete {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(PagerError::CheckpointIncomplete);
        }

        self.vfs.delete(&wal_path)?;
        self.vfs.delete(&shm_path)?;
        self.wal_pages.clear();

        // The cached handle (#437), if any, points at the `-shm` file
        // just deleted above — drop it so a future switch back to WAL
        // reopens fresh rather than reusing a stale fd to a since-
        // deleted (or reused-inode) file. The cached resume hint
        // (ADR-0027) is stale for the same reason: the `-wal` file it
        // describes no longer exists.
        self.wal_shm = None;
        self.wal_resume = None;
        Ok(())
    }

    /// Allocates a page: pops one off the freelist if it's non-empty,
    /// otherwise extends the database by one page. Returns the allocated
    /// page's (1-based) number. Updates the freelist trunk/count fields
    /// (and, when extending, the page-count field) on page 1 in the same
    /// call, so a subsequent `flush` persists both the allocation and the
    /// header bookkeeping together.
    pub fn allocate_page(&mut self) -> Result<u32, PagerError> {
        let header = self.read_page(1)?;
        let page_count = read_be_u32(&header, PAGE_COUNT_OFFSET)?;
        let freelist_trunk_page = read_be_u32(&header, FREELIST_TRUNK_PAGE_OFFSET)?;
        let freelist_page_count = read_be_u32(&header, FREELIST_PAGE_COUNT_OFFSET)?;

        if freelist_trunk_page == 0 {
            let new_page_num = page_count.saturating_add(1);
            self.dirty
                .insert(new_page_num, vec![0u8; self.page_size as usize]);
            let page1 = self.get_page_mut(1)?;
            write_be_u32(page1, PAGE_COUNT_OFFSET, new_page_num)?;
            return Ok(new_page_num);
        }

        let trunk_buf = self.read_page(freelist_trunk_page)?;
        let mut trunk = TrunkPage::parse(&trunk_buf)?;

        let (allocated, new_trunk_page) = if let Some(leaf) = trunk.leaves.pop() {
            let trunk_buf = self.get_page_mut(freelist_trunk_page)?;
            trunk.write(trunk_buf)?;
            (leaf, freelist_trunk_page)
        } else {
            (freelist_trunk_page, trunk.next_trunk)
        };

        let page1 = self.get_page_mut(1)?;
        write_be_u32(page1, FREELIST_TRUNK_PAGE_OFFSET, new_trunk_page)?;
        write_be_u32(
            page1,
            FREELIST_PAGE_COUNT_OFFSET,
            freelist_page_count.saturating_sub(1),
        )?;
        Ok(allocated)
    }

    /// Returns `page_num` to the freelist: appended to the current trunk
    /// page's leaf array if it has room, otherwise `page_num` itself
    /// becomes the new trunk page (pointing at the old one). Updates the
    /// freelist trunk/count fields on page 1 in the same call.
    pub fn deallocate_page(&mut self, page_num: u32) -> Result<(), PagerError> {
        let header = self.read_page(1)?;
        let freelist_trunk_page = read_be_u32(&header, FREELIST_TRUNK_PAGE_OFFSET)?;
        let freelist_page_count = read_be_u32(&header, FREELIST_PAGE_COUNT_OFFSET)?;

        let max_leaves = freelist::max_leaves_per_trunk(self.page_size) as usize;
        let new_trunk_page = if freelist_trunk_page != 0 {
            let trunk_buf = self.read_page(freelist_trunk_page)?;
            let mut trunk = TrunkPage::parse(&trunk_buf)?;
            if trunk.leaves.len() < max_leaves {
                trunk.leaves.push(page_num);
                let trunk_buf = self.get_page_mut(freelist_trunk_page)?;
                trunk.write(trunk_buf)?;
                freelist_trunk_page
            } else {
                let new_trunk = TrunkPage {
                    next_trunk: freelist_trunk_page,
                    leaves: vec![],
                };
                let buf = self.get_page_mut(page_num)?;
                new_trunk.write(buf)?;
                page_num
            }
        } else {
            let new_trunk = TrunkPage {
                next_trunk: 0,
                leaves: vec![],
            };
            let buf = self.get_page_mut(page_num)?;
            new_trunk.write(buf)?;
            page_num
        };

        let page1 = self.get_page_mut(1)?;
        write_be_u32(page1, FREELIST_TRUNK_PAGE_OFFSET, new_trunk_page)?;
        write_be_u32(
            page1,
            FREELIST_PAGE_COUNT_OFFSET,
            freelist_page_count.saturating_add(1),
        )?;
        Ok(())
    }
}

/// The journal mode recorded on page 1's write/read-version bytes (18/19)
/// — [`crate::header::DatabaseHeader::journal_mode`]'s same detection
/// logic, read directly off a raw page-1 buffer rather than through a
/// parsed `DatabaseHeader` (a full header round-trip isn't needed just to
/// read two bytes). Used once by [`Pager::open`] to seed `journal_mode`;
/// kept in sync afterward by [`Pager::set_journal_mode`] rather than
/// re-derived from disk on every access.
fn journal_mode_from_page1(page1: &[u8]) -> JournalMode {
    if page1.get(18..20) == Some(&[2u8, 2u8][..]) {
        JournalMode::Wal
    } else {
        JournalMode::Legacy
    }
}

/// Overwrites page 1's write/read-version bytes (offsets 18/19 —
/// `crate::header::DatabaseHeader::journal_mode`'s detection bytes) with
/// `value` in place — shared by [`Pager::switch_journal_to_wal`]/
/// [`Pager::switch_wal_to_journal`]. Avoids direct slice indexing
/// (`clippy::indexing_slicing` is denied crate-wide) the same way
/// `write_be_u32` above does: `get_mut` + `copy_from_slice` rather than
/// `page1[18] = ...`.
fn patch_journal_mode_bytes(page1: &mut [u8], value: [u8; 2]) -> Result<(), PagerError> {
    let len = page1.len();
    let slice = page1.get_mut(18..20).ok_or({
        PagerError::Page(PageError::ShortRead {
            page_num: 1,
            expected: 20,
            got: len,
        })
    })?;
    slice.copy_from_slice(&value);
    Ok(())
}

fn journal_to_pager_error(err: JournalError) -> PagerError {
    match err {
        JournalError::Vfs(source) => PagerError::Vfs(source),
        other => PagerError::Journal(other),
    }
}

/// A checksum salt, not a security-sensitive secret — SQLite's own
/// `cksumInit` just needs to differ across journal generations so a
/// stale record from an unrelated journal doesn't validate. Nanosecond
/// clock jitter XORed with the process id is unpredictable enough for
/// that without pulling in a `rand` dependency this crate doesn't
/// otherwise need.
fn random_nonce() -> u32 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    nanos ^ std::process::id()
}

/// Replays a hot journal's pages into `db_file` and deletes the journal
/// (#172). Called from [`Pager::open`] once the journal's header magic is
/// confirmed valid and the EXCLUSIVE lock secured; a journal whose header/
/// records don't parse surfaces as [`PagerError::Journal`] rather than
/// being silently ignored, since that's a corrupt-journal condition
/// distinct from "no hot journal at all". `db_file` is the one fd
/// [`Pager::open`] already holds the lock on — recovery must never open a
/// second, independent handle to the same path (#359).
fn recover_hot_journal<V: Vfs>(
    vfs: &V,
    db_file: &AnyVfsFile,
    journal_path: &Path,
) -> Result<(), PagerError> {
    let journal_file = vfs.open_read(journal_path)?;
    let size = journal_file.size()?;
    let mut journal_bytes = vec![0u8; size as usize];
    let n = journal_file.read_at(&mut journal_bytes, 0)?;
    journal_bytes.truncate(n);

    let recovered = journal::recover(&journal_bytes, db_file).map_err(journal_to_pager_error)?;
    db_file.truncate(
        (recovered.initial_page_count as u64).saturating_mul(recovered.page_size as u64),
    )?;
    db_file.sync()?;
    vfs.delete(journal_path)?;
    Ok(())
}

/// Shared by [`Pager::read_page`] and [`Pager::get_page_mut`]: WAL overlay
/// first, then the underlying file.
fn read_page(
    wal_pages: &HashMap<u32, Rc<[u8]>>,
    source: &WritablePageSource,
    page_num: u32,
) -> Result<Vec<u8>, PageError> {
    if let Some(page) = wal_pages.get(&page_num) {
        return Ok(page.to_vec());
    }
    source.read_page(page_num).map(|bytes| bytes.to_vec())
}

/// Reads and merges committed WAL frames from `path`'s adjacent `-wal`
/// file, if one exists and is large enough to hold a header. A missing,
/// empty, or sub-header-length `-wal` file (the common case: a fully
/// checkpointed WAL truncates to empty) is not an error and yields no
/// overlay pages.
fn read_wal_pages<V: Vfs>(
    vfs: &V,
    path: &Path,
    page_size: u32,
) -> Result<HashMap<u32, Rc<[u8]>>, PagerError> {
    let wal_path = companion_path(path, "-wal");
    if !vfs.exists(&wal_path)? {
        return Ok(HashMap::new());
    }

    let wal_file = vfs.open_read(&wal_path)?;
    let size = wal_file.size()?;
    if size < wal::HEADER_LEN as u64 {
        return Ok(HashMap::new());
    }

    let mut bytes = vec![0u8; size as usize];
    let n = wal_file.read_at(&mut bytes, 0)?;
    bytes.truncate(n);
    if bytes.len() < wal::HEADER_LEN {
        return Ok(HashMap::new());
    }

    let to_pager_error = |source| PagerError::Wal {
        path: wal_path.display().to_string(),
        source,
    };

    let header = wal::WalHeader::parse(&bytes).map_err(to_pager_error)?;
    if header.page_size != page_size {
        return Err(to_pager_error(wal::WalError::InvalidPageSize {
            page_size: header.page_size,
        }));
    }

    let (pages, _committed_db_size) = wal::committed_pages(&header, &bytes);
    Ok(pages
        .into_iter()
        .map(|(page_num, content)| (page_num, Rc::from(content)))
        .collect())
}

impl PageSource for Pager {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        if let Some(page) = self.dirty.get(&page_num) {
            return Ok(Rc::from(page.as_slice()));
        }
        if let Some(page) = self.wal_pages.get(&page_num) {
            return Ok(Rc::clone(page));
        }
        if let Some(cached) = self.page_cache.borrow_mut().get(page_num) {
            return Ok(cached);
        }
        // #509: once the cache is full, every miss is also an eviction —
        // recycle the evicted page's buffer in place (`Rc::get_mut`,
        // which only succeeds if nothing else still holds a clone of it)
        // instead of paying a fresh allocation and zero-fill for the
        // incoming page. Falls back to the ordinary allocating read
        // whenever there's nothing to evict yet, or the victim is still
        // referenced elsewhere.
        let victim = self.page_cache.borrow_mut().evict_one_if_full();
        let bytes = match victim {
            Some(mut recycled) => match Rc::get_mut(&mut recycled) {
                Some(buf) => {
                    self.source.read_page_into(page_num, buf)?;
                    recycled
                }
                None => self.source.read_page(page_num)?,
            },
            None => self.source.read_page(page_num)?,
        };
        self.page_cache
            .borrow_mut()
            .insert(page_num, Rc::clone(&bytes));
        Ok(bytes)
    }
}

/// Lets a write-capable `Pager` be shared as a read-only `Rc<dyn
/// PageSource>` (e.g. `Rc::new(RefCell::new(pager))`, unsized to
/// `Rc<dyn PageSource>`) while a second `Rc` clone of the same
/// `RefCell` is kept concrete for `&mut Pager` write access (VDBE's
/// `Vm::with_writable_db`, #194) — a single underlying `Pager` serves
/// both `TableCursor`'s read traversal and the write opcodes without
/// duplicating page state.
impl PageSource for std::cell::RefCell<Pager> {
    fn read_page(&self, page_num: u32) -> Result<Rc<[u8]>, PageError> {
        self.borrow().read_page(page_num)
    }
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
    use crate::vfs::MemoryVfs;
    use std::path::PathBuf;

    fn db_with_journal(journal_bytes: Option<&[u8]>) -> (MemoryVfs, PathBuf) {
        let mut vfs = MemoryVfs::new();
        let page = vec![0u8; 512];
        vfs.insert("/test.db", page);
        if let Some(bytes) = journal_bytes {
            vfs.insert("/test.db-journal", bytes.to_vec());
        }
        (vfs, PathBuf::from("/test.db"))
    }

    #[test]
    fn no_journal_opens_cleanly() {
        let (vfs, path) = db_with_journal(None);
        assert!(Pager::open(&vfs, &path, 512).is_ok());
    }

    /// A hot journal whose header doesn't actually parse (just the bare
    /// 8-byte magic, no fields) can't be safely recovered — surfaces as
    /// [`PagerError::Journal`] rather than being silently ignored.
    #[test]
    fn hot_journal_with_unparseable_header_is_an_error() {
        let (vfs, path) = db_with_journal(Some(&JOURNAL_MAGIC));
        let result = Pager::open(&vfs, &path, 512);
        assert!(matches!(result, Err(PagerError::Journal(_))));
    }

    /// A well-formed hot journal recording no page changes (n_rec = 0,
    /// e.g. a transaction that opened but never wrote anything before
    /// crashing) recovers as a no-op: `open` succeeds and the main file
    /// is unchanged.
    #[test]
    fn hot_journal_with_zero_records_recovers_as_noop() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![7u8; 512]);
        let header = journal::JournalHeader {
            n_rec: 0,
            nonce: 42,
            initial_page_count: 1,
            sector_size: 512,
            page_size: 512,
        }
        .serialize(JOURNAL_MAGIC);
        let mut journal_bytes = vec![0u8; 512];
        journal_bytes[..journal::JOURNAL_HEADER_LEN].copy_from_slice(&header);
        vfs.insert("/test.db-journal", journal_bytes);

        let pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(pager.read_page(1).unwrap(), vec![7u8; 512].into());
        assert!(!vfs.exists(Path::new("/test.db-journal")).unwrap());
    }

    /// A crash mid-write: the main file already holds a corrupted page,
    /// and a well-formed journal records its original content. `open`
    /// must restore it before serving any page, and delete the journal.
    #[test]
    fn hot_journal_with_one_record_restores_original_page_and_deletes_journal() {
        let mut vfs = MemoryVfs::new();
        let page_size = 512u32;
        let mut db = vec![7u8; page_size as usize];
        db.extend(vec![0xFFu8; page_size as usize]); // corrupted page 2
        vfs.insert("/test.db", db);

        let original_page_2 = vec![0xAAu8; page_size as usize];
        let nonce = 42;
        let header = journal::JournalHeader {
            n_rec: 1,
            nonce,
            initial_page_count: 2,
            sector_size: page_size,
            page_size,
        }
        .serialize(JOURNAL_MAGIC);
        let mut journal_bytes = vec![0u8; page_size as usize];
        journal_bytes[..journal::JOURNAL_HEADER_LEN].copy_from_slice(&header);
        journal_bytes.extend_from_slice(&2u32.to_be_bytes());
        journal_bytes.extend_from_slice(&original_page_2);
        journal_bytes
            .extend_from_slice(&journal::page_checksum(nonce, &original_page_2).to_be_bytes());
        vfs.insert("/test.db-journal", journal_bytes);

        let pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();
        assert_eq!(pager.read_page(2).unwrap(), (original_page_2).into());
        assert!(!vfs.exists(Path::new("/test.db-journal")).unwrap());
    }

    /// A second connection already holding RESERVED on the main file is
    /// either mid-transaction or already rolling this same journal back
    /// itself — replaying it here too would race it (oracle:
    /// `sqlite3OsCheckReservedLock` in `pager.c`'s `hasHotJournal`, #359).
    /// `open` must refuse rather than recover, leaving the journal and the
    /// (still-corrupted) main file untouched.
    #[test]
    fn hot_journal_open_fails_when_another_connection_holds_reserved() {
        use crate::vfs::test_lock_probe::lock_held_by_subprocess;
        use crate::vfs::UnixVfs;
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sqlite-rs-pager-reserved-race-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");

        let page_size = 512u32;
        let mut db = vec![7u8; page_size as usize];
        db.extend(vec![0xFFu8; page_size as usize]); // corrupted page 2
        std::fs::write(&path, &db).unwrap();

        let original_page_2 = vec![0xAAu8; page_size as usize];
        let nonce = 42;
        let header = journal::JournalHeader {
            n_rec: 1,
            nonce,
            initial_page_count: 2,
            sector_size: page_size,
            page_size,
        }
        .serialize(JOURNAL_MAGIC);
        let mut journal_bytes = vec![0u8; page_size as usize];
        journal_bytes[..journal::JOURNAL_HEADER_LEN].copy_from_slice(&header);
        journal_bytes.extend_from_slice(&2u32.to_be_bytes());
        journal_bytes.extend_from_slice(&original_page_2);
        journal_bytes
            .extend_from_slice(&journal::page_checksum(nonce, &original_page_2).to_be_bytes());
        let journal_path = dir.join("test.db-journal");
        std::fs::write(&journal_path, &journal_bytes).unwrap();

        let (start, len) = crate::vfs::lock::reserved_byte_range();
        let result = lock_held_by_subprocess(&path, "wrlock", start, len, || {
            Pager::open(&UnixVfs, &path, page_size)
        });

        match result {
            Err(PagerError::Vfs(crate::vfs::VfsError::Locked { .. })) => {}
            Err(other) => panic!("expected Locked, got {other:?}"),
            Ok(_) => panic!("expected Locked, got Ok"),
        }
        assert!(
            journal_path.exists(),
            "a journal another connection may still be rolling back must not be deleted"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            db,
            "the main file must not be touched when recovery is refused"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Regression for the flush-time locking gap the V5 review found:
    /// before this fix, `flush` never escalated past the plain SHARED
    /// lock `open` takes, so a concurrent connection already RESERVED (or
    /// higher) — e.g. mid-commit itself — could not block a racing
    /// `flush`, and the two could interleave journal/page writes. Now
    /// `flush` must fail fast with `Locked` instead.
    #[test]
    fn flush_fails_when_another_connection_holds_reserved() {
        use crate::vfs::test_lock_probe::lock_held_by_subprocess;
        use crate::vfs::UnixVfs;
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sqlite-rs-pager-flush-race-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        let page_size = 512u32;
        std::fs::write(&path, vec![7u8; page_size as usize]).unwrap();

        let mut pager = Pager::open(&UnixVfs, &path, page_size).unwrap();
        *pager.get_page_mut(1).unwrap() = vec![9u8; page_size as usize];

        let (start, len) = crate::vfs::lock::reserved_byte_range();
        let result = lock_held_by_subprocess(&path, "wrlock", start, len, || pager.flush());

        match result {
            Err(PagerError::Vfs(crate::vfs::VfsError::Locked { .. })) => {}
            Err(other) => panic!("expected Locked, got {other:?}"),
            Ok(_) => panic!("expected Locked, got Ok"),
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            vec![7u8; page_size as usize],
            "a blocked flush must not have written any page to the main file"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn zeroed_persist_mode_journal_is_not_hot() {
        let (vfs, path) = db_with_journal(Some(&[0u8; 8]));
        assert!(Pager::open(&vfs, &path, 512).is_ok());
    }

    #[test]
    fn empty_journal_file_is_not_hot() {
        let (vfs, path) = db_with_journal(Some(&[]));
        assert!(Pager::open(&vfs, &path, 512).is_ok());
    }

    #[test]
    fn short_journal_file_is_not_hot() {
        let (vfs, path) = db_with_journal(Some(&JOURNAL_MAGIC[..4]));
        assert!(Pager::open(&vfs, &path, 512).is_ok());
    }

    #[test]
    fn wal_file_exactly_at_header_len_is_parsed_not_skipped_as_too_short() {
        // A `-wal` file of exactly `wal::HEADER_LEN` (32) bytes must be
        // handed to WalHeader::parse, not treated as "too short to hold a
        // header" — pins read_wal_pages's two `size < HEADER_LEN` checks
        // against mutation to `==`/`<=`, which would wrongly skip this
        // length instead of parsing it. All-zero bytes make an invalid
        // magic, so a real parse attempt surfaces as PagerError::Wal
        // rather than the Ok(empty-overlay) that skipping would produce.
        let (mut vfs, path) = db_with_journal(None);
        vfs.insert("/test.db-wal", vec![0u8; 32]);
        let result = Pager::open(&vfs, &path, 512);
        assert!(matches!(result, Err(PagerError::Wal { .. })));
    }

    /// 001-architecture Req-4's "Reader takes a SHARED lock before
    /// serving pages" scenario: a live `Pager` must hold the journal-mode
    /// SHARED lock (blocking a concurrent EXCLUSIVE lock attempt from
    /// another process) and release it once dropped.
    #[test]
    fn open_acquires_shared_lock_released_on_drop() {
        use crate::vfs::lock::exclusive_lock_available;
        use crate::vfs::UnixVfs;
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sqlite-rs-pager-lock-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        std::fs::write(&path, vec![0u8; 512]).unwrap();

        let vfs = UnixVfs;
        let pager = Pager::open(&vfs, &path, 512).unwrap();

        assert!(
            !exclusive_lock_available(&path),
            "an open Pager must hold a SHARED lock blocking a concurrent EXCLUSIVE lock"
        );

        drop(pager);

        assert!(
            exclusive_lock_available(&path),
            "dropping the Pager must release the SHARED lock"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 001-architecture Req-4's WAL reader-mark scenario (#45): opening a
    /// `Pager` against a db with an adjacent `-shm` file must claim a WAL
    /// reader-mark slot, blocking a concurrent EXCLUSIVE lock attempt on
    /// that slot from another process, and release it on drop — the same
    /// shape as the journal-mode SHARED lock test above, one layer up.
    #[test]
    fn open_claims_wal_read_lock_when_shm_present_released_on_drop() {
        use crate::vfs::shm::slot_is_free_test_only;
        use crate::vfs::UnixVfs;
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "sqlite-rs-pager-wal-lock-test-{}-{n}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        std::fs::write(&path, vec![0u8; 512]).unwrap();
        let shm_path = dir.join("test.db-shm");
        std::fs::write(&shm_path, vec![0u8; 32768]).unwrap();

        let vfs = UnixVfs;
        let pager = Pager::open(&vfs, &path, 512).unwrap();

        let claimed_slot = (1..=4)
            .find(|&slot| !slot_is_free_test_only(&shm_path, slot))
            .expect("Pager::open must claim exactly one reader-mark slot");

        drop(pager);

        assert!(
            slot_is_free_test_only(&shm_path, claimed_slot),
            "dropping the Pager must release the WAL reader-mark lock"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// 007-pager write-path Requirement 4/5's core roundtrip: a page
    /// mutated via `get_page_mut` reads back the new bytes immediately
    /// (before flush), and is still readable identically after `flush`
    /// clears the dirty set — from both `Pager::read_page` and a fresh
    /// `Pager::open` over the same underlying file.
    #[test]
    fn get_page_mut_then_flush_roundtrips() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);

        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        let page = pager.get_page_mut(2).unwrap();
        page.fill(9u8);
        assert_eq!(pager.read_page(2).unwrap(), vec![9u8; 512].into());
        // Untouched page is unaffected.
        assert_eq!(pager.read_page(1).unwrap(), vec![1u8; 512].into());

        pager.flush().unwrap();

        assert_eq!(pager.read_page(2).unwrap(), vec![9u8; 512].into());

        let reopened = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(reopened.read_page(2).unwrap(), vec![9u8; 512].into());
        assert_eq!(reopened.read_page(1).unwrap(), vec![1u8; 512].into());
    }

    /// #320: a page cached by an earlier `read_page` must not survive a
    /// later `get_page_mut` write to the same page — without the
    /// `invalidate` call in `get_page_mut`, this would return the stale
    /// pre-write bytes from the cache instead of the flushed new ones.
    #[test]
    fn cached_page_is_invalidated_by_a_later_write() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![1u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        // Populate the cache with page 1's original bytes.
        assert_eq!(pager.read_page(1).unwrap(), vec![1u8; 512].into());

        pager.get_page_mut(1).unwrap().fill(9u8);
        pager.flush().unwrap();

        assert_eq!(pager.read_page(1).unwrap(), vec![9u8; 512].into());
    }

    /// #469: `Pager::read_page`'s cache-hit branch (`PageSource for
    /// Pager`, not the `PageCache` helper below) must return the same
    /// `Rc<[u8]>` allocation on a repeat read of an already-cached page —
    /// a refcount bump, not a fresh clone of the page bytes — which is
    /// what #467's `Payload::Local` zero-copy sharing relies on.
    #[test]
    fn read_page_cache_hit_shares_the_same_rc_allocation() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![7u8; 512]);
        let pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        let first = pager.read_page(1).unwrap();
        let second = pager.read_page(1).unwrap();

        assert!(
            Rc::ptr_eq(&first, &second),
            "expected a cache hit to share the page's Rc allocation, not clone it"
        );
    }

    #[test]
    fn page_cache_hit_returns_the_same_bytes_as_the_original_read() {
        let mut cache = PageCache::new(2);
        assert_eq!(cache.get(1), None);
        cache.insert(1, (vec![1u8; 4]).into());
        assert_eq!(cache.get(1), Some(Rc::from(vec![1u8; 4])));
    }

    #[test]
    fn page_cache_evicts_least_recently_used_at_capacity() {
        let mut cache = PageCache::new(2);
        cache.insert(1, (vec![1u8]).into());
        cache.insert(2, (vec![2u8]).into());
        // Touch page 1 so page 2 becomes the least-recently-used entry.
        assert_eq!(cache.get(1), Some(Rc::from(vec![1u8])));
        cache.insert(3, Rc::from(vec![3u8]));

        assert_eq!(cache.get(1), Some(Rc::from(vec![1u8])));
        assert_eq!(cache.get(2), None, "page 2 should have been evicted");
        assert_eq!(cache.get(3), Some(Rc::from(vec![3u8])));
    }

    #[test]
    fn page_cache_invalidate_removes_the_entry() {
        let mut cache = PageCache::new(2);
        cache.insert(1, Rc::from(vec![1u8]));
        cache.invalidate(1);
        assert_eq!(cache.get(1), None);
    }

    #[test]
    fn flush_with_no_dirty_pages_is_a_no_op() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![7u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.flush().unwrap();
        assert_eq!(pager.read_page(1).unwrap(), Rc::from(vec![7u8; 512]));
    }

    #[test]
    fn synchronous_defaults_to_full() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![1u8; 512]);
        let pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(pager.synchronous(), SynchronousMode::Full);
    }

    #[test]
    fn set_synchronous_roundtrips_and_is_never_a_pending_transaction_error() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        // Unlike `set_journal_mode`, allowed even with a dirty page —
        // stock SQLite lets `synchronous` change mid-transaction (#645).
        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.set_synchronous(SynchronousMode::Off);
        assert_eq!(pager.synchronous(), SynchronousMode::Off);
    }

    /// #645/ADR-0036: `Full` (the default) fsyncs both the journal and
    /// the main file on a rollback-journal commit — the two-fsync
    /// protocol this pager used unconditionally before `synchronous`
    /// existed.
    #[test]
    fn synchronous_full_syncs_journal_and_main_file_on_rollback_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        let before = vfs.sync_calls();
        pager.flush().unwrap();
        assert_eq!(
            vfs.sync_calls() - before,
            2,
            "journal fsync + main-file fsync"
        );
    }

    /// #645/ADR-0036: `Normal` keeps the journal fsync (still needed for
    /// `recover_hot_journal` to be safe) but skips the main-file fsync.
    #[test]
    fn synchronous_normal_skips_main_file_sync_on_rollback_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_synchronous(SynchronousMode::Normal);

        pager.get_page_mut(2).unwrap().fill(9u8);
        let before = vfs.sync_calls();
        pager.flush().unwrap();
        assert_eq!(vfs.sync_calls() - before, 1, "journal fsync only");
    }

    /// #645/ADR-0036: `Off` skips every commit-time fsync.
    #[test]
    fn synchronous_off_skips_all_syncs_on_rollback_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_synchronous(SynchronousMode::Off);

        pager.get_page_mut(2).unwrap().fill(9u8);
        let before = vfs.sync_calls();
        pager.flush().unwrap();
        assert_eq!(vfs.sync_calls() - before, 0);
    }

    /// #645/ADR-0036: `Full` fsyncs the WAL on every commit.
    #[test]
    fn synchronous_full_syncs_wal_frame_on_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        let before = vfs.sync_calls();
        pager.flush().unwrap();
        assert_eq!(vfs.sync_calls() - before, 1, "WAL frame fsync");
    }

    /// #645/ADR-0036: `Normal`/`Off` in WAL mode skip the per-commit
    /// frame fsync — matching stock SQLite's documented behavior of only
    /// syncing WAL+NORMAL at checkpoint boundaries.
    #[test]
    fn synchronous_normal_skips_wal_frame_sync_on_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();
        pager.set_synchronous(SynchronousMode::Normal);

        pager.get_page_mut(2).unwrap().fill(9u8);
        let before = vfs.sync_calls();
        pager.flush().unwrap();
        assert_eq!(vfs.sync_calls() - before, 0);
    }

    /// #388: `PRAGMA journal_mode=WAL` creates a fresh `-wal`/`-shm` and
    /// flips page 1's write/read-version bytes (18/19) to `2, 2`.
    #[test]
    fn set_journal_mode_wal_creates_wal_and_shm_and_flips_header_bytes() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![0u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.set_journal_mode(JournalMode::Wal).unwrap();

        assert!(vfs.exists(Path::new("/test.db-wal")).unwrap());
        assert!(vfs.exists(Path::new("/test.db-shm")).unwrap());
        let page1 = pager.read_page(1).unwrap();
        assert_eq!(page1.get(18..20), Some(&[2u8, 2u8][..]));
    }

    /// A request for the mode already active is a no-op — no `-wal`/
    /// `-shm` created for a database already in `Legacy` mode.
    #[test]
    fn set_journal_mode_legacy_when_already_legacy_is_a_no_op() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![0u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.set_journal_mode(JournalMode::Legacy).unwrap();

        assert!(!vfs.exists(Path::new("/test.db-wal")).unwrap());
        assert!(!vfs.exists(Path::new("/test.db-shm")).unwrap());
    }

    /// A mode switch must refuse to run with dirty (unflushed) pages
    /// pending — mirrors stock SQLite's refusal to change journal_mode
    /// mid-transaction (the VDBE-level `Vm::autocommit` check normally
    /// prevents reaching this at all, but `Pager::set_journal_mode` is a
    /// public API and must be independently safe).
    #[test]
    fn set_journal_mode_errors_with_a_pending_transaction() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![0u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.get_page_mut(1).unwrap();

        let result = pager.set_journal_mode(JournalMode::Wal);
        assert!(matches!(result, Err(PagerError::PendingTransaction)));
    }

    /// #388: `PRAGMA journal_mode=DELETE` while WAL is active checkpoints
    /// every pending WAL frame into the main file (even one that targets
    /// page 1 itself — the regression this pins: the version-byte patch
    /// must be applied *after* the checkpoint, to a freshly re-read page
    /// 1, not to a stale pre-checkpoint copy) before deleting `-wal`/
    /// `-shm` and flipping the header bytes back to `1, 1`.
    #[test]
    fn set_journal_mode_legacy_checkpoints_pending_wal_frames_targeting_page_one() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![0u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        // Simulate a committed WAL frame targeting page 1 — the write
        // path that would normally append this (#389) is out of scope
        // for this ticket, so it's synthesized directly here, the same
        // way `src/pager/checkpoint.rs`'s own tests do.
        let any_vfs = AnyVfs::new(vfs.clone());
        let wal_path = companion_path(Path::new("/test.db"), "-wal");
        let header = wal::WalHeader::new(true, 512, 0x1111, 0x2222, 1);
        let mut writer = wal::WalWriter::create(&any_vfs, &wal_path, header).unwrap();
        let mut new_page1 = vec![7u8; 512];
        new_page1[18] = 9; // must not survive -- overwritten by the post-checkpoint patch
        new_page1[19] = 9;
        writer.append_frame(1, &new_page1, 1).unwrap();
        writer.sync().unwrap();

        pager.set_journal_mode(JournalMode::Legacy).unwrap();

        assert!(!vfs.exists(Path::new("/test.db-wal")).unwrap());
        assert!(!vfs.exists(Path::new("/test.db-shm")).unwrap());
        let reopened = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        let page1 = reopened.read_page(1).unwrap();
        assert_eq!(page1.get(18..20), Some(&[1u8, 1u8][..]));
        assert_eq!(
            page1.first(),
            Some(&7u8),
            "the checkpointed frame's other bytes must survive"
        );
    }

    /// Round trip: Legacy -> WAL -> Legacy preserves data written while
    /// WAL was active.
    #[test]
    fn set_journal_mode_round_trip_preserves_data() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.set_journal_mode(JournalMode::Wal).unwrap();
        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.flush().unwrap();
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![9u8; 512]));

        pager.set_journal_mode(JournalMode::Legacy).unwrap();

        assert!(!vfs.exists(Path::new("/test.db-wal")).unwrap());
        assert!(!vfs.exists(Path::new("/test.db-shm")).unwrap());
        let page1 = pager.read_page(1).unwrap();
        assert_eq!(page1.get(18..20), Some(&[1u8, 1u8][..]));
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![9u8; 512]));
    }

    /// #389: `flush` in WAL mode must append a WAL frame rather than
    /// writing straight into the main file — the main file's own bytes
    /// for the written page must stay exactly as they were before the
    /// commit, with only the `-wal` file (verified here via
    /// `wal::committed_pages`) reflecting the new content.
    #[test]
    fn flush_in_wal_mode_appends_a_frame_and_leaves_the_main_file_untouched() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.flush().unwrap();

        // The main file's page 2 must be untouched — only the WAL frame
        // (and this same connection's in-memory `wal_pages` overlay) has
        // the new content.
        let db_file = vfs.open_read(Path::new("/test.db")).unwrap();
        let mut raw_page2 = vec![0u8; 512];
        db_file.read_at(&mut raw_page2, 512).unwrap();
        assert_eq!(
            raw_page2,
            vec![2u8; 512],
            "flush in WAL mode must never write the main db file directly"
        );

        let wal_file = vfs.open_read(Path::new("/test.db-wal")).unwrap();
        let size = wal_file.size().unwrap();
        let mut wal_bytes = vec![0u8; size as usize];
        wal_file.read_at(&mut wal_bytes, 0).unwrap();
        let header = wal::WalHeader::parse(&wal_bytes).unwrap();
        let (pages, db_size) = wal::committed_pages(&header, &wal_bytes);
        assert_eq!(db_size, 2);
        assert_eq!(pages.get(&2), Some(&vec![9u8; 512]));

        // The writer's own connection sees its just-committed write
        // immediately, without re-claiming a reader slot.
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![9u8; 512]));
    }

    /// #422: a concurrent connection (e.g. a real `sqlite3` client)
    /// auto-checkpointing and deleting `-wal`/`-shm` out from under this
    /// `Pager` — while it still believes `journal_mode` is `Wal` — must
    /// not fail the next commit. `flush_wal_locked` should transparently
    /// recreate a fresh `-wal`/`-shm` pair and succeed, exactly as if this
    /// were a first WAL write after a deliberate mode switch.
    #[test]
    fn flush_in_wal_mode_recovers_when_wal_and_shm_vanish() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        // A concurrent connection auto-checkpoints and removes both
        // companion files, but this `Pager`'s in-memory `journal_mode`
        // still (correctly) says `Wal`.
        vfs.delete(Path::new("/test.db-wal")).unwrap();
        vfs.delete(Path::new("/test.db-shm")).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.flush().unwrap();

        assert!(vfs.exists(Path::new("/test.db-wal")).unwrap());
        assert!(vfs.exists(Path::new("/test.db-shm")).unwrap());
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![9u8; 512]));

        let wal_file = vfs.open_read(Path::new("/test.db-wal")).unwrap();
        let size = wal_file.size().unwrap();
        let mut wal_bytes = vec![0u8; size as usize];
        wal_file.read_at(&mut wal_bytes, 0).unwrap();
        let header = wal::WalHeader::parse(&wal_bytes).unwrap();
        let (pages, db_size) = wal::committed_pages(&header, &wal_bytes);
        assert_eq!(db_size, 2);
        assert_eq!(pages.get(&2), Some(&vec![9u8; 512]));
    }

    /// ADR-0027: two consecutive commits from the same `Pager` must both
    /// be correct once the second one resumes from the cached
    /// `wal_resume` hint instead of rescanning the whole `-wal` file —
    /// the checksum chain `wal::committed_pages` verifies on read would
    /// break immediately if the cached offset/running-checksum state
    /// were wrong.
    #[test]
    fn flush_wal_mode_second_commit_resumes_correctly_from_cached_hint() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.flush().unwrap();
        assert!(pager.wal_resume.is_some());

        pager.get_page_mut(2).unwrap().fill(11u8);
        pager.flush().unwrap();

        let wal_file = vfs.open_read(Path::new("/test.db-wal")).unwrap();
        let size = wal_file.size().unwrap();
        let mut wal_bytes = vec![0u8; size as usize];
        wal_file.read_at(&mut wal_bytes, 0).unwrap();
        let header = wal::WalHeader::parse(&wal_bytes).unwrap();
        let (pages, db_size) = wal::committed_pages(&header, &wal_bytes);
        assert_eq!(db_size, 2);
        assert_eq!(pages.get(&2), Some(&vec![11u8; 512]));
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![11u8; 512]));
    }

    /// ADR-0027: a mode round trip (WAL -> Legacy -> WAL) must invalidate
    /// the cached `wal_resume` hint, since `switch_wal_to_journal`
    /// deletes the old `-wal` file and `switch_journal_to_wal` creates an
    /// unrelated one with fresh salts — resuming against the stale hint
    /// would append onto (or validate against) a generation that no
    /// longer exists.
    #[test]
    fn flush_wal_mode_after_mode_round_trip_does_not_reuse_stale_hint() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        pager.get_page_mut(2).unwrap().fill(9u8);
        pager.flush().unwrap();
        assert!(pager.wal_resume.is_some());

        pager.set_journal_mode(JournalMode::Legacy).unwrap();
        assert!(pager.wal_resume.is_none());
        pager.set_journal_mode(JournalMode::Wal).unwrap();
        assert!(pager.wal_resume.is_none());

        pager.get_page_mut(2).unwrap().fill(11u8);
        pager.flush().unwrap();

        let wal_file = vfs.open_read(Path::new("/test.db-wal")).unwrap();
        let size = wal_file.size().unwrap();
        let mut wal_bytes = vec![0u8; size as usize];
        wal_file.read_at(&mut wal_bytes, 0).unwrap();
        let header = wal::WalHeader::parse(&wal_bytes).unwrap();
        let (pages, db_size) = wal::committed_pages(&header, &wal_bytes);
        assert_eq!(db_size, 2);
        assert_eq!(pages.get(&2), Some(&vec![11u8; 512]));
    }

    /// ADR-0027: a concurrent writer appending frames to `-wal` between
    /// two commits from this `Pager` must be detected by the resume
    /// hint's size check and force a full rescan, rather than resuming
    /// the checksum chain from a stale cached offset and corrupting the
    /// file — the same hazard ADR-0026 accepted the full-rescan cost to
    /// avoid in the first place.
    #[test]
    fn flush_wal_mode_falls_back_to_rescan_when_wal_grew_from_elsewhere() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 3).unwrap();
        contents.extend(vec![2u8; 512]);
        contents.extend(vec![3u8; 512]);
        vfs.insert("/test.db", contents);

        let mut writer_a = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        writer_a.set_journal_mode(JournalMode::Wal).unwrap();
        writer_a.get_page_mut(2).unwrap().fill(9u8);
        writer_a.flush().unwrap();
        assert!(writer_a.wal_resume.is_some());

        // A second connection, sharing the same underlying `-wal` file,
        // commits a frame `writer_a` never learns about through its own
        // cache.
        let mut writer_b = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        writer_b.get_page_mut(3).unwrap().fill(7u8);
        writer_b.flush().unwrap();

        // `writer_a`'s cached hint still reflects the file as it was
        // after its own commit, not `writer_b`'s — the size check inside
        // `WalWriter::open_existing` must catch the mismatch and rescan.
        writer_a.get_page_mut(2).unwrap().fill(11u8);
        writer_a.flush().unwrap();

        let wal_file = vfs.open_read(Path::new("/test.db-wal")).unwrap();
        let size = wal_file.size().unwrap();
        let mut wal_bytes = vec![0u8; size as usize];
        wal_file.read_at(&mut wal_bytes, 0).unwrap();
        let header = wal::WalHeader::parse(&wal_bytes).unwrap();
        let (pages, db_size) = wal::committed_pages(&header, &wal_bytes);
        assert_eq!(db_size, 3);
        assert_eq!(pages.get(&2), Some(&vec![11u8; 512]));
        assert_eq!(pages.get(&3), Some(&vec![7u8; 512]));
    }

    /// #389's "readers don't block writers, writers don't block readers,
    /// reader sees a consistent snapshot" invariant: a `Pager` opened
    /// before a commit keeps its pre-commit view even after a second
    /// `Pager` commits a WAL frame, while a third, freshly-opened `Pager`
    /// sees the new data.
    #[test]
    fn reader_keeps_its_snapshot_across_a_concurrent_wal_commit() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 2).unwrap();
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);
        let mut writer = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        writer.set_journal_mode(JournalMode::Wal).unwrap();

        // Reader B opens before the commit below and must never see it.
        let reader_before = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(
            reader_before.read_page(2).unwrap(),
            Rc::from(vec![2u8; 512])
        );

        writer.get_page_mut(2).unwrap().fill(9u8);
        writer.flush().unwrap();

        assert_eq!(
            reader_before.read_page(2).unwrap(),
            Rc::from(vec![2u8; 512]),
            "a reader opened before the commit must keep its own snapshot"
        );

        // Reader C, opened fresh after the commit, must see the new data.
        let reader_after = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(reader_after.read_page(2).unwrap(), Rc::from(vec![9u8; 512]));
    }

    /// #389: `rollback` in WAL mode must discard dirty pages without ever
    /// touching the `-wal` file — frames are only ever appended at commit
    /// time, never speculatively.
    #[test]
    fn rollback_in_wal_mode_never_touches_the_wal_file() {
        let mut vfs = MemoryVfs::new();
        vfs.insert("/test.db", vec![0u8; 512]);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        pager.set_journal_mode(JournalMode::Wal).unwrap();

        let wal_size_before = vfs
            .open_read(Path::new("/test.db-wal"))
            .unwrap()
            .size()
            .unwrap();

        pager.get_page_mut(1).unwrap().fill(0xAA);
        pager.rollback().unwrap();

        let wal_size_after = vfs
            .open_read(Path::new("/test.db-wal"))
            .unwrap()
            .size()
            .unwrap();
        assert_eq!(
            wal_size_before, wal_size_after,
            "rollback must never append a frame"
        );
        assert_eq!(
            pager.read_page(1).unwrap().get(18..20),
            Some(&[2u8, 2u8][..])
        );
    }

    /// A one-page database (empty freelist) allocates by extending the
    /// file, bumping the header's page-count field.
    #[test]
    fn allocate_with_empty_freelist_extends_file() {
        let mut vfs = MemoryVfs::new();
        let mut header = vec![0u8; 512];
        write_be_u32(&mut header, PAGE_COUNT_OFFSET, 1).unwrap();
        vfs.insert("/test.db", header);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        let allocated = pager.allocate_page().unwrap();
        assert_eq!(allocated, 2);
        let new_header = pager.read_page(1).unwrap();
        assert_eq!(read_be_u32(&new_header, PAGE_COUNT_OFFSET).unwrap(), 2);
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![0u8; 512]));
    }

    /// Deallocating a page with no existing freelist makes it the sole
    /// trunk page; allocating again pops that same page straight back
    /// off, without touching the page-count field.
    #[test]
    fn deallocate_then_allocate_round_trips_single_page() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![0u8; 512 * 3];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 3).unwrap();
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.deallocate_page(3).unwrap();
        let after_dealloc = pager.read_page(1).unwrap();
        assert_eq!(
            read_be_u32(&after_dealloc, FREELIST_TRUNK_PAGE_OFFSET).unwrap(),
            3
        );
        assert_eq!(
            read_be_u32(&after_dealloc, FREELIST_PAGE_COUNT_OFFSET).unwrap(),
            1
        );

        let allocated = pager.allocate_page().unwrap();
        assert_eq!(allocated, 3);
        let after_alloc = pager.read_page(1).unwrap();
        assert_eq!(
            read_be_u32(&after_alloc, FREELIST_TRUNK_PAGE_OFFSET).unwrap(),
            0
        );
        assert_eq!(
            read_be_u32(&after_alloc, FREELIST_PAGE_COUNT_OFFSET).unwrap(),
            0
        );
        // Page count untouched — this allocation came from the freelist,
        // not from extending the file.
        assert_eq!(read_be_u32(&after_alloc, PAGE_COUNT_OFFSET).unwrap(), 3);
    }

    /// A second deallocated page joins the existing trunk's leaf array
    /// instead of becoming a new trunk, and allocation pops leaves before
    /// ever consuming the trunk page itself.
    #[test]
    fn deallocate_appends_to_existing_trunk_leaves() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![0u8; 512 * 4];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 4).unwrap();
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();

        pager.deallocate_page(3).unwrap();
        pager.deallocate_page(4).unwrap();
        let after_dealloc = pager.read_page(1).unwrap();
        assert_eq!(
            read_be_u32(&after_dealloc, FREELIST_TRUNK_PAGE_OFFSET).unwrap(),
            3
        );
        assert_eq!(
            read_be_u32(&after_dealloc, FREELIST_PAGE_COUNT_OFFSET).unwrap(),
            2
        );
        let trunk = TrunkPage::parse(&pager.read_page(3).unwrap()).unwrap();
        assert_eq!(trunk.leaves, vec![4]);

        // Leaf pops first...
        assert_eq!(pager.allocate_page().unwrap(), 4);
        // ...then the trunk page itself, once its leaf array is empty.
        assert_eq!(pager.allocate_page().unwrap(), 3);
        let after_alloc = pager.read_page(1).unwrap();
        assert_eq!(
            read_be_u32(&after_alloc, FREELIST_TRUNK_PAGE_OFFSET).unwrap(),
            0
        );
        assert_eq!(
            read_be_u32(&after_alloc, FREELIST_PAGE_COUNT_OFFSET).unwrap(),
            0
        );
    }

    /// Once a trunk page's leaf array is full, the next deallocated page
    /// becomes a new trunk pointing at the old one, chaining trunks
    /// instead of overflowing the array.
    #[test]
    fn deallocate_overflows_into_new_trunk_when_full() {
        // Pre-fill trunk page 3 at exactly `max_leaves_per_trunk(512)`
        // capacity, so the next deallocation must overflow into a new
        // trunk rather than requiring hundreds of individual calls here.
        let page_size = 512u32;
        let max_leaves = freelist::max_leaves_per_trunk(page_size);
        let full_trunk = TrunkPage {
            next_trunk: 0,
            leaves: (100..100 + max_leaves).collect(),
        };
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![0u8; page_size as usize * 4];
        write_be_u32(&mut contents, PAGE_COUNT_OFFSET, 4).unwrap();
        write_be_u32(&mut contents, FREELIST_TRUNK_PAGE_OFFSET, 3).unwrap();
        write_be_u32(&mut contents, FREELIST_PAGE_COUNT_OFFSET, max_leaves).unwrap();
        let trunk_start = page_size as usize * 2;
        full_trunk
            .write(&mut contents[trunk_start..trunk_start + page_size as usize])
            .unwrap();
        vfs.insert("/test.db", contents);
        let mut pager = Pager::open(&vfs, Path::new("/test.db"), page_size).unwrap();

        pager.deallocate_page(4).unwrap();

        let after = pager.read_page(1).unwrap();
        assert_eq!(read_be_u32(&after, FREELIST_TRUNK_PAGE_OFFSET).unwrap(), 4);
        assert_eq!(
            read_be_u32(&after, FREELIST_PAGE_COUNT_OFFSET).unwrap(),
            max_leaves + 1
        );
        let new_trunk = TrunkPage::parse(&pager.read_page(4).unwrap()).unwrap();
        assert_eq!(new_trunk.next_trunk, 3);
        assert!(new_trunk.leaves.is_empty());
    }

    #[test]
    fn pager_reads_pages_identically_to_vfs_page_source() {
        let mut vfs = MemoryVfs::new();
        let mut contents = vec![1u8; 512];
        contents.extend(vec![2u8; 512]);
        vfs.insert("/test.db", contents);

        let pager = Pager::open(&vfs, Path::new("/test.db"), 512).unwrap();
        assert_eq!(pager.read_page(1).unwrap(), Rc::from(vec![1u8; 512]));
        assert_eq!(pager.read_page(2).unwrap(), Rc::from(vec![2u8; 512]));
    }

    mod fixtures {
        use super::*;
        use crate::btree::TableCursor;
        use crate::header::DatabaseHeader;
        use crate::record::{decode_record, TextEncoding, Value};
        use crate::schema::read_schema;
        use crate::vfs::UnixVfs;

        fn header_of(vfs: &UnixVfs, path: &Path) -> DatabaseHeader {
            let file = vfs.open_read(path).unwrap();
            let mut buf = [0u8; 100];
            file.read_at(&mut buf, 0).unwrap();
            DatabaseHeader::parse(&buf).unwrap()
        }

        fn text(v: &Value) -> &str {
            match v {
                Value::Text(s) => s,
                other => panic!("expected text, got {other:?}"),
            }
        }

        /// 001-architecture Req-4's "Hot journal is never ignored" scenario,
        /// upgraded by #172 from refuse-and-explain to actual recovery: the
        /// fixture's main file already has ~1999 uncommitted, spilled rows
        /// written into it (see tools/gen_fixtures.sh) and a *real*
        /// `sqlite3`-written journal recording their pre-images. `Pager::open`
        /// must replay that journal — proving interop with a stock `sqlite3`
        /// journal, not just our own — leaving only the one row genuinely
        /// committed before the transaction started.
        ///
        /// Copies the fixture pair into a scratch temp dir first: recovery
        /// mutates the main file and deletes the journal in place, and the
        /// checked-in fixture under `tests/corpus/fixtures/` must stay
        /// byte-identical for every other test that reads it.
        #[test]
        fn hot_journal_fixture_recovers_committed_state() {
            let dir = std::env::temp_dir().join(format!(
                "sqlite-rs-hot-journal-recovery-test-{}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let db_path = dir.join("hot_journal.db");
            std::fs::copy(
                "tests/corpus/fixtures/journalstates/hot_journal.db",
                &db_path,
            )
            .unwrap();
            std::fs::copy(
                "tests/corpus/fixtures/journalstates/hot_journal.db-journal",
                dir.join("hot_journal.db-journal"),
            )
            .unwrap();

            let rows = read_table_t_at(&db_path);

            assert_eq!(rows, vec![(1, "committed-before".to_string())]);
            assert!(!dir.join("hot_journal.db-journal").exists());

            std::fs::remove_dir_all(&dir).unwrap();
        }

        /// "Zero behavior change on at-rest fixtures": the same assertions
        /// `src/btree/mod.rs`'s `single_page_table_first_row` makes against
        /// `VfsPageSource` must hold identically through `Pager`.
        #[test]
        fn table_single_page_fixture_reads_identically_through_pager() {
            let vfs = UnixVfs;
            let path = Path::new("tests/corpus/fixtures/btrees/table_single_page.db");
            let header = header_of(&vfs, path);
            let pager = Pager::open(&vfs, path, header.page_size).unwrap();
            let mut cursor = TableCursor::new(pager, &header, 1);

            let row = cursor.first_row().unwrap().unwrap();
            let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
            assert_eq!(text(&values[0]), "table");
            assert_eq!(text(&values[1]), "t");
            assert_eq!(text(&values[4]), "CREATE TABLE t(a INTEGER, b TEXT)");
        }

        /// Freelist/pointer-map awareness: the auto-vacuum fixture's
        /// pointer-map page (page 2) sits between sqlite_master (page 1)
        /// and table `t`'s actual root — root page is discovered via
        /// `read_schema`, never hardcoded, so this proves the b-tree
        /// cursor's pointer-following traversal is unaffected by the
        /// interleaved pointer-map page when run through `Pager`.
        #[test]
        fn autovacuum_fixture_reads_identically_through_pager() {
            let vfs = UnixVfs;
            let path = Path::new("tests/corpus/fixtures/features/autovacuum.db");
            let header = header_of(&vfs, path);

            let schema_pager = Pager::open(&vfs, path, header.page_size).unwrap();
            let mut schema_cursor = TableCursor::new(schema_pager, &header, 1);
            let schemas = read_schema(&mut schema_cursor, header.text_encoding).unwrap();
            let t = schemas
                .iter()
                .find(|s| s.name == "t")
                .expect("table t in sqlite_master");

            let pager = Pager::open(&vfs, path, header.page_size).unwrap();
            let mut cursor = TableCursor::new(pager, &header, t.root_page);
            let row = cursor.first_row().unwrap().unwrap();
            let values = decode_record(&row.payload, TextEncoding::Utf8).unwrap();
            assert_eq!(text(&values[1]), "auto-vacuum full");
        }

        fn int(v: &Value) -> i64 {
            match v {
                Value::Integer(i) => *i,
                other => panic!("expected integer, got {other:?}"),
            }
        }

        /// Opens `name` and returns every row of table `t` as `(a, b)`,
        /// discovering `t`'s root page via `read_schema` (never
        /// hardcoded) and merging any pending WAL frames through `Pager`.
        fn read_table_t(name: &str) -> Vec<(i64, String)> {
            let path = Path::new("tests/corpus/fixtures/journalstates").join(name);
            read_table_t_at(&path)
        }

        fn read_table_t_at(path: &Path) -> Vec<(i64, String)> {
            let vfs = UnixVfs;
            let header = header_of(&vfs, path);

            let schema_pager = Pager::open(&vfs, path, header.page_size).unwrap();
            let mut schema_cursor = TableCursor::new(schema_pager, &header, 1);
            let schemas = read_schema(&mut schema_cursor, header.text_encoding).unwrap();
            let t = schemas
                .iter()
                .find(|s| s.name == "t")
                .expect("table t in sqlite_master");

            let pager = Pager::open(&vfs, path, header.page_size).unwrap();
            let mut cursor = TableCursor::new(pager, &header, t.root_page);
            let mut rows = Vec::new();
            let mut row = cursor.first_row().unwrap();
            while let Some(r) = row {
                let values = decode_record(&r.payload, header.text_encoding).unwrap();
                rows.push((int(&values[0]), text(&values[1]).to_string()));
                row = cursor.next_row().unwrap();
            }
            rows
        }

        /// 001-architecture Req-4's "Read a database with uncheckpointed
        /// WAL" scenario: three separate commits to the same page, none
        /// checkpointed into the main file — all three rows must be
        /// visible, matching `sqlite3` (see tools/gen_fixtures.sh).
        #[test]
        fn wal_pending_fixture_shows_uncheckpointed_rows() {
            assert_eq!(
                read_table_t("wal_pending.db"),
                vec![
                    (1, "one".to_string()),
                    (2, "two".to_string()),
                    (3, "three".to_string()),
                ]
            );
        }

        /// Both checksum-endianness paths must decode identically — this
        /// fixture is `wal_pending.db`'s content with magic flipped to
        /// 0x377f0683 and every checksum recomputed in big-endian
        /// arithmetic (spike #7 finding 2).
        #[test]
        fn wal_pending_bigendian_fixture_decodes_identically() {
            assert_eq!(
                read_table_t("wal_pending_bigendian.db"),
                read_table_t("wal_pending.db")
            );
        }

        /// A committed frame lifted from an unrelated WAL generation
        /// (different salts) is appended after this fixture's own last
        /// commit — it must never surface, and the WAL's own two
        /// legitimate rows must still be visible.
        #[test]
        fn wal_pending_stale_fixture_rejects_foreign_frame() {
            let rows = read_table_t("wal_pending_stale.db");
            assert_eq!(
                rows,
                vec![(10, "ten".to_string()), (11, "eleven".to_string())]
            );
            assert!(!rows.iter().any(|(_, b)| b.contains("STALE-FRAME")));
        }

        /// A big transaction spills dirty pages into the WAL as non-commit
        /// frames, then rolls back — none of the ~1999 rolled-back rows
        /// may surface; only the pre-existing committed row (already
        /// flushed to the main file by an earlier checkpoint, before this
        /// WAL generation began) is visible.
        #[test]
        fn wal_pending_trailing_fixture_shows_only_committed_row() {
            assert_eq!(
                read_table_t("wal_pending_trailing.db"),
                vec![(1, "committed-before".to_string())]
            );
        }
    }
}
