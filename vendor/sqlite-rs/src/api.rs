// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! The embedding API: a `Connection` an application links against.
//!
//! This is the supported surface (spec 013). Everything else this crate
//! exports — `btree`, `codegen`, `pager`, `parser`, `planner`, `vdbe`,
//! `vfs` — is the engine, and a consumer should not have to name any of it.
//!
//! ## Why a worker thread
//!
//! The engine's page-source graph is `Rc`/`RefCell` by decision
//! (ADR-0013, ADR-0017: a read path that pays no atomic refcount cost), and
//! `Rc` is not `Send`. A handle a connection pool, an async task, or a
//! trait with `Send + Sync` bounds can hold therefore cannot own the engine
//! directly, and *no* wrapper fixes that: `Mutex<T>` is `Send` only when
//! `T: Send`, so wrapping the pager graph in a lock changes nothing at the
//! type level. The two achievable designs are an `Arc`/lock refactor of
//! `Pager` and `PageSource`, which ADR-0013/ADR-0017 rejected on read-path
//! cost, or a thread that owns the engine and is spoken to over a channel.
//! ADR-0041 chose the second; `sqlx`'s own SQLite driver does the same for
//! a C `sqlite3*`.
//!
//! One constraint makes it the only *expressible* design rather than merely
//! the preferred one. `make check-mvl-limit` forbids named lifetime
//! parameters in `src/`, so no type here may hold an
//! [`Execution`](crate::vdbe::Execution) — it borrows its `Program`, and a
//! field of that type would need a lifetime. The execution has to live
//! inside a single worker stack frame, which is exactly what this design
//! gives it.
//!
//! Rows cross the channel without copying: ADR-0039 made [`Value`]'s
//! payloads `Arc`, so a `Value` is `Send + Sync` already.
//!
//! ## What is here so far
//!
//! `Connection` with [`Connection::open`], [`Connection::execute`] and the
//! statement-level counters spec 013 Requirement 1 asks for. Streaming
//! reads, prepared statements and explicit transactions are separate
//! phases; the protocol below is shaped to take them.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::header::{DatabaseHeader, DEFAULT_PAGE_SIZE};
use crate::pager::Pager;
// Re-exported rather than merely imported, so the whole embedding API is
// reachable from this one module. A caller binding a parameter or reading a
// column needs `Value`, and having to name `sqlite_rs::record` for it would
// leave the facade incomplete in exactly the way Requirement 6 is about.
pub use crate::record::Value;
use crate::schema::{TableSchema, ViewSchema};
use crate::vdbe::{Opcode, Program};
use crate::vfs::{MemoryVfs, UnixVfs, Vfs};

/// SQLite primary result codes, from `sqlite3.h` at the pinned 3.53.4.
///
/// Only the ones this module can actually produce are defined; the full
/// list is not this crate's to publish.
mod code {
    /// `SQLITE_ERROR` — generic error.
    pub const ERROR: i32 = 1;
    /// `SQLITE_BUSY` — the database file is locked.
    pub const BUSY: i32 = 5;
    /// `SQLITE_READONLY` — attempt to write a read-only database.
    pub const READONLY: i32 = 8;
    /// `SQLITE_IOERR` — a disk I/O error.
    pub const IOERR: i32 = 10;
    /// `SQLITE_CORRUPT` — the database disk image is malformed.
    pub const CORRUPT: i32 = 11;
    /// `SQLITE_CANTOPEN` — unable to open the database file.
    pub const CANTOPEN: i32 = 14;
    /// `SQLITE_MISMATCH` — data type mismatch.
    pub const MISMATCH: i32 = 20;
    /// `SQLITE_MISUSE` — the library was used incorrectly.
    pub const MISUSE: i32 = 21;
    /// `SQLITE_RANGE` — a bind index is out of range.
    pub const RANGE: i32 = 25;
}

/// How to open a database file.
///
/// Mirrors the three modes a `sqlite://` URL can ask for, which is what a
/// consumer configures (spec 013 Requirement 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpenMode {
    /// Open an existing database and refuse every statement that would
    /// write to it.
    ///
    /// Enforced per statement rather than by the pager, and that is a
    /// deliberate divergence from stock SQLite recorded under ADR-0004.
    /// [`Pager::open`] calls `Vfs::open_write` unconditionally and there is
    /// no read-only pager; the read-only page source that does exist
    /// (`VfsPageSource`) bypasses `Pager` entirely and so merges no WAL
    /// frames, which would silently serve stale data for a WAL database.
    /// Refusing writes above the pager is the honest option until spec 007
    /// grows a read-only pager: the file is opened for writing, but nothing
    /// this connection accepts will write to it.
    ReadOnly,
    /// Open an existing database for reading and writing. Fails if the file
    /// does not exist, and creates nothing.
    ReadWrite,
    /// Open a database for reading and writing, creating a valid empty one
    /// if no file exists yet.
    ReadWriteCreate,
}

/// How a transaction acquires its locks.
///
/// Matches SQLite's `BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE]` (#356, #395).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionBehavior {
    /// Take no write lock until the first write — SQLite's default.
    ///
    /// Cheapest to start and the most likely to meet
    /// [`Error::Busy`](Error::Busy) later, because two deferred
    /// transactions can both begin and then collide.
    #[default]
    Deferred,
    /// Take the RESERVED lock at `BEGIN`, so a competing writer is refused
    /// immediately rather than at commit.
    ///
    /// The right choice for a read-then-write sequence that must not lose a
    /// race after doing its reads.
    Immediate,
    /// Take the EXCLUSIVE lock at `BEGIN`, excluding readers too.
    Exclusive,
}

impl TransactionBehavior {
    /// The `BEGIN` statement this behaviour issues.
    fn statement(self) -> &'static str {
        match self {
            TransactionBehavior::Deferred => "BEGIN DEFERRED",
            TransactionBehavior::Immediate => "BEGIN IMMEDIATE",
            TransactionBehavior::Exclusive => "BEGIN EXCLUSIVE",
        }
    }
}

/// Why an API call failed.
///
/// Deliberately flat: every payload is a `String`, an `i32` or a `Copy`
/// enum, and layer errors arrive as already-formatted text rather than as
/// wrapped values. Two things fall out that matter more than a
/// [`source`](std::error::Error::source) chain would.
///
/// It is unconditionally `Send + Sync + 'static`, which it *must* be —
/// every error travels back from the worker thread over a channel, so an
/// error type that borrowed from engine state could not be returned at all.
/// And it derives [`PartialEq`], so a test can assert the exact error
/// instead of substring-matching a message.
///
/// The price is that the originating layer error is not recoverable from
/// here. That is a real loss, and a small one: the sixteen engine error
/// enums barely implement `source()` themselves, and the message they
/// format is the diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The SQL could not be parsed.
    Parse {
        /// What the parser objected to.
        message: String,
        /// 1-based line of the offending token.
        line: u32,
        /// 1-based column of the offending token.
        column: u32,
    },
    /// The statement parsed but could not be compiled.
    Compile {
        /// What the compiler objected to.
        message: String,
    },
    /// A named parameter form (`:name`, `@name`, `$name`) was used.
    ///
    /// Separate from [`Error::Compile`] because it is the one compile
    /// failure a consumer is likely to hit by habit rather than by mistake:
    /// binding by name is what most drivers do. Positional `?`/`?NNN` is
    /// what this crate supports.
    NamedParameter {
        /// The placeholder as written, sigil included.
        placeholder: String,
    },
    /// The number of bound parameters did not match what the statement
    /// wants.
    ///
    /// Stricter than stock SQLite, which leaves an unbound parameter NULL.
    /// A deliberate divergence under ADR-0004: refusing to run is the safe
    /// direction, and catching a transposed or short argument list is the
    /// stated value of spec 013 Requirement 3.
    ParamCount {
        /// How many the statement reads.
        expected: usize,
        /// How many the caller supplied.
        found: usize,
    },
    /// More than one statement was given where exactly one was required.
    MultipleStatements {
        /// How many statements the text contained.
        count: usize,
    },
    /// The engine halted with a SQLite result code — a constraint
    /// violation, typically.
    ///
    /// `code` is the *extended* code (e.g. 2067 for a UNIQUE violation);
    /// [`Error::sqlite_code`] narrows it to the primary one.
    Sqlite {
        /// The extended SQLite result code.
        code: i32,
        /// The engine's message, if it supplied one.
        message: String,
    },
    /// The database is locked by another connection or process (spec 007's
    /// `VfsError::Locked`). Retryable — see [`Error::is_retryable`].
    Busy {
        /// The path that was locked.
        path: String,
    },
    /// The database file could not be opened.
    CannotOpen {
        /// The path that could not be opened.
        path: String,
        /// Why.
        message: String,
    },
    /// A write was attempted on a connection opened [`OpenMode::ReadOnly`].
    ReadOnly {
        /// The statement that was refused.
        statement: String,
    },
    /// The database image is malformed.
    Corrupt {
        /// What was malformed.
        message: String,
    },
    /// An I/O error.
    Io {
        /// What failed.
        message: String,
    },
    /// The connection's worker thread is no longer running.
    ///
    /// Returned rather than blocking, per spec 013 Requirement 4. Reachable
    /// two ways: the connection was closed, or the worker panicked (which
    /// would be a bug in this crate).
    ConnectionClosed,
    /// A column was read as a type its value cannot convert to.
    TypeMismatch {
        /// The column, named if the statement has usable names, else its
        /// index rendered as text.
        column: String,
        /// The Rust type asked for.
        expected: &'static str,
        /// The SQLite storage class actually there.
        found: &'static str,
    },
    /// A column name was read that this row does not have.
    ///
    /// Note that by-name access only sees real column names for a
    /// single-table `SELECT`; see [`Rows::column_names`].
    ColumnNotFound {
        /// The name that was asked for.
        name: String,
    },
    /// A column index was read that is past the end of the row.
    ColumnIndexOutOfRange {
        /// The index that was asked for.
        index: usize,
        /// How many columns the row has.
        len: usize,
    },
    /// A [`Statement`] was used after its worker discarded it.
    ///
    /// Not reachable while the handle is alive — `Statement` owns its
    /// registration and finalizes it on drop — so this is a guard against
    /// an internal inconsistency rather than a caller error.
    StatementFinalized,
    /// Execution failed for a reason with no more specific variant.
    Execution {
        /// What went wrong.
        message: String,
    },
}

impl Error {
    /// The primary SQLite result code for this error, as
    /// `sqlite3_errcode()` reports it.
    ///
    /// Primary, not extended: `sqlite3_errcode()` returns the low byte and
    /// `sqlite3_extended_errcode()` the whole word, so both are offered
    /// here under the names that match. A caller switching on "is this a
    /// constraint violation" wants 19; one distinguishing UNIQUE from
    /// NOT NULL wants 2067 and should call
    /// [`Error::extended_sqlite_code`].
    pub fn sqlite_code(&self) -> i32 {
        // The extended-to-primary rule is the low byte (`sqlite3.h`: every
        // extended code is `primary | (n<<8)`).
        self.extended_sqlite_code() & 0xff
    }

    /// The extended SQLite result code for this error, as
    /// `sqlite3_extended_errcode()` reports it.
    pub fn extended_sqlite_code(&self) -> i32 {
        match self {
            Error::Parse { .. } | Error::Compile { .. } | Error::NamedParameter { .. } => {
                code::ERROR
            }
            Error::ParamCount { .. } | Error::ColumnIndexOutOfRange { .. } => code::RANGE,
            Error::TypeMismatch { .. } => code::MISMATCH,
            Error::ColumnNotFound { .. } => code::ERROR,
            Error::MultipleStatements { .. }
            | Error::ConnectionClosed
            | Error::StatementFinalized => code::MISUSE,
            Error::Sqlite { code, .. } => *code,
            Error::Busy { .. } => code::BUSY,
            Error::CannotOpen { .. } => code::CANTOPEN,
            Error::ReadOnly { .. } => code::READONLY,
            Error::Corrupt { .. } => code::CORRUPT,
            Error::Io { .. } => code::IOERR,
            Error::Execution { .. } => code::ERROR,
        }
    }

    /// Whether retrying the same call could succeed without any change on
    /// the caller's part.
    ///
    /// True only for [`Error::Busy`]: the lock it names is held by someone
    /// else and may be released. Every other variant describes something
    /// that will fail identically on a retry.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Busy { .. })
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Parse {
                message,
                line,
                column,
            } => {
                write!(f, "syntax error (line {line}, column {column}): {message}")
            }
            Error::Compile { message } => write!(f, "cannot compile statement: {message}"),
            Error::NamedParameter { placeholder } => write!(
                f,
                "named parameter {placeholder} is not supported — bind by position with ? or ?NNN"
            ),
            Error::ParamCount { expected, found } => write!(
                f,
                "statement wants {expected} parameter(s) but {found} were bound"
            ),
            Error::MultipleStatements { count } => write!(
                f,
                "expected a single statement but found {count} — use execute_batch"
            ),
            Error::Sqlite { code, message } => {
                if message.is_empty() {
                    write!(f, "SQLite error {code}")
                } else {
                    write!(f, "{message} (SQLite error {code})")
                }
            }
            Error::Busy { path } => write!(f, "database is locked: {path}"),
            Error::CannotOpen { path, message } => {
                write!(f, "cannot open {path}: {message}")
            }
            Error::ReadOnly { statement } => {
                write!(f, "connection is read-only; refused: {statement}")
            }
            Error::Corrupt { message } => write!(f, "database image is malformed: {message}"),
            Error::Io { message } => write!(f, "I/O error: {message}"),
            Error::ConnectionClosed => write!(f, "connection is closed"),
            Error::StatementFinalized => write!(f, "statement has been finalized"),
            Error::TypeMismatch {
                column,
                expected,
                found,
            } => write!(
                f,
                "column {column} holds {found}, which cannot be read as {expected}"
            ),
            Error::ColumnNotFound { name } => write!(f, "no such column: {name}"),
            Error::ColumnIndexOutOfRange { index, len } => {
                write!(f, "column index {index} is out of range for a row of {len}")
            }
            Error::Execution { message } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {}

/// A connection to one database.
///
/// `Send + Sync` and cheap to clone: every clone talks to the same worker
/// thread, and the thread is joined when the last clone drops. Statements
/// are serialized, which is what spec 013 Requirement 4 specifies — a
/// pointer store's throughput is irrelevant, and reachability is the point.
#[derive(Debug, Clone)]
pub struct Connection {
    inner: Arc<Shared>,
}

/// The shared half of a [`Connection`], so clones address one worker.
#[derive(Debug)]
struct Shared {
    /// `SyncSender` rather than `Sender` deliberately: `Sender<T>` is
    /// `Send` but not `Sync`, and a handle several threads hold at once
    /// needs both.
    ///
    /// `Option` so [`Shared::drop`] can *close* the channel before joining.
    /// This is load-bearing rather than tidy: the worker's loop ends when
    /// `recv` fails, which only happens once every sender is gone, so
    /// joining while still holding this one deadlocks — the drop waits for
    /// a thread that is waiting for the drop.
    requests: Option<SyncSender<Request>>,
    /// Taken by [`Shared::drop`] to join the worker. `Mutex` because a
    /// `JoinHandle` has to be owned to be joined, and because `Shared` is
    /// reachable from several threads until the last clone goes.
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        // Order matters. Dropping the sender closes the channel, so the
        // worker's `recv` returns `Err` and its loop ends; only then is
        // there anything to join. Field-drop order would run this *after*
        // `drop`, hence the explicit `take`.
        self.requests = None;

        // Then wait for it, so a caller that drops a connection and
        // immediately reopens the same path cannot race its predecessor's
        // file locks — the `Pager` releases those when the worker's stack
        // unwinds, which has not happened yet when the channel closes.
        //
        // `Mutex::get_mut` rather than `lock`: this is `&mut self`, so
        // there is no contention to wait on and a poisoned mutex cannot
        // block the join.
        if let Ok(slot) = self.worker.get_mut() {
            if let Some(handle) = slot.take() {
                handle.join().ok();
            }
        }
    }
}

/// What the API asks the worker to do.
///
/// Every variant carries its own reply channel, so several threads holding
/// clones of one [`Connection`] each wait on their own answer while the
/// worker serves them in arrival order.
enum Request {
    /// Run exactly one statement.
    Execute {
        /// The statement text.
        sql: String,
        /// Values for its `?`/`?NNN` placeholders.
        params: Vec<Value>,
        /// Where to send the outcome.
        reply: SyncSender<Result<Applied, Error>>,
    },
    /// Run every statement in a script, stopping at the first failure.
    ExecuteBatch {
        /// The script.
        sql: String,
        /// Where to send the outcome.
        reply: SyncSender<Result<(), Error>>,
    },
    /// Run one statement and stream its rows back.
    Query {
        /// The statement text.
        sql: String,
        /// Values for its `?`/`?NNN` placeholders.
        params: Vec<Value>,
        /// Where to send the stream's head, or the failure to start it.
        reply: SyncSender<Result<QueryStream, Error>>,
    },
    /// List the table names in the catalog.
    TableNames {
        /// Where to send them.
        reply: SyncSender<Result<Vec<String>, Error>>,
    },
    /// Read the connection-scoped counters.
    Counters {
        /// Where to send them.
        reply: SyncSender<Counters>,
    },
    /// Compile one statement and keep it.
    Prepare {
        /// The statement text.
        sql: String,
        /// Where to send the handle, or the failure to compile.
        reply: SyncSender<Result<PreparedHandle, Error>>,
    },
    /// Run a kept statement, discarding rows.
    StatementExecute {
        /// Which statement.
        id: u64,
        /// Values for its placeholders.
        params: Vec<Value>,
        /// Where to send the outcome.
        reply: SyncSender<Result<Applied, Error>>,
    },
    /// Run a kept statement and stream its rows.
    StatementQuery {
        /// Which statement.
        id: u64,
        /// Values for its placeholders.
        params: Vec<Value>,
        /// Where to send the stream's head.
        reply: SyncSender<Result<QueryStream, Error>>,
    },
    /// How many times a kept statement has been recompiled.
    StatementRepreparations {
        /// Which statement.
        id: u64,
        /// Where to send the count.
        reply: SyncSender<Result<u64, Error>>,
    },
    /// Discard a kept statement.
    ///
    /// No reply: `Statement::drop` cannot wait on one usefully, and there
    /// is nothing a caller could do with the answer.
    Finalize {
        /// Which statement.
        id: u64,
    },
    /// Set how long a contended lock is waited for.
    SetBusyTimeout {
        /// The new timeout.
        timeout: Duration,
        /// Acknowledgement, so the setting is in effect before the caller
        /// continues.
        reply: SyncSender<()>,
    },
}

/// How many rows the worker batches per channel send.
///
/// The bound on peak memory is roughly four of these: one batch being
/// filled on the worker, two in the channel, one held by the caller.
/// Independent of the result size, which is the property Requirement 7
/// actually asks for.
const CHUNK_ROWS: usize = 64;

/// Batches the result channel holds before the worker has to wait.
///
/// Two, not one, and the reason is a liveness one rather than throughput.
/// The worker sends [`Chunk::Done`] after the final batch, so with a single
/// slot a caller that ran a small query and never read it would leave the
/// worker blocked on that `Done` — and blocked workers serve nobody, so the
/// *next* statement on the connection would hang. With two slots, any
/// result that fits in one batch completes and frees the worker whether the
/// caller reads it or not, which covers the case that is easy to hit by
/// accident:
///
/// ```ignore
/// let rows = conn.query("SELECT 1")?;   // never read
/// conn.execute("INSERT ...")?;          // must not hang
/// ```
///
/// It is a mitigation, not a guarantee: a result spanning more batches than
/// this still parks the worker until the caller reads or drops its [`Rows`].
/// That is inherent to one worker streaming one execution — see [`Rows`].
const CHUNK_SLOTS: usize = 2;

/// What the worker returns when it has compiled and kept a statement.
struct PreparedHandle {
    id: u64,
    param_count: usize,
    column_names: Arc<Vec<String>>,
}

/// One compiled statement the worker is holding for a [`Statement`].
struct KeptStatement {
    /// Kept so the statement can be recompiled after a schema change, and
    /// so `is_schema_changing` can be re-evaluated on each run.
    sql: String,
    program: Program,
    column_names: Arc<Vec<String>>,
    /// The schema generation this was compiled against.
    generation: u64,
    /// How many times it has been recompiled — SQLite's
    /// `SQLITE_STMTSTATUS_REPREPARE`.
    repreparations: u64,
}

/// The head of a streamed result: its column names, and the channel its
/// rows arrive on.
struct QueryStream {
    column_names: Arc<Vec<String>>,
    chunks: Receiver<Chunk>,
}

/// One message on a result stream.
enum Chunk {
    /// Up to [`CHUNK_ROWS`] rows, in emission order.
    Rows(Vec<Vec<Value>>),
    /// The statement finished normally.
    Done,
    /// The statement failed part-way through.
    Failed(Error),
}

/// What one [`Connection::execute`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Applied {
    /// Rows this statement changed, or `None` if it was not a counting
    /// statement.
    changes: Option<u64>,
}

/// The connection-scoped counters, retained across statements exactly as
/// `sqlite3_changes()`/`sqlite3_last_insert_rowid()` are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Counters {
    changes: u64,
    last_insert_rowid: i64,
}

impl Connection {
    /// Opens `path`, creating a valid empty database if no file exists.
    ///
    /// The default mode, matching what a `sqlite://<path>?mode=rwc` URL
    /// asks for.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with(path, OpenMode::ReadWriteCreate)
    }

    /// Opens `path` in `mode`.
    pub fn open_with(path: impl AsRef<Path>, mode: OpenMode) -> Result<Self, Error> {
        Self::spawn(Target::File(path.as_ref().to_path_buf()), mode)
    }

    /// Opens a private in-memory database, discarded when the last clone of
    /// this connection drops.
    ///
    /// Backed by `MemoryVfs`, so it exercises the same pager, journal and
    /// b-tree code a file does rather than a separate code path.
    pub fn open_in_memory() -> Result<Self, Error> {
        Self::spawn(Target::Memory, OpenMode::ReadWriteCreate)
    }

    /// Spawns the worker and waits for it to report whether it opened.
    ///
    /// Opening happens *on the worker thread* because the engine state it
    /// produces is not `Send` — the `Pager` cannot be built here and moved
    /// there. So the outcome comes back over a channel like everything
    /// else, and a failure to open leaves no thread behind.
    fn spawn(target: Target, mode: OpenMode) -> Result<Self, Error> {
        let (request_tx, request_rx) = sync_channel::<Request>(0);
        let (open_tx, open_rx) = sync_channel::<Result<(), Error>>(0);

        let handle = std::thread::Builder::new()
            .name("sqlite-rs-connection".to_string())
            .spawn(move || worker_main(target, mode, request_rx, open_tx))
            .map_err(|e| Error::Io {
                message: format!("could not spawn the connection's worker thread: {e}"),
            })?;

        match open_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                inner: Arc::new(Shared {
                    requests: Some(request_tx),
                    worker: Mutex::new(Some(handle)),
                }),
            }),
            Ok(Err(e)) => {
                // The worker returns straight after reporting a failure;
                // join it so no thread outlives the failed open. Safe to
                // join here without closing `request_tx` first: the worker
                // has already left its serve loop.
                handle.join().ok();
                Err(e)
            }
            // The worker vanished without reporting — only reachable if it
            // panicked, which is a bug here rather than a caller error.
            Err(_) => {
                handle.join().ok();
                Err(Error::ConnectionClosed)
            }
        }
    }

    /// Runs one statement with no parameters, returning how many rows it
    /// changed.
    ///
    /// Zero for a statement that is not an `INSERT`/`UPDATE`/`DELETE`; see
    /// [`Connection::changes`] for the retained count, which a
    /// non-counting statement deliberately leaves alone.
    pub fn execute(&self, sql: &str) -> Result<u64, Error> {
        self.execute_with(sql, Vec::new())
    }

    /// Runs one statement with `params` bound to its `?`/`?NNN`
    /// placeholders, 1-based, returning how many rows it changed.
    ///
    /// The count must match the statement's placeholder count exactly, or
    /// this fails with [`Error::ParamCount`] rather than binding NULLs.
    pub fn execute_with(&self, sql: &str, params: Vec<Value>) -> Result<u64, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::Execute {
            sql: sql.to_string(),
            params,
            reply: reply_tx,
        })?;
        let applied = self.recv(reply_rx)??;
        Ok(applied.changes.unwrap_or(0))
    }

    /// Runs every statement in `sql`, stopping at the first failure.
    ///
    /// For schema setup, where a caller has a script rather than a
    /// statement. Not a transaction: statements that already ran stay
    /// applied, exactly as `sqlite3_exec` leaves them.
    pub fn execute_batch(&self, sql: &str) -> Result<(), Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::ExecuteBatch {
            sql: sql.to_string(),
            reply: reply_tx,
        })?;
        self.recv(reply_rx)?
    }

    /// Runs one statement and streams its rows.
    ///
    /// See [`Rows`] — in particular that holding an undrained `Rows` blocks
    /// every other statement on this connection until it is read or
    /// dropped.
    pub fn query(&self, sql: &str) -> Result<Rows, Error> {
        self.query_with(sql, Vec::new())
    }

    /// Runs one statement with `params` bound, and streams its rows.
    pub fn query_with(&self, sql: &str, params: Vec<Value>) -> Result<Rows, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::Query {
            sql: sql.to_string(),
            params,
            reply: reply_tx,
        })?;
        let stream = self.recv(reply_rx)??;
        Ok(Rows {
            column_names: stream.column_names,
            chunks: stream.chunks,
            buffered: Vec::new().into_iter(),
            finished: false,
        })
    }

    /// Runs one statement and collects every row.
    ///
    /// The convenience form for a result a caller knows is small — a
    /// pointer store's dozen rows. Use [`Connection::query`] for anything
    /// whose size depends on the data.
    pub fn query_all(&self, sql: &str) -> Result<Vec<Row>, Error> {
        self.query(sql)?.into_vec()
    }

    /// Runs one statement with `params` bound and collects every row.
    pub fn query_all_with(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Row>, Error> {
        self.query_with(sql, params)?.into_vec()
    }

    /// Runs one statement and returns its first row, or `None` if it
    /// produced none.
    ///
    /// Remaining rows are discarded. The `LIMIT 1` existence probe a
    /// consumer writes by hand otherwise.
    pub fn query_row(&self, sql: &str) -> Result<Option<Row>, Error> {
        self.query_row_with(sql, Vec::new())
    }

    /// Runs one statement with `params` bound and returns its first row.
    pub fn query_row_with(&self, sql: &str, params: Vec<Value>) -> Result<Option<Row>, Error> {
        let mut rows = self.query_with(sql, params)?;
        let first = rows.next_row()?;
        // Dropped here, which abandons the rest of the stream and frees the
        // worker rather than leaving it blocked on a send nobody reads.
        drop(rows);
        Ok(first)
    }

    /// The names of the tables in this database, in catalog order.
    ///
    /// Requirement 6 asks that the facade cover everything the engine
    /// offers a consumer, and enumerating tables is one of those things —
    /// `schema::read_schema` has always been able to, but only by reaching
    /// past this module.
    ///
    /// This reads the decoded catalog rather than querying `sqlite_master`,
    /// and that is not merely an optimisation: `sqlite_master` is currently
    /// **not** queryable through `SELECT` at all
    /// (`resolve_from_table_schema` does not resolve it, so
    /// `SELECT name FROM sqlite_master` fails to compile). Introspection is
    /// plan.md's V7; until then this is how a consumer lists tables.
    pub fn table_names(&self) -> Result<Vec<String>, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::TableNames { reply: reply_tx })?;
        self.recv(reply_rx)?
    }

    /// Compiles one statement and keeps it, so it can be run repeatedly
    /// with different parameters.
    ///
    /// The value is not speed: for a dozen statements at commit frequency,
    /// compiling once saves nothing measurable. It is that a handle owning
    /// its parameter slots reports a wrong argument count instead of
    /// writing a valid row that points at the wrong thing (spec 013
    /// Requirement 3).
    pub fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::Prepare {
            sql: sql.to_string(),
            reply: reply_tx,
        })?;
        let handle = self.recv(reply_rx)??;
        Ok(Statement {
            conn: self.clone(),
            id: handle.id,
            param_count: handle.param_count,
            column_names: handle.column_names,
        })
    }

    /// Begins a deferred transaction.
    ///
    /// See [`Transaction`] — dropping the handle without committing rolls
    /// back.
    pub fn transaction(&self) -> Result<Transaction, Error> {
        self.transaction_with(TransactionBehavior::Deferred)
    }

    /// Begins a transaction with the given locking behaviour.
    pub fn transaction_with(&self, behavior: TransactionBehavior) -> Result<Transaction, Error> {
        self.execute(behavior.statement())?;
        Ok(Transaction {
            conn: self.clone(),
            done: false,
        })
    }

    /// Sets `PRAGMA <name> = <value>` on this connection.
    ///
    /// A convenience over [`Connection::execute`] for the settings a pool
    /// or a durability policy configures — `journal_mode`, `synchronous`.
    /// Spec 013 scopes this to exactly that; the PRAGMA *catalogue* is
    /// plan.md's V7, and the introspection pragmas (`table_info` and
    /// friends) live in the CLI binary per ADR-0029, so they are not
    /// reachable from here.
    ///
    /// `value` is interpolated into the statement, because that is the only
    /// form SQLite accepts — `PRAGMA` does not take bound parameters. Pass
    /// a literal from your own code, not something a user typed.
    pub fn pragma(&self, name: &str, value: &str) -> Result<(), Error> {
        self.execute(&format!("PRAGMA {name} = {value}")).map(drop)
    }

    /// Sets how long a contended lock is waited for before
    /// [`Error::Busy`] is returned.
    ///
    /// Zero — the default — means fail immediately, matching stock SQLite,
    /// where `sqlite3_busy_timeout` is unset until asked for.
    ///
    /// The wait applies only to statements run *outside* an explicit
    /// transaction. Inside one, a contended lock is reported straight away:
    /// retrying a single statement of a transaction cannot be correct,
    /// because its mutations sit in the same pending set as every earlier
    /// statement's. Retry the transaction instead. Stock SQLite behaves the
    /// same way with `SQLITE_BUSY` at `COMMIT`.
    pub fn set_busy_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::SetBusyTimeout {
            timeout,
            reply: reply_tx,
        })?;
        self.recv(reply_rx)
    }

    /// Rows changed by the most recent counting statement, as
    /// `sqlite3_changes()` reports it.
    ///
    /// A statement that is not an `INSERT`/`UPDATE`/`DELETE` does not reset
    /// this — so a `SELECT` after a `DELETE` of two rows still reports two.
    /// That retention rule is the whole reason this is not just
    /// [`Connection::execute`]'s return value.
    pub fn changes(&self) -> Result<u64, Error> {
        Ok(self.counters()?.changes)
    }

    /// Rowid of the most recent successful `INSERT` into a rowid table, as
    /// `sqlite3_last_insert_rowid()` reports it.
    ///
    /// Zero if this connection has inserted nothing yet. Like
    /// [`Connection::changes`], a statement that inserts nothing leaves the
    /// value standing.
    pub fn last_insert_rowid(&self) -> Result<i64, Error> {
        Ok(self.counters()?.last_insert_rowid)
    }

    fn counters(&self) -> Result<Counters, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.send(Request::Counters { reply: reply_tx })?;
        self.recv(reply_rx)
    }

    /// Hands `request` to the worker, or reports the worker is gone.
    fn send(&self, request: Request) -> Result<(), Error> {
        self.inner
            .requests
            .as_ref()
            .ok_or(Error::ConnectionClosed)?
            .send(request)
            .map_err(|_| Error::ConnectionClosed)
    }

    /// Waits for the worker's answer, or reports the worker is gone.
    ///
    /// The two halves are separate because either can be the one to notice:
    /// `send` fails if the worker died before the request, `recv` fails if
    /// it died while serving it. Both mean the same thing to a caller, and
    /// neither blocks forever — which is what Requirement 4 asks for.
    fn recv<T>(&self, reply: Receiver<T>) -> Result<T, Error> {
        reply.recv().map_err(|_| Error::ConnectionClosed)
    }
}

/// A result stream, read one row at a time.
///
/// Rows arrive from the connection's worker thread in batches, so peak
/// memory is bounded by the batch size and the pager's page cache rather
/// than by the size of the result (spec 013 Requirement 7). Reading the
/// first ten rows of a million-row table costs the same as reading the
/// first ten of a ten-row one.
///
/// # Holding an undrained `Rows` can block the connection
///
/// The engine stays inside one execution until the result is drained or
/// this handle is dropped, so while a large result is outstanding, any
/// other statement on the same connection — including from another thread
/// holding a clone — waits.
///
/// A result that fits in one batch is safe: it completes and frees the
/// worker whether or not it is read (see `CHUNK_SLOTS`). A larger one is
/// not:
///
/// ```ignore
/// let rows = conn.query("SELECT a FROM big")?;  // thousands of rows
/// let more = conn.query("SELECT 1")?;           // blocks until `rows` goes
/// ```
///
/// Dropping `Rows` releases the worker immediately, so this is a wait
/// rather than a permanent deadlock — but a thread that holds a `Rows`
/// while waiting on another thread that needs the same connection will
/// hang. **Drain it, drop it, or bind it to a short scope.** Requirement 4
/// accepts serialized access; this is its sharp edge.
///
/// Deliberately not an [`Iterator`]: [`Rows::next_row`] returns
/// `Result<Option<Row>, Error>`, and collapsing that into
/// `Option<Result<Row, Error>>` to fit the trait makes "the stream ended"
/// and "the stream failed" the same shape at the call site. ADR-0040
/// rejected an `Iterator` impl on `Execution` for this reason and the
/// reasoning carries.
#[derive(Debug)]
pub struct Rows {
    column_names: Arc<Vec<String>>,
    chunks: Receiver<Chunk>,
    buffered: std::vec::IntoIter<Vec<Value>>,
    /// Set once the stream has ended, normally or otherwise, so a caller
    /// that keeps polling gets `None` rather than a channel error.
    finished: bool,
}

impl Rows {
    /// The result's column names.
    ///
    /// Available before the first row is read, and empty for a statement
    /// that returns no columns.
    ///
    /// **Real names only for a single-table `SELECT`.** A join or a
    /// compound (`UNION`) reports `column1`, `column2`, … because that is
    /// what `codegen::result_column_names` can currently derive
    /// (`src/codegen/prepare.rs:181`). By-index access is unaffected;
    /// by-name access on a join will not find the name a caller expects.
    /// Stated rather than hidden — the fix belongs in the name resolver,
    /// not here.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Reads the next row, or `None` once the result is exhausted.
    ///
    /// Blocks until the worker produces a row.
    pub fn next_row(&mut self) -> Result<Option<Row>, Error> {
        loop {
            if let Some(values) = self.buffered.next() {
                return Ok(Some(Row {
                    values,
                    column_names: Arc::clone(&self.column_names),
                }));
            }
            if self.finished {
                return Ok(None);
            }
            match self.chunks.recv() {
                Ok(Chunk::Rows(batch)) => self.buffered = batch.into_iter(),
                Ok(Chunk::Done) => {
                    self.finished = true;
                    return Ok(None);
                }
                Ok(Chunk::Failed(e)) => {
                    self.finished = true;
                    return Err(e);
                }
                // The worker vanished mid-stream without saying why, which
                // means it panicked — a bug here, not a caller error.
                Err(_) => {
                    self.finished = true;
                    return Err(Error::ConnectionClosed);
                }
            }
        }
    }

    /// Reads every remaining row into a `Vec`.
    ///
    /// For results a caller knows are small. Defeats the streaming
    /// property by construction, which is fine when a dozen rows is the
    /// whole answer and is the wrong choice otherwise.
    pub fn into_vec(mut self) -> Result<Vec<Row>, Error> {
        let mut out = Vec::new();
        while let Some(row) = self.next_row()? {
            out.push(row);
        }
        Ok(out)
    }
}

/// One result row.
#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Value>,
    column_names: Arc<Vec<String>>,
}

impl Row {
    /// How many columns this row has.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether this row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// This row's column names — see [`Rows::column_names`] for when they
    /// are real names.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// The raw [`Value`] at `index`, or `None` if the row is shorter.
    ///
    /// The escape hatch from [`Row::get`]'s conversions, for a caller that
    /// wants to switch on the storage class itself.
    pub fn value(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Reads column `index` (0-based) as `T`.
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T, Error> {
        let value = self.values.get(index).ok_or(Error::ColumnIndexOutOfRange {
            index,
            len: self.values.len(),
        })?;
        T::from_value(value).map_err(|expected| Error::TypeMismatch {
            column: self
                .column_names
                .get(index)
                .cloned()
                .unwrap_or_else(|| index.to_string()),
            expected,
            found: storage_class(value),
        })
    }

    /// Reads the column called `name` as `T`.
    ///
    /// Case-insensitive, matching SQLite's column-name comparison. See
    /// [`Rows::column_names`]: on a join or a compound the names are
    /// positional placeholders, so this will not find a base-table name.
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T, Error> {
        let index = self
            .column_names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::ColumnNotFound {
                name: name.to_string(),
            })?;
        self.get(index)
    }
}

/// A type a [`Value`] can be read as.
///
/// Conversions are the ones SQLite's own `sqlite3_column_*` family
/// performs without reinterpreting storage: an `INTEGER` reads as `i64`,
/// and as `f64` because that is lossless for the range in practice; a
/// `REAL` does not read as `i64`, because truncating silently is how a
/// rowid becomes wrong. `Option<T>` is how a nullable column is read —
/// `NULL` into a non-`Option` type is a [`Error::TypeMismatch`] rather
/// than a default value.
pub trait FromValue: Sized {
    /// Converts `value`, or returns the name of the type that was wanted
    /// so the caller can build the error with the column's identity.
    fn from_value(value: &Value) -> Result<Self, &'static str>;
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            Value::Integer(v) => Ok(*v),
            _ => Err("i64"),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            Value::Real(v) => Ok(*v),
            // Widening an integer is lossless and is what
            // `sqlite3_column_double` does.
            Value::Integer(v) => Ok(*v as f64),
            _ => Err("f64"),
        }
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            // SQLite has no boolean storage class; 0 is false and every
            // other integer is true, as its own `CASE`/`WHERE` do.
            Value::Integer(v) => Ok(*v != 0),
            _ => Err("bool"),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            Value::Text(v) => Ok(v.to_string()),
            _ => Err("String"),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            Value::Blob(v) => Ok(v.to_vec()),
            _ => Err("Vec<u8>"),
        }
    }
}

impl FromValue for Value {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        Ok(value.clone())
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self, &'static str> {
        match value {
            Value::Null => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }
}

/// The SQLite storage-class name for `value`, for error messages.
fn storage_class(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Integer(_) => "INTEGER",
        Value::Real(_) => "REAL",
        Value::Text(_) => "TEXT",
        Value::Blob(_) => "BLOB",
    }
}

/// An open transaction on a [`Connection`].
///
/// Dropping this handle without calling [`Transaction::commit`] rolls the
/// transaction back. That is the point of the type: a `?` that returns early
/// out of a function holding one cannot leave a half-finished transaction
/// open, which is the failure mode a consumer storing pointers to data it
/// cannot otherwise find must not have.
///
/// Derefs to its [`Connection`], so every `execute`/`query` method is
/// available on it directly and the statements run inside the transaction.
///
/// # Durability
///
/// What [`Transaction::commit`] guarantees is set by `PRAGMA synchronous`
/// (#645, ADR-0036), which this crate implements at all three levels:
///
/// * `FULL` — the journal (or WAL frame) is fsynced before the commit
///   returns, so a committed transaction survives an OS crash or power
///   loss. This is what a pointer store should use.
/// * `NORMAL` — syncs are skipped at the points SQLite skips them; a
///   commit survives a *process* crash but not necessarily a power loss.
/// * `OFF` — no fsync. A commit survives a process crash only.
///
/// This crate does not weaken the mode it is set to; the sync points are
/// `src/pager.rs`'s, and `PRAGMA synchronous` selects between them rather
/// than being accepted and ignored.
///
/// Nesting is not supported (no `SAVEPOINT`): a second `BEGIN` while this
/// handle is open is refused by the engine.
#[derive(Debug)]
pub struct Transaction {
    conn: Connection,
    done: bool,
}

impl Transaction {
    /// Commits the transaction.
    pub fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        self.conn.execute("COMMIT").map(drop)
    }

    /// Rolls the transaction back.
    ///
    /// The same thing dropping the handle does, but with the error
    /// reported rather than discarded.
    pub fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        self.conn.execute("ROLLBACK").map(drop)
    }

    /// The connection this transaction runs on.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl std::ops::Deref for Transaction {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self.conn
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // Errors are unreportable from `drop`. Rolling back is the safe
        // direction regardless: if this fails because the worker is already
        // gone, the transaction was never committed either, so the database
        // on disk is unchanged — which is the outcome a rollback wanted.
        // A caller who needs to see the error calls `rollback()`.
        self.conn.execute("ROLLBACK").ok();
    }
}

/// A compiled statement, ready to run with parameters.
///
/// Owns its registration on the connection's worker and finalizes it on
/// drop. Holds a clone of the [`Connection`], so the worker stays alive for
/// as long as any statement does.
///
/// # Schema changes
///
/// A statement recompiles itself if the schema changed since it was
/// prepared, which is what `sqlite3_prepare_v2` does on `SQLITE_SCHEMA`.
/// That is not a convenience: a program addresses tables by root page, and
/// a `DROP` can return that page to the freelist for a later `CREATE` to
/// reuse — so running a stale program could read a page belonging to a
/// different table. [`Statement::reprepare_count`] reports how often it has
/// happened, mirroring `SQLITE_STMTSTATUS_REPREPARE`.
#[derive(Debug)]
pub struct Statement {
    conn: Connection,
    id: u64,
    param_count: usize,
    column_names: Arc<Vec<String>>,
}

impl Statement {
    /// How many parameters this statement reads.
    ///
    /// The largest `?NNN` index it uses, matching
    /// `sqlite3_bind_parameter_count` — so `?3` alone reports 3.
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    /// This statement's result column names.
    ///
    /// Subject to the same limit as [`Rows::column_names`]: real names only
    /// for a single-table `SELECT`.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Runs the statement with `params` bound, returning rows changed.
    pub fn execute(&self, params: Vec<Value>) -> Result<u64, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.conn.send(Request::StatementExecute {
            id: self.id,
            params,
            reply: reply_tx,
        })?;
        let applied = self.conn.recv(reply_rx)??;
        Ok(applied.changes.unwrap_or(0))
    }

    /// Runs the statement with `params` bound and streams its rows.
    ///
    /// The same caveat as [`Connection::query`]: see [`Rows`].
    pub fn query(&self, params: Vec<Value>) -> Result<Rows, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.conn.send(Request::StatementQuery {
            id: self.id,
            params,
            reply: reply_tx,
        })?;
        let stream = self.conn.recv(reply_rx)??;
        Ok(Rows {
            column_names: stream.column_names,
            chunks: stream.chunks,
            buffered: Vec::new().into_iter(),
            finished: false,
        })
    }

    /// Runs the statement and collects every row.
    pub fn query_all(&self, params: Vec<Value>) -> Result<Vec<Row>, Error> {
        self.query(params)?.into_vec()
    }

    /// Runs the statement and returns its first row, if any.
    pub fn query_row(&self, params: Vec<Value>) -> Result<Option<Row>, Error> {
        let mut rows = self.query(params)?;
        let first = rows.next_row()?;
        drop(rows);
        Ok(first)
    }

    /// How many times this statement has been recompiled because the
    /// schema changed under it.
    ///
    /// SQLite's `SQLITE_STMTSTATUS_REPREPARE`: "the number of times that
    /// the prepared statement has been automatically regenerated due to
    /// schema changes". Zero for a statement whose schema has held still,
    /// which is what makes "compiled once" an observable claim rather than
    /// an assertion about internals.
    pub fn reprepare_count(&self) -> Result<u64, Error> {
        let (reply_tx, reply_rx) = sync_channel(0);
        self.conn.send(Request::StatementRepreparations {
            id: self.id,
            reply: reply_tx,
        })?;
        self.conn.recv(reply_rx)?
    }

    /// The connection this statement belongs to.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // Fire and forget: there is no reply to wait for and nothing a
        // caller could do with a failure. If the worker is already gone it
        // has dropped every kept statement with it.
        self.conn.send(Request::Finalize { id: self.id }).ok();
    }
}

/// What the worker should open.
enum Target {
    /// A real file, through `UnixVfs`.
    File(PathBuf),
    /// A private in-memory database, through `MemoryVfs`.
    Memory,
}

/// The engine state one connection owns, and the only place it is touched.
struct Engine {
    pager: std::rc::Rc<std::cell::RefCell<Pager>>,
    header: DatabaseHeader,
    mode: OpenMode,
    /// Threaded from each statement into the next, so a multi-statement
    /// transaction is one unit (`execute_transaction_step`'s contract).
    autocommit: bool,
    counters: Counters,
    /// The decoded catalog, reused across statements and dropped after any
    /// statement that can change the schema.
    ///
    /// Correctness, not only speed: a prepared program addresses tables by
    /// root page, so compiling against a stale catalog after a `DROP`/
    /// `CREATE` could read a recycled page. Invalidating on every possibly
    /// schema-changing statement is the conservative rule the CLI already
    /// uses (`src/bin/sqlite-rs/exec.rs::is_schema_changing`).
    catalog: Option<(Vec<TableSchema>, Vec<ViewSchema>)>,
    /// Statements compiled and kept for a [`Statement`] handle.
    statements: std::collections::HashMap<u64, KeptStatement>,
    /// Source of [`KeptStatement`] keys.
    next_statement_id: u64,
    /// Bumped whenever the catalog is invalidated, so a kept statement can
    /// tell it was compiled against an older schema (013/Req 8).
    schema_generation: u64,
    /// How long a contended lock is waited for before giving up.
    ///
    /// Zero by default, matching stock SQLite — `sqlite3_busy_timeout` is
    /// unset until a caller sets it, and a facade that silently retried for
    /// seconds would hide contention rather than report it.
    busy_timeout: Duration,
}

/// Sends a reply, tolerating a caller that has stopped waiting.
///
/// A dropped receiver is not an error worth reporting: the caller gave up
/// — its thread unwound, or it was interrupted between sending the request
/// and reading the answer — so there is nobody to tell, and no reason for
/// the worker to stop serving this connection's other handles.
fn answer<T>(reply: &SyncSender<T>, value: T) {
    reply.send(value).ok();
}

/// The worker thread's body: open, report, then serve requests until the
/// last [`Connection`] clone drops.
fn worker_main(
    target: Target,
    mode: OpenMode,
    requests: Receiver<Request>,
    open_reply: SyncSender<Result<(), Error>>,
) {
    let mut engine = match Engine::open(target, mode) {
        Ok(engine) => {
            if open_reply.send(Ok(())).is_err() {
                // The caller gave up between spawning us and hearing back,
                // so there is nobody to serve. Drop the engine, releasing
                // its file locks.
                return;
            }
            engine
        }
        Err(e) => {
            answer(&open_reply, Err(e));
            return;
        }
    };
    // Dropped before the first request is served: `Connection::spawn` has
    // its answer, and holding it would keep a channel alive for nothing.
    drop(open_reply);

    while let Ok(request) = requests.recv() {
        match request {
            Request::Execute { sql, params, reply } => {
                answer(&reply, engine.execute_one(&sql, params));
            }
            Request::ExecuteBatch { sql, reply } => {
                answer(&reply, engine.execute_batch(&sql));
            }
            Request::Query { sql, params, reply } => {
                engine.stream(&sql, params, &reply);
            }
            Request::TableNames { reply } => {
                let names = engine
                    .catalog()
                    .map(|(schemas, _)| schemas.iter().map(|s| s.name.clone()).collect());
                answer(&reply, names);
            }
            Request::Counters { reply } => {
                answer(&reply, engine.counters);
            }
            Request::Prepare { sql, reply } => {
                answer(&reply, engine.prepare(&sql));
            }
            Request::StatementExecute { id, params, reply } => {
                answer(&reply, engine.statement_execute(id, params));
            }
            Request::StatementQuery { id, params, reply } => {
                engine.statement_stream(id, params, &reply);
            }
            Request::StatementRepreparations { id, reply } => {
                answer(&reply, engine.repreparations(id));
            }
            Request::Finalize { id } => {
                engine.statements.remove(&id);
            }
            Request::SetBusyTimeout { timeout, reply } => {
                engine.busy_timeout = timeout;
                answer(&reply, ());
            }
        }
    }
}

impl Engine {
    fn open(target: Target, mode: OpenMode) -> Result<Self, Error> {
        let (header, pager) = match target {
            Target::File(path) => Self::open_file(&path, mode)?,
            Target::Memory => Self::open_memory()?,
        };
        Ok(Self {
            pager: std::rc::Rc::new(std::cell::RefCell::new(pager)),
            header,
            mode,
            autocommit: true,
            counters: Counters::default(),
            catalog: None,
            statements: std::collections::HashMap::new(),
            next_statement_id: 0,
            schema_generation: 0,
            busy_timeout: Duration::ZERO,
        })
    }

    fn open_file(path: &Path, mode: OpenMode) -> Result<(DatabaseHeader, Pager), Error> {
        let exists = UnixVfs.exists(path).map_err(|e| Error::CannotOpen {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;

        if !exists {
            if mode != OpenMode::ReadWriteCreate {
                // Requirement 2 is explicit that this creates nothing, so
                // the check is here rather than letting `open_write` bring
                // the file into existence as a side effect.
                return Err(Error::CannotOpen {
                    path: path.display().to_string(),
                    message: "no such database, and this mode does not create one".to_string(),
                });
            }
            // Give the file a valid empty page 1 before anything tries to
            // parse a header out of it — the same bootstrap the CLI's
            // `exec` does, and proven against the oracle in
            // `tests/corpus/bootstrap_oracle_test.rs`.
            let file = UnixVfs
                .create_or_open_write(path)
                .map_err(|e| vfs_open_error(path, &e))?;
            file.write_at(&DatabaseHeader::new_empty_page1(DEFAULT_PAGE_SIZE), 0)
                .map_err(|e| vfs_open_error(path, &e))?;
        }

        crate::dump::open(&UnixVfs, path).map_err(|e| open_error(path, &e))
    }

    fn open_memory() -> Result<(DatabaseHeader, Pager), Error> {
        const PATH: &str = "/sqlite-rs-memory.db";
        let mut vfs = MemoryVfs::new();
        vfs.insert(PATH, DatabaseHeader::new_empty_page1(DEFAULT_PAGE_SIZE));
        crate::dump::open(&vfs, Path::new(PATH)).map_err(|e| open_error(Path::new(PATH), &e))
    }

    /// Reads the catalog, or reuses the cached decode.
    fn catalog(&mut self) -> Result<&(Vec<TableSchema>, Vec<ViewSchema>), Error> {
        if self.catalog.is_none() {
            let borrowed = self.pager.borrow();
            let mut cursor = crate::btree::TableCursor::new(&*borrowed, &self.header, 1);
            let decoded =
                crate::schema::read_schema_and_views(&mut cursor, self.header.text_encoding)
                    .map_err(|e| Error::Corrupt {
                        message: format!("cannot read the schema: {e}"),
                    })?;
            drop(borrowed);
            self.catalog = Some(decoded);
        }
        self.catalog.as_ref().ok_or(Error::Execution {
            message: "catalog cache was empty immediately after filling it".to_string(),
        })
    }

    /// Runs one statement, waiting out a contended lock up to
    /// [`Engine::busy_timeout`].
    ///
    /// Retrying is only correct while this connection is in autocommit. In
    /// autocommit the statement *is* the transaction, so rolling the pager
    /// back and running it again is a faithful retry of the whole unit.
    /// Inside an explicit transaction it is not: the statement's own
    /// mutations are already in the pager's dirty set alongside every
    /// earlier statement's, and re-running one of them would double-apply
    /// it. A `Busy` there is the *transaction's* to retry, which is also
    /// what stock SQLite does with `SQLITE_BUSY` at `COMMIT`.
    ///
    /// The rollback before each retry is what makes this safe.
    /// `Pager::flush` documents that a contended escalation surfaces
    /// `VfsError::Locked` "before any byte of this transaction is journaled
    /// or written" and leaves `dirty` intact for the caller to retry or
    /// roll back (`src/pager.rs:524`) — so the dirty set at that point is
    /// exactly this statement's work, and clearing it returns the engine to
    /// the state the statement started from.
    fn run_with_retry(&mut self, sql: &str, params: Vec<Value>) -> Result<Applied, Error> {
        let deadline = Instant::now().checked_add(self.busy_timeout);
        let mut attempt: u32 = 0;
        loop {
            let was_autocommit = self.autocommit;
            match self.run(sql, params.clone()) {
                Err(Error::Busy { path }) if was_autocommit => {
                    // Discard the half-applied statement before retrying.
                    if let Ok(mut pager) = self.pager.try_borrow_mut() {
                        pager.rollback().ok();
                    }
                    let Some(delay) = self.backoff(deadline, attempt) else {
                        return Err(Error::Busy { path });
                    };
                    std::thread::sleep(delay);
                    attempt = attempt.saturating_add(1);
                }
                other => return other,
            }
        }
    }

    /// How long to sleep before retry `attempt`, or `None` once the
    /// deadline has passed.
    ///
    /// The ladder mirrors stock SQLite's default busy handler
    /// (`sqliteDefaultBusyCallback`): short sleeps first so an
    /// uncontended-in-practice lock is picked up almost immediately,
    /// lengthening so a genuinely long-held lock is not spun on.
    fn backoff(&self, deadline: Option<Instant>, attempt: u32) -> Option<Duration> {
        const LADDER_MS: [u64; 7] = [1, 2, 5, 10, 20, 50, 100];
        let deadline = deadline?;
        let now = Instant::now();
        let remaining = deadline.checked_duration_since(now)?;
        if remaining.is_zero() {
            return None;
        }
        let step = LADDER_MS
            .get(attempt as usize)
            .copied()
            .unwrap_or_else(|| LADDER_MS.last().copied().unwrap_or(100));
        Some(Duration::from_millis(step).min(remaining))
    }

    fn execute_one(&mut self, sql: &str, params: Vec<Value>) -> Result<Applied, Error> {
        let statement = self.single_statement(sql)?;
        self.run_with_retry(&statement, params)
    }

    fn execute_batch(&mut self, sql: &str) -> Result<(), Error> {
        for statement in crate::parser::split_statements(sql) {
            self.run_with_retry(&statement, Vec::new())?;
        }
        Ok(())
    }

    /// Compiles and runs one statement, updating the connection-scoped
    /// counters and invalidating the catalog if it could have changed.
    fn run(&mut self, sql: &str, params: Vec<Value>) -> Result<Applied, Error> {
        let program = self.compile(sql)?;
        self.run_compiled(sql, &program, params)
    }

    /// Checks an already-compiled `program` against this connection's mode
    /// and the supplied parameters, runs it, and invalidates the catalog if
    /// it could have changed the schema.
    ///
    /// Split out from [`Engine::run`] so a kept statement takes exactly the
    /// same path as an ad-hoc one — a second copy of these checks is how a
    /// prepared statement ends up honouring a different contract from
    /// `execute`.
    fn run_compiled(
        &mut self,
        sql: &str,
        program: &Program,
        params: Vec<Value>,
    ) -> Result<Applied, Error> {
        if self.mode == OpenMode::ReadOnly && writes(program) {
            return Err(Error::ReadOnly {
                statement: sql.to_string(),
            });
        }

        let wanted = program.param_count();
        if wanted != params.len() {
            return Err(Error::ParamCount {
                expected: wanted,
                found: params.len(),
            });
        }

        let outcome = self.step(program, params)?;

        if is_schema_changing(sql) {
            self.invalidate_catalog();
        }
        Ok(outcome)
    }

    /// Drops the decoded catalog and moves the schema generation on.
    ///
    /// The generation is what lets a kept statement notice it was compiled
    /// against an older schema. Correctness, not caching: a program
    /// addresses tables by root page, and a `DROP` can hand that page back
    /// to the freelist for a later `CREATE` to reuse — so running a stale
    /// program could read a page that now belongs to a different table
    /// (013/Req 8).
    fn invalidate_catalog(&mut self) {
        self.catalog = None;
        self.schema_generation = self.schema_generation.saturating_add(1);
    }

    /// Compiles `sql` and keeps it, returning the handle's fields.
    fn prepare(&mut self, sql: &str) -> Result<PreparedHandle, Error> {
        let statement = self.single_statement(sql)?;
        let program = self.compile(&statement)?;
        let column_names = Arc::new(self.column_names_of(&statement)?);
        let param_count = program.param_count();

        let id = self.next_statement_id;
        self.next_statement_id = self.next_statement_id.saturating_add(1);
        self.statements.insert(
            id,
            KeptStatement {
                sql: statement,
                program,
                column_names: Arc::clone(&column_names),
                generation: self.schema_generation,
                repreparations: 0,
            },
        );
        Ok(PreparedHandle {
            id,
            param_count,
            column_names,
        })
    }

    /// Takes a kept statement out of the table, recompiling it first if the
    /// schema has moved under it.
    ///
    /// Taken rather than borrowed because running it needs `&mut self`. The
    /// caller must put it back — see [`Engine::statement_execute`].
    fn take_refreshed(&mut self, id: u64) -> Result<KeptStatement, Error> {
        let mut kept = self
            .statements
            .remove(&id)
            .ok_or(Error::StatementFinalized)?;
        if kept.generation != self.schema_generation {
            // Recompile rather than fail, which is what
            // `sqlite3_prepare_v2` does on `SQLITE_SCHEMA`. A failure would
            // be safe too, but it would push a retry loop onto every
            // caller for something the connection can do itself.
            match self.compile(&kept.sql) {
                Ok(program) => {
                    let names = self.column_names_of(&kept.sql)?;
                    kept.program = program;
                    kept.column_names = Arc::new(names);
                    kept.generation = self.schema_generation;
                    kept.repreparations = kept.repreparations.saturating_add(1);
                }
                Err(e) => {
                    // The statement no longer compiles — its table was
                    // dropped, say. Keep it (so the id stays valid and the
                    // error is repeatable) and report why.
                    self.statements.insert(id, kept);
                    return Err(e);
                }
            }
        }
        Ok(kept)
    }

    fn statement_execute(&mut self, id: u64, params: Vec<Value>) -> Result<Applied, Error> {
        let kept = self.take_refreshed(id)?;
        let result = self.run_compiled(&kept.sql, &kept.program, params);
        self.statements.insert(id, kept);
        result
    }

    fn repreparations(&mut self, id: u64) -> Result<u64, Error> {
        self.statements
            .get(&id)
            .map(|kept| kept.repreparations)
            .ok_or(Error::StatementFinalized)
    }

    /// [`Engine::stream`] for a kept statement.
    fn statement_stream(
        &mut self,
        id: u64,
        params: Vec<Value>,
        reply: &SyncSender<Result<QueryStream, Error>>,
    ) {
        let kept = match self.take_refreshed(id) {
            Ok(kept) => kept,
            Err(e) => {
                answer(reply, Err(e));
                return;
            }
        };

        if let Err(e) = self.check_runnable(&kept.sql, &kept.program, params.len()) {
            self.statements.insert(id, kept);
            answer(reply, Err(e));
            return;
        }

        let (chunk_tx, chunk_rx) = sync_channel::<Chunk>(CHUNK_SLOTS);
        let head = QueryStream {
            column_names: Arc::clone(&kept.column_names),
            chunks: chunk_rx,
        };
        if reply.send(Ok(head)).is_err() {
            self.statements.insert(id, kept);
            return;
        }
        self.drain(&kept.program, params, &chunk_tx);
        self.statements.insert(id, kept);
    }

    /// The mode and arity checks, shared by the ad-hoc and kept paths.
    fn check_runnable(
        &self,
        sql: &str,
        program: &Program,
        param_count: usize,
    ) -> Result<(), Error> {
        if self.mode == OpenMode::ReadOnly && writes(program) {
            return Err(Error::ReadOnly {
                statement: sql.to_string(),
            });
        }
        let wanted = program.param_count();
        if wanted != param_count {
            return Err(Error::ParamCount {
                expected: wanted,
                found: param_count,
            });
        }
        Ok(())
    }

    /// Splits `sql` and insists on exactly one statement.
    fn single_statement(&self, sql: &str) -> Result<String, Error> {
        let statements = crate::parser::split_statements(sql);
        let count = statements.len();
        statements
            .into_iter()
            .next()
            .filter(|_| count == 1)
            .ok_or(Error::MultipleStatements { count })
    }

    /// Runs `program`, threading the transaction state and folding the
    /// counters.
    fn step(&mut self, program: &Program, params: Vec<Value>) -> Result<Applied, Error> {
        let mut vm =
            crate::vdbe::Vm::with_shared_writable_db(std::rc::Rc::clone(&self.pager), self.header);
        vm.autocommit = self.autocommit;
        vm.bind_params(params);

        let mut execution = crate::vdbe::Execution::new(vm, program);
        // Rows are discarded here; `execute` reports a count, not results.
        // Draining through `next_row` rather than a collecting entry point
        // keeps this on the one loop ADR-0040 specifies, so the eventual
        // streaming path cannot diverge from this one.
        while execution.next_row().map_err(exec_error)?.is_some() {}

        self.autocommit = execution.autocommit();
        let changes = program.counts_changes().then(|| execution.changes());
        if let Some(changed) = changes {
            self.counters.changes = changed;
        }
        if let Some(rowid) = execution.last_insert_rowid() {
            self.counters.last_insert_rowid = rowid;
        }
        Ok(Applied { changes })
    }

    /// Runs `sql` and streams its rows to `reply`'s receiver.
    ///
    /// The whole execution lives in this one stack frame, which is what
    /// makes it expressible at all: `Execution` borrows its `Program`, and
    /// no type in this module may carry a lifetime (see the module docs).
    /// So the worker stays inside this call until the result is drained or
    /// the caller drops its [`Rows`] — and while it does, this connection
    /// serves nothing else.
    ///
    /// That is a real constraint on callers, not an implementation detail:
    /// holding an undrained `Rows` blocks every other statement on the same
    /// connection until it is read or dropped. It is a wait rather than a
    /// deadlock — dropping `Rows` closes the channel, the next send fails,
    /// and the worker returns here — but a thread that holds a `Rows` while
    /// waiting on another thread that needs the same connection will hang.
    /// Requirement 4 accepts serialized access; this is its sharp edge, and
    /// `Rows`'s own documentation repeats it.
    fn stream(
        &mut self,
        sql: &str,
        params: Vec<Value>,
        reply: &SyncSender<Result<QueryStream, Error>>,
    ) {
        let started = self.start_stream(sql, params.len());
        let (program, column_names) = match started {
            Ok(pair) => pair,
            Err(e) => {
                answer(reply, Err(e));
                return;
            }
        };

        // Bounded, so a slow reader applies backpressure to the engine
        // rather than letting rows pile up. See `CHUNK_SLOTS` for why the
        // bound is two and not one.
        let (chunk_tx, chunk_rx) = sync_channel::<Chunk>(CHUNK_SLOTS);
        let head = QueryStream {
            column_names,
            chunks: chunk_rx,
        };
        if reply.send(Ok(head)).is_err() {
            // The caller gave up before reading anything; nothing ran, so
            // there is nothing to unwind.
            return;
        }

        self.drain(&program, params, &chunk_tx);
    }

    /// Compiles `sql`, checks it against this connection's mode and its
    /// parameter arity, and works out its column names.
    fn start_stream(
        &mut self,
        sql: &str,
        param_count: usize,
    ) -> Result<(Program, Arc<Vec<String>>), Error> {
        let program = self.compile(sql)?;
        self.check_runnable(sql, &program, param_count)?;
        let names = self.column_names_of(sql)?;
        Ok((program, Arc::new(names)))
    }

    /// The result column names for `sql`, empty for a statement that is not
    /// a `SELECT`.
    fn column_names_of(&mut self, sql: &str) -> Result<Vec<String>, Error> {
        use crate::parser::error::ParseOutcome;

        if !is_select(sql) {
            return Ok(Vec::new());
        }
        let ParseOutcome::Accepted(select) = crate::parser::parse_select(sql) else {
            // Unreachable: `compile` already parsed this successfully.
            return Ok(Vec::new());
        };
        let (schemas, _) = self.catalog()?;
        Ok(crate::codegen::result_column_names(&select, schemas))
    }

    /// Drives `program` to completion, sending rows in batches.
    fn drain(&mut self, program: &Program, params: Vec<Value>, chunks: &SyncSender<Chunk>) {
        let mut vm =
            crate::vdbe::Vm::with_shared_writable_db(std::rc::Rc::clone(&self.pager), self.header);
        vm.autocommit = self.autocommit;
        vm.bind_params(params);
        let mut execution = crate::vdbe::Execution::new(vm, program);

        let mut batch: Vec<Vec<Value>> = Vec::new();
        loop {
            match execution.next_row() {
                Ok(Some(row)) => {
                    batch.push(row);
                    if batch.len() >= CHUNK_ROWS
                        && chunks
                            .send(Chunk::Rows(std::mem::take(&mut batch)))
                            .is_err()
                    {
                        // The caller dropped its `Rows`. Abandon the
                        // execution here: dropping it releases its cursors,
                        // which is Requirement 7's "abandoning a statement
                        // releases it".
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    // A mid-stream failure. Send it rather than the rows
                    // already batched: a partial result the caller cannot
                    // tell is partial would be worse than no result.
                    answer(chunks, Chunk::Failed(exec_error(e)));
                    return;
                }
            }
        }

        if !batch.is_empty() && chunks.send(Chunk::Rows(batch)).is_err() {
            return;
        }

        // Only a stream that ran to completion updates the connection's
        // state. An abandoned one returns above, leaving the counters and
        // the autocommit flag as they were.
        self.autocommit = execution.autocommit();
        if program.counts_changes() {
            self.counters.changes = execution.changes();
        }
        if let Some(rowid) = execution.last_insert_rowid() {
            self.counters.last_insert_rowid = rowid;
        }
        answer(chunks, Chunk::Done);
    }

    fn compile(&mut self, sql: &str) -> Result<Program, Error> {
        // One entry point for every statement kind, which is what
        // `sqlite3_prepare_v2` presents and what #695's lift made possible:
        // `compile_statement` answers `Unrecognized("SELECT")` for a read,
        // and the SELECT pipeline needs its FROM tables resolved and its
        // views and CTEs expanded first.
        if is_select(sql) {
            return self.compile_select(sql);
        }
        let (schemas, views) = self.catalog()?;
        crate::codegen::compile_statement(sql, schemas, views).map_err(|e| Error::Compile {
            message: e.to_string(),
        })
    }

    fn compile_select(&mut self, sql: &str) -> Result<Program, Error> {
        use crate::parser::error::ParseOutcome;

        let select = match crate::parser::parse_select(sql) {
            ParseOutcome::Accepted(select) => *select,
            ParseOutcome::Unsupported { message, span }
            | ParseOutcome::Invalid { message, span } => {
                return Err(Error::Parse {
                    message,
                    line: span.line,
                    column: span.column,
                })
            }
        };
        let stats = std::collections::HashMap::new();
        let (schemas, views) = self.catalog()?;
        match crate::codegen::compile_select_program(&select, false, schemas, views, &stats) {
            Ok(crate::codegen::SelectOutcome::Program(program)) => Ok(program),
            Ok(crate::codegen::SelectOutcome::Eqp(_)) => Err(Error::Compile {
                message: "EXPLAIN QUERY PLAN has no rows to execute".to_string(),
            }),
            Err(e) => Err(prepare_error(&e)),
        }
    }
}

/// Whether `program` can modify the database.
///
/// Keyed on `OpenWrite` and the DDL opcodes, not on `Insert`/`Delete`.
/// Those two also target *ephemeral* cursors: a materialized FROM-subquery
/// emits an `Insert` (`src/codegen/subquery/from_clause.rs:296`) in a plain
/// `SELECT`, so keying on them would refuse read queries in
/// [`OpenMode::ReadOnly`]. `OpenWrite` is the only way to obtain a writable
/// table cursor, and every DML path emits one.
fn writes(program: &Program) -> bool {
    program.instructions.iter().any(|i| {
        matches!(
            i.opcode,
            Opcode::OpenWrite
                | Opcode::CreateTable
                | Opcode::DropTable
                | Opcode::CreateIndex
                | Opcode::DropIndex
                | Opcode::CreateView
                | Opcode::Analyze
                | Opcode::SetJournalMode
        )
    })
}

/// Whether `sql` should go through the `SELECT` compile pipeline.
fn is_select(sql: &str) -> bool {
    let head = sql.trim_start();
    ["SELECT", "VALUES", "WITH"]
        .iter()
        .any(|kw| starts_with_keyword(head, kw))
}

/// Whether `sql` can change the `sqlite_master` catalog.
///
/// Conservative by design: any statement starting with `CREATE`, `DROP` or
/// `ALTER` invalidates the cached catalog, even one that fails or turns out
/// to be a no-op. The cost of an unnecessary re-read is one b-tree walk;
/// the cost of a missed one is compiling against a stale root page.
fn is_schema_changing(sql: &str) -> bool {
    let head = sql.trim_start();
    ["CREATE", "DROP", "ALTER"]
        .iter()
        .any(|kw| starts_with_keyword(head, kw))
}

fn starts_with_keyword(head: &str, keyword: &str) -> bool {
    head.get(..keyword.len())
        .is_some_and(|h| h.eq_ignore_ascii_case(keyword))
}

/// Classifies a failure to open, distinguishing a contended lock from a
/// genuinely unopenable file.
///
/// spec 007's `VfsError::Locked` has to arrive as the busy variant even
/// when it surfaces during open, since that is when a competing writer's
/// lock is most likely to be met. Matched structurally, for the reason
/// given on [`exec_error`].
fn open_error(path: &Path, e: &crate::dump::DumpError) -> Error {
    use crate::dump::DumpError;
    use crate::pager::PagerError;
    use crate::vfs::VfsError;

    let locked = matches!(
        e,
        DumpError::Vfs(VfsError::Locked { .. })
            | DumpError::Pager(PagerError::Vfs(VfsError::Locked { .. }))
    );
    if locked {
        return Error::Busy {
            path: path.display().to_string(),
        };
    }
    Error::CannotOpen {
        path: path.display().to_string(),
        message: e.to_string(),
    }
}

/// The same classification for a raw `VfsError`, used on the bootstrap
/// path before a `Pager` exists.
fn vfs_open_error(path: &Path, e: &crate::vfs::VfsError) -> Error {
    use crate::vfs::VfsError;

    if matches!(e, VfsError::Locked { .. }) {
        return Error::Busy {
            path: path.display().to_string(),
        };
    }
    Error::CannotOpen {
        path: path.display().to_string(),
        message: e.to_string(),
    }
}

fn prepare_error(e: &crate::codegen::PrepareError) -> Error {
    let message = e.to_string();
    if let Some(placeholder) = named_placeholder_of(&message) {
        return Error::NamedParameter { placeholder };
    }
    Error::Compile { message }
}

/// Recovers the placeholder from codegen's named-parameter refusal so the
/// API can report [`Error::NamedParameter`] rather than a generic compile
/// failure.
///
/// Reading it back out of the message is not elegant. The alternative is a
/// dedicated `CodegenError` variant, which is a change to a shared engine
/// error enum with sixteen match sites — worth doing, and worth doing on
/// its own rather than inside the facade. Recorded so the seam is visible.
fn named_placeholder_of(message: &str) -> Option<String> {
    let rest = message.strip_prefix("unsupported: named parameter ")?;
    let placeholder = rest.split_whitespace().next()?;
    Some(placeholder.to_string())
}

fn exec_error(e: crate::vdbe::ExecError) -> Error {
    use crate::pager::PagerError;
    use crate::vdbe::ExecError;
    use crate::vfs::VfsError;

    match e {
        // The engine's route for constraint violations: `Halt` carries the
        // extended SQLite result code codegen chose.
        ExecError::Halted { code, message } => Error::Sqlite {
            code,
            message: message.unwrap_or_default(),
        },
        // Lock contention, matched structurally rather than by message
        // text. Requirement 5 makes this a *distinct, retryable* variant,
        // so classifying it on a substring would be exactly the wrong
        // trade: a reworded `Display` would silently turn every busy error
        // into a permanent one, and the caller's retry loop would vanish
        // without a test failing.
        ExecError::FlushFailed(PagerError::Vfs(VfsError::Locked { path })) => Error::Busy { path },
        other => Error::Execution {
            message: other.to_string(),
        },
    }
}
