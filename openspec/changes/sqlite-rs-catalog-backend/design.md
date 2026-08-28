# A pure-Rust SQLite catalog backend

## The five gaps, measured

Read from the `sqlite-rs` working tree at `/Users/jjverhoeks/git/schuberg/sqlite-rs`, version 0.18.5, on 2026-08-28.

### 1. No `sqlx` driver, and SQE only speaks `sqlx`

SQE has no direct SQLite calls. Access runs through the vendored catalog's `sqlx = { version = "0.9.0", features = ["any"] }` (`vendor/iceberg-rust/crates/catalog/sql/Cargo.toml:39`). Writing a `sqlx` driver for sqlite-rs means implementing `Database`, `Connection`, `Row`, `Column`, `Value`, `TypeInfo` and `Arguments`, and it would need everything in gaps 2 to 5 solved first. Bypassing `sqlx` instead means implementing 14 async methods on iceberg's `Catalog` trait (`vendor/iceberg-rust/crates/iceberg/src/catalog/mod.rs:54-110`).

Fourteen methods over two tables is smaller than a `sqlx` driver, and it does not commit SQE to a shape another project controls. That is the choice this design makes.

The cost is stated plainly: this backend serves SQLite only. `[catalog] type = "sql"` keeps the sqlx path for Postgres and MySQL, and dropping only the `sqlx/sqlite` feature removes `libsqlite3-sys` from the graph while leaving those backends intact.

### 2. The engine is `!Send`, and `Catalog` requires `Send + Sync`

```rust
pub trait Catalog: Debug + Sync + Send {
```

sqlite-rs holds its page source as `Rc<dyn PageSource>` and its writable pager as `Rc<RefCell<Pager>>` (`src/vdbe/exec.rs:1113-1118`). Neither is `Send`, deliberately: their ADR-0017 put the writable VM's page source behind `Rc<RefCell<_>>`, and ADR-0013 rejected genericizing `Vm` over `P: PageSource` because one `Vm` shares a page source across N cursors through cheap `Rc` clones. Asking them to swap `Rc` for `Arc` taxes every cursor clone on the read path to serve a requirement that lives at the connection boundary.

Worth being precise about the constraint, because it rules out the obvious shortcut: `Rc` does not implement `Send` at all, so owning every clone does not let us move the graph to another thread. There is no "move the connection into the runtime" option to fall back on, and no safe wrapper that manufactures one. Confinement is the only route without patching the engine.

The pattern is standard rather than exotic. `sqlx`'s own SQLite driver runs each connection on a spawned thread behind a `flume` channel (`sqlx-sqlite-0.9.0/src/connection/worker.rs`) for the same reason: a C `sqlite3*` is no more shareable than an `Rc`. So the actor below is what SQE would need even if it were talking to the C library through anything other than sqlx's pool.

So the boundary moves to us. One OS thread owns the engine state; the `Catalog` impl is a handle holding an `mpsc::Sender`, which is `Send + Sync` whatever sits behind it.

### 3. There is no rows-affected count, and the commit path depends on one

This is the gap that shapes the design. `iceberg-catalog-sql::update_table` implements optimistic concurrency as a compare-and-swap:

```sql
UPDATE iceberg_tables
   SET metadata_location = ?, previous_metadata_location = ?
 WHERE catalog_name = ? AND table_name = ? AND table_namespace = ?
   AND metadata_location = ?          -- the CAS predicate
```

then:

```rust
if update_result.rows_affected() == 0 {
    return Err(Error::new(ErrorKind::CatalogCommitConflicts, ...).with_retryable(true));
}
```

sqlite-rs returns `Result<(Vec<Vec<Value>>, bool), ExecError>` from `execute_transaction_step`: rows and the new autocommit flag. No changes count, and no `sqlite3_changes()` equivalent exists in the VDBE (searched `src/vdbe/`).

Two ways to get the same guarantee, and only one of them is available today.

**Ask upstream for `changes()`.** Correct, small, and blocks this change on another team's release. Worth requesting anyway; not worth waiting for.

**SELECT the current pointer, compare, then UPDATE, both inside one transaction.** Available now, and sound here for reasons that must be written down rather than assumed:

- The actor is the only writer in this process. No second in-process connection exists to interleave, because the engine cannot be shared across threads in the first place.
- Another process is excluded by the pager's file locks. `Pager` escalates SHARED to EXCLUSIVE before touching the journal or the main file, and refuses a second WAL writer with `VfsError::Locked` rather than interleaving frames (their spec 007).
- The transaction covers both statements, so a crash between them rolls back.

The important part is what it must NOT become: a check that reads the pointer, releases, and then writes. If the SELECT and the UPDATE are not in one transaction, this is a lost update that presents as a table quietly rolling back to an older snapshot. The scenario in the spec asserts the conflict is *reported*, not that the rows look right.

### 4. No API to create a database, but the primitive is public

`examples/README.md`: "This crate has no API to create a brand-new database file from nothing -- only to open an already-valid one." Their CLI does it anyway, with `DatabaseHeader::new_empty_page1(DEFAULT_PAGE_SIZE)` (`src/header.rs:295`, `pub`). SQE writes those bytes to a new path and opens it. That is the whole workaround, and it is why `mode=rwc` semantics (`crates/sqe-cli/src/embedded.rs:497`, `crates/sqe-catalog/src/mount.rs:493`) survive the swap.

### 5. Two SQL-semantics gaps on the `CREATE TABLE` path

Both are ours to work around, and the second is the only one that touches interop.

**`IF NOT EXISTS` is ignored.** `if_not_exists` appears in
`src/parser/grammar.rs` and `src/parser/printer.rs` and nowhere else: the
clause parses, codegen drops it. SQE's bootstrap issues
`CREATE TABLE IF NOT EXISTS` for both catalog tables on every open, so a second
open would attempt to recreate them.

Workaround: read the schema first (`read_schema`, which the adapter already
calls to resolve tables) and issue `CREATE TABLE` only for tables that are
absent. That is one comparison against a list SQE already has in memory, and it
is what `IF NOT EXISTS` would do anyway.

**A composite `PRIMARY KEY` is not enforced and creates no index.** From
`src/codegen/stmt/insert.rs`: "A composite `PRIMARY KEY(...)`/`UNIQUE(...)`
*table* constraint with no backing `CREATE INDEX`/on-disk index (this codebase
doesn't auto-create `sqlite_autoindex_*` entries yet) has no real index to seek
against, so it still isn't enforced."

SQE's catalog key is exactly that shape, `(catalog_name, table_namespace,
table_name)`. **This was measured on 2026-08-28** with sqlite-rs 0.18.5 and
stock `sqlite3` 3.51.0, and the result decides the change:

| Schema shape | Rust engine write | Stock `sqlite3` afterwards |
|---|---|---|
| Declared composite `PRIMARY KEY`, created by stock (what sqlx, Spark's `JdbcCatalog` and pyiceberg's `SqlCatalog` all write) | accepts the row, does not maintain `sqlite_autoindex_*` | **corrupt**: `integrity_check` reports "row 2 missing from index", `count(*)` answers from the index and undercounts |
| Declared composite `PRIMARY KEY`, created by sqlite-rs | accepts duplicates | **malformed**: `INSERT` and `integrity_check` fail with "database disk image is malformed (11)" |
| No declared PK, explicit named `CREATE UNIQUE INDEX` | enforced, SQLite code 2067 on a duplicate | **ok**: `integrity_check` clean, index complete, both engines agree |

The distinction is exact: **sqlite-rs maintains a named index and ignores the
implicit `sqlite_autoindex_*`.** A table with no PK and a real index round-trips
perfectly between the two engines, in both creation directions. A table with a
declared composite PK does not, and the failure is silent on the write side.

Three consequences, and none of them is a detail:

**The schema must not declare the composite primary key.** An earlier draft of
this design proposed declaring it for DDL compatibility *and* adding an explicit
index. That combination is the malformed case: the declared PK is the poison, not
the missing index. SQE creates `iceberg_tables` and
`iceberg_namespace_properties` without the constraint and with a named
`CREATE UNIQUE INDEX` on the same columns, for both engines, so one schema serves
both. Iceberg clients query rows rather than inspect constraints, so their DML is
unaffected.

**The Rust engine must refuse to write a foreign catalog.** A warehouse created
by sqlx, Spark or pyiceberg carries a declared PK and an `sqlite_autoindex_*`
that this engine will not maintain. Writing to it corrupts it quietly. So the
engine adapter detects `sqlite_autoindex_*` in `sqlite_master` at open and, if
present, either refuses writes or refuses the mount, per task 4d. Reads are safe
and stay allowed. Failing closed here is the difference between a feature and an
incident.

**Adoption is not migration.** You cannot repair a foreign catalog by adding a
named index: the unmaintained autoindex is still there. Switching an existing
warehouse to the Rust engine means recreating the two catalog tables, which is a
migration SQE is not going to perform silently. New warehouses get the Rust
engine; existing ones keep the C engine until upstream maintains autoindexes.

## The actor boundary

```
Catalog handle (Send + Sync + Debug)        one OS thread
  ┌──────────────────────────┐              ┌────────────────────────────────┐
  │ mpsc::Sender<Request>    ─┼─────────────▶│ Rc<RefCell<Pager>>, header     │
  │ (cloned per session)      │              │ HashMap<String, Program> cache │
  └──────────────────────────┘              │ autocommit: bool               │
        ▲ oneshot::Receiver                  └────────────────────────────────┘
        └──────────────────────────────────────────────┘
```

Each request carries its SQL, its bound parameters and a `oneshot::Sender` for the reply. The async `Catalog` methods send and await; the thread is a plain blocking loop. `spawn_blocking` is not a substitute: it hands work to an arbitrary pool thread, and the state must live on one specific thread for the `Rc` to be sound.

Consequences worth accepting deliberately:

- **Metadata access serializes.** One writer and one reader at a time per catalog. SQE's SQLite traffic is metadata behind the session-context cache and the table-metadata cache, so this is a throughput characteristic, not a correctness one. It is also the first thing to measure, not the last: task 6.2 exists because "low QPS" is an assumption until a number backs it.
- **One thread per attached catalog.** `ATTACH` mounts are per-session in SQE; the thread must be owned by the catalog instance and joined on drop, or a session churning mounts leaks threads. The test for that is task 4.4.
- **Panics must not poison silently.** If the thread dies, every pending oneshot drops, and the handle must turn that into an error rather than hanging forever. Task 4.5.

## Scopes 1 and 2: the pointer store, and the constraint the second exposes

Scope 1 is one catalog: `sqe-cli --embedded` attaches `iceberg` at
`~/.sqe/warehouse/sqe.db`, created on first use, persisting across restarts.
One engine, one thread, one file, usually one process.

Scope 2 is many: `ATTACH '<path>' AS name (TYPE sqlite)`, `--catalog
NAME=PATH`, `[catalogs.*]`, several live at once and joined across three-part
names. `crates/sqe-coordinator/src/runtime_catalog.rs` holds them
process-locally, keyed by name, each entry one `Arc<dyn iceberg::Catalog>`
cloned into every session context. So an attach is process-wide, and N sessions
querying one attached catalog share one instance rather than building their own.

That sharing is what makes scope 2 tractable, and it is also where the trap is.
Nothing stops two attachments naming the same file:

```sql
ATTACH '/data/warehouse/sqe.db' AS prod (TYPE sqlite);
ATTACH '/data/warehouse/sqe.db' AS prod_readonly (TYPE sqlite);
```

Under the C backend those are two sqlx pools whose SQLite handles coordinate
through an in-process mutex keyed on the inode, which is why stock SQLite ships
one. Under sqlite-rs they would be two independent engines, and the file locks
do NOT save us: POSIX `fcntl` locks are owned by `(process, inode)`, so a second
lock request from the same process succeeds, and `src/vfs/lock.rs:96` documents
the sharper half of it, that closing *any* fd this process holds on the file
drops the lock. Two engines in one process is therefore not a busy error, it is
interleaved journal writes.

**The design answer is a registry keyed on the file, not on the mount.** One
process-wide map from canonicalized path to a refcounted engine owner:

```
ATTACH prod            ─┐
ATTACH prod_readonly   ─┼──▶ engine("/data/warehouse/sqe.db")  ──▶ one thread
[catalogs.archive]     ─┘                                          one Pager
ATTACH other  ─────────────▶ engine("/data/other.db")         ──▶ its own thread
```

Three details this must get right, each of which is a way to be subtly wrong:

- **Canonicalize before keying.** A relative path, a symlink and a `..` segment
  can all name one inode. Keying on the string the user typed reintroduces the
  problem the registry exists to solve. `std::fs::canonicalize` on the resolved
  path, and it must happen before the engine is created rather than after.
- **Only the last owner joins the thread.** The handle is cloned into every
  session context, so a `Drop` on the handle cannot join. The join belongs to an
  inner owner held by `Arc`: when the refcount reaches zero, signal shutdown and
  join. A handle-level join kills a live engine; no join at all leaks a thread
  per attach.
- **`DETACH` must decrement, not close.** Detaching `prod_readonly` while `prod`
  is still attached leaves the engine running. Detaching the last one releases
  it, and re-attaching afterwards builds a fresh engine.

Cross-catalog queries need nothing beyond this. Each attached catalog is an
independent DataFusion `CatalogProvider`, so a join across two of them talks to
two engines on two threads. Within one catalog, the metadata reads a plan issues
serialize behind that catalog's thread, which is the throughput bet task 6.2
measures rather than assumes.

## The type stays `sqlite`; the engine is a switch

No new `CatalogKind`, no `TYPE sqlite_rs`. A user attaching a database should not
have to know which SQLite implementation SQE linked, and a config that leaks that
choice into every ATTACH statement makes every script engine-specific.

Selection is two-level: the `sql-sqlite-rs` feature compiles the Rust engine in,
and `[catalog] sqlite_engine = "rust" | "c"` picks when both are compiled. With
one compiled in, that one is used and the config value is validated against what
exists rather than silently ignored. `mount.rs` resolves the engine once per
attach and hands the chosen engine to the same `Catalog` construction path.

## Scope 3: any SQLite database, not just ours

Attaching a file with no Iceberg catalog schema exposes the file's own tables as
queryable relations, joinable against Iceberg tables in one query. That is what
makes "any SQLite db works" true rather than "any SQLite db SQE wrote".

**Mode detection at attach time.** Read the schema once (`read_schema`, already
on the open path). If `iceberg_tables` and `iceberg_namespace_properties` are
both present, it is a catalog. Otherwise it is data. An explicit option
overrides, because auto-detection should be a convenience and never a trap:
`ATTACH '<path>' AS x (TYPE sqlite, MODE data)` forces the data reading of a
file that happens to contain those table names.

**Shape.** Catalog `x`, schema `main`, tables as the file declares them. `main`
matches SQLite's own name for the primary database and leaves room for its
`ATTACH`ed schemas later without renaming anything.

**Read-only, deliberately.** Writing into someone else's SQLite database through
an Iceberg-shaped engine is a different feature with a different blast radius.
`INSERT` into a scope-3 table is refused rather than approximated, the same
decision the Hive external path made for `INSERT OVERWRITE`.

**Type mapping is the part that will bite.** SQLite columns have a declared type
that implies an affinity, not a constraint: a column declared `INTEGER` can hold
text in one row and a blob in the next. So the mapping is affinity-based
(`INTEGER` to Int64, `REAL` to Float64, `TEXT` to Utf8, `BLOB` to Binary,
declared-typeless to Utf8), and a value that does not fit its column's Arrow type
is an error naming the table, column and row rather than a silent NULL or a
silent cast. Their spec 008 defines the affinity rules; SQE implements the
mapping, not the semantics. `NUMERIC`, `DATE` and `DATETIME` need explicit
decisions and get them in the tasks rather than being discovered at runtime.

**Pushdown.** Projection first (generate `SELECT a, b FROM t`), then `LIMIT`, then
simple conjunctive filters on comparable types. Anything not translatable stays
in DataFusion as a post-filter. Correct-and-slow before clever-and-wrong: a
predicate pushed down with different NULL or collation semantics than DataFusion
would apply returns wrong rows, and the differential test against stock `sqlite3`
is what keeps that honest.

**The materialization gap, and why scope 3 waits on it.**
`execute_with_db_and_params` returns `Vec<Vec<Value>>` and there is no
incremental row API anywhere in `src/vdbe/`, so the whole result set is built in
memory before SQE sees a batch. A dozen catalog rows do not care. A million-row
table does: the engine allocates the entire table as `Vec<Vec<Value>>` before the
first `RecordBatch` exists.

Two interim options and one real fix. Chunk with `LIMIT`/`OFFSET`, which bounds
memory but re-scans for every chunk and is quadratic on offset. Or cap scope 3 at
a configurable row count and refuse beyond it, which is honest but narrow. The
real fix is upstream: an incremental statement API, which is now a requirement in
the spec drafted into their repo. Scope 3 ships with the cap plus a documented
ceiling, and lifts it when the upstream API lands. Shipping unbounded
materialization and calling it a data source would be the wrong trade.

## What SQE actually calls

The public surface used, with signatures, so the adapter's blast radius is one file:

| Purpose | Call |
|---|---|
| Open | `dump::open(&UnixVfs, path) -> (DatabaseHeader, Pager)` |
| Create | `DatabaseHeader::new_empty_page1(page_size) -> Vec<u8>` |
| Schema | `read_schema(&mut TableCursor::new(&pager, &header, 1), header.text_encoding)` |
| Parse | `parse_select(sql) -> ParseOutcome`, `split_statements(sql)` |
| Compile | `compile_select_with_catalog(&select, &table, &schemas) -> Program` |
| Read | `execute_with_db_and_params(&program, Rc<dyn PageSource>, header, Vec<Value>)` |
| Write | `execute_transaction_step(&program, Rc<RefCell<Pager>>, header, autocommit) -> (Vec<Vec<Value>>, bool)` |
| Values | `record::Value`, `format::format_query_value` |

Every one of these is an internals-shaped API on a pre-1.0 crate whose changelog says "minor bumps may break the public API". Pin the exact version, keep all of it inside `sqlite_rs_engine.rs`, and let the `Catalog` impl see only SQE's own row type. A version bump then breaks one file with a compile error rather than fourteen methods with behavior changes.

## Why not a `sqlx` driver

It is the smaller change to SQE and the larger change overall: it needs gaps 2 and 3 closed upstream, it puts SQE's timeline behind another project's release, and it makes SQE a consumer of two moving APIs (sqlx's driver traits and sqlite-rs's internals) instead of one. A driver remains the right long-term answer for the ecosystem. The spec drafted into their repo (`.openspec/specs/012-embedding-api/`, six requirements, with ADR-0033) keeps it as a `SHOULD` and makes the rows-affected count its Requirement 1; its Requirement 4 specifies upstream the same worker-thread confinement SQE builds here. This change does not wait for any of it.

## Interop is the contract, and the file is the witness

Both backends must read and write the same two tables in the same file, or rollback is a migration rather than a config flip. Same schema, same column names, same composite primary key, same `previous_metadata_location` bookkeeping.

The test that closes it cannot be SQE reading its own writes. It must be cross-engine, in both directions: create and populate with the C backend, then read and commit with `sqlite_rs`, then read back with the C backend again. A single-direction test passes on two engines that are consistently wrong in the same way, which is the failure mode the RENAME COLUMN work already taught us to distrust.

## SQLite has one role today, and this change adds a second

Verified rather than assumed: today SQLite is the Iceberg catalog's pointer store
and nothing else. Two tables, written only by `iceberg-catalog-sql`:
`iceberg_tables` maps `(catalog_name, table_namespace, table_name)` to a
`metadata_location` string, `iceberg_namespace_properties` maps a namespace
property key to its value. One row per table. No user SQL reaches it (DataFusion
executes queries against Parquet on object storage) and no user data is in it.
Every other SQLite reference in the tree is a test: `maintenance_lease.rs`,
`maintenance_scheduler.rs`, `maintenance.rs` and `runtime_catalog.rs` mention it
only to build a real catalog under `--features test-sqlite`.

So there is no existing consumer to migrate, and scopes 1 and 2 keep that shape:
the blast radius is the `Catalog` trait and nothing behind it.

**Scope 3 breaks that symmetry on purpose, and the risk profile changes with
it.** For the pointer store the workload is a few hundred rows written at commit
frequency, so the serialized actor costs nothing and the engine's query coverage
barely matters. As a data source, SQLite is on the read path: arbitrary schemas,
arbitrary affinities, arbitrary row counts, and result sets the engine currently
materializes whole. The correctness question changes shape too. A pointer-store
bug loses a table silently, which is why tasks 0.4, 3.1b and 3.4 gate scopes 1
and 2. A data-source bug returns wrong rows to a user's query, which is why
scope 3's gate is a differential test against stock `sqlite3` on the same file,
and why it ships read-only with a documented row ceiling rather than as an
unbounded scanner.

## Which scope ships first

Scope 1, because that is where the C-toolchain requirement actually hurts and
where a single writer is the natural shape. Scope 2 follows in the same change
rather than a later one, because the registry is 100 lines and retrofitting it
after two engines have already raced on one file is a debugging session nobody
should have to run.

Server deployments are a different question and this change does not answer it:
a shared catalog across coordinators wants Postgres, not a SQLite file, whichever
engine reads it.

## Readiness

What is ready to run now: the whole of section 0. Nothing in it needs SQE code,
a branch, or a decision.

What is blocked, and on what: everything in sections 1 to 8 waits on task 0.4.
If stock `sqlite3` refuses to write into a table whose declared composite
primary key has no `sqlite_autoindex_*`, this backend becomes open-only on
C-created catalogs, which removes the create path from section 3, the
create-from-nothing half of section 5, and the `mode=rwc` behavior from section
4. That is a smaller, still-useful change; it is not the change described here.

What would be nice and is not required: any of the upstream work. Tier 1 of the
upstream estimate (rows-affected count, `IF NOT EXISTS` codegen, composite-key
enforcement) removes three workarounds worth about two or three days of the
fifteen to twenty-four. Its value is correctness margin, not schedule.

## Sequencing

The engine adapter and the actor come before any `Catalog` method, because both gaps 2 and 3 are settled there. The `Catalog` impl then lands read-only (list, exists, load) before write (create, drop, rename, update), so the interop test can run against a C-backed warehouse before SQE writes to one.

## The risk this change carries

A wrong `update_table` loses a table pointer, and it does so quietly. Everything else here is either a compile error or a visibly failed query. That is why the conflict scenario is asserted on the returned error rather than on the rows, why the SELECT and UPDATE share a transaction, and why the differential suite runs the same 149 coordinator tests against both backends rather than trusting a new implementation of an old contract.
