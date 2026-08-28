## Why

SQE statically links a C library to store table pointers. `libsqlite3-sys 0.37` reaches the build through two parents (`cargo tree -p sqe-catalog --features sql-sqlite -i libsqlite3-sys`): `sqlx-sqlite 0.9`, which the vendored `iceberg-catalog-sql` uses, and `rusqlite 0.39`, which has **zero call sites** anywhere in the workspace and exists only because it is the crate that requests `features = ["bundled"]` (`crates/sqe-catalog/Cargo.toml:164`). Both declare `links = "sqlite3"`, which cargo permits only once, and that collision is what pins rusqlite to 0.39 and blocks the bump in #389.

The cost is not theoretical. `cargo install sqe-cli` needs a C toolchain to compile the amalgamation, the CVE surface of the catalog path is C, and the least sovereign component in the embedded binary is the thing holding the catalog. SQE argues for inspectable, replaceable stacks; this is the part of its own stack that is neither.

`sqlite-rs` (Lab271, Apache-2.0, `#![forbid(unsafe_code)]` outside a vendored POSIX-bindings carve-out, **zero runtime dependencies**) is a byte-compatible SQLite reimplementation with V1 through V7 complete: read core, CRUD, joins and aggregates, transactions, WAL, polish. Its verification method is byte-level diff against a pinned `sqlite3` 3.53.4 oracle plus proptest, fuzzing and mutation, the same discipline SQE uses for the Iceberg matrix and Trino parity.

The first read of that project concluded it was not consumable, because it exposes no `sqlx` driver, no rusqlite-compatible API and no C ABI. Reading its `examples/` corrected that. **Every primitive SQE needs is already public**, and the remaining distance is glue SQE can write:

```rust
let (header, pager) = dump::open(&UnixVfs, &path)?;                        // open
let schemas = read_schema(&mut TableCursor::new(&pager, &header, 1), enc)?; // schema
let program = compile_select_with_catalog(&select, &table, &schemas)?;      // compile once
let rows = execute_with_db_and_params(&program, source, header, params)?;   // bind many
let (rows, autocommit) = execute_transaction_step(&program, pager, header, autocommit)?;
```

## Three scopes, one engine

`TYPE sqlite` stays the user-facing name in all three. The engine behind it is a build and config choice, not a new catalog kind: nobody should have to know which SQLite implementation SQE linked in order to attach a file.

**Scope 1, the embedded catalog.** `sqe-cli --embedded` attaches one catalog named `iceberg` at `~/.sqe/warehouse/` (`WarehouseMode::default_persistent`, `crates/sqe-cli/src/embedded.rs:89`), opened as `sqlite://<path>/sqe.db?mode=rwc`. One file holding Iceberg metadata pointers, created on first use, persisting across restarts. This is where the C-toolchain requirement actually hurts.

**Scope 2, attached catalogs.** `ATTACH '<path>' AS name (TYPE sqlite)`, `--catalog NAME=PATH`, `[catalogs.*]`, several at once, queried and joined across three-part names. The registry holding them is process-local and keyed by name (`crates/sqe-coordinator/src/runtime_catalog.rs`), each entry one `Arc<dyn iceberg::Catalog>` cloned into every session context, so an attach is process-wide rather than per-session.

Scope 2 is not a bigger scope 1. It exposes a constraint scope 1 hides: two attachments can name the same file, and POSIX `fcntl` locks are owned by `(process, inode)`, so two engine instances inside one process do not exclude each other. The answer is a per-file engine registry rather than a per-mount one.

**Scope 3, any SQLite database.** Attaching a file that does not hold an Iceberg catalog exposes its own tables as queryable relations, joinable against Iceberg tables in the same query. This is the DuckDB `sqlite_scanner` shape, and it is the scope that makes "any SQLite db should work" true rather than "any SQLite db SQE wrote".

Scope 3 changes the risk profile, and the design says so plainly. Scopes 1 and 2 issue about a dozen known statements against a two-table schema SQE controls; scope 3 reads arbitrary schemas with arbitrary column affinities, so the engine's read coverage and its type semantics become load-bearing where before they were not. It also hits a gap the catalog scopes never feel: `execute_with_db_and_params` returns `Vec<Vec<Value>>`, fully materialized, and there is no incremental row API in `src/vdbe/`. Fine for a dozen rows, wrong for a million.

## What the spike already settled

Task 0.4 ran on 2026-08-28 against sqlite-rs 0.18.5 and stock `sqlite3` 3.51.0, before any implementation, and it narrowed the change:

**sqlite-rs maintains a named index and ignores the implicit `sqlite_autoindex_*`.** Writing into a stock-created table with a declared composite `PRIMARY KEY` leaves rows out of that index, after which stock SQLite undercounts and `PRAGMA integrity_check` reports "row N missing from index". Creating the same DDL with sqlite-rs yields a file stock SQLite calls "malformed (11)" on any write. A table with no declared PK and an explicit `CREATE UNIQUE INDEX` round-trips cleanly in both directions with uniqueness enforced by both engines.

So the change gains three constraints and loses one assumption:

- SQE's catalog schema drops the declared composite `PRIMARY KEY` and uses a named unique index instead, for both engines, so one schema serves both.
- The Rust engine treats a foreign catalog (one carrying an `sqlite_autoindex_*`) as read-only and refuses writes, naming `sqlite_engine = "c"` as the fix. Writing anyway corrupts it silently.
- Switching an existing warehouse to the Rust engine is a migration, not a config flip, because the unmaintained autoindex cannot be repaired by adding an index. New warehouses get the Rust engine; existing ones keep the C engine until upstream maintains autoindexes.

The assumption lost: "both engines read and write one file format" is true only for the schema shape SQE controls. Rollback stays a config flip; adoption does not.

Scope 3 is untouched by all of this, because it is read-only. That makes it the safest part of the change and a candidate to ship first.

## What Changes

- A pure-Rust engine behind `TYPE sqlite`, selected by the `sql-sqlite-rs` feature plus `[catalog] sqlite_engine = "rust" | "c"`. No new `CatalogKind`, no new ATTACH type. Both engines can be compiled in, and the config picks; with only one compiled in, that one is used.
- Scopes 1 and 2: iceberg's `Catalog` trait (14 async methods) implemented directly over the same `iceberg_tables` / `iceberg_namespace_properties` columns `iceberg-catalog-sql` uses, with one deliberate difference forced by the spike: a named unique index in place of the declared composite `PRIMARY KEY`. Iceberg clients query rows rather than inspect constraints, so Spark's `JdbcCatalog` and pyiceberg's `SqlCatalog` read and write it unchanged; their own `CREATE TABLE IF NOT EXISTS` is a no-op against an existing table.
- An actor boundary: one OS thread owns the `!Send` engine state and a compiled-`Program` cache; the handle is a channel sender, `Send + Sync + Debug` as the trait requires.
- A process-wide engine registry keyed on the canonicalized file path, refcounted, so N attachments of one file share one engine and one thread.
- Optimistic concurrency without a changes count: the metadata-pointer compare-and-swap becomes SELECT-then-UPDATE inside one transaction, still returning `CatalogCommitConflicts` with `retryable(true)`.
- Scope 3: mode detection at attach time (Iceberg catalog schema present or not, overridable with an explicit option), a read-only `TableProvider` over the file's tables in a `main` schema, affinity-based type mapping, and projection, filter and limit pushdown into generated SQL.
- A pinned exact version of `sqlite-rs` and a single adapter module, because that project is pre-1.0 and states minor bumps may break its public API.

This change does **not** replace `[catalog] type = "sql"` for Postgres and MySQL: those keep the sqlx path, and sqlx without its `sqlite` feature does not pull `libsqlite3-sys`.

## Capabilities

### New Capabilities
- `sqlite-rs-catalog`: the engine selection, the actor boundary, the per-file registry, the `Catalog` implementation over the shared schema, the transactional compare-and-swap, and the interop contract with the C engine.
- `sqlite-data-source`: attaching a non-Iceberg SQLite database and querying its tables (scope 3), including mode detection, type mapping and pushdown.

### Modified Capabilities
- `pluggable-catalogs`: `TYPE sqlite` gains an engine choice and a data-source mode. The type name and its options are otherwise unchanged.

## Success Criteria

- Every test that passes against the C engine passes against the Rust engine: `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite` (149 tests) and the `sqe-cli` embedded and `ATTACH` suites, run once per engine.
- **Interop proven in both directions** on SQE's own schema shape, asserted on the file rather than on our own reads: a warehouse created by one engine is readable and writable by the other, and `integrity_check` under stock `sqlite3` is clean after every Rust-engine write.
- **A foreign catalog is refused, not corrupted:** opening a sqlx/Spark/pyiceberg-created warehouse with the Rust engine allows reads and refuses writes with an actionable error.
- **Scope 1:** a warehouse created on first use survives a restart, and a second process reads what the first committed.
- **Scope 2:** three catalogs attached at once, a join across two returns the C engine's rows, `DETACH` releases the engine and its thread, and two attachments of one file share a single engine rather than racing.
- **Scope 3:** an arbitrary SQLite file created by stock `sqlite3` attaches, its tables are listed and queried, a join against an Iceberg table returns correct rows, and every declared column type maps to a documented Arrow type or a documented refusal.
- A concurrent-commit conflict returns `CatalogCommitConflicts` with `retryable(true)`, not a lost update. This is the one failure that silently loses a table.
- With the C engine out of the feature set, `cargo tree -i libsqlite3-sys` finds nothing and `sqe-cli` builds with no C toolchain.
- The catalog statement corpus is contributed upstream as a fixture family, so their oracle covers our workload.

## Rollback

`[catalog] sqlite_engine = "c"` returns every catalog to the C engine with no data migration, because both read and write the same schema in the same file format. Scope 3 is additive: nothing existing depends on it, and dropping the feature removes the capability without touching the catalog path. Rollback is a config flip, and the interop test is what proves it.
