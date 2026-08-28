# Replacing SQE's SQLite with sqlite-rs (Lab271): evaluation and gap analysis

Date: 2026-08-28. Subject: <https://github.com/Lab271/sqlite-rs>, v0.18.5, Apache-2.0.
Question asked: can we swap the SQLite library SQE uses for this pure-Rust reimplementation?

## Verdict

Not today, and the blocker is not SQL feature coverage. It is the integration
surface: SQE never calls SQLite directly, it calls `sqlx`, and sqlite-rs exposes
no `sqlx` driver, no rusqlite-compatible API, and no C ABI. The two projects are
one adapter layer apart, and that adapter needs two things sqlite-rs does not
have yet (bound parameters, a `Send` pager).

The direction is right and worth investing in. SQE argues sovereignty for a
living, and its own dependency tree currently statically links a C library for
catalog metadata. The recommendation below is a collaboration, not a swap.

## What SQE actually uses

Every SQLite access in SQE goes through `sqlx`, inside the vendored Iceberg SQL
catalog. There are **zero** `rusqlite::` call sites in the workspace or in
`vendor/`, verified by grep.

| Consumer | Path | How |
|---|---|---|
| JDBC-style Iceberg catalog | `crates/sqe-catalog/Cargo.toml:52,62` (`sql`, `sql-sqlite` features) | `sqlx` `AnyPool` via `iceberg-catalog-sql` |
| Embedded persistent warehouse | `crates/sqe-cli/src/embedded.rs:497` (`sqlite://<path>?mode=rwc`) | same |
| `ATTACH ... TYPE sqlite` | `crates/sqe-catalog/src/mount.rs:446` | same |
| Coordinator registry tests | `crates/sqe-coordinator/Cargo.toml:14` (`test-sqlite`) | same |

The vendored catalog pins `sqlx = { version = "0.9.0", features = ["any"] }`
(`vendor/iceberg-rust/crates/catalog/sql/Cargo.toml:39`) and issues, per
`vendor/iceberg-rust/crates/catalog/sql/src/catalog.rs`:

- `CREATE TABLE IF NOT EXISTS` with a three-column composite `PRIMARY KEY` (290, 304)
- `SELECT` with subqueries and set operations (395, 399)
- `INSERT`, `UPDATE`, `DELETE`, existence probes (459, 545, 608, 876, 947)
- all of it through `sqlx` **bound parameters** inside transactions, from a
  **connection pool**

### The rusqlite dependency is not dead weight

`rusqlite 0.39` with `features = ["bundled"]`
(`crates/sqe-catalog/Cargo.toml:164`) has no call sites, which looks removable.
It is not. `cargo tree -p sqe-catalog --features sql-sqlite -i libsqlite3-sys`
shows both `rusqlite 0.39.0` and `sqlx-sqlite 0.9.0` as parents of
`libsqlite3-sys 0.37.0`, and only rusqlite requests `bundled`. Cargo unifies
features per crate, so rusqlite is the lever that makes the whole graph link the
vendored C amalgamation instead of a system `libsqlite3`. Removing it without
replacing that lever changes build requirements on every machine.

Both crates also declare `links = "sqlite3"`, which cargo permits only once.
That is why the version pins are commented in two places and why a rusqlite bump
is blocked (issue #389). A pure-Rust engine would delete this entire class of
problem.

## What sqlite-rs is

Sources: repository README, `Cargo.toml`, `src/lib.rs`, `src/pager.rs`,
`src/bin/sqlite-rs/exec.rs`, GitHub contents API, all read 2026-08-28.

- A binary-compatible reimplementation of SQLite 3.x: same file format, same SQL
  dialect. **Zero runtime dependencies** (`[dependencies]` is empty; `proptest`,
  `criterion`, `rusqlite`, `md-5` are dev-only). `#![forbid(unsafe_code)]` with a
  single carve-out in `src/sys/` for vendored POSIX bindings.
- Phases V1 through V7 complete: read core, single-table queries, CRUD, joins
  and aggregates, transactions, WAL and CTEs, polish. V8 to V12 (virtual tables,
  JSON, FTS5) unscheduled. SQE needs none of V8 to V12.
- Verification is oracle-diff: byte-level comparison against a pinned stock
  `sqlite3` 3.53.4, plus property tests, fuzzing and mutation runs. The README's
  own framing is that "safety is not correctness". This is the same
  differential-testing discipline SQE uses for the Iceberg matrix and the Trino
  parity suites, and it is the main reason to take the project seriously.
- Public API is the engine, not a facade: `pub mod btree, codegen, dump, format,
  header, integrity, pager, parser, planner, record, schema, sys, vdbe, vfs`.
  There is no `Connection`, `Statement`, `db.rs` or `api.rs`. The shipped
  consumer is a CLI (`src/bin/sqlite-rs/`, commands `query`, `repl`, `dump`).
- The de-facto embedding path, read out of its own CLI (`exec.rs`):
  `dump::open(&UnixVfs, path) -> (header, pager)`, then per statement
  `read_schema_and_views(...)`, `compile_statement(&stmt, schemas, views) ->
  Program`, `execute_transaction_step(&program, Rc::clone(&pager), header,
  autocommit)`.

## The three gaps

### 1. No consumable API (blocking, and the real one)

SQE consumes SQLite only as a `sqlx` driver. Closing this needs one of:

- **a `sqlx` driver for sqlite-rs**: implement `Database`, `Connection`, `Row`,
  `Column`, `Value`, `TypeInfo`, `Arguments` and the transaction manager. This is
  the smallest change to SQE (a feature flag) and the largest change outside it.
  It also depends on gaps 2 and 3.
- **a rusqlite-shaped facade plus a data-layer fork**: rewrite the vendored
  catalog's access layer off `sqlx`. In this repo that means carrying the fork
  through every `vendor/iceberg-rust` rebase, which is already the most
  expensive maintenance line we have. Priced honestly, this is worse than it
  looks.
- **a C ABI shim** exporting the `sqlite3_*` symbols so `libsqlite3-sys` links
  against sqlite-rs. Maximum compatibility, and it reintroduces exactly the
  `unsafe` boundary the project exists to remove.

A fourth option avoids the adapter entirely: a **new `Catalog` implementation**
in SQE that bypasses `iceberg-catalog-sql`, owns its own schema and SQL, and
talks to sqlite-rs directly. Only viable for the embedded warehouse (see
staging), and it is new code SQE maintains, not a swap.

### 2. Bound parameters / prepared statements (blocking, partly unconfirmed)

`sqlx`'s entire surface is `query(...).bind(...)`, and the vendored catalog uses
it throughout. Nothing in the paths read (`lib.rs` module list, `exec.rs`,
README) exposes a bind or prepare API; `compile_statement` takes a parsed
statement and `run_exec` takes a SQL string.

**Unconfirmed:** whether the VDBE already has a bind-parameter opcode with no
public surface. That is a question for Lab271, not a conclusion. What is certain
is that no caller-facing binding exists on the documented paths, and without it
a consumer must inline literals, which puts escaping correctness on the
consumer for metadata that includes user-supplied namespace and table names.

### 3. Send/Sync, not file locking (blocking for the coordinator only)

This one is narrower than expected, and in a good way. **Multi-process locking
is implemented and clearly reasoned:** `Pager::open` takes a journal-mode SHARED
lock and a WAL reader-mark lock, escalates to EXCLUSIVE before touching the
journal or main file (the doc comment names the interleaving race this prevents),
and `claim_wal_write_lock` refuses a second concurrent writer with a `Locked`
error rather than racing the `mxFrame` publish.

What is missing is intra-process sharing. `Pager` is `Rc`/`RefCell`-based
(`page_cache: RefCell<PageCache>`, shared as `Rc<RefCell<Pager>>`), with no
`Send`/`Sync` impls. A `sqlx` pool hands connections across tokio tasks in a
multi-threaded coordinator, so the pool route is blocked at the pager type. A
single-threaded or thread-confined embedded use is not.

## What SQE would gain

- **No C toolchain for `cargo install sqe-cli`.** The bundled amalgamation is
  the only C compilation in the embedded build.
- **The `links = "sqlite3"` collision disappears**, unblocking #389 and removing
  a pin that currently constrains two unrelated crates.
- **No C CVE surface** in the catalog path, and no `unsafe` outside a
  POSIX-bindings carve-out.
- **The sovereignty argument applied inward.** SQE's pitch is that the stack
  should be inspectable and replaceable; the catalog metadata store is currently
  the least sovereign part of the embedded binary.

Against that: this is metadata, not the data path. Low QPS, so no performance
argument either way (their benchmark claim of 10x on point lookups is irrelevant
here). But a correctness bug loses table pointers, which is the whole catalog.
Risk is low-probability, high-consequence, which argues for a differential
harness rather than a cutover.

## Staged path

**Stage 0, no code (hours).** Can sqlite-rs even read a SQE warehouse? Create
one with `sqe-cli --embedded`, then run `sqlite-rs query` and `sqlite-rs dump`
against `~/.sqe/warehouse/sqe.db` and diff against `sqlite3`. This exercises the
composite-primary-key DDL and the catalog's actual rows with zero integration
work, and it answers the file-format compatibility question before anyone writes
an adapter.

**Stage 1, the highest-value action (days, and it is theirs not ours).**
Contribute SQE's Iceberg catalog statement corpus to their oracle suite: the
exact DDL, inserts, existence probes and transactional updates from
`vendor/iceberg-rust/crates/catalog/sql/src/catalog.rs`. Their methodology is
already oracle-diff against pinned `sqlite3`; adding a real workload makes their
coverage match our need without SQE taking a dependency. Cheapest way to move
the verdict.

**Stage 2, bounded integration (weeks).** The embedded warehouse only, via a new
`Catalog` impl that does not go through `iceberg-catalog-sql`. Single process,
single thread, SQE owns the SQL, so gaps 2 and 3 are avoidable rather than
blocking. This is also the deployment where the C-toolchain win actually lands.
It is new code with its own correctness burden; do it only if Stage 1 shows the
oracle corpus passing.

**Stage 3, the real swap (months, gated on upstream).** A `sqlx` driver, once
sqlite-rs has bound parameters and a `Send` pager. Then SQE's change is a
feature flag and the existing suites do the rest.

## The harness already exists

Whatever stage runs, SQE can A/B it against the C backend with tests it already
has:

```bash
RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite
cargo test -p sqe-cli                       # embedded warehouse + ATTACH suites
```

That is the strongest reason to treat this as evaluable rather than speculative:
the oracle is not hypothetical, it is a green suite today.

## What would change the verdict

1. A `sqlx` driver, or a public `Connection`-shaped API with bound parameters.
2. A `Send` pager (`Arc` plus interior locking) so a pool can hold connections.
3. Stage 0 and Stage 1 passing on SQE's own catalog corpus.
4. Confirmation on the unconfirmed items: bind-parameter support in the VDBE,
   `CREATE INDEX` / `ALTER TABLE` / foreign-key coverage (the phase table does
   not enumerate them), and whether a non-POSIX VFS is in scope, since
   `UnixVfs` and the `src/sys` carve-out are POSIX-only.

## Sources

SQE side: `crates/sqe-catalog/Cargo.toml:52,62,164`,
`vendor/iceberg-rust/crates/catalog/sql/Cargo.toml:39`,
`vendor/iceberg-rust/crates/catalog/sql/src/catalog.rs:290,304,395,459,876`,
`crates/sqe-cli/src/embedded.rs:497`, `crates/sqe-catalog/src/mount.rs:446`,
`crates/sqe-coordinator/Cargo.toml:14`, `cargo tree` output quoted above.

sqlite-rs side, read 2026-08-28: repository README, `Cargo.toml`, `src/lib.rs`,
`src/pager.rs`, `src/bin/sqlite-rs/exec.rs`, GitHub contents API for `src/` and
`src/bin/sqlite-rs/`. Items marked unconfirmed were not verifiable from those
files and should be asked rather than assumed.
