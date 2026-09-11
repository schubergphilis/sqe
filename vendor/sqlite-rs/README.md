# sqlite-rs (SQE vendored snapshot)

A library-only snapshot of [Lab271/sqlite-rs](https://github.com/Lab271/sqlite-rs),
a pure-Rust, `#![deny(unsafe_code)]` reimplementation of SQLite that reads and
writes the stock on-disk format. SQE uses it as the metadata store behind the
`sql-sqlite-rs` feature of `sqe-catalog` (`SqliteRsCatalog`), replacing the C
`libsqlite3` that `sqlx`'s SQLite driver links.

## Provenance

| | |
|---|---|
| Branch | `feat/013-api-facade` (PR #705, the spec-013 embedding API) |
| Commit | `0ef830d` (2026-09-11; previously `e788241`, 2026-09-09) |
| Crate version | 0.18.10 |
| Runtime dependencies | none |
| Snapshot contents | `src/` minus `src/bin/`, `LICENSE` |

The snapshot is taken from the facade *branch* rather than `main` because the
supported embedding surface (`sqlite_rs::api::{Connection, Statement, Rows,
Transaction, Error}`) lives there until #705 merges. Everything SQE calls is in
`src/api.rs`; the rest of the tree is the engine it sits on.

`Cargo.toml` here is SQE's, not upstream's: the CLI binary, test-helper
binaries, `[[test]]`/`[[bench]]` targets and dev-dependencies are dropped and
target auto-discovery is switched off. The `[lints]` tables are copied so the
snapshot compiles under the same rules as upstream.

## Why vendored

The crate is `publish = false`. Its documented consumption paths are a private
JFrog Cargo registry (upstream ADR-0038) or a git dependency; neither is
reachable from SQE's CI runners without new credentials, and cargo fetches
optional git dependencies at resolve time even when the feature that needs
them is off. A path dependency is the same pattern SQE already uses for
`vendor/iceberg-rust`.

## Snapshot history

- `0ef830d` (2026-09-11): `Connection::transaction()` now holds the
  connection for the `Transaction`'s lifetime; other threads' calls wait
  instead of nesting, and a second `transaction()` on the owning thread is
  `Error::TransactionActive` (upstream ADR-0045, spec 013 Requirement 4). A
  raw `execute("BEGIN")` is still unguarded. `SqliteRsCatalog` keeps its own
  per-connection mutex on top for one cycle.
- `e788241` (2026-09-09): first snapshot, the facade as merged into PR #705.

Not in this snapshot: the bare-`?` parameter-numbering fix (upstream ADR-0044,
branch `fix/bare-param-numbering-text-order`). It branches from `main`, has
no `src/api.rs`, and conflicts with the facade branch in
`src/codegen/expr/value.rs`, so it cannot be vendored until Lab271 lands one
on the other. Defects 1 and 2 below stay worked around until then.

## Known upstream gaps SQE codes around

These are tracked upstream; `SqliteRsCatalog` avoids them rather than waiting.

- **Bare `?` parameter numbering** (fixed upstream on a branch, see above):
  `UPDATE t SET v = ? WHERE k = ?` swaps its two values, and a covering-index
  `SELECT` over an autoindex restarts the parameter counter mid-statement.
  The catalog does every update as `DELETE` + `INSERT` under `BEGIN IMMEDIATE`
  and projects one non-indexed column in `list_tables`.
- **#707** `sqlite_master` is not reachable from `SELECT` (schema presence
  goes through `Connection::table_names()`); **#708** `rowid` is not
  selectable.
- **#697** `CREATE TABLE IF NOT EXISTS` ignores its guard and corrupts the
  file on the second run. The catalog reads `Connection::table_names()` and
  only issues a plain `CREATE TABLE` when the table is absent
  (`sqlite_master` itself is not queryable through the engine yet).
- **#687** a declared composite `PRIMARY KEY` on a table *created here* gets
  no `sqlite_autoindex_*`, so stock `sqlite3` calls the file malformed on the
  next write. Fresh catalog files use a named `CREATE UNIQUE INDEX` instead.
  Existing files created by the sqlx path keep their autoindexes; #685 (merged)
  makes writing through them safe.
- **In-process locking**: POSIX `fcntl` locks are per process, so two
  `Connection`s on the same file in one process do not lock against each other
  and can lose a commit silently. The catalog keeps exactly one `Connection`
  per canonical path, process-wide.

## Refresh recipe

```bash
cd /path/to/sqlite-rs && git fetch origin
rm -rf /path/to/sqlengine/vendor/sqlite-rs/{src,LICENSE}
git archive --format=tar origin/feat/013-api-facade src LICENSE \
  | tar -x -C /path/to/sqlengine/vendor/sqlite-rs
rm -rf /path/to/sqlengine/vendor/sqlite-rs/src/bin
# keep SQE's Cargo.toml and this README; update the commit in the table above
# and compare [package]/[lints] in upstream's Cargo.toml against ours
cargo check -p sqlite-rs && cargo test -p sqe-catalog --features sql-sqlite-rs
```

Once #705 merges, switch the branch above to `main` (or the release tag) and
re-run the recipe.
