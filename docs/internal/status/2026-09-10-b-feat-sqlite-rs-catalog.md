**STATUS 2026-09-10 (b): the embedded SQLite catalog runs on pure-Rust sqlite-rs behind `sql-sqlite-rs` (`feat/sqlite-rs-catalog`); sqlx stays the fallback.**

`vendor/sqlite-rs/` is a library-only snapshot of Lab271's facade branch
(`feat/013-api-facade` @ `e788241`, PR #705; zero runtime deps, `src/bin`
dropped, SQE's own Cargo.toml). Vendored rather than a git dependency
because cargo fetches optional git deps at resolve time and the repo is
private to the runners. `sqe_catalog::sqlite_rs_catalog::SqliteRsCatalog`
implements `iceberg::Catalog` over `sqlite_rs::api::Connection`, same
two-table schema and statements as the vendored sqlx catalog, every call
on `spawn_blocking`. Rules from Lab271's measured in-process data loss:
one `Connection` per canonical path process-wide (`Weak` registry, no
pool), no `CREATE TABLE IF NOT EXISTS` (#697; `table_names()` probe),
named `UNIQUE` indexes instead of a declared composite PK on fresh files
(#687), and `Busy` -> retryable. Wired into `TYPE sqlite` (`mount.rs`,
preferred when compiled in) and sqe-cli `--embedded` (`sqlite-rs`
feature, default-off). Running the catalog's real statements through
the facade found **four engine defects**, each pinned by a test and
worked around in the module (docs header lists them): (1) an `UPDATE`
binds only its first parameter, so every update is DELETE + INSERT under
`BEGIN IMMEDIATE` (`replace_row`, also the commit CAS); (2) a
covering-index SELECT on an autoindexed table miscounts parameters
(`list_tables` projects an unread column); (3) `rowid` is not
selectable; (4) `sqlite_master` is not queryable. Verified: 7 catalog
tests (fresh file, reopen, CAS, one-connection, **sqlx-written file
adopted, written, read back by sqlx**, stock `sqlite3` integrity ok
throughout), sqe-cli embedded 54/54 under both engines, clippy clean
both configs, hakari no drift; end-to-end `sqe-cli --embedded` built
with `--features sqlite-rs` creates a warehouse and adopts a sqlx one.
Also shipped: `chore/drop-rusqlite` (!980). **NEXT:** send Lab271 the
four defects plus the sample `sqe.db` files; flip `sqlite-rs` on by
default once #705 merges and the defects close (re-vendor per
`vendor/sqlite-rs/README.md`); then drop `sql-sqlite` from sqe-cli's
default set so the C library leaves the embedded binary.
