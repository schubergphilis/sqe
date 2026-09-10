# sqlite-rs catalog backend tasks

> **Status 2026-09-10.** Scopes 1 and 2 shipped (!981 port, !982 default
> flip) on a vendored snapshot of Lab271's embedding-API branch
> (`feat/013-api-facade` @ `e788241`, PR #705) rather than a released
> version. Their facade (`sqlite_rs::api::Connection`, `Send + Sync` over an
> owned worker thread, rows-affected count, create-if-missing) made
> sections 1 and 2 below theirs, not ours; SQE's adapter is
> `crates/sqe-catalog/src/sqlite_rs_catalog.rs`. Items struck through
> below were made unnecessary by that; items left open are scope 3 (any
> SQLite file as a data source), the runtime engine setting, and the
> measurements.

## 0. Spike before committing to any of this

- [x] 0.1 Done 2026-09-10 the other way round: files `sqe-cli --embedded` created (sqlx engine) open under sqlite-rs, take writes, and read back under sqlx with stock `sqlite3` `integrity_check` = ok. Test `a_file_written_by_the_sqlx_path_roundtrips_through_sqlite_rs`.
- [x] 0.2 Done: the facade's API cost is one function (`replace_row`) for the UPDATE defect and nothing else; the port took a day, not weeks.
- [x] 0.3 Settled: `Connection::changes()` and the `u64` return of `execute_with` exist (013 Requirement 1). Task 3.4 stays a transaction anyway, because of the UPDATE binding defect below.
- [x] 0.4 **Settled 2026-08-28** (sqlite-rs 0.18.5, stock `sqlite3` 3.51.0). sqlite-rs maintains a NAMED index and ignores the implicit `sqlite_autoindex_*`. Writing into a stock-created table with a declared composite `PRIMARY KEY` corrupts it silently: `integrity_check` reports "row N missing from index" and `count(*)` answers from the stale index. Creating that same DDL with sqlite-rs produces a file stock `sqlite3` calls "malformed (11)" on any write. A table with NO declared PK and an explicit `CREATE UNIQUE INDEX` round-trips perfectly in both directions, with uniqueness enforced by both engines. **Superseded in part by upstream #685 (merged 2026-09):** an existing autoindex is now recovered and maintained, so the adoption half is fixed; the creation half (#687) still stands and 3.1b is how SQE avoids it.
- [x] 0.5 (added) Four further engine defects found 2026-09-10 by running the catalog's statements through the facade, each pinned by a test in the module: `UPDATE` binds only its first parameter; a covering-index `SELECT` on an autoindexed table miscounts parameters; `rowid` is not selectable; `sqlite_master` is not queryable. Workarounds in the module header; reported to Lab271.

## 1. Engine adapter (one file, one blast radius)

- [x] 1.1 Feature `sql-sqlite-rs` on `sqe-catalog`. Pinned to a vendored snapshot (`vendor/sqlite-rs`, commit in its README) rather than a version: the crate is `publish = false` and its registry is not reachable from CI.
- [x] 1.2 ~~`sqlite_rs_engine.rs` over `DatabaseHeader`/`Program`~~ Replaced by upstream's `sqlite_rs::api` (spec 013): open/create, `execute_with`/`query_all_with` with `Vec<Value>`. Only `sqlite_rs_catalog.rs` names a `sqlite_rs::` type.
- [x] 1.3 ~~Compiled-`Program` cache~~ Upstream `Statement` exists; not used, because compile cost is unmeasurable at commit frequency and a prepared statement would also hit the UPDATE defect.
- [x] 1.4 Error mapping: `sqlite_rs::api::Error` -> `iceberg::Error` with `is_retryable()` (Busy) carried as `retryable`, extended result code in the message.
- [x] 1.5 Test: `fresh_file_gets_named_unique_indexes_and_a_valid_file` creates from nothing and asserts stock `sqlite3` reads the schema and `integrity_check` = ok.

## 2. Actor boundary

- [x] 2.1–2.3 ~~SQE-owned actor~~ Upstream's `Connection` is exactly this (013 Requirement 4: `Send + Sync + Clone`, one worker thread, joined when the last clone drops). SQE wraps it in `Arc` and a process-wide `Weak` registry.
- [x] 2.4 Covered by every async test (multi-thread tokio, `spawn_blocking` per call) and by the coordinator/sqe-write rigs.
- [x] 2.5 Thread lifetime: registry holds `Weak`, so the worker ends with the last catalog on a path; covered by `namespaces_and_tables_roundtrip_and_persist_across_reopen` (drop, reopen, fresh worker). No thread-count assertion; upstream's `worker_thread_joins_on_drop` test owns that.
- [ ] 2.6 Dead-worker error path: upstream returns `Error::ConnectionClosed`; no SQE test forces it.

## 3. `Catalog` implementation, read path first

- [x] 3.1 Schema bootstrap creates only what is absent (via `Connection::table_names()`, since `sqlite_master` is not queryable), no `IF NOT EXISTS`, no declared composite `PRIMARY KEY`.
- [x] 3.1b Named `CREATE UNIQUE INDEX` (`iceberg_tables_pk`, `iceberg_namespace_properties_pk`) on fresh files, table and index in one transaction. Files the sqlx engine created keep their autoindexes and are left as found.
- [x] 3.1c Duplicate `create_table` is rejected on both a fresh file (named index) and an adopted file (recovered autoindex, code 2067).
- [x] 3.2 Read methods ported. The `UNION` became two selects plus a set (same result); `list_tables` projects an unread non-index column (defect 0.5).
- [x] 3.3 Write methods ported. Every UPDATE is DELETE + INSERT of the key under `BEGIN IMMEDIATE` (`replace_row`).
- [x] 3.4 `update_table` = read under `BEGIN IMMEDIATE`, compare the stored pointer, replace, commit; mismatch -> `CatalogCommitConflicts` with `retryable(true)`.
- [x] 3.5 Test `replace_row_refuses_when_the_stored_pointer_moved` (unit): stale pointer refused with the conflict error, row untouched, then the current pointer swaps and `previous_metadata_location` is kept. (A catalog-level "stale handle" test is not valid: iceberg's `Transaction::commit` reloads inside `update_table`.)
- [ ] 3.6 Interrupt between compare and write: relies on `Transaction`'s drop-rollback; no SQE test kills mid-transaction.

## 4. Wiring, scope 1 (the embedded catalog)

- [x] 4.1 `TYPE sqlite` in `mount.rs` resolves to sqlite-rs when `sql-sqlite-rs` is compiled in, else sqlx; no new `CatalogKind`, no new ATTACH type.
- [ ] 4.2 `[catalog] sqlite_engine = "rust" | "c"` runtime setting. Not built: selection is by cargo feature (`sqe-cli`: `sqlite-rs` default, `sqlite-c` fallback, exactly one required). Revisit only if one binary must carry both.
- [x] 4.3 Embedded CLI: `open_embedded_sqlite_catalog` picks the engine; `~/.sqe/warehouse/sqe.db` is created on first use.
- [x] 4.4 Persistence across processes: `sqe-cli` embedded suite 54/54 under both engines; manual two-process run on a fresh and an adopted warehouse 2026-09-10.

## 4b. Wiring, scope 2 (attached catalogs)

- [x] 4b.1 Process-wide registry keyed on the canonical path, `Weak<Connection>`; two opens of one file share one worker (`shares_connection_with`).
- [x] 4b.2 Canonicalized before keying (`canonical_db_path`, parent canonicalized when the file does not exist yet). Test `one_connection_per_file_across_path_spellings`.
- [x] 4b.3 Lifecycle is `Arc` refcounting; the worker joins when the last `Arc<Connection>` drops.
- [x] 4b.4 `DETACH` drops the catalog handle, which decrements; no parser change.
- [x] 4b.5 `--catalog NAME=PATH` goes through the same opener. `[catalog.backend] type = "jdbc"` with a `sqlite:` URL routes to sqlite-rs (`build_backend_catalog` guarded arm); `TYPE jdbc` likewise (`build_jdbc`).
- [ ] 4b.6 ATTACH the same file under two names, write through one, read through the other, under `integrity_check`. Registry behaviour is covered at the catalog level; the SQL-level test is not written.
- [ ] 4b.7 Three catalogs attached, cross-catalog join, rows identical to the C engine.
- [ ] 4b.8 DETACH one of two attachments leaves the engine alive; last release frees it.
- [ ] 4b.9 `SHOW CATALOGS` and the registry agree after attach/detach.

## 4c. Scope 3: any SQLite database as a data source

- [ ] 4c.1–4c.9 Not started. Upstream's streaming `Rows` (013 Requirement 7, ADR-0040) removes the row-ceiling concern in 4c.6; the type-mapping and mode-detection decisions still stand as written.

## 4d. Fail closed on a foreign catalog

- [x] 4d.1–4d.4 **Direction reversed by upstream #685.** A stock-created catalog with `sqlite_autoindex_*` is no longer foreign: sqlite-rs recovers and maintains the autoindex, uniqueness is enforced through it, and writes are safe. SQE therefore adopts such files read-write and does not add a second index to them. Test `a_file_written_by_the_sqlx_path_roundtrips_through_sqlite_rs`.
- [x] 4d.5 Every write test asserts stock `sqlite3` `integrity_check` = ok afterwards (`assert_oracle_ok`).

## 5. Interop and parity, which is where this change is won or lost

- [x] 5.1 C-engine create and populate, Rust-engine load and commit, C-engine read back: one test, in that order.
- [x] 5.2 Rust engine creates from nothing; stock `sqlite3` lists the schema and rows (oracle assertions in the fresh-file test).
- [x] 5.3 `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite -- maintenance::` 36/36 and `-p sqe-write --features test-sqlite -- write_handler::` 115/115 on sqlite-rs (the rigs' `sqlite://` JDBC URL now resolves to it).
- [x] 5.4 `sqe-cli` embedded suite 54/54 under `sqlite-rs` and under `sqlite-c`; `mount_sqlite_test` 5/5 under the default engine.
- [x] 5.5 Dependency win: `cargo tree -i libsqlite3-sys` and `-i sqlx` match nothing for sqe-cli, sqe-coordinator, sqe-worker; the default `sqe-cli` binary has zero `sqlite3_*` symbols. Container build without a C compiler not yet tried (aws-lc-sys still needs one for TLS).

## 6. Measure, then decide the default

- [ ] 6.1 Catalog-operation latency, both engines.
- [ ] 6.2 N sessions through one worker thread vs the pooled C engine.
- [ ] 6.3 Cross-catalog planning fan-out.
- [x] 6.4 Default decided without 6.1–6.3, on correctness and the sovereignty argument rather than numbers: sqlite-rs is the default since !982, with `sqlite-c` as the fallback. Revisit if 6.1–6.3 show a problem.

## 7. Upstream, and the part that helps regardless

- [x] 7.1 Lab271 added SQE's catalog statements as a consumer fixture family (their `test/013-sqe-consumer-corpus`); sample `sqe.db` files and first-run parameter values sent 2026-09-10 for their Requirement 7 workload test.
- [x] 7.2 Autoindex corruption filed; fixed upstream as #685. #687 (creating the autoindex) and #697 (`IF NOT EXISTS` ignored) remain open.
- [x] 7.2b Gaps became spec 013 (rows-affected, create API, streaming rows, named-parameter rejection) and are implemented on the facade branch. The four defects of 0.5 are the new list, sent 2026-09-10.
- [x] 7.3 Spec landed as an amendment to their 013 (Requirement 5 error type + ADR-0043), not as a separate 012.
- [ ] 7.4 Tier-1 order answer pending: #687, then #697, then the UPDATE binding defect.

## 8. Docs and project state

- [x] 8.1 `docs/site/book/src/quickstart/embedded-sqlite-catalog.md`: engine table, interop guarantee, one connection per file. `operations/catalogs.md`: feature list for `TYPE jdbc`.
- [ ] 8.1b Scope 3 docs: not applicable until 4c exists.
- [x] 8.2 `README.md` embedded paragraph, `nextsteps.md` status blocks (a)(b)(c) of 2026-09-10. `CHANGELOG.md` entry: at release time.
- [x] 8.3 Evaluation doc has the "2026-09-10: ported, and what the port found" section, including the two stale statements it corrects.
