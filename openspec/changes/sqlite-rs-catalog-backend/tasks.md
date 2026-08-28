# sqlite-rs catalog backend tasks

## 0. Spike before committing to any of this

- [ ] 0.1 Point `sqlite-rs query` and `sqlite-rs dump` at a warehouse `sqe-cli --embedded` created, and diff against `sqlite3`. Zero SQE code. If the file does not round-trip, nothing below matters.
- [ ] 0.2 Wire the actor plus three operations (`create_namespace`, `create_table`, `load_table`) against that file in a throwaway binary. The question this answers is not "does it work" but "how much does their internals-shaped API cost to call", which decides whether tasks 1 to 6 are two weeks or six.
- [ ] 0.3 Confirm the rows-affected gap is real by asking upstream, not by grepping. If `changes()` exists or lands, task 3.4 simplifies to a single conditional `UPDATE`.
- [x] 0.4 **Settled 2026-08-28** (sqlite-rs 0.18.5, stock `sqlite3` 3.51.0). sqlite-rs maintains a NAMED index and ignores the implicit `sqlite_autoindex_*`. Writing into a stock-created table with a declared composite `PRIMARY KEY` corrupts it silently: `integrity_check` reports "row N missing from index" and `count(*)` answers from the stale index. Creating that same DDL with sqlite-rs produces a file stock `sqlite3` calls "malformed (11)" on any write. A table with NO declared PK and an explicit `CREATE UNIQUE INDEX` round-trips perfectly in both directions, with uniqueness enforced by both engines. Consequences are in the design; tasks 3.1, 3.1b and 4d below carry them.

## 1. Engine adapter (one file, one blast radius)

- [ ] 1.1 New crate feature `sql-sqlite-rs` on `sqe-catalog`, `sqlite-rs` pinned to an exact version (`=0.18.5`), never a caret range. Pre-1.0 minors break their public API by policy.
- [ ] 1.2 `sqlite_rs_engine.rs`: open, create-if-missing via `DatabaseHeader::new_empty_page1`, and a `query`/`execute` pair taking SQL plus `Vec<Value>` and returning SQE's own row type. Nothing outside this file may name a `sqlite_rs::` type.
- [ ] 1.3 Compiled-`Program` cache keyed on the SQL string, so the catalog's fixed statement set compiles once per catalog rather than per call.
- [ ] 1.4 Map `ExecError` / `VfsError` onto SQE errors, with `VfsError::Locked` becoming a retryable variant. A busy file under WAL is an expected outcome, not an error to log.
- [ ] 1.5 Test: create a database from nothing, and assert stock `sqlite3` opens it and reports an empty schema. Our own reader agreeing with our own writer proves nothing.

## 2. Actor boundary

- [ ] 2.1 Request enum, `mpsc` inbound, `oneshot` per reply, one OS thread per catalog instance owning `Rc<RefCell<Pager>>` and the header.
- [ ] 2.2 Handle type that is `Send + Sync + Debug` (the `Catalog` trait's bound) and cheap to clone per session.
- [ ] 2.3 Join the thread on handle drop. `ATTACH` mounts are per-session, so a leak here is a thread leak under session churn, not a one-off.
- [ ] 2.4 Test: the handle is used from several tokio tasks on a multi-threaded runtime and every operation succeeds.
- [ ] 2.5 Test: attach and detach in a loop, and assert the process thread count does not grow.
- [ ] 2.6 Test: kill the engine thread, then issue an operation, and assert it returns an error rather than blocking on a reply that can never arrive.

## 3. `Catalog` implementation, read path first

- [ ] 3.1 Schema bootstrap: read the schema first and create only what is absent, because `IF NOT EXISTS` parses and is then ignored. Columns identical to `iceberg-catalog-sql`'s, and **no declared composite `PRIMARY KEY`**. Per task 0.4 the declared constraint is what makes the file malformed to stock SQLite, so declaring it "for DDL compatibility" is the one thing not to do.
- [ ] 3.1b Create a named `CREATE UNIQUE INDEX` on those columns instead, for both engines, so one schema serves both. Measured: a named index is maintained and enforced by sqlite-rs (code 2067) and by stock `sqlite3` (code 19), and `integrity_check` stays clean after either writes.
- [ ] 3.1c Test: a second `create_table` for the same identifier is rejected. Without 3.1b this test passes silently against the C backend and fails only against `sqlite_rs`, which is the whole reason it runs against both.
- [ ] 3.2 Read methods: `list_namespaces`, `get_namespace`, `namespace_exists`, `list_tables`, `table_exists`, `load_table`. The `UNION` in the namespace listing and the `LIMIT 1` probes are the shapes to port exactly.
- [ ] 3.3 Write methods: `create_namespace`, `update_namespace`, `drop_namespace`, `create_table`, `drop_table`, `rename_table`, `register_table`, `update_table`.
- [ ] 3.4 `update_table` as SELECT-then-UPDATE inside ONE transaction, returning `CatalogCommitConflicts` with `retryable(true)` when the stored pointer moved. Not two statements outside a transaction; that is a lost update wearing a correct-looking result.
- [ ] 3.5 Test: a stale commit is refused and the stored pointer is still the winner's. Assert on the error, never on the row count.
- [ ] 3.6 Test: interrupt between the compare and the write, and assert the on-disk pointer is pre-commit.

## 4. Wiring, scope 1 (the embedded catalog)

- [ ] 4.1 Engine selection in `mount.rs`: `TYPE sqlite` resolves to the Rust or C engine from `[catalog] sqlite_engine` plus what the features compiled in. NO new `CatalogKind` and no new ATTACH type; a script must not have to name the engine.
- [ ] 4.2 `[catalog] sqlite_engine = "rust" | "c"` with validation against what is compiled in. An unavailable engine is a startup error, never a silent fallback to the other one.
- [ ] 4.3 Embedded CLI: the persistent `~/.sqe/warehouse` path can select it, created on first use.
- [ ] 4.4 Test: create on first use, write, restart the process, read back. Then a second process reads what the first committed, which is the inter-process half of spec 007's locks doing its job.

## 4b. Wiring, scope 2 (attached catalogs)

- [ ] 4b.1 Process-wide engine registry keyed on the CANONICALIZED file path, refcounted. Two attachments of one file must share one engine: `fcntl` locks are per `(process, inode)`, so two engines in one process are not excluded by them, and the failure is interleaved journal writes rather than a busy error.
- [ ] 4b.2 Canonicalize before keying, not after. A relative path, a symlink and a `..` segment can name one inode, and keying on the typed string reintroduces exactly the problem the registry exists to solve.
- [ ] 4b.3 Refcount lifecycle: the handle clones into every session context, so the join belongs to an inner `Arc`-held owner that shuts down and joins at zero. A handle-level join kills a live engine; no join leaks a thread per attach.
- [ ] 4b.4 `DETACH` decrements the per-file refcount rather than closing the engine. The ATTACH surface itself needs no parser change, since the type name is unchanged.
- [ ] 4b.5 `--catalog NAME=PATH` and `[catalogs.*]` honor the engine setting.
- [ ] 4b.6 Test: `ATTACH` the same file under two names, write through one, read through the other, and assert one engine and one thread exist. Without 4b.1 this test corrupts rather than fails, so run it under `integrity_check`.
- [ ] 4b.7 Test: three catalogs attached at once, a join across two of them, rows identical to the C backend.
- [ ] 4b.8 Test: `DETACH` one of two attachments of one file leaves the engine alive; detaching the last releases it, and re-attaching builds a fresh one.
- [ ] 4b.9 Test: `SHOW CATALOGS` and the registry agree after attach and detach, matching the existing backend's behavior.

## 4c. Scope 3: any SQLite database as a data source

- [ ] 4c.1 Mode detection at attach: catalog when `iceberg_tables` and `iceberg_namespace_properties` are both present, data otherwise, with `MODE catalog|data` as an explicit override so auto-detection is a convenience and not a trap.
- [ ] 4c.2 Read-only `TableProvider` over the file's tables, exposed as catalog `<name>`, schema `main`. `INSERT` is refused rather than approximated.
- [ ] 4c.3 Affinity-based type mapping: INTEGER to Int64, REAL to Float64, TEXT to Utf8, BLOB to Binary, declared-typeless to Utf8. Decide `NUMERIC`, `DATE` and `DATETIME` here, in the task, rather than discovering them at runtime.
- [ ] 4c.4 A value that does not fit its column's Arrow type errors naming table, column and row. SQLite affinity is not a constraint, so this case is normal input, not corruption, and a silent NULL or a silent cast is the wrong answer.
- [ ] 4c.5 Pushdown in order: projection, then `LIMIT`, then simple conjunctive filters on comparable types. Anything not translatable stays a DataFusion post-filter.
- [ ] 4c.6 Row ceiling: a configurable cap with a clear error when exceeded, because `execute_with_db_and_params` materializes the whole result as `Vec<Vec<Value>>` and there is no incremental API in `src/vdbe/`. Document the ceiling; lift it when the upstream streaming requirement lands.
- [ ] 4c.7 Test: a database created by stock `sqlite3` attaches, lists its tables, and returns identical rows for the same query run through `sqlite3` itself. This differential test is scope 3's gate, the way 0.4 is scopes 1 and 2's.
- [ ] 4c.8 Test: a join between a scope-3 table and an Iceberg table returns correct rows.
- [ ] 4c.9 Test: a column holding mixed storage classes produces the documented error rather than wrong values, and a pushed-down filter returns exactly what DataFusion's own filter would.

## 4d. Fail closed on a foreign catalog

- [ ] 4d.1 At open, read `sqlite_master` and detect `sqlite_autoindex_*` (or any declared composite `PRIMARY KEY`) on the catalog tables. Its presence means the file was created by an engine whose index this one will not maintain.
- [ ] 4d.2 With the Rust engine, a foreign catalog is READ-ONLY: reads are safe and stay allowed, writes are refused with an error that names the reason and points at `sqlite_engine = "c"`. Measured: writing anyway corrupts the file quietly, so this guard is the difference between a feature and an incident.
- [ ] 4d.3 No silent migration. Adding a named index to a foreign catalog does not repair it, because the unmaintained autoindex remains; switching an existing warehouse means recreating the two catalog tables, and SQE will not do that behind the user's back.
- [ ] 4d.4 Test: open a warehouse created through the C engine with the Rust engine, assert reads work and a write is refused with that error rather than succeeding.
- [ ] 4d.5 Test: after any Rust-engine write to a SQE-created warehouse, stock `sqlite3` reports `integrity_check` ok. This is the assertion that would have caught the corruption; run it in every write test, not once.

## 5. Interop and parity, which is where this change is won or lost

- [ ] 5.1 Test: C-engine create and populate, Rust-engine load and commit, C-engine read back. All three steps, in that order, in one test.
- [ ] 5.2 Test: the Rust engine creates from nothing, the C engine lists namespaces and tables, and the listings match row for row.
- [ ] 5.3 Run `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite` and the equivalent with `sql-sqlite-rs`. Same pass count, zero failures, both runs recorded.
- [ ] 5.4 Run the `sqe-cli` embedded and `ATTACH` suites against both, including persistence across a restart.
- [ ] 5.5 Assert the dependency win: with `sql-sqlite` and `rusqlite` out of the feature set, `cargo tree -i libsqlite3-sys` is empty, and `sqe-cli` builds in a container with no C compiler.

## 6. Measure, then decide the default

- [ ] 6.1 Catalog-operation latency, both backends, on the embedded warehouse: create table, load table, commit. The serialized actor is a throughput bet and this is the number that settles it.
- [ ] 6.2 Concurrency behavior under the coordinator: N sessions issuing metadata operations through one actor thread, against the pooled C backend as the reference.
- [ ] 6.3 Scope 2 specifically: a cross-catalog join whose planning issues metadata reads to several attached catalogs at once. Reads to different catalogs run on different threads; reads to the same catalog serialize, and this is the number that says whether that matters.
- [ ] 6.4 Only after 6.1 to 6.3: decide whether the Rust engine becomes the embedded default. Shipping it as opt-in with numbers beats shipping it as default without them.

## 7. Upstream, and the part that helps regardless

- [ ] 7.1 Contribute SQE's catalog statement corpus to sqlite-rs as a fixture family, so their pinned-oracle harness covers this workload. Highest value per hour in this whole change, and it pays off even if SQE never ships the backend.
- [ ] 7.2 **File the autoindex corruption first, as a bug rather than a gap.** With the reproduction from task 0.4: sqlite-rs writing into a valid stock-created table with a declared composite `PRIMARY KEY` leaves rows out of `sqlite_autoindex_*`, so stock SQLite then undercounts and `integrity_check` fails. It is a silent-corruption bug in a project whose headline claim is byte compatibility, and it is worth more to them than everything else on this list.
- [ ] 7.2b File the five gaps found: no incremental row API (the whole result set materializes as `Vec<Vec<Value>>`, which is what caps scope 3), no rows-affected count, no library API to create a database (their `examples/README.md` documents this one), `IF NOT EXISTS` ignored after parsing, and composite-key constraints neither enforced nor backed by an auto-created index (their `insert.rs` documents this one). The last two are SQL-semantics gaps owned by V3/V7 rather than by the embedding-API spec.
- [ ] 7.3 Offer `.openspec/specs/012-embedding-api/` (six requirements) and ADR-0033 as a PR against their repo. Two edits are needed for their tooling to see it, and they are theirs to make, not ours to assume: a row in `.openspec/README.md`'s Specs table and a value-block entry in `plan.md`. `tools/assurance.py` reads both.
- [ ] 7.4 Ask which of Tier 1 they intend to take, in order: composite-key enforcement (also fixes their own byte-compatibility claim), then the rows-affected count, then `IF NOT EXISTS`. SQE ships without all three; the answer decides which workarounds are permanent.

## 8. Docs and project state

- [ ] 8.1 `docs/site/book/src/getting-started/catalogs.md`: the engine setting, the interop guarantee, and that `TYPE sqlite` is unchanged for users.
- [ ] 8.1b `docs/site/book/src/getting-started/cli.md` and the SQL reference: scope 3, its read-only status, the type mapping table, and the row ceiling. A data source whose limits are undocumented gets discovered as a bug report.
- [ ] 8.2 `README.md` roadmap line, `nextsteps.md` status, `CHANGELOG.md` entry.
- [ ] 8.3 Note in `docs/internal/sqlite-rs-evaluation-2026-08-28.md` that the SQE-side path superseded the wait-for-a-driver conclusion, with the two facts that changed it (public bind API, public `new_empty_page1`).
