## 1. Quack Protocol Research and Wire Codec

- [x] 1.1 Read `duckdb/duckdb-quack` source (MIT, v1.5-variegata) and document the wire protocol in `docs/quack-protocol.md`: endpoint, content type, message schema, version constants, auth flow
- [x] 1.2 Confirm encoding: DuckDB `BinarySerializer` with `SerializationCompatibility::FromIndex(7)`, NOT bincode/msgpack. Result chunks use DuckDB native `DataChunk` (not Arrow IPC)
- [x] 1.3 Decide serialisation strategy: link `duckdb-rs` (bundled feature) in `sqe-quack-wire` to reuse DuckDB's own serialiser. Rationale captured in design.md
- [ ] 1.4 Create `crates/sqe-quack-wire/` with `Cargo.toml` (depends on `duckdb` with `bundled` feature), `lib.rs`, message enum mirroring `quack_message.json`
- [ ] 1.5 Implement `encode(msg, header, &mut Vec<u8>)` + `decode(body, &duckdb::Connection)` using DuckDB's BinarySerializer/Deserializer bindings (or a thin FFI shim if `duckdb-rs` does not expose them directly)
- [ ] 1.6 Implement `arrow_to_data_chunk(batch: &RecordBatch, conn: &Connection) -> DataChunk` and `data_chunk_to_arrow(chunk: &DataChunk) -> RecordBatch` helpers
- [ ] 1.7 Round-trip test: encode every message variant, decode, assert equality
- [ ] 1.8 Compatibility test: capture real `POST /quack` bodies from `duckdb` CLI talking to a `quack_serve()` instance, replay through our codec, assert byte-identical re-encoding
- [ ] 1.9 Pin supported DuckDB extension version in `docs/quack-protocol.md` and `sqe-quack-wire/README.md`: `v1.5-variegata`, `quack_version = 1`

## 2. sqe-quack-server crate (Option A)

- [ ] 2.1 Create `crates/sqe-quack-server/` with `Cargo.toml` (depends on `axum`, `tower`, `moka`, `sqe-quack-wire`, `sqe-coordinator`, `sqe-auth`, `sqe-sql`)
- [ ] 2.2 `QuackServer::serve()`: `axum::Router` with `GET /` (identification) and `POST /quack` (RPC); graceful shutdown via `tokio_util::sync::CancellationToken`
- [ ] 2.3 `handle_connection_request`: extract `auth_string`, call `sqe-auth::validate_bearer`, create `SqeQuackSession`, store in moka cache keyed by `connection_id`; on failure return `ErrorResponse("SQE-AUTH: ...")`
- [ ] 2.4 `handle_prepare_request`: lookup session by `connection_id`, parse SQL (DuckDB dialect via `sqe-sql`), translate, policy-rewrite, plan, execute; if result fits return `PrepareResponse { needs_more_fetch: false, results }`, else cache stream and return `result_uuid` with `needs_more_fetch: true`
- [ ] 2.5 `handle_fetch_request`: lookup pending result by `uuid`, send next batch of `DataChunk`s; clear cache entry when stream ends
- [ ] 2.6 `handle_append_request`: lookup session, route the chunk through the write path (re-uses Phase 2c machinery) with the user's session context
- [ ] 2.7 `handle_disconnect_message`: invalidate moka entry, return `SuccessResponse`
- [ ] 2.8 Cancellation: per-`connection_id` cancellation token threaded into `sqe-coordinator` execution
- [ ] 2.9 Wire `QuackServer` into the existing `sqe-coordinator` binary behind cargo feature `quack-server`; expose `[quack_server]` TOML config with `bind_addr` (default `127.0.0.1:9494`), `idle_timeout`, `max_inflight_fetches`, `fetch_batch_bytes` (default 4 MiB matching DuckDB's `quack_fetch_batch_bytes`)
- [ ] 2.10 Catalog identification: implement `quack_identify` table function equivalent so DuckDB clients can discover server capabilities
- [ ] 2.11 Unit tests: handshake, auth success/failure, simple `SELECT 1`, `SHOW TABLES`, error propagation, fetch loop for results that exceed batch size
- [ ] 2.12 Integration test: real DuckDB CLI connects via `ATTACH 'quack:localhost:9494'`, runs `SHOW TABLES`, runs `FROM ns.t LIMIT 10`, asserts results

## 3. DuckDB Dialect Support in sqe-sql

- [ ] 3.1 Add `duckdb-dialect` feature flag to `sqe-sql/Cargo.toml`
- [ ] 3.2 Wire `sqlparser::dialect::DuckDbDialect` selection into `sqe-sql/src/parser.rs`; dialect chosen per session via `SqeSession::dialect()`
- [ ] 3.3 Implement `crates/sqe-sql/src/dialect/duckdb_translate.rs`: AST walker that converts DuckDB-flavoured nodes to DataFusion-compatible nodes
  - [ ] 3.3.1 `LIST_VALUE([...])` -> array literal
  - [ ] 3.3.2 `STRUCT_PACK(k := v, ...)` -> struct literal
  - [ ] 3.3.3 `epoch()`, `epoch_ms()`, `date_part(...)` -> DataFusion equivalents
  - [ ] 3.3.4 `IF(...)` -> `CASE WHEN ... THEN ... ELSE ... END`
  - [ ] 3.3.5 `FROM 'path.parquet'` -> reject with `UNSUPPORTED_DIALECT` unless `read_parquet` TVF is enabled
- [ ] 3.4 Unsupported-feature detector: visit AST, return `UnsupportedDialect { feature: &'static str, hint: String }` for `PIVOT`, `UNPIVOT`, `ASOF JOIN`, `PRAGMA`, custom UDF call sites
- [ ] 3.5 Dialect status doc `docs/duckdb-dialect-status.md`: which DuckDB SQL features translate, which do not, how the mapping behaves
- [ ] 3.6 Unit tests: every translation rule has a paired test (DuckDB input, expected DataFusion AST)
- [ ] 3.7 Property test: round-trip a corpus of TPC-H SF1 queries from DuckDB dialect through translation and assert results match the same queries run against DataFusion-native parsing

## 4. sqe-quack-client crate (Option B, client side)

- [ ] 4.1 Create `crates/sqe-quack-client/` with `Cargo.toml` (depends on `reqwest` with `rustls-tls` feature, `sqe-quack-wire`)
- [ ] 4.2 `QuackClient::connect(uri, token)`: parse `quack:host[:port]`, derive `http(s)://host:port/quack`, POST `ConnectionRequest`, parse `ConnectionResponse`, store `connection_id`
- [ ] 4.3 `QuackClient::execute(sql)`: POST `PrepareRequest`, drive `FetchRequest` loop until `needs_more_fetch = false`, yield `RecordBatch` via the `data_chunk_to_arrow` helper from `sqe-quack-wire`
- [ ] 4.4 Disconnect cleanup: POST `DisconnectMessage` on drop; ignore errors
- [ ] 4.5 Unit tests with a mock Quack server (the `sqe-quack-server` from task 2 used as a fixture)
- [ ] 4.6 Integration test: client connects to a real DuckDB instance running `quack_serve('quack:localhost')`, executes a query, reads batches, asserts row equality

## 5. sqe-worker-duckdb crate (Option B, worker)

- [ ] 5.1 Create `crates/sqe-worker-duckdb/` with `Cargo.toml` depending on `duckdb-rs`
- [ ] 5.2 Worker binary: configurable bind addr, registers with coordinator with `WorkerKind::Duckdb`
- [ ] 5.3 Embedded DuckDB setup: `INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;`
- [ ] 5.4 Iceberg bridge: create DuckDB secret pointing at SQE's REST catalog with the current user bearer token
- [ ] 5.5 Token refresh: `ALTER SECRET` before every query execution; on `AUTH_FAILED` from Iceberg, surface to coordinator
- [ ] 5.6 Plan fragment execution: receive `PlanFragment { sql_text, schema }`, run via DuckDB, stream Arrow batches back to coordinator over existing Flight transport
- [ ] 5.7 Resource limits: configurable `memory_limit`, `threads`; pass to DuckDB via `PRAGMA`
- [ ] 5.8 Health check endpoint: coordinator pings worker, worker responds with DuckDB version + extension status
- [ ] 5.9 Worker shutdown: drain in-flight queries, close DuckDB connection, exit cleanly
- [ ] 5.10 Unit tests: worker lifecycle, query execution against an in-memory Iceberg fixture
- [ ] 5.11 Integration test: coordinator dispatches a TPC-H Q1 fragment to a DuckDB worker, asserts result parity with the DataFusion worker

## 6. Policy Enforcement for DuckDB Backend

- [ ] 6.1 Create `crates/sqe-policy/src/sql_text.rs::SqlTextRewriter`
- [ ] 6.2 Parse user SQL with sqlparser-rs; collect all table references in FROM clauses
- [ ] 6.3 For each table reference: look up applicable policy, build `PolicyDecision` (row filter + column masks) using the same internal builder as `PlanRewriter`
- [ ] 6.4 Rewrite each table reference: wrap as `(SELECT <projected columns with masks> FROM <table> WHERE <row_filter>) AS <fresh_alias>`
- [ ] 6.5 Re-serialise rewritten AST to SQL string
- [ ] 6.6 Audit log emits the same `PolicyApplied` event as the `PlanRewriter` path; include the rewritten SQL
- [ ] 6.7 Unit tests: row filter only, masks only, both, nested subqueries with masked columns referenced in outer SELECT
- [ ] 6.8 Parity test suite: take a corpus of (query, policy) pairs, run through both `PlanRewriter` (DataFusion) and `SqlTextRewriter` (DuckDB), assert identical row counts and column values

## 7. Coordinator Worker Selection

- [ ] 7.1 Extend `crates/sqe-coordinator/src/scheduler.rs` with `WorkerKind` enum
- [ ] 7.2 Worker registration: workers announce their kind at registration; coordinator stores in registry
- [ ] 7.3 Session variable `worker_kind`: `SET worker_kind = 'duckdb'` stored in `SqeSession`; defaults to `'datafusion'`
- [ ] 7.4 Planner picks worker kind from session, dispatches accordingly
- [ ] 7.5 When `worker_kind = 'duckdb'`: skip DataFusion-AST translation, run `SqlTextRewriter` on the original SQL, send SQL fragment to DuckDB worker
- [ ] 7.6 When `worker_kind = 'datafusion'`: existing path unchanged
- [ ] 7.7 Refuse `worker_kind = 'duckdb'` if no DuckDB workers registered: clear error message
- [ ] 7.8 Coordinator config `[workers]` section gains `kinds` list to gate which kinds are usable
- [ ] 7.9 Unit tests: session variable parse + apply, worker selection logic, error on missing worker kind

## 8. Documentation

- [ ] 8.1 `docs/quack-server.md`: how to enable, how to connect from DuckDB CLI, supported features, authentication
- [ ] 8.2 `docs/duckdb-worker.md`: how to deploy a DuckDB worker, configuration, when to use over DataFusion, known limitations
- [ ] 8.3 `docs/duckdb-dialect-status.md`: full feature matrix (supported / partial / not supported)
- [ ] 8.4 Update `README.md` roadmap with Quack support entries
- [ ] 8.5 Update `nextsteps.md` with phase markers

## 9. Benchmarks

- [ ] 9.1 Add benchmark harness mode `WORKER_KIND=duckdb` for `scripts/benchmark-test.sh`
- [ ] 9.2 Run TPC-H SF1 against both worker kinds; commit JSON to `benchmarks/results/`
- [ ] 9.3 Run a list/struct-heavy workload (e.g., GitHub Archive sample) to validate the DuckDB-shines hypothesis; commit results
- [ ] 9.4 Document where DuckDB wins and where DataFusion wins in `docs/duckdb-worker.md`

## 10. End-to-End Integration

- [ ] 10.1 Integration test: DuckDB CLI -> Quack server -> coordinator -> DataFusion worker, runs a multi-table join with row filter and masked column, assert result and audit log
- [ ] 10.2 Integration test: DuckDB CLI -> Quack server -> coordinator -> DuckDB worker (session sets `worker_kind = 'duckdb'`), same query, assert identical result
- [ ] 10.3 Integration test: bad token returns `AUTH_FAILED` and connection closes
- [ ] 10.4 Integration test: unsupported dialect feature returns `UNSUPPORTED_DIALECT` with hint
- [ ] 10.5 Live test against `dbt-duckdb` adapter pointed at a SQE Quack endpoint; run a minimal dbt project (1 source + 1 model + 1 test)
