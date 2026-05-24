## Context

Quack is DuckDB's client-server protocol. A DuckDB instance running `quack_serve()` accepts HTTP requests from clients that `ATTACH 'quack:host'`. Reference details captured in `docs/quack-protocol.md`; key facts that shape this design:

- Transport is **HTTP/1.1 with keep-alive**, endpoint `POST /quack`, content type `application/vnd.duckdb`.
- Wire format is DuckDB's `BinarySerializer` (the same code path that produces DuckDB WAL files). Result payloads are serialised `DataChunk` (DuckDB's native columnar batch), **not Arrow IPC**.
- Ten message types: `CONNECTION_REQUEST/RESPONSE`, `PREPARE_REQUEST/RESPONSE`, `FETCH_REQUEST/RESPONSE`, `APPEND_REQUEST`, `SUCCESS_RESPONSE`, `DISCONNECT_MESSAGE`, `ERROR_RESPONSE`. Schemas are stable within `quack_version`.
- Current `quack_version = 1`; protocol stabilises in DuckDB v2.0 (Sep 2026).
- Token-based auth: `auth_string` field in the `CONNECTION_REQUEST`. The server's auth function decides validity.

For SQE, this opens two integration points using the same protocol:

- **Server side (Option A)**: SQE accepts incoming HTTP requests on the Quack endpoint and translates inbound DuckDB sessions to SQE's session/catalog/auth/policy stack. DuckDB clients see SQE as another DuckDB server.
- **Client side (Option B)**: SQE runs DuckDB as an alternative execution backend behind a worker boundary. The coordinator sends SQL fragments to the DuckDB worker over Quack and reads back result chunks.

Both paths share the wire codec (`sqe-quack-wire`), which links `duckdb-rs` to reuse DuckDB's `BinarySerializer` + `DataChunk` Serialize/Deserialize. Hand-rolling the codec was considered and rejected: the protocol is pre-release and the maintenance cost of tracking DuckDB's internal serialiser drift outweighs the ~25 MB binary cost of linking libduckdb.

Decisions from exploration:

- DuckDB SQL dialect is parsed via sqlparser-rs `DuckDbDialect`, not by forking the DuckDB parser. Translation is best-effort; unsupported nodes return a clear error.
- Policy enforcement on the DuckDB worker side happens at SQL text level. We do not attempt to interpose between DuckDB's parser and its optimizer.
- Quack `TOKEN '...'` is treated as an opaque bearer. The user passes an OIDC access token as the Quack token; SQE validates via `sqe-auth` exactly like a Flight SQL bearer.
- DuckDB worker uses the same Iceberg REST catalog as the rest of SQE, configured through the DuckDB Iceberg extension. The bearer token is forwarded.
- We pin to `quack_version = 1` and the `v1.5-variegata` extension release. Forward compatibility is a Phase 2 problem.

## Goals / Non-Goals

**Goals:**
- Accept incoming Quack connections; serve standard DuckDB-CLI workflows (`ATTACH`, `SHOW TABLES`, `FROM ns.table`, prepared statements, transactions where supported)
- Translate the common subset of DuckDB SQL to LogicalPlan
- Route plan fragments to DuckDB workers when a session/query is tagged for it
- Enforce row filters and column masks against DuckDB-backed queries
- All new code paths behind cargo features; default build unaffected

**Non-Goals:**
- 100% DuckDB SQL dialect parity. Translation covers SELECT, projections, joins, aggregates, window functions, simple list/struct literals, common functions. Niche DuckDB-specific features (`PIVOT`, `UNPIVOT`, `ASOF JOIN`, `PRAGMA`, custom UDFs) return a clear "not supported" error.
- Cross-engine query splitting: a single query runs on one worker kind, chosen at planning time
- Writeback through DuckDB workers: writes stay on the DataFusion path (Phase 2c machinery)
- DuckDB extensions beyond Iceberg + httpfs are out of scope

## Architecture

### High level

```
                      ┌───────────────┐
   DuckDB CLI/dbt ───▶│ axum HTTP/1.1 │   POST /quack
                      │ listener      │   application/vnd.duckdb
                      │ (port 9494)   │   (Option A: server)
                      └──────┬────────┘
                             │ Bytes (BinarySerializer body)
                             ▼
                      ┌───────────────┐
                      │ sqe-quack-    │
                      │ wire          │   decode using duckdb-rs
                      │ codec         │   into QuackMessage variant
                      └──────┬────────┘
                             │ QuackMessage + header
                             ▼
                      ┌───────────────┐
                      │ sqe-quack-    │
                      │ server        │   moka session cache,
                      │ session       │   per-connection state
                      └──────┬────────┘
                             │ SqeSession API
                             ▼
                      ┌───────────────┐
   SQL string ───────▶│ sqe-sql       │
   (DuckDB dialect)   │ DuckDB-dialect│
                      │ parser +      │
                      │ translator    │
                      └──────┬────────┘
                             │ LogicalPlan
                             ▼
                      ┌───────────────┐
                      │ sqe-policy    │
                      │ PlanRewriter  │
                      │ (row/mask)    │
                      └──────┬────────┘
                             │ LogicalPlan (rewritten)
                             ▼
                      ┌───────────────┐
                      │ sqe-planner   │
                      │ scheduler     │
                      └──────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
       ┌─────────────┐               ┌─────────────┐
       │ DataFusion  │               │ sqe-worker- │
       │ worker      │               │ duckdb      │
       │ (existing)  │               │ (Option B)  │
       └──────┬──────┘               └──────┬──────┘
              │                             │
              │ RecordBatch                 │ Quack HTTP (sqe-quack-client)
              │                             ▼
              │                     ┌─────────────┐
              │                     │  embedded   │
              │                     │   DuckDB    │
              │                     │ (in worker  │
              │                     │  process)   │
              │                     └──────┬──────┘
              │                            │ RecordBatch (via fetch_arrow)
              └──────────────┬─────────────┘
                             ▼
                      ┌───────────────┐
                      │ Arrow ->      │
                      │ DataChunk     │
                      │ converter     │
                      └──────┬────────┘
                             │ DataChunk
                             ▼
                      ┌───────────────┐
                      │ sqe-quack-    │
                      │ wire          │   encode back
                      │ codec         │   to BinarySerializer
                      └──────┬────────┘
                             │ Bytes
                             ▼
                          to client
```

### Crate layout

```
crates/
  sqe-quack-wire/            # protocol codec, message types
    src/
      messages.rs           # ten message variants + MessageHeader
      codec.rs              # encode/decode against duckdb-rs BinarySerializer
      data_chunk.rs         # RecordBatch <-> DataChunk conversion helpers
  sqe-quack-server/          # Option A
    src/
      app.rs                # axum Router, POST /quack handler
      session.rs            # SqeQuackSession (wraps coordinator Session)
      connection.rs         # handle CONNECTION_REQUEST/RESPONSE
      prepare.rs            # PREPARE_REQUEST -> SQE plan + execute
      fetch.rs              # FETCH_REQUEST -> next batch
      append.rs             # APPEND_REQUEST -> write path
  sqe-quack-client/          # Option B (client primitives)
    src/
      connection.rs         # reqwest client + auth handshake
      session.rs            # send PrepareRequest, drive Fetch loop
  sqe-worker-duckdb/         # Option B (worker)
    src/
      lib.rs
      runtime.rs            # embedded DuckDB instance per worker
      iceberg_bridge.rs     # configure DuckDB Iceberg extension
      executor.rs           # execute SQL, stream batches back
```

### Quack wire protocol (mapped to Rust types)

We mirror the message schema from `quack_message.json` in the upstream repo:

```rust
#[repr(u64)]
pub enum MessageType {
    Invalid = 0,
    ConnectionRequest = 1,
    ConnectionResponse = 2,
    PrepareRequest = 3,
    PrepareResponse = 4,
    FetchRequest = 5,
    FetchResponse = 6,
    AppendRequest = 7,
    SuccessResponse = 8,
    DisconnectMessage = 9,
    ErrorResponse = 10,
}

pub struct MessageHeader {
    pub r#type: MessageType,
    pub connection_id: String,
    pub client_query_id: Option<u64>,
}

pub enum QuackMessage {
    ConnectionRequest {
        auth_string: String,
        client_duckdb_version: String,
        client_platform: String,
        min_supported_quack_version: u64,
        max_supported_quack_version: u64,
    },
    ConnectionResponse {
        server_duckdb_version: String,
        server_platform: String,
        quack_version: u64,
    },
    PrepareRequest { sql_query: String },
    PrepareResponse {
        result_types: Vec<DuckLogicalType>,
        result_names: Vec<String>,
        needs_more_fetch: bool,
        results: Vec<duckdb::DataChunk>,
        result_uuid: i128,
    },
    FetchRequest { uuid: i128 },
    FetchResponse {
        results: Vec<duckdb::DataChunk>,
        batch_index: Option<u64>,
    },
    AppendRequest {
        schema_name: String,
        table_name: String,
        append_chunk: duckdb::DataChunk,
    },
    SuccessResponse,
    DisconnectMessage,
    ErrorResponse { message: String },
}

pub fn encode(msg: &QuackMessage, header: &MessageHeader, out: &mut Vec<u8>) -> Result<()>;
pub fn decode(body: &[u8], db: &duckdb::Connection) -> Result<(MessageHeader, QuackMessage)>;
```

The `encode`/`decode` functions bridge through `duckdb-rs`. We require a `duckdb::Connection` because `BinarySerializer` is tied to DuckDB's catalog/type resolution context. A tiny in-memory DuckDB instance per process is sufficient; we do not need a per-request connection.

`DuckLogicalType` is `duckdb-rs`'s logical type wrapper. `DataChunk` is also from `duckdb-rs`.

### RecordBatch <-> DataChunk conversion

SQE's planner produces Arrow `RecordBatch`. The Quack wire wants `DataChunk`. We convert:

- **Arrow -> DataChunk** (for sending results to a Quack client): register the Arrow batch with a per-session DuckDB in-memory connection (`Appender::append_record_batch`), then read it out as a `DataChunk`. This costs a copy. The DuckDB extension already does this internally when its scan reads an external table.
- **DataChunk -> Arrow** (for receiving result fragments from a DuckDB worker, Option B): DuckDB has a native `result.fetch_arrow()` API. We can request Arrow output from the worker directly.

The Option B path is cheaper because we control the worker and can ask DuckDB to emit Arrow. The Option A path always pays the Arrow -> DataChunk conversion cost because that is what the protocol demands.

### Option A: sqe-quack-server

The server is an `axum` HTTP/1.1 app with one route: `POST /quack`. Per-connection state lives in a moka cache keyed by `connection_id`. The cache TTL matches DuckDB's `idle_timeout`.

```rust
pub struct QuackServer {
    coordinator: Arc<Coordinator>,
    auth: Arc<dyn AuthBackend>,
    sessions: Arc<moka::future::Cache<String, SqeQuackSession>>,
    bind_addr: SocketAddr,
    duckdb_ctx: Arc<duckdb::Connection>,   // shared codec context, NOT for query exec
}

pub async fn serve(server: Arc<QuackServer>) -> Result<()> {
    let app = Router::new()
        .route("/", get(|| async { "DuckDB Quack RPC endpoint (SQE-backed)" }))
        .route("/quack", post(handle_quack))
        .layer(Extension(server.clone()));
    axum::serve(TcpListener::bind(server.bind_addr).await?, app).await?;
    Ok(())
}

async fn handle_quack(
    Extension(server): Extension<Arc<QuackServer>>,
    body: Bytes,
) -> impl IntoResponse {
    let (header, msg) = match quack_wire::decode(&body, &server.duckdb_ctx) {
        Ok(v) => v,
        Err(e) => return error_response(&server, "", None, &e.to_string()),
    };
    let response = match msg {
        QuackMessage::ConnectionRequest { auth_string, .. } => {
            handle_connection_request(&server, auth_string).await
        }
        QuackMessage::PrepareRequest { sql_query } => {
            handle_prepare(&server, &header, sql_query).await
        }
        QuackMessage::FetchRequest { uuid } => {
            handle_fetch(&server, &header, uuid).await
        }
        QuackMessage::AppendRequest { schema_name, table_name, append_chunk } => {
            handle_append(&server, &header, schema_name, table_name, append_chunk).await
        }
        QuackMessage::DisconnectMessage => handle_disconnect(&server, &header).await,
        _ => Err(QuackError::unsupported(format!("{:?}", header.r#type))),
    };
    encode_response(&server, &header, response)
}
```

`handle_prepare` calls into the existing SQL execution path:

1. Parse `sql_query` with `sqlparser-rs` using `DuckDbDialect` (via `sqe-sql`)
2. Translate the DuckDB AST to a DataFusion-compatible AST or directly to `LogicalPlan` (see "SQL dialect translation" below)
3. Run policy rewriter (`sqe-policy`)
4. Plan and execute through `sqe-planner` + `sqe-coordinator`
5. Drain the resulting `RecordBatchStream`, convert each batch to `DataChunk`, and:
   - If the entire result fits in `quack_fetch_batch_bytes` (default 4 MiB), return `PrepareResponse { needs_more_fetch: false, results: vec![...] }`
   - Otherwise, return the first batch with `needs_more_fetch: true` and a fresh `result_uuid`; remaining batches stay in a per-connection result cache awaiting `FetchRequest`s

The shared `duckdb_ctx` is a process-singleton `duckdb::Connection` used only for serialisation context, not for query execution. The actual query runs through `sqe-coordinator` against DataFusion or a remote DuckDB worker.

#### Connection lifecycle

```
                    Client                          Server (SQE)
                       |                                   |
                       |   POST /quack                     |
                       |   body: ConnectionRequest         |
                       |     (auth_string = OIDC bearer)   |
                       |---------------------------------->|
                       |                                   | sqe-auth validates bearer
                       |                                   | new SqeSession created
                       |                                   | connection_id = uuid()
                       |   200 OK                          | cache.insert(connection_id, ...)
                       |   body: ConnectionResponse        |
                       |     (connection_id in header)     |
                       |<----------------------------------|
                       |                                   |
                       |   POST /quack                     |
                       |   body: PrepareRequest            |
                       |     (header.connection_id, sql)   |
                       |---------------------------------->|
                       |                                   | cache.get(connection_id)
                       |                                   | parse + policy + plan + exec
                       |                                   | first batch -> DataChunk
                       |   200 OK                          |
                       |   body: PrepareResponse           |
                       |<----------------------------------|
                       |                                   |
                       |   ... FetchRequest / FetchResponse loop until needs_more_fetch=false ...
                       |                                   |
                       |   POST /quack                     |
                       |   body: DisconnectMessage         |
                       |---------------------------------->|
                       |   200 OK                          | cache.invalidate(connection_id)
                       |   body: SuccessResponse           |
                       |<----------------------------------|
```

### Option B: sqe-worker-duckdb

```rust
pub struct DuckDbWorker {
    db: duckdb::Connection,                 // embedded DuckDB instance
    iceberg_catalog_url: String,
    bearer_token_source: Arc<TokenSource>,  // provides current user OIDC token
}

impl DuckDbWorker {
    pub fn new(config: DuckDbWorkerConfig) -> Result<Self> {
        let db = duckdb::Connection::open_in_memory()?;
        db.execute_batch("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")?;
        // configure iceberg secret pointing at our REST catalog
        db.execute_batch(&format!(
            "CREATE SECRET sqe_iceberg (TYPE iceberg, ENDPOINT '{}', TOKEN '{{token}}');",
            config.iceberg_catalog_url
        ))?;
        Ok(Self { db, /* ... */ })
    }

    pub async fn execute(&self, frag: PlanFragment, token: &str) -> Result<RecordBatchStream> {
        // refresh secret with current user token
        self.db.execute_batch(&format!(
            "ALTER SECRET sqe_iceberg SET TOKEN '{}';", token
        ))?;
        let sql = frag.sql_text;  // already policy-rewritten
        let mut stmt = self.db.prepare(&sql)?;
        // stream arrow record batches out
        let arrow_iter = stmt.query_arrow([])?;
        Ok(stream_from_arrow_iter(arrow_iter))
    }
}
```

The worker runs in its own process (same supervision as `sqe-worker`). The coordinator's worker registry tags it with `WorkerKind::Duckdb`.

### Worker selection

```rust
pub enum WorkerKind {
    Datafusion,
    Duckdb,
}

pub struct WorkerSelector {
    available: Vec<WorkerEntry>,
    policy: SelectionPolicy,
}

pub enum SelectionPolicy {
    /// Always use DataFusion (default)
    Datafusion,
    /// Always use DuckDB (per-session opt-in)
    Duckdb,
    /// Cost-based: pick based on heuristics over LogicalPlan shape
    /// (kept simple: DuckDB for small-data interactive, DataFusion otherwise)
    Adaptive,
}
```

Session-level opt-in via `SET worker_kind = 'duckdb'` (parsed as a session variable, not pushed to DuckDB).

### SQL dialect translation

The translation layer in `sqe-sql/src/dialect/duckdb.rs`:

- Recognised and translated:
  - All standard SELECT/JOIN/CTE/WINDOW shapes
  - `LIST_VALUE([...])` -> DataFusion array literal
  - `STRUCT_PACK(a := 1, b := 2)` -> DataFusion struct literal
  - DuckDB time functions (`epoch`, `epoch_ms`, `date_part`) mapped to DataFusion equivalents where they exist
  - `FROM 'path/to/file.parquet'` -> recognised but rejected with a clear error unless `read_parquet` TVF is enabled
- Passed through unchanged when shape is identical
- Rejected with explicit "not supported in this backend, file an issue" error:
  - `PIVOT` / `UNPIVOT`
  - `ASOF JOIN`
  - `PRAGMA`
  - DuckDB-specific UDFs

When the session is bound to `WorkerKind::Duckdb`, we skip the AST translation entirely and pass SQL through after policy text rewrite. DuckDB parses it natively.

### Policy enforcement: two paths

For DataFusion workers (unchanged):

```rust
LogicalPlan -> PlanRewriter::rewrite -> LogicalPlan (with row filters + masks injected) -> DataFusion optimizer
```

For DuckDB workers (new):

```rust
SQL string -> SqlTextRewriter::rewrite -> SQL string (with row filters as WHERE, masks as projection CASE) -> DuckDB
```

`SqlTextRewriter` lives in `sqe-policy/src/sql_text.rs`. It uses sqlparser-rs to parse the user SQL, walks the AST, finds `TableScan` analogues (table references in FROM), looks up the policy, and:

- Wraps the table reference in a subquery with the row filter as WHERE
- Replaces the bare column references with `CASE WHEN <mask_condition> THEN <mask_expr> ELSE <column> END` for masked columns
- Re-serialises the AST back to SQL via sqlparser-rs

The risk here is correctness: SQL text rewriting on a user query is fiddly. We mitigate by:

1. Using AST-level rewriting (never string concatenation)
2. Wrapping the original FROM target in a derived table with a fresh alias, so column references in the outer query remain valid
3. A dedicated property-based test suite that compares row counts and column values from DataFusion (LogicalPlan rewrite) and DuckDB (text rewrite) for the same policy + query

### Authentication flow

The token travels in `ConnectionRequestMessage.auth_string`. There is no separate `Auth` frame.

```
DuckDB CLI                                      SQE quack-server
  │                                                    │
  │ CREATE SECRET (TYPE quack, TOKEN '<oidc-jwt>');    │
  │ ATTACH 'quack:sqe:9494' AS sqe;                    │
  │                                                    │
  │ POST /quack                                        │
  │   body: ConnectionRequest {                        │
  │     auth_string = "<oidc-jwt>",                    │
  │     client_duckdb_version = "v1.5.2",              │
  │     min..max_supported_quack_version = 1..1        │
  │   }                                                │
  │───────────────────────────────────────────────────▶│
  │                                                    │ sqe-auth validates bearer
  │                                                    │ new SqeSession + connection_id
  │ 200 OK                                             │
  │   body: ConnectionResponse {                       │
  │     server_duckdb_version = "v1.5.2",              │
  │     quack_version = 1                              │
  │   }                                                │
  │   header.connection_id = "<uuid>"                  │
  │◀───────────────────────────────────────────────────│
```

`sqe-auth` already validates bearer tokens for Flight SQL. The Quack token is the bearer; no new auth code is required.

Subsequent `PREPARE_REQUEST`s reuse the `connection_id`. If `sqe-auth` rejects the token (expired, revoked), the server returns `ERROR_RESPONSE` with the validation reason; the client must re-`ATTACH` after refreshing.

### Error model

`ERROR_RESPONSE` carries a single `message` string. DuckDB does not define structured error codes on the wire. We prefix our messages with a code-like marker so clients can pattern-match if they want:

| Internal error | Wire message format |
|---|---|
| Auth failure (invalid/expired token) | `"SQE-AUTH: <details>"` |
| Parse error | `"SQE-PARSE: <details>"` |
| Unsupported dialect feature | `"SQE-DIALECT: feature `<name>` not supported. See https://.../duckdb-dialect-status.md"` |
| Policy denial | `"SQE-POLICY: access denied"` (no further detail per security policy) |
| Catalog not found | `"SQE-CATALOG: namespace `<name>` not found"` |
| Execution error | `"SQE-EXEC: <DuckDB-style error text>"` |

Plain DuckDB clients see this as a normal error string. SQE-aware clients can split on the prefix.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Quack protocol churn between DuckDB releases | Pin to extension `v1.5-variegata` + `quack_version = 1`; gate behind cargo feature; documented support matrix; re-evaluate at DuckDB v2.0 |
| SQL dialect coverage gap surprises users | Explicit `SQE-DIALECT` error with feature name; track gaps in `docs/duckdb-dialect-status.md` |
| SQL-text policy rewriting introduces correctness bugs | Property-based test parity against LogicalPlan rewriter |
| Binary size: linking duckdb-rs in coordinator (~25 MB) | Gate behind `quack-server` feature; coordinator binary without the feature stays slim; documented in build profiles |
| Two enforcement points in `sqe-policy` increase audit surface | Single internal `PolicyDecision` data structure; both rewriters call the same decision builder; audit log emits the same event regardless of enforcement path |
| OIDC token expiry mid-session | If `sqe-auth` rejects mid-session, server returns `SQE-AUTH` error; client must re-`ATTACH` with fresh token |
| Arrow -> DataChunk conversion overhead in Option A | Use DuckDB's `Appender::append_record_batch` (zero-copy where possible); benchmarked in task 9.x |
| `BinarySerializer` compatibility version drift | Pinned to `SerializationCompatibility::FromIndex(7)`; codec tests load fixture frames captured from upstream `quack_serve` |

## Open Questions

- Does the DuckDB Iceberg extension support write operations against Polaris? If yes, do we route writes through it? (Out of scope for this change; flagged for follow-up.)
- Should `WorkerKind::Duckdb` be a per-session setting (`SET worker_kind = 'duckdb'`) or a per-query hint? Per-session is simpler; per-query gives finer control. Default: per-session.
- Do we want to expose SQE as `quack_version >= 2` once DuckDB stabilises? Decision deferred; current Phase 1 ships v1 only.
