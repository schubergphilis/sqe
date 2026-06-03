# Quack Protocol Reference (as of DuckDB extension v1.5-variegata)

Reference notes for implementing a Quack-compatible server and client in Rust. Extracted from the `duckdb/duckdb-quack` source (MIT, ~356 commits, May 2026) and the DuckDB v1.5.2+ release. Cross-checked against the announcement post and the in-repo `docs/usage.md`.

The Quack protocol is **pre-release** and the DuckDB project plans to stabilise it for v2.0 in September 2026. Treat this document as a snapshot, not a stable contract.

## Status of upstream documentation

The upstream documentation has two surfaces with mismatched naming:

- **README + source code**: `quack_serve`, `quack_stop`, `quack:` URI scheme, HTTP endpoint `POST /quack`, content type `application/vnd.duckdb`.
- **`docs/usage.md` and FAQ**: `rpc_start`, `rpc_stop`, `POST /rpc`, MIME type `application/duckdb`.

The source is authoritative. The `rpc_*` doc names appear to be an older or aspirational naming. We follow the source.

The FAQ states "Quack uses HTTP v2.0". The source uses `httplib` (a small C++ HTTP/1.1 library) with `keep_alive_max_count(128)`. We treat the wire as **HTTP/1.1 with keep-alive**, not HTTP/2.

## Transport

| Field | Value |
|---|---|
| Protocol | HTTP/1.1, keep-alive enabled |
| Default port | 9494 |
| URI scheme | `quack:host[:port]` (HTTPS by default for non-localhost, plain HTTP for localhost) |
| Endpoint | `POST /quack` |
| Content-Type (request and response) | `application/vnd.duckdb` |
| TLS | Optional. Server generates self-signed cert via `quack_generate_keys()`. Production deployments expected to terminate TLS at a reverse proxy. |
| CORS | Server returns `Access-Control-Allow-Origin: *` on `OPTIONS /quack` and on every response |

There is also a root path that returns a plain-text identification string:

```http
GET / HTTP/1.1

HTTP/1.1 200 OK
Content-Type: text/plain

This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.
```

Useful for sniffing whether a host speaks Quack.

## Wire format

Every request body and every response body is a serialised `QuackMessage`. The serializer is DuckDB's `BinarySerializer` with `SerializationCompatibility::FromIndex(7)`. This is the same code path DuckDB uses for its Write-Ahead Log files.

Each message on the wire is:

```
[ serialized MessageHeader (BinarySerializer Begin/End block) ]
[ serialized message body  (BinarySerializer Begin/End block) ]
```

`BinarySerializer` uses field-tagged encoding. Every field has a numeric ID, a type, and a value. Optional fields can be omitted. The schema (with stable field IDs) is captured in `src/include/quack_message.json` in the upstream repo and reproduced below for stability.

## Message header

| Field ID | Name | Type | Notes |
|---|---|---|---|
| 1 | `type` | `MessageType` (enum) | See message types below |
| 2 | `connection_id` | `string` | Server-assigned, returned in `CONNECTION_RESPONSE` |
| 3 | `client_query_id` | `optional_idx` (u64) | Monotonic per-client query ID for log correlation |

`MessageType` is an enum encoded as `idx_t`:

```
INVALID = 0
CONNECTION_REQUEST = 1
CONNECTION_RESPONSE = 2
PREPARE_REQUEST = 3
PREPARE_RESPONSE = 4
FETCH_REQUEST = 5
FETCH_RESPONSE = 6
APPEND_REQUEST = 7
SUCCESS_RESPONSE = 8
DISCONNECT_MESSAGE = 9
ERROR_RESPONSE = 10
```

The exact wire ordering of enum tags depends on DuckDB's `EnumUtil`; do not hard-code numeric values. Always go through the named enum.

## Message bodies

### ConnectionRequest

Initial handshake. Sent once per connection.

| Field | Type | Notes |
|---|---|---|
| 1 `auth_string` | string | Bearer token. Server's auth function decides validity |
| 2 `client_duckdb_version` | string | e.g. `"v1.5.2"` |
| 3 `client_platform` | string | e.g. `"osx_arm64"` |
| 4 `min_supported_quack_version` | idx_t | client min |
| 5 `max_supported_quack_version` | idx_t | client max |

### ConnectionResponse

| Field | Type | Notes |
|---|---|---|
| 1 `server_duckdb_version` | string | |
| 2 `server_platform` | string | |
| 3 `quack_version` | idx_t | Currently `1` |

Header carries the server-assigned `connection_id`; clients echo it in subsequent requests.

### PrepareRequest

| Field | Type | Notes |
|---|---|---|
| 1 `sql_query` | string | Raw SQL |

### PrepareResponse

| Field | Type | Notes |
|---|---|---|
| 1 `result_types` | `vector<LogicalType>` | Per-column DuckDB type |
| 2 `result_names` | `vector<string>` | Column names |
| 3 `needs_more_fetch` | bool | If true, client must follow up with `FETCH_REQUEST` using `result_uuid` |
| 4 `results` | `vector<DataChunkWrapper>` | Optional first batch of rows |
| 5 `result_uuid` | hugeint_t | Server-side handle for follow-up fetches |

The server may inline the entire result if it fits; otherwise it returns a `result_uuid` and the client pulls more via `FETCH_REQUEST`.

### FetchRequest

| Field | Type | Notes |
|---|---|---|
| 1 `uuid` | hugeint_t | Result handle from `PrepareResponse` |

### FetchResponse

| Field | Type | Notes |
|---|---|---|
| 1 `results` | `vector<DataChunkWrapper>` | Batched chunks |
| 2 `batch_index` | `optional_idx` | Sequence number for ordering |

### AppendRequest

Bulk insert from client to server.

| Field | Type | Notes |
|---|---|---|
| 1 `schema_name` | string | Target schema |
| 2 `table_name` | string | Target table |
| 3 `append_chunk` | `DataChunkWrapper` | Row data |

### SuccessResponse

Empty body. Used to acknowledge `DisconnectMessage`, `AppendRequest`, etc.

### DisconnectMessage

Empty body. Client signals end of session. Server responds with `SuccessResponse` and closes the connection.

### ErrorResponse

| Field | Type | Notes |
|---|---|---|
| 1 `message` | string | Raw error message |

## DataChunk wire format

Results travel as `DataChunkWrapper`, which serialises one DuckDB `DataChunk` (vectorised columnar batch). The wrapper has a single field:

| Field ID | Name | Type |
|---|---|---|
| 300 | `chunk` | `DataChunk` |

A `DataChunk` is DuckDB's native columnar batch type. Its serialisation includes:

- Number of columns
- Per-column `LogicalType` (recursive for nested types)
- Per-column `Vector` data (validity bitmap + data buffer + optional dictionary/auxiliary buffers)

This is **not Arrow IPC**. DuckDB has its own columnar layout. The two formats are not interchangeable without conversion.

For SQE to read these, we either:

1. Link `libduckdb` and let DuckDB's C++ code deserialise into a DataChunk, then convert to Arrow inside our process; or
2. Reimplement DuckDB's `BinarySerializer` and `DataChunk::Serialize` semantics in Rust.

Option 1 ties us to a specific DuckDB version but gets correctness for free. Option 2 is purer Rust but the maintenance cost tracks DuckDB releases. Decision recorded in `openspec/changes/duckdb-quack-protocol-support/design.md` (Open Questions section).

## Authentication

The server's `quack_authentication_function` (default `quack_check_token`) is a SQL scalar function with signature `(sid VARCHAR, token VARCHAR) -> BOOLEAN`. The default implementation compares the token against `quack_default_token`.

Users can plug their own auth by registering a scalar function with that signature and pointing the setting at it.

The token travels in `ConnectionRequestMessage.auth_string`. There is no separate `Auth` frame. Once `ConnectionResponse` returns, the connection is authenticated for the lifetime of that connection.

Per-query authorisation: `quack_authorization_function` is `(sid VARCHAR, query VARCHAR) -> BOOLEAN`. Default allows everything. Called server-side before executing each `PrepareRequest`.

## Pushdown semantics

The server supports the following pushdowns when a client `ATTACH`es and then scans a remote table:

- **Projection pushdown**: only requested columns are returned
- **Filter pushdown**: constant comparisons (`=`, `<`, `>`, `<=`, `>=`, `<>`), `IS NULL`, `IS NOT NULL`, `IN (...)`, and `AND`/`OR` combinations

Filters are evaluated server-side. Other predicates (function calls, joins) execute on the client.

For SQE-as-server: the SQL the client sends is already the filtered/projected SQL. We do not need to extract pushdowns from a separate field. The SQL string carries everything.

## Logging

The extension registers two log types:

- `quack` log: structured per-message (`message_type`, `connection_id`, `client_query_id`, `query`, `duration_ms`, `error`)
- `HTTP` log: per-request URL + status

For SQE compatibility, we should emit equivalent structured logs from the server crate.

## Compatibility matrix

| Server `quack_version` | Client `min..max` | Behaviour |
|---|---|---|
| 1 | min<=1<=max | OK |
| 1 | min>1 | Server returns `ErrorResponse` |
| Future N | client max < N | Server should downgrade if possible; otherwise reject |

Current `quack_version = 1`. The protocol is expected to bump versions before v2.0 stabilisation.

## Things SQE will need to handle differently from DuckDB

- **Iceberg-backed catalogs**: DuckDB Quack assumes its own catalog. Our `Attach` returns SQE's Iceberg catalog tree. DuckDB clients see Iceberg namespaces as schemas.
- **OIDC tokens vs static tokens**: the auth function receives an opaque string. We treat it as an OIDC bearer and validate via `sqe-auth`. Bare static tokens are still accepted if `sqe-auth` is configured for them.
- **Result format**: SQE's existing query engine produces Arrow `RecordBatch`. We must convert each `RecordBatch` to a DuckDB `DataChunk` before serialising. This conversion is non-trivial but tractable (both are columnar, both have validity bitmaps).
- **Policy enforcement**: server-side SQL goes through `sqe-policy` SQL-text rewriter (see `openspec/changes/duckdb-quack-protocol-support/design.md`) before reaching the planner.

## References

- Upstream repo: https://github.com/duckdb/duckdb-quack (MIT)
- Announcement: https://duckdb.org/2026/05/12/quack-remote-protocol
- DuckDB docs (overview): https://duckdb.org/docs/current/quack/overview
- FAQ: https://duckdb.org/quack/faq
- Local reference clone: `/tmp/duckdb-quack-src/` (during research; delete after Phase 1)
