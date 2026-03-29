# Speaking Arrow {#sec:flightsql}

> The wire protocol is the user experience.
> Everything else is implementation detail.

The engine can parse SQL. It can authenticate users. It can plan queries, push them through DataFusion, and produce Arrow record batches. None of that matters if clients can't talk to it.

The wire protocol -- the thing between the client and the engine -- determines everything a user actually feels. Latency. Compatibility. The quality of error messages. Whether their favourite tool works at all. You can build the most elegant query engine in the world, and if the only way to reach it is a custom binary protocol that nobody supports, it's a toy.

We needed to pick a protocol. The choice shaped the entire surface area of SQE.


## The Obvious Options (and Why We Rejected Them)

### REST

The default reflex for any modern service: slap a JSON API on it. POST a query, GET the results. Easy to build, easy to debug with curl, supported everywhere.

The problem is serialisation. A query engine's job is to produce columnar data -- Arrow record batches. A REST API would mean converting those batches to JSON on the server, sending them over the wire as text, and then converting them back to columnar format on the client. For a result set with a million rows and twenty columns, that's three serialisation steps where zero are needed.

JSON also destroys type fidelity. A Decimal(38,18) becomes a floating-point number. A timestamp loses its timezone. A null in a struct column becomes ambiguous. You can work around all of this with careful schema metadata, but at that point you're building a type system on top of a format that doesn't have one.

We did end up building a REST-ish interface -- the Trino-compatible HTTP endpoint -- but it's a compatibility layer for tools that only speak Trino wire protocol, not the primary interface. More on that in a moment.

### JDBC Directly

JDBC is the standard for database connectivity in the Java world. Every SQL tool on the planet speaks it. DBeaver, IntelliJ, Tableau, dbt (through the Python DBAPI bridge) -- if your engine has a JDBC driver, you're immediately compatible with the entire ecosystem.

But JDBC is a Java API, not a wire protocol. The JDBC specification defines interfaces (`Connection`, `Statement`, `ResultSet`) that a driver implements. What goes over the wire is up to the driver. PostgreSQL uses its own binary protocol. MySQL uses its own. Trino uses HTTP/JSON. Each driver is a custom implementation tightly coupled to a specific engine.

Building a custom JDBC driver means writing and maintaining a Java library that speaks a custom protocol. You're shipping two codebases: the engine in Rust, the driver in Java. Every schema change, every new SQL feature, every error code needs to be coordinated across both. For a small team, that's a tax you pay on every feature forever.

### gRPC

Getting warmer. gRPC gives you binary serialisation (protobuf), HTTP/2 multiplexing, streaming, and client generation in every major language. It's what Kubernetes uses. It's what most modern infrastructure speaks.

But raw gRPC is just a transport. You'd still need to define the protobuf messages for query requests, result schemas, record batches, metadata discovery, authentication, and error handling. You'd be inventing a protocol from scratch -- one that no existing tool supports.

Unless someone already defined that protocol for you.


## Arrow Flight SQL

Arrow Flight is a gRPC-based protocol, designed by the Apache Arrow project, for moving Arrow-formatted data between processes. It defines a small set of RPCs -- `Handshake`, `GetFlightInfo`, `DoGet`, `DoPut`, `DoAction` -- that handle authentication, metadata exchange, and bidirectional data streaming.

Arrow Flight SQL is a layer on top of Flight that adds SQL semantics. It defines specific protobuf messages for executing SQL statements, discovering catalog metadata (schemas, tables, columns, type info), creating prepared statements, and handling transactions. It's a complete SQL client protocol built on Arrow IPC over gRPC.

The key property: **zero serialisation overhead for query results.** The engine produces Arrow record batches internally. Flight SQL sends those batches over the wire in Arrow IPC format -- the same in-memory layout, byte-for-byte. The client receives them and can work with them directly. No JSON. No text parsing. No type conversion.

There's a practical benefit too. The Arrow Flight SQL JDBC driver exists. It's maintained by the Apache Arrow project. Any tool that speaks JDBC can connect to any Flight SQL server through this driver. One driver for every Flight SQL engine. We get JDBC compatibility without writing a JDBC driver.

| Protocol | Serialisation overhead | Ecosystem support | Implementation cost |
|---|---|---|---|
| REST/JSON | High (text encode/decode) | Universal | Low |
| Custom JDBC | None (custom binary) | Java only | High (maintain driver) |
| Raw gRPC | Low (protobuf) | Narrow (custom clients) | Medium |
| Flight SQL | Zero (Arrow IPC) | Growing (JDBC driver, ADBC, Python, Rust, Go) | Medium |

Flight SQL was the only option that gave us zero-copy results *and* broad client compatibility without maintaining a custom driver.


## The FlightSqlService Trait

The `arrow-flight` crate provides a Rust trait called `FlightSqlService` with over twenty methods. Each method corresponds to a Flight SQL RPC endpoint. The trait has default implementations that return `Unimplemented` for everything, so you only need to override the methods you care about.

Here is the struct that implements it in SQE:

```rust
#[derive(Clone)]
pub struct SqeFlightSqlService {
    session_manager: Arc<SessionManager>,
    query_handler: Arc<QueryHandler>,
    config: SqeConfig,
    worker_registry: Option<Arc<WorkerRegistry>>,
    query_tracker: Arc<QueryTracker>,
    worker_secret: String,
}
```

Six fields. The `SessionManager` handles OIDC authentication and session state. The `QueryHandler` routes SQL through parsing, planning, policy enforcement, and DataFusion execution. The `WorkerRegistry` tracks distributed workers (optional -- absent in single-node mode). The `QueryTracker` records query history for the `system.runtime.queries` virtual table.

Of the 20+ trait methods, SQE implements these:

| Method | Purpose | SQE behaviour |
|---|---|---|
| `do_handshake` | Authentication | OIDC password grant via Basic auth |
| `get_flight_info_statement` | Plan a SQL query | Plans query, returns schema + ticket |
| `do_get_statement` | Execute and stream results | Runs query, streams Arrow batches |
| `do_get_fallback` | Handle custom ticket types | Executes our `FetchResults` tickets |
| `get_flight_info_catalogs` | List catalogs | Returns warehouse name |
| `do_get_catalogs` | Fetch catalog data | Returns warehouse from config |
| `get_flight_info_schemas` | List schemas metadata | Returns schema for schema listing |
| `do_get_schemas` | Fetch schema names | Runs `SHOW SCHEMAS` internally |
| `get_flight_info_tables` | List tables metadata | Returns schema for table listing |
| `do_get_tables` | Fetch table names | Enumerates all schemas and tables |
| `do_get_table_types` | Supported table types | Returns `["TABLE", "VIEW"]` |
| `get_flight_info_sql_info` | Server capabilities | Reports name, version, Arrow version |
| `do_get_sql_info` | Fetch server info data | Builds `SqlInfoData` response |
| `get_flight_info_xdbc_type_info` | Type system metadata | Returns type info schema |
| `do_get_xdbc_type_info` | Fetch type details | Reports all supported SQL types |
| `get_flight_info_prepared_statement` | Prepared statement metadata | Plans query, returns schema |
| `do_get_prepared_statement` | Execute prepared statement | Decodes handle, runs query |
| `do_put_statement_update` | Execute DML (INSERT, etc.) | Runs statement, returns row count |
| `do_put_statement_ingest` | Bulk data upload | Decodes Arrow stream, writes to table |
| `do_put_prepared_statement_query` | Bind parameters | Returns handle unchanged (no-op) |
| `do_put_prepared_statement_update` | Execute prepared DML | Decodes handle, runs statement |
| `do_action_create_prepared_statement` | Create prepared statement | Plans query, stores SQL in handle |
| `do_action_close_prepared_statement` | Close prepared statement | No-op (stateless handles) |
| `do_action_cancel_query` | Cancel running query | Cancels via QueryTracker |
| `do_action_fallback` | Custom actions | Handles worker heartbeats |

And these are explicitly `Unimplemented`:

| Method | Why skipped |
|---|---|
| `get_flight_info_substrait_plan` | No Substrait support |
| `do_put_substrait_plan` | No Substrait support |
| `do_action_create_prepared_substrait_plan` | No Substrait support |
| `do_action_begin_transaction` | No transaction support |
| `do_action_end_transaction` | No transaction support |
| `do_action_begin_savepoint` | No savepoint support |
| `do_action_end_savepoint` | No savepoint support |

The pattern: we implement everything a SQL client needs to discover metadata, execute queries, upload data, and manage prepared statements. We skip Substrait (an alternative query representation we don't use) and transactions (Iceberg commits are atomic per-table, not cross-table).

::: {.datafusion}
**DataFusion deep dive:** The `FlightSqlService` trait has a `type FlightService` associated type that must
be `Self`. This is how the `arrow-flight` crate connects the SQL-layer trait to the underlying gRPC
`FlightService` implementation. When you implement `FlightSqlService`, you automatically get the
`FlightService` gRPC methods routed to the right SQL-specific handlers based on the protobuf
message types in the request. The routing logic is in the trait's default `do_get` and `do_put`
implementations -- they decode the `Any` message in the ticket and dispatch to the specific handler.
:::


## The Three-Phase Pipeline

Every SQL query follows the same three-phase pipeline through Flight SQL: Handshake, GetFlightInfo, DoGet.

### Phase 1: Handshake

The client connects and authenticates. In SQE, this means OIDC password grant -- the client sends a username and password, and the coordinator exchanges them for a JWT via Keycloak (or whatever OIDC provider is configured).

```rust
async fn do_handshake(
    &self,
    request: Request<Streaming<HandshakeRequest>>,
) -> Result<
    Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
    Status,
> {
    // Extract Basic auth: base64(username:password)
    let authorization = request
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::invalid_argument("Authorization header not present"))?
        .to_str()
        .map_err(|e| Status::internal(format!("Authorization header not parsable: {e}")))?
        .to_string();

    // ... decode base64, split on ':', extract username and password ...

    let session = self
        .session_manager
        .authenticate(username, password)
        .await
        .map_err(|e| {
            warn!(username = username, error = %e, "Authentication failed");
            Status::unauthenticated(format!("Authentication failed: {e}"))
        })?;

    let result = HandshakeResponse {
        protocol_version: 0,
        payload: session.id.as_bytes().to_vec().into(),
    };

    let output = futures::stream::iter(vec![Ok(result)]);
    let token = format!("Bearer {}", session.id);
    let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
        Response::new(Box::pin(output));
    response.metadata_mut().append(
        "authorization",
        MetadataValue::from_str(&token)
            .map_err(|e| Status::internal(format!("Failed to create auth metadata: {e}")))?,
    );

    Ok(response)
}
```

The handshake does three things:

1. Decodes the Basic auth header (base64-encoded `username:password`)
2. Calls `session_manager.authenticate()`, which performs the OIDC password grant against Keycloak and creates a session holding the JWT
3. Returns the session ID as a bearer token in both the response payload and the `authorization` response header

The client stores this token and sends it as `Authorization: Bearer <session-id>` on every subsequent request. The Flight SQL JDBC driver handles this automatically.

There's a second authentication path that we added later. Some clients -- particularly backend services that have already obtained a JWT through their own OIDC flow -- want to skip the handshake entirely and just send the JWT as a bearer token. The `get_session_from_request` method handles both:

```rust
fn get_session_from_request<T>(
    &self,
    request: &Request<T>,
) -> Result<Arc<sqe_core::Session>, Status> {
    let token = // ... extract bearer token from Authorization header ...

    // Try session lookup first (handshake flow)
    if let Some(session) = self.session_manager.get_session(token) {
        return Ok(session);
    }

    // If the token looks like a JWT (contains dots), treat it as a raw
    // access token -- create an ad-hoc session
    if token.contains('.') {
        let username = metadata
            .get("x-trino-user")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        let session = sqe_core::Session::new(
            username,
            token.to_string(),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        );
        return Ok(Arc::new(session));
    }

    Err(Status::unauthenticated("Invalid or expired session token"))
}
```

First it checks if the token is a session ID from a prior handshake. If not, it checks if the token looks like a JWT (JWTs always contain dots as delimiters between header, payload, and signature). If it's a JWT, it creates an ad-hoc session with that token directly. This is the same pattern the Trino-compat HTTP endpoint uses, which means backend services can use either protocol with the same authentication approach.

### Phase 2: GetFlightInfo

The client sends a SQL query and gets back metadata -- the result schema and a ticket for fetching the actual data. No execution happens yet.

```rust
async fn get_flight_info_statement(
    &self,
    query: CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let session = self.get_session_from_request(&request)?;
    let sql = &query.query;

    // Encode the SQL into a ticket so do_get can find it later
    let fetch = FetchResults {
        handle: sql.clone(),
    };
    let ticket = Ticket {
        ticket: fetch.as_any().encode_to_vec().into(),
    };
    let endpoint = FlightEndpoint {
        ticket: Some(ticket),
        location: vec![],
        expiration_time: None,
        app_metadata: vec![].into(),
    };

    // Plan the query to extract the schema without executing it
    let schema = self
        .query_handler
        .get_schema(&session, sql)
        .await
        .map_err(|e| Status::internal(format!("Query planning failed: {e}")))?;

    let info = FlightInfo::new()
        .try_with_schema(&schema)
        .map_err(|e| Status::internal(format!("Failed to encode schema: {e}")))?
        .with_descriptor(FlightDescriptor::new_cmd(vec![]))
        .with_endpoint(endpoint)
        .with_total_records(-1)
        .with_ordered(false);

    Ok(Response::new(info))
}
```

Two things happen here. First, the coordinator calls `query_handler.get_schema()`, which plans the query through DataFusion (including policy enforcement and optimization) and extracts the output schema without actually executing it. This gives the client the column names and types before any data flows.

Second, it encodes the SQL string into a `FetchResults` protobuf message and wraps it in a `Ticket`. The `FlightInfo` response contains this ticket inside a `FlightEndpoint`. When the client wants the actual data, it presents this ticket back to the server.

The `total_records` is -1, meaning "unknown". We could execute the query during GetFlightInfo and report the exact count, but that would mean running the query twice (or caching the entire result set in memory). We chose not to.

The `location` field in the endpoint is empty, which means "fetch from the same server." In a distributed setup, you could return different locations pointing to different workers -- but SQE's distributed execution is handled internally by the coordinator, not by directing clients to specific workers. The client always talks to the coordinator.

::: {.deadend}
**Dead end: executing during GetFlightInfo.** We initially tried executing the full query during
`get_flight_info_statement` and caching the result batches, so `do_get_statement` could just
stream them from memory. This worked for small result sets but fell apart on queries returning
millions of rows -- the coordinator would hold the entire result in memory between the two
calls. Worse, some clients wait minutes between GetFlightInfo and DoGet (DBeaver displays the
schema in a tab before the user clicks "fetch data"). We switched to plan-only during GetFlightInfo
and execute-on-demand during DoGet.
:::

### Phase 3: DoGet

The client presents the ticket and receives a stream of Arrow record batches.

```rust
async fn do_get_statement(
    &self,
    ticket: TicketStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    let session = self.get_session_from_request(&request)?;
    let sql = &ticket.statement_handle;

    let sql_str = std::str::from_utf8(sql)
        .map_err(|e| Status::internal(format!("Invalid statement handle: {e}")))?;

    let batches = self
        .query_handler
        .execute(&session, sql_str)
        .await
        .map_err(|e| Status::internal(format!("Query execution failed: {e}")))?;

    Self::batches_to_stream(batches)
}
```

The handler extracts the SQL from the ticket, executes it through the full pipeline (parse, plan, policy enforcement, DataFusion execution, possibly distributed to workers), and converts the resulting `Vec<RecordBatch>` into a `FlightStream`.

The `batches_to_stream` helper converts record batches into Flight data frames:

```rust
fn batches_to_stream(
    batches: Vec<RecordBatch>,
) -> Result<Response<FlightStream>, Status> {
    if batches.is_empty() {
        let stream = futures::stream::empty();
        let flight_stream: FlightStream = Box::pin(stream);
        return Ok(Response::new(flight_stream));
    }

    let schema = batches[0].schema();
    let flight_data = batches_to_flight_data(&schema, batches)
        .map_err(|e| Status::internal(format!("Failed to encode flight data: {e}")))?
        .into_iter()
        .map(Ok);

    let stream: FlightStream = Box::pin(stream::iter(flight_data));
    Ok(Response::new(stream))
}
```

The `batches_to_flight_data` function (from the `arrow-flight` crate) converts Arrow record batches into the Arrow IPC format used by Flight. The schema is sent first as a separate message, followed by the data for each batch. On the client side, the Flight library decodes these back into record batches -- same schema, same memory layout.

There's a subtle bug we hit early on, captured in the comment: "Using Schema::empty() here caused clients to hang because get_flight_info sends the real query schema but do_get sent a 0-column schema, confusing the FlightRecordBatchStream decoder." When a query returns zero rows, you still need to return a stream that's consistent with the schema you promised in GetFlightInfo. We solved it by returning a genuinely empty stream -- no schema message at all -- rather than an empty schema.


## The Fallback: Tickets That Don't Match

Not all Flight SQL clients use the standard `TicketStatementQuery` message. Some older clients, and some that pre-date the full Flight SQL specification, send raw tickets with custom protobuf messages. SQE handles this through `do_get_fallback`:

```rust
async fn do_get_fallback(
    &self,
    request: Request<Ticket>,
    message: Any,
) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
    let session = self.get_session_from_request(&request)?;

    if message.type_url == FetchResults::type_url() {
        let fetch: FetchResults = Message::decode(&*message.value)
            .map_err(|e| Status::internal(format!("Failed to decode ticket: {e}")))?;

        let batches = self
            .query_handler
            .execute(&session, &fetch.handle)
            .await
            .map_err(|e| Status::internal(format!("Query execution failed: {e}")))?;

        return Self::batches_to_stream(batches);
    }

    Err(Status::unimplemented(format!(
        "Unsupported ticket type: {}",
        message.type_url
    )))
}
```

The `FetchResults` is a custom protobuf message defined in the SQE codebase. It carries a single string field: the SQL query handle. When `get_flight_info_statement` creates a ticket, it encodes the SQL into this message. The fallback handler checks the `type_url`, decodes the message, and executes the query. This is the path most JDBC clients actually take, because the Arrow Flight SQL JDBC driver uses the generic `DoGet` RPC, not the SQL-specific `DoGetStatement` one.

::: {.fieldreport}
**Field report:** The Arrow Flight SQL JDBC driver (version 15.0) routes all queries through
`do_get_fallback`, not `do_get_statement`. We discovered this during the first DBeaver test.
The standard `do_get_statement` handler was never hit. Without the fallback, DBeaver
connected successfully, planned queries, displayed schemas, and then returned zero rows
for every query. The fix was straightforward once we understood the routing, but it took
an afternoon of packet captures to figure out why.
:::


## The Metadata Surface

SQL clients don't just execute queries. Before a user types their first SELECT, the client tool has already made a dozen metadata calls: "What catalogs exist? What schemas? What tables? What are the column types? What SQL features does this server support?"

DBeaver is particularly thorough. On connection, it calls `GetSqlInfo`, `GetCatalogs`, `GetDbSchemas`, `GetTables`, `GetTableTypes`, and `GetXdbcTypeInfo` -- all before the user has even opened a query tab.

SQE implements all of these. For catalogs, it returns the warehouse name from the config. For schemas, it runs `SHOW SCHEMAS` internally. For tables, it iterates every schema and runs `SHOW TABLES` in each one -- not the most efficient approach, but it works and it respects the user's access permissions because every internal query runs through the same session and policy enforcement.

The type info endpoint is surprisingly dense. JDBC clients use it to understand what SQL types the server supports, how they map to XDBC types, what their precision and scale ranges are, and how literals are formatted. SQE reports boolean, tinyint, smallint, integer, bigint, real, double, decimal, varchar, varbinary, date, time, and timestamp -- each with its XDBC metadata:

```rust
builder.append(XdbcTypeInfo {
    type_name: "decimal".into(),
    data_type: XdbcDataType::XdbcDecimal,
    column_size: Some(38),
    create_params: Some(vec!["precision".into(), "scale".into()]),
    nullable: Nullable::NullabilityNullable,
    case_sensitive: false,
    searchable: Searchable::Full,
    fixed_prec_scale: true,
    sql_data_type: XdbcDataType::XdbcDecimal,
    minimum_scale: Some(0),
    maximum_scale: Some(38),
    num_prec_radix: Some(10),
    ..Default::default()
});
```

This is the kind of code that doesn't make it into conference talks. Fifteen type definitions, each with a dozen fields. No cleverness. Just correctness. But if any of these are wrong -- if you report `maximum_scale: 18` when DataFusion actually supports 38 -- some JDBC client somewhere will silently truncate your decimal values and you'll spend a week figuring out why your financial calculations don't round-trip.


## Connecting From Everywhere

### DBeaver

DBeaver connects through the Arrow Flight SQL JDBC driver. The connection configuration:

- **Driver**: Apache Arrow Flight SQL
- **URL**: `jdbc:arrow-flight-sql://localhost:50051?useEncryption=false`
- **Username/Password**: OIDC credentials (passed to `do_handshake`)

On connection, DBeaver walks the metadata surface, builds its schema tree, and is ready for queries. Every query goes through the GetFlightInfo/DoGet pipeline. The JDBC driver handles the streaming, buffering, and type conversion from Arrow to Java `ResultSet` objects.

The `useEncryption=false` parameter disables TLS for local development. In production, SQE supports TLS through a configurable certificate and key in the TOML config, and the URL becomes `jdbc:arrow-flight-sql://coordinator.internal:50051`.

### Python (adbc_driver_flightsql)

ADBC (Arrow Database Connectivity) is the Arrow-native replacement for ODBC/JDBC. The `adbc_driver_flightsql` package provides a Python client that speaks Flight SQL and returns results as PyArrow tables -- zero copy from the engine to pandas.

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(
    "grpc://localhost:50051",
    db_kwargs={
        "username": "alice",
        "password": "secret",
    }
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM warehouse.orders LIMIT 10")

# Returns a PyArrow Table -- columnar, zero-copy
table = cursor.fetch_arrow_table()
df = table.to_pandas()
```

The driver calls `do_handshake` with the credentials, stores the bearer token, and uses it for all subsequent operations. `cursor.execute()` calls `GetFlightInfo`, and `fetch_arrow_table()` calls `DoGet`. The Arrow data arrives in the same IPC format that DataFusion produced. No JSON. No pandas-to-Arrow conversion. The bytes that left the engine are the bytes that land in the DataFrame.

For programmatic clients that manage their own OIDC flow:

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(
    "grpc://localhost:50051",
    db_kwargs={
        "adbc.flight.sql.authorization_header": f"Bearer {jwt_token}",
    }
)
```

This skips the handshake entirely and passes the JWT directly. SQE's `get_session_from_request` detects the JWT format and creates an ad-hoc session.

### Rust (arrow-flight client)

The `sqe-cli` crate shows the Rust client pattern:

```rust
pub struct FlightClient {
    inner: FlightSqlServiceClient<Channel>,
}

impl FlightClient {
    pub async fn connect(
        url: &str,
        username: &str,
        password: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let channel = build_channel(url).await?;
        let mut inner = FlightSqlServiceClient::new(channel);

        let token = inner
            .handshake(username, password)
            .await
            .map_err(|e| format!("Authentication failed: {e}"))?;

        inner.set_token(String::from_utf8(token.to_vec())?);
        Ok(Self { inner })
    }
}
```

The `FlightSqlServiceClient` from the `arrow-flight` crate handles all the gRPC plumbing. `handshake()` sends the Basic auth, receives the bearer token, and `set_token()` stores it for subsequent calls. Query execution is two calls:

```rust
async fn execute(&mut self, sql: &str) -> Result<QueryResult, Box<dyn std::error::Error>> {
    let info = self
        .inner
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| format!("Query failed: {e}"))?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();

    for endpoint in info.endpoint {
        if let Some(ticket) = endpoint.ticket {
            let stream = self
                .inner
                .do_get(ticket)
                .await
                .map_err(|e| format!("Failed to fetch results: {e}"))?;

            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            all_batches.extend(batches);
        }
    }

    batches_to_result(&all_batches)
}
```

The `execute()` call maps to GetFlightInfo. The response can contain multiple endpoints (in distributed scenarios, pointing to different servers), each with a ticket. The client iterates through all endpoints, calls `do_get` on each, and collects the record batches. For SQE in single-node mode, there's always exactly one endpoint pointing back to the coordinator.

### The Trino-Compat Escape Hatch

Some tools only speak the Trino HTTP protocol. For those, SQE runs a second server on a separate port that implements the Trino wire protocol -- `POST /v1/statement` with JSON responses and `nextUri` pagination.

The Trino-compat layer isn't Flight SQL. It's the JSON-serialised protocol we rejected at the start of this chapter. But it exists because pragmatism beats purity. If a team's existing tooling only speaks Trino, they can connect to SQE without changing anything. The performance cost is real -- JSON serialisation, text parsing, type coercion -- but it's a migration bridge, not the primary interface.

The CLI supports both:

```
sqe-cli --protocol flight -e "SELECT 1"    # Arrow Flight SQL (default)
sqe-cli --protocol http -e "SELECT 1"      # Trino-compat HTTP
```


## Streaming Results and Backpressure

Flight SQL runs on gRPC, which runs on HTTP/2. HTTP/2 has flow control built in -- the client can signal the server to slow down when it can't consume data fast enough. This is backpressure, and it matters.

Consider a query that scans a billion-row table. The engine can produce record batches far faster than most clients can process them. Without backpressure, the server would buffer the entire result set in memory, waiting for the client to catch up. With HTTP/2 flow control, the server's send buffer fills up, the gRPC stream blocks, DataFusion's execution pauses at the point where it would produce the next batch, and no memory is wasted on buffering.

SQE's current implementation collects all record batches into memory before streaming:

```rust
let batches = self
    .query_handler
    .execute(&session, sql_str)
    .await?;

Self::batches_to_stream(batches)
```

This means the full result materialises in the coordinator's memory before the first byte reaches the client. For most analytical queries -- which return aggregated results, filtered subsets, or sampled data -- this is fine. For queries that return millions of rows, it's a problem.

The fix is straightforward in principle: instead of `execute()` returning a `Vec<RecordBatch>`, it would return a `SendableRecordBatchStream` -- DataFusion's native streaming type -- and we'd convert that directly into a Flight stream. Each batch would flow from DataFusion through the gRPC stream to the client as it's produced, with HTTP/2 backpressure preventing the coordinator from running ahead.

We haven't done this yet. It's a clear next step, and it's the kind of improvement that becomes urgent exactly once someone runs `SELECT * FROM a_very_large_table` in production.

::: {.datafusion}
**DataFusion deep dive:** DataFusion's `collect()` function gathers all batches into a Vec. For streaming,
you'd use `execute_stream()` on the physical plan, which returns a `SendableRecordBatchStream`.
The `FlightDataEncoderBuilder` in the `arrow-flight` crate can wrap this stream directly,
producing a `Stream<Item = Result<FlightData, FlightError>>` that maps one-to-one onto the gRPC
response stream. The types line up. The plumbing is waiting. It's an afternoon of work that
we keep deprioritising in favour of features with more visible impact.
:::


## Error Propagation

When a query fails mid-stream -- a Parquet file is corrupted, a worker crashes, a token expires -- the error needs to reach the client. gRPC has a native mechanism for this: the `Status` type, which carries a status code and a message.

For errors during planning (GetFlightInfo), this is straightforward. The handler returns `Err(Status::internal("Query planning failed: ..."))`, and the gRPC framework sends it as a response with an error code. The client gets a clean error message.

For errors during streaming (DoGet), it's slightly different. The stream is already open. The client has already received the schema and possibly some batches. An error is sent as the final message in the stream, which terminates it. The Flight SQL client libraries handle this and surface it as an exception.

SQE maps internal errors to gRPC status codes:

| Situation | gRPC status |
|---|---|
| Missing or invalid auth | `UNAUTHENTICATED` |
| Malformed request | `INVALID_ARGUMENT` |
| Query planning failure | `INTERNAL` |
| Execution failure | `INTERNAL` |
| Unsupported operation | `UNIMPLEMENTED` |

We don't use `PERMISSION_DENIED` for policy violations. The security model (covered in Chapter 8) is designed around *invisible* denial -- columns you can't see simply don't appear in the schema, rows you can't access are silently filtered. The client never receives an error saying "you don't have permission to read column X" because the client never knows column X exists.


## Mounting the Service

The Flight SQL service is a tonic gRPC server. Setting it up is a few lines:

```rust
let flight_service =
    SqeFlightSqlService::new(session_manager, query_handler, config.clone());

let addr = format!("0.0.0.0:{}", config.coordinator.flight_sql_port).parse()?;

tonic::transport::Server::builder()
    .add_service(
        arrow_flight::flight_service_server::FlightServiceServer::new(flight_service)
    )
    .serve(addr)
    .await?;
```

Note the type wrapping: `SqeFlightSqlService` implements `FlightSqlService`, but the tonic server expects a `FlightService` (the raw gRPC trait). The `FlightServiceServer::new()` call takes anything that implements `FlightService`, and the `FlightSqlService` trait provides a blanket implementation of `FlightService` that routes incoming gRPC calls to the appropriate SQL-specific methods.

In production, optional TLS is layered on top:

```rust
let tls_config = sqe_coordinator::tls::build_server_tls_config(&config.coordinator.tls)?;

let mut server_builder = tonic::transport::Server::builder();
if let Some(tls) = tls_config {
    server_builder = server_builder.tls_config(tls)?;
}
```

The coordinator also starts the Trino-compat HTTP server on a separate port, the Prometheus metrics server on a third port, and optionally a worker health-check background task. Four network surfaces, each doing one thing, each on its own port. The Flight SQL port is the primary interface. Everything else is supplementary.


## The Wire Protocol Is the User Experience

I started this chapter by saying the wire protocol is the user experience. After building this, I believe it more strongly than before.

The choice of Flight SQL determined which clients work out of the box (DBeaver, any JDBC tool, Python via ADBC, Rust via arrow-flight). It determined the serialisation overhead (none for Arrow-aware clients). It determined how authentication flows (gRPC metadata headers carrying bearer tokens). It determined how errors surface (gRPC status codes). It determined whether backpressure is possible (yes, because HTTP/2).

It also determined what we didn't have to build. We didn't write a JDBC driver. We didn't invent a protocol. We didn't build a type-mapping layer between our internal representation and the wire format. The engine produces Arrow. The wire carries Arrow. The client receives Arrow. Every step that doesn't exist is a step that can't have bugs.

The 20+ methods in the FlightSqlService trait looked intimidating at first. Most of them are metadata endpoints that JDBC clients expect -- catalogs, schemas, tables, types. Once we understood the pattern (plan in GetFlightInfo, execute in DoGet, metadata via dedicated endpoints), the implementation was mechanical. The interesting code is all in the `QueryHandler` and the `SessionManager`. The Flight SQL layer is plumbing.

Good plumbing is invisible. Users don't think about the wire protocol. They open DBeaver, type a connection string, and run queries. The protocol's job is to never be the reason something doesn't work. Flight SQL has held up its end of that bargain.

::: {.fieldreport}
**Field report:** The first integration test -- Flight SQL handshake with OIDC, followed by a SELECT query
against an Iceberg table via Polaris -- passed on March 14, the same day the crates were scaffolded.
From empty repository to authenticated query results over Arrow Flight SQL in one day. The
protocol was not the hard part. Polaris credential vending was the hard part. The protocol
just worked.
:::

::: {.ailog}
**AI Logbook:** The AI implemented all 24 `FlightSqlService` trait methods — including the metadata surface for catalogs, schemas, tables, and XDBC type info — across two sessions. The human decided which methods to implement and which to leave as `Unimplemented` (Substrait, transactions). The `do_get_fallback` handler that turned out to be the actual path JDBC clients use was discovered during the first DBeaver test by the human; the AI had implemented the standard `do_get_statement` path first, which no real client hit.
:::
