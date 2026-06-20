# Findings — Quack wire protocol & Worker (`sqe-quack-*`, `sqe-worker`)

**Scope:** The brand-new Quack wire protocol crate (`sqe-quack-wire`: `varint.rs`, `codec.rs`, `message.rs`,
`data_chunk.rs`, `arrow_bridge.rs`, `lib.rs`), the Quack server (`sqe-quack-server`), the Quack client
(`sqe-quack-client`), and the stateless worker (`sqe-worker`). The dominant risk is the wire decoder, which
trusts wire-supplied length/count fields before allocating, indexing, and recursing. The Quack server runs
`decode_message` on the raw body **before authentication** (`app.rs:76`), so every decoder weakness below is
reachable pre-auth by an unauthenticated network client. The workspace uses the default unwind panic strategy
(no `panic = "abort"`), so a panic inside a handler kills the in-flight request but the process survives.
EXCEPT stack overflow (recursion) and allocator failure (memory bomb), which abort the whole process. The
`quack_query` TVF is registered with a user-supplied host (`sqe-coordinator/src/session_context.rs:502-505`),
so the client-side decoder is reachable from any coordinator user.

> **QUACK-05 independently verified by the dispatcher**: the quack server binds `0.0.0.0` with plain
> `axum::serve` (`sqe_server.rs:930-938`), unlike the Flight path immediately below it which applies
> `build_server_tls_config`.

---

### QUACK-01 — critical — Unbounded recursion in `LogicalType`/`Vector` decode -> stack overflow -> process abort (pre-auth)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/data_chunk.rs:470-485` (`LogicalType::decode`), `:330-336`/`:357-366` (`ExtraTypeInfo::decode_inner` List/Array/Struct recurse), `:721`/`:743`/`:772` (`Vector::decode` recurses); entered pre-auth via `crates/sqe-quack-server/src/app.rs:76`
- **Evidence:**
  ```rust
  // data_chunk.rs:330-336 — List arm recurses with no depth counter
  ExtraTypeInfoType::List => {
      d.expect_field(200)?;
      let child = LogicalType::decode(d)?;   // -> decode_inner -> LogicalType::decode -> ...
      d.expect_object_end()?;
      Ok(ExtraTypeInfo::List { child: Box::new(child) })
  }
  // app.rs:76 — runs before any auth, for any header.type
  let (request_header, request_body) = match decode_message(&body) { ... };
  ```
- **Impact:** A few KB of nested `LIST<LIST<LIST<...>>>` type markers (within the 2 MB default axum body
  limit) drives unbounded mutual recursion. Rust stack overflow hits the guard page and aborts the process via
  SIGSEGV/SIGABRT. not a catchable panic. An unauthenticated attacker who can reach `POST /quack` (bound
  `0.0.0.0:{quack_port}`) crashes the entire coordinator process with one small request. Same class on the client
  decode path (`Vector::decode`) reachable from a malicious server via `quack_query`.
- **Fix:** Thread a depth counter (`remaining_depth: u8`) through `LogicalType::decode`,
  `ExtraTypeInfo::decode_inner`, and `Vector::decode`; return `WireError` once a small cap (e.g. 32) is exceeded.
- **Effort:** small

---

### QUACK-02 — critical — Memory-bomb: `Vec::with_capacity(n)` / `vec![0; n]` on unbounded wire counts -> OOM abort (pre-auth)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/message.rs:382,391,409-410,478-479`; `data_chunk.rs:341,378,567,685,705,770,876,891`; reachable pre-auth via `app.rs:76`
- **Evidence:**
  ```rust
  // message.rs:409-410 (FetchResponse/PrepareResponse results list)
  let results_count = d.read_list_count()? as usize;   // read_u64 -> up to u64::MAX
  let mut out = Vec::with_capacity(results_count);
  // data_chunk.rs:685 (validity bits, count = row_count, u32 up to ~4.29e9)
  let mut bits = Vec::with_capacity(count);
  ```
- **Impact:** `read_list_count()`/`read_u64()` accept any varint up to `u64::MAX`, cast straight to `usize`, and
  pre-allocate. A mid-range count (e.g. `row_count = 4e9` on the validity path at `data_chunk.rs:685`, ~4 GB) is
  a real allocation that trips `handle_alloc_error` or the OOM-killer and aborts the process. independent of
  panic strategy. Unauthenticated, single request, whole-process kill. `DataChunk::decode` is reachable pre-auth
  because `decode_message` fully decodes `AppendRequest` before auth.
- **Fix:** Before allocating, bound every wire count against the bytes remaining in the buffer (each list element
  costs >= 1 byte). Reject when `count > deserializer.remaining().len()`; use `Vec::new()` + `reserve` only after
  the count is validated.
- **Effort:** small

---

### QUACK-03 — high — Fixed-width column decode slices wire bytes without length check -> OOB panic (remote DoS)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/arrow_bridge.rs:1560-1569` (`scalar_buffer_le`), callers `:1393-1402`, `:1427-1494`; `data_chunk.rs:778-781` produces the unvalidated `Fixed(bytes)`
- **Evidence:**
  ```rust
  // arrow_bridge.rs:1560-1569 — no check that bytes.len() >= row_count * width
  fn scalar_buffer_le<T: FromLeBytesScalar>(bytes: &[u8], row_count: usize) -> ScalarBuffer<T> {
      let width = std::mem::size_of::<T>();
      let values: Vec<T> = (0..row_count)
          .map(|i| T::from_le(&bytes[i * width..(i + 1) * width]))  // panics on OOB
          .collect();
  ```
- **Impact:** Nothing checks `data_ptr.len() == row_count * physical_width`. A server (or MITM) returning
  `row_count = 1_000_000` with a 4-byte data buffer makes `scalar_buffer_le::<i32>` slice out of bounds ->
  index-out-of-bounds panic. Reachable on the coordinator from any user via
  `SELECT * FROM quack_query('quack:attacker:9495','...')`. Panic is caught at the task boundary (unwind), so it
  kills the query/request: remote DoS (>= high per the wire-panic rule).
- **Fix:** In `Vector::decode` for the fixed path, validate `bytes.len() >= count * logical_type.physical_width()`
  before constructing `VectorData::Fixed`. In `scalar_buffer_le`/`fixed_to_array` use `bytes.get(...)` and bail.
- **Effort:** small

---

### QUACK-04 — high — UUID/Decimal/Enum decode index past buffer end -> OOB panic (remote DoS)

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/arrow_bridge.rs:1433-1434` (UUID), `:1479-1485` (Decimal), `:1504-1516` (Enum)
- **Evidence:**
  ```rust
  // arrow_bridge.rs:1433-1434 — off+16 unchecked against bytes.len()
  let off = i * 16;
  builder.append_value(&bytes[off..off + 16]) ...
  ```
- **Impact:** Same trust mismatch as QUACK-03 for variable-physical-width types. A malicious quack server returns
  a Decimal/UUID/Enum column whose `row_count` exceeds the supplied byte buffer, panicking the coordinator query
  task. The Decimal path additionally trusts `precision` from the wire (`data_chunk.rs:326`) to pick the slice
  width. Remote DoS via the `quack_query` TVF.
- **Fix:** Validate buffer length against `row_count * width` once in `fixed_to_array`, use checked slicing, and
  validate `precision`/`scale`/enum dict size against sane bounds in `ExtraTypeInfo::decode_inner`.
- **Effort:** small

---

### QUACK-05 — high — Quack server has no TLS: bearer tokens sent in cleartext

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/bin/sqe_server.rs:930-938` (plain `axum::serve`), vs. `:953-963` (Flight gets `build_server_tls_config`). Token carried in `crates/sqe-quack-server/src/app.rs:254-264`
- **Evidence:**
  ```rust
  // sqe_server.rs:930-935 — no TLS wrapper, no rate limiter
  let bind = format!("0.0.0.0:{quack_port}");
  let listener = tokio::net::TcpListener::bind(&bind).await ...;
  axum::serve(listener, quack_app).await
  ```
- **Impact:** The Quack `ConnectionRequest.auth_string` is the user's OIDC bearer token (`app.rs:262-264`). The
  endpoint binds `0.0.0.0` over plain HTTP/1.1. Any on-path observer (LAN, proxy, mirror port) captures the
  bearer token and replays it against Flight SQL/Polaris/S3 as that user. full per-user data access. Distinct
  from the Flight SQL TLS resolved in prior audits: the Quack path has none, and no config gate even exists. The
  client even accepts a `quacks:`/`https:` scheme (`client.rs:231-244`) implying TLS the server never provides.
- **Fix:** Reuse `[coordinator.tls]` to wrap the Quack listener (axum-server with rustls), and refuse to start
  the Quack endpoint over plaintext unless an explicit `allow_insecure` flag is set.
- **Effort:** medium

---

### QUACK-06 — high — Worker Flight `do_exchange` skips the worker-secret check (shuffle injection / result poisoning)

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-worker/src/flight_service.rs:420-561` (`do_exchange`, no `verify_worker_secret`), contrast `do_get` at `:208` and `refresh_credentials` at `:342`
- **Evidence:**
  ```rust
  // flight_service.rs:420-424 — straight to into_inner(), no secret check
  async fn do_exchange(&self, request: Request<Streaming<FlightData>>) -> ... {
      let mut stream = request.into_inner();
      let first_msg = stream.next().await ...
  ```
- **Impact:** `do_get` and `do_action("refresh_credentials")` both gate on `verify_worker_secret`, but
  `do_exchange` does not. An attacker with network access to a worker can open a shuffle stream and push arbitrary
  `RecordBatch`es into a stage's receiver (`sender.send(batch)` at `:503`), poisoning aggregate/join results for
  an in-flight distributed query, or drain the partition channel. Caveat keeping it `high`: the attacker must
  guess a live `query_id`+`stage_id` (typically UUIDs), and it is moot when `worker_secret` is empty
  (already-warned unauthenticated mode). Still breaks the trust boundary the other two handlers enforce.
- **Fix:** Call `self.verify_worker_secret(request.metadata())?` at the top of `do_exchange`, before
  `into_inner()`, exactly as `do_get` does.
- **Effort:** trivial

---

### QUACK-07 — high — Worker Flight service has no TLS: S3 credentials + worker secret in cleartext

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-worker/src/main.rs:86-89` (no `.tls_config`); secret as gRPC metadata header `x-sqe-worker-secret` (`flight_service.rs:43,159-162`); S3 creds in the ticket (`executor.rs:88-94`)
- **Evidence:**
  ```rust
  // main.rs:86-89 — no TLS on the worker listener
  tonic::transport::Server::builder()
      .add_service(flight_service.into_server())
      .serve(addr).await?;
  ```
- **Impact:** The worker's `do_get` ticket carries the user's live S3 access key, secret, and session token, and
  `refresh_credentials` carries fresh STS creds. The shared `worker_secret` travels as a plaintext metadata
  header. With no TLS, an on-path observer harvests both the S3 credentials (read/write the data lake as that
  user) and the worker secret (then drive `do_get`/`refresh_credentials` directly). Distinct from the resolved
  Flight SQL coordinator TLS: the worker has no TLS config path at all.
- **Fix:** Add a worker TLS config block and wrap the tonic server with `.tls_config(...)` (rustls), refusing
  plaintext unless an explicit insecure flag is set. At minimum, document and WARN loudly at startup.
- **Effort:** medium

---

### QUACK-08 — medium — No rate limiting / brute-force protection on the Quack auth endpoint

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/bin/sqe_server.rs:927-938` (no rate limiter), contrast Flight at `:946-950`. Auth in `crates/sqe-quack-server/src/app.rs:249-292`
- **Evidence:**
  ```rust
  // sqe_server.rs:927-929 — no governor / rate limiter layer
  let quack_state = sqe_quack_server::QuackServerState::new(Arc::clone(&auth_chain), executor);
  let quack_app = sqe_quack_server::router(quack_state);
  ```
- **Impact:** Prior audits added `governor` rate limiting to the Flight and Trino auth paths. The Quack
  `ConnectionRequest -> auth_provider.authenticate` path has none, so it is an un-throttled oracle for
  bearer-token/credential brute force and an amplification point for auth-provider load (each attempt may hit the
  OIDC introspection endpoint). Combined with QUACK-05 (no TLS), the weakest auth surface in the system.
- **Fix:** Wrap the Quack router in the same `tower-governor` layer used for Flight/Trino, keyed on peer IP, with
  a stricter bucket on `ConnectionRequest`.
- **Effort:** small

---

### QUACK-09 — medium — Integer-cast truncation of wire counts/sizes (`as usize`/`as u16`) hides mismatches and under-allocates

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/codec.rs:237-247` (`read_u8`/`read_u16`/`read_u32` truncate a `u64` varint), `data_chunk.rs:369-376`, `:764-768`, `message.rs:299-304`
- **Evidence:**
  ```rust
  // codec.rs:237-239 — a u64 varint of 0x1_0000_0001 silently becomes 1
  pub fn read_u8(&mut self) -> crate::Result<u8> { self.read_u64().map(|v| v as u8) }
  ```
- **Impact:** `read_u8`/`read_u16`/`read_u32` accept any 10-byte varint and truncate, so a field that "should" be
  a small count can be encoded as a huge varint whose low bits match an expected value, defeating count
  cross-checks. Truncation can make a validated-small count later index against a buffer sized for the real large
  count, feeding QUACK-03/04. On 32-bit targets `len as usize` truncation of a 64-bit length is an
  under-allocation -> OOB.
- **Fix:** In `read_u8`/`read_u16`/`read_u32`, reject values that don't fit
  (`u8::try_from(v).map_err(|_| WireError::VarintOverflow)`). Compare counts as `usize` without lossy `as u16`.
- **Effort:** small

---

### QUACK-10 — medium — Quack server pre-auth decode amplification: full body parse for every unauthenticated request

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-server/src/app.rs:72-97` (decode before any auth check), `:57-62` (no `DefaultBodyLimit` override)
- **Evidence:**
  ```rust
  // app.rs:76 — decode runs on the raw body for any unauthenticated caller
  let (request_header, request_body) = match decode_message(&body) { ... };
  ```
- **Impact:** Every weakness in QUACK-01/02/03/04/09 is exploitable before authentication because
  `decode_message` runs on the raw body for all message types, including the dangerous `DataChunk` path. The only
  backstop is axum's implicit 2 MB default body limit, more than enough to encode a stack-overflow recursion or
  multi-GB allocation count. No "authenticate the header before decoding the body" split.
- **Fix:** Decode only the header first, reject response (server-only) message types up front, require a valid
  session for body types that carry a `DataChunk`, and set an explicit `DefaultBodyLimit::max(...)`.
- **Effort:** small

---

### QUACK-11 — low — `unreachable!()` in width dispatch relies on an invariant decode does not enforce

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/arrow_bridge.rs:139`, `:786`, `:919`, `:1516`
- **Evidence:**
  ```rust
  // arrow_bridge.rs:1516
  4 => Ok(Arc::new(... UInt32Type ...)),
  _ => unreachable!(),
  ```
- **Impact:** These `unreachable!()` arms are sound today because `enum_physical_width`/`decimal_physical_width`
  only return known widths. The risk is latent: a future change to either width function turns a wire-controlled
  type into a guaranteed process-abort panic with no error path. Defense-in-depth only; not currently reachable.
- **Fix:** Replace the `unreachable!()` arms with `return Err(WireError::UnsupportedLogicalType(...))`.
- **Effort:** trivial

---

### QUACK-12 — low — `remaining()` and `read_u16_le` rely on `self.buf.len() - self.pos` not underflowing

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-quack-wire/src/codec.rs:163-165` (`remaining`), `:167-184`, `:197-205`, `:299-340`
- **Evidence:**
  ```rust
  // codec.rs:163-168
  pub fn remaining(&self) -> &'a [u8] { &self.buf[self.pos..] }
  if self.buf.len() - self.pos < 2 { return Err(...UnexpectedEof); }
  ```
- **Impact:** The decoder's bounds checks are written as `self.buf.len() - self.pos < N`, correct only because
  `pos` is never advanced past `buf.len()`. The invariant is implicit: any future read helper that advances `pos`
  without a preceding check turns these subtractions into a `usize` underflow panic (debug) or a wildly large
  value that defeats the check (release) -> OOB. Latent maintainability/robustness hazard in the most
  security-sensitive primitive.
- **Fix:** Use `self.buf.len().checked_sub(self.pos)` or compare with `self.pos + N > self.buf.len()` (checked
  add); add a debug-assert that `pos <= buf.len()` at the top of each reader.
- **Effort:** trivial
