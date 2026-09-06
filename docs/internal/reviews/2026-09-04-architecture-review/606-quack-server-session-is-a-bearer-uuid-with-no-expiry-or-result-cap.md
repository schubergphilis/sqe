# 606: Quack server sessions are bearer UUIDs that outlive the token, results are fully materialised with no cap, and the policy-denial masking branch is dead

- Scope: sqe-quack-server (app.rs, session.rs, query_executor.rs), sqe-coordinator (quack_executor.rs)
- Severity: medium
- Category: security
- Effort: M (days)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #613

## Problem

Three independent gaps in the DuckDB-compatible endpoint.

**1. Session model.** After `ConnectionRequest` authenticates the bearer token
once, the server returns a random `connection_id`
(`crates/sqe-quack-server/src/app.rs`, `handle_connection_request`) and every
later request is authorised by that id alone:
`app.rs:239` (`state.sessions.get(&request_header.connection_id)`). The
store is a moka cache with `time_to_idle(600s)` and no time-to-live
(`crates/sqe-quack-server/src/session.rs:52`). Consequences:

- A session that receives one query every nine minutes lives forever, long
  after the OIDC token it was minted from has expired. `identity_to_core_session`
  copies `expires_at` into the core session (`session.rs:20-27`) but nothing on
  the request path checks it; the executor only maps a mid-query auth failure
  to an error (`quack_executor.rs:50`).
- The `connection_id` travels in every request body over the same HTTP channel
  as the query; without TLS it is a replayable bearer credential. There is no
  binding to client IP or to the original token.

**2. Result size.** `QueryExecutor::execute` returns `Vec<RecordBatch>`
(`crates/sqe-quack-server/src/query_executor.rs`, trait doc: "Returns all
result batches in memory; result streaming follows in a later iteration").
`handle_prepare_request` encodes every batch into a single `PrepareResponse`
with `needs_more_fetch: false` (`app.rs:346`). There is no row cap, byte cap, or
statement timeout on this path, while the Flight SQL path streams. A
`SELECT * FROM lineitem` over Quack holds the whole result on the coordinator
heap, then a second encoded copy, then the HTTP body.

**3. Dead masking branch.** `app.rs:263-270` maps `QueryError::Policy` to the
generic "SQE-POLICY: access denied" so denial reasons do not leak. But the
production mapper never produces that variant: `sqe_error_to_query_error`
(`crates/sqe-coordinator/src/quack_executor.rs:50-75`) routes every
`SqeError`, including `Sourced { code: AccessDenied }`, to
`QueryError::Execution(msg)`. Clients therefore receive
`SQE-EXEC: <full policy/catalog message>`. Either the masking is wanted (then
it is broken) or it is not (then the branch is misleading); today the code
claims one thing and does another.

## Why it matters

The Quack endpoint is a second front door with weaker session semantics than
Flight SQL: a leaked connection id gives indefinite access, an ordinary query
can OOM the coordinator, and the policy-denial hygiene the code advertises is
not applied.

## Proposed change

1. `SessionStore`: add `time_to_live` equal to the shorter of a configured
   maximum (`quack.session_max_secs`) and the token's `expires_at`; on each
   request compare `core_session.token_expiry` with `Utc::now()` and return
   `SQE-AUTH: session expired` when past. Store a hash of the client key from
   `client_key()` and reject requests from a different key unless
   `security.trust_forwarded_for` says otherwise.
2. Implement the Quack `FetchRequest`/`FetchResponse` pair the protocol already
   defines (`MessageType::FetchResponse` is listed as server-only in
   `app.rs:100-110`). Return the first N rows in `PrepareResponse` with
   `needs_more_fetch: true` and stream the rest per fetch, bounded by
   `quack.max_result_bytes` per response. Wire the coordinator's existing
   streaming executor instead of `QueryHandler::execute`'s collecting form.
3. In `sqe_error_to_query_error`, map `SqeErrorCode::AccessDenied` and the
   policy-denial variants to `QueryError::Policy(msg)` so the masking branch is
   live, and add a test that asserts the wire message is the generic one.
4. Emit the same `AuditEvent` for Quack queries as for Flight SQL (verify in
   `QueryHandler::execute` that the audit call is on this path; if it is not,
   add it).

## Acceptance criteria

- Test: a session created with a token that expires in 1 s is refused after
  2 s even if polled every 500 ms.
- Test: a 10M-row query over Quack completes via multiple fetches and peak
  coordinator RSS stays under the configured cap.
- Test: a deny-all policy yields exactly `SQE-POLICY: access denied` on the
  wire and the detailed reason appears only in the server log.

## Related

- Issue 604 (client side of the same protocol), 605 (decoder).
- 200-series review (sqe-auth session manager) for the Flight SQL session TTL
  semantics that Quack should mirror.

## Resolution (2026-09-05)

All three gaps closed on `fix/identity-transport-trust-p1`.

### 1. Session model

`crates/sqe-quack-server/src/session.rs`

- `SessionStore::new(idle_timeout, max_lifetime)` now sets a moka `time_to_live`
  alongside the existing `time_to_idle`. Defaults: 600 s idle, 8 h absolute
  (`DEFAULT_SESSION_IDLE`, `DEFAULT_SESSION_MAX_LIFETIME`), overridable per
  deployment via `QuackServerState::with_session_limits(idle, max)`.
- `Session` gained `client_key: String`, the resolved client IP that
  `client_key()` computes in `app.rs`. `handle_connection_request` stamps it at
  connect time.
- New `SessionStore::get_authorized(connection_id, client_key) ->
  Result<Session, SessionRejection>` with
  `SessionRejection::{Unknown, Expired, ClientMismatch}`. `Expired` fires when
  `core_session.token_expiry() <= Utc::now()` and invalidates the entry on the
  way out, so the token's own `exp` — not a cache window — is what actually
  bounds the session. `ClientMismatch` logs at `warn` with both keys and returns
  the *same* wire string as `Unknown`, so the endpoint is not an oracle for
  which connection ids exist.
- `crates/sqe-quack-server/src/app.rs`: the `x-forwarded-for` resolution moved
  out of the `ConnectionRequest` arm into `handle_quack`, so Prepare, Fetch and
  Disconnect all resolve a client key and all go through the new `authorize()`
  helper. Wire strings: `SQE-AUTH: session expired` for `Expired`,
  `SQE-AUTH: unknown connection_id` for `Unknown` and `ClientMismatch`.

### 2. Result streaming and caps

- `crates/sqe-quack-server/src/query_executor.rs`: `QueryExecutor::execute` now
  returns `QueryStream { schema: SchemaRef, batches: BoxStream<'static,
  Result<RecordBatch, QueryError>> }` instead of `Vec<RecordBatch>`.
  `QueryStream::from_batches` keeps already-materialised results (test stubs,
  the DML fallback) a one-liner. `futures` added to the crate's dependencies.
- `crates/sqe-quack-server/src/pending.rs` (new): `PendingResults`, a moka cache
  (`time_to_idle` 5 min) keyed by a random `i128` result uuid, holding the
  owning `connection_id`, the column names, a `tokio::Mutex<Option<BoxStream>>`,
  and running byte / page counters. `remove_for_connection` drops every parked
  stream of a session.
- `handle_prepare_request` pulls batches until the stream ends or the page's
  accumulated `get_array_memory_size()` reaches `limits.max_response_bytes`
  (default 8 MiB). Stream ended -> reply as before (`needs_more_fetch: false`,
  `result_uuid: 0`). Otherwise the remainder is parked and the response carries
  `needs_more_fetch: true` plus the uuid.
- New `FetchRequest` arm: authorises the session, requires the uuid's stored
  `connection_id` to match, pulls one more capped page, and replies
  `FetchResponse { results, batch_index: Some(page_no) }`. When the stream is
  exhausted the entry is removed and the reply carries an empty `results` — the
  end-of-result signal `sqe-quack-client` already relies on.
- `limits.max_result_bytes` (default 256 MiB) is charged across prepare + every
  fetch; exceeding it drops the pending entry and returns
  `SQE-EXEC: result exceeds <n> bytes; narrow the query`. Both bounds are
  settable via `QuackServerState::with_result_limits(response, total)`.
- Pending entries are also dropped on `DisconnectMessage` and whenever
  `authorize()` sees an `Expired` session.
- `result_types` now derive from the *stream schema*, so a zero-row result still
  reports its columns. `crates/sqe-quack-wire/src/arrow_bridge.rs` gained
  `arrow_type_to_logical(&DataType)` and `arrow_schema_to_logical(&Schema)`
  (the mapping previously existed only inline inside `column_to_vector`, fused
  with value encoding). `Dictionary` (DuckDB `ENUM`) is not derivable from the
  type alone — its dictionary values live in the array — so the server falls
  back to the first encoded chunk's types in that one case.
- `crates/sqe-coordinator/src/quack_executor.rs`: `CoordinatorExecutor` now
  calls `QueryHandler::execute_stream(session, sql, None)` and maps stream-item
  `DataFusionError`s to `QueryError::Execution`. `execute_stream` serves
  `SELECT` only and returns `SqeError::NotImplemented` for anything else, so
  that one variant falls back to the buffered `execute` (its own doc comment
  prescribes this) and DML/DDL over Quack keeps working.

### 3. Policy-denial masking

`sqe_error_to_query_error` now routes `SqeError::Sourced { code:
SqeErrorCode::AccessDenied, .. }` to `QueryError::Policy`, which is what makes
the server's `SQE-POLICY: access denied` branch live. `InternalError` still maps
to `Internal`; every other code still maps to `Execution`.

### Audit

`QueryHandler::execute_stream` **does** emit the same `AuditEvent` as `execute`:
it hands the actor, resources, policy summary and timing to
`crate::streaming::StreamFinalizer`, which logs the `AuditKind::Query` event
when the stream ends (EOF, error, or client drop) rather than at call return.
No change was needed, and `query_handler.rs` was not touched.

### Tests

- `crates/sqe-quack-wire/src/arrow_bridge.rs`:
  `schema_derived_types_match_value_derived_types`,
  `empty_schema_still_yields_column_types`,
  `dictionary_is_rejected_by_the_type_only_path`.
- `crates/sqe-quack-server/src/session.rs`: `unknown_connection_id_is_rejected`,
  `expired_token_is_rejected_and_invalidated`, `client_key_mismatch_is_rejected`.
- `crates/sqe-quack-server/tests/session_limits.rs` (new):
  `polling_does_not_keep_an_expired_session_alive` (1 s token, polled every
  500 ms for 2 s), `prepare_from_a_different_client_key_is_refused`.
- `crates/sqe-quack-server/tests/fetch.rs` (new):
  `large_result_pages_through_multiple_fetches` (200 x 1 000 Int32 rows under a
  64 KiB response cap; concatenation equals the input, terminal fetch empty),
  `total_result_cap_aborts_and_drops_the_pending_entry`,
  `small_result_is_still_inlined_with_no_pending_entry`,
  `zero_row_result_still_reports_names_and_types`,
  `fetch_rejects_unknown_and_foreign_result_uuids`,
  `disconnect_drops_parked_results`.
- `crates/sqe-coordinator/src/quack_executor.rs`: `access_denied_maps_to_policy`,
  `other_sourced_codes_keep_their_mapping`.
- The pre-existing `executor_policy_error_does_not_leak_reason_to_client` in
  `tests/prepare.rs` still asserts the exact `SQE-POLICY: access denied` string
  and stays green.

### Not done

The acceptance criterion "peak coordinator RSS stays under the configured cap on
a 10M-row query" is asserted structurally (per-response byte cap plus a total
cap, with the stream parked rather than materialised) rather than by an RSS
measurement; a real RSS assertion needs the docker integration rig, not a unit
test.
