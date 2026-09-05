# 606: Quack server sessions are bearer UUIDs that outlive the token, results are fully materialised with no cap, and the policy-denial masking branch is dead

- Scope: sqe-quack-server (app.rs, session.rs, query_executor.rs), sqe-coordinator (quack_executor.rs)
- Severity: medium
- Category: security
- Effort: M (days)
- Status: open

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
