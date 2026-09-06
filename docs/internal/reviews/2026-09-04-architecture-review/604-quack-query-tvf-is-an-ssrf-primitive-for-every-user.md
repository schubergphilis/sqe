# 604: quack_query() is registered in every session and lets any user make the coordinator POST to arbitrary URLs, blocking a runtime thread and materialising the reply unbounded

- Scope: sqe-quack-client (tvf.rs, tableprovider.rs, client.rs), sqe-coordinator (session_context.rs)
- Severity: high
- Category: security
- Effort: S (hours) for the gate, M (days) for a safe version
- Status: open
- Issue: #611

## Problem

`crates/sqe-coordinator/src/session_context.rs:915-918` registers
`sqe_quack_client::QuackQueryTvf` as the table function `quack_query` in every
session context, with no config flag, allow-list, or role check.

The TVF executes at planning time:

- `crates/sqe-quack-client/src/tvf.rs:59` calls `QuackTableProvider::new(&uri, token, &sql)`
  from `TableFunctionImpl::call`, which is a synchronous trait method invoked
  inside the coordinator's tokio runtime.
- `crates/sqe-quack-client/src/tableprovider.rs:40-42` connects and runs the
  remote query with `reqwest::blocking::Client`
  (`crates/sqe-quack-client/src/client.rs:58-59`, 60 second timeout). The
  calling tokio worker thread is blocked for the whole round trip.
- The full result is held as `Vec<RecordBatch>` in the provider
  (`tableprovider.rs:32-33`) with no row or byte cap, then served via
  `MemTable` (`tableprovider.rs:75`).
- The reply is decoded by `sqe-quack-wire`'s arrow bridge, which panics on
  hostile input (issue 605).

What an ordinary authenticated user can therefore do with one `SELECT`:

- `SELECT * FROM quack_query('http://169.254.169.254/latest/...', 'x')`:
  reach the cloud metadata service, internal Polaris, Ranger, Keycloak admin
  ports, or any pod the coordinator can route to. The request body is a Quack
  frame, but the target sees a POST to `/quack` from the coordinator's identity
  and network position, and connection-level side effects and timing leak
  regardless of the reply parsing.
- Pass a bearer token as a SQL string literal; it lands in the audit log query
  text unless `redact_pii` happens to match it.
- Point it at a slow or malicious Quack server: 60 seconds of a runtime thread
  per call, or an unbounded allocation, or a decoder panic taking the
  coordinator down.

This is the same class of bug as the `read_parquet` table-valued function that
reached `/etc/shadow` and IMDS (closed in !190); the fix pattern there was to
gate and constrain the TVF.

## Why it matters

The coordinator holds the most privileged network position in the deployment
(catalog, object store, policy backend, IdP). A TVF that turns SQL into
outbound HTTP from that position is a textbook SSRF, and it is on by default.

## Proposed change

1. Immediate: register `quack_query` only when `[quack.client] enabled = true`
   (default false), and only for sessions whose identity has a configured
   role (`quack.client.allowed_roles`). Reuse the same gate that read_parquet
   ended up with.
2. Constrain the target: `quack.client.allowed_hosts` (exact host:port list),
   refuse literal IPs in link-local/loopback/private ranges unless listed,
   refuse redirects (`reqwest::redirect::Policy::none()`).
3. Never take a token from SQL. Forward the session's own bearer token
   (`Session.bearer_token`) or a configured service token; remove the 3-arg
   form.
4. Move the network call out of planning: make the provider lazy, do the
   request in `scan()` via `tokio::task::spawn_blocking` (or switch to the
   async reqwest client) and stream batches with a `quack.client.max_result_bytes`
   cap enforced while decoding.

## Acceptance criteria

- With default config, `SELECT * FROM quack_query('quack:localhost:9495','SELECT 1')`
  fails with "function not found" or a policy error.
- With the feature enabled and an allow-list of `quack-peer:9495`, a call to
  `169.254.169.254` is rejected before any socket is opened (unit test on the
  host check).
- A test with a stub server that sleeps 5 s shows other queries on the same
  coordinator are not delayed (no runtime-thread starvation).
- A stub server returning 1 GiB of rows fails the query with a size-cap error
  and coordinator RSS stays bounded.

## Related

- Memory: "Book anecdote: TVF IMDS pivot" (read_parquet, closed !190).
- Issue 605 (decoder robustness), 606 (server-side Quack session model).
- 200-series review for the MCP read-only gate: `quack_query` also bypasses
  any "read-only SQL" classification that inspects table names.
