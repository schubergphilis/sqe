# Findings — Web UI & Flight SQL surface (`sqe-coordinator` web_ui + flight_sql)

**Scope:** The brand-new read-only ops dashboard (`web_ui.rs`, `web_ui/dashboard.html`,
`metrics_history.rs`, `query_tracker.rs`), the route/binding wiring (`bin/sqe_server.rs`,
`sqe-core/src/config.rs` `[metrics] web_ui`), and the Flight SQL protocol surface (`flight_sql.rs`).
Traced the actual router: there is **no auth middleware/layer or tonic interceptor** on either the
web/health server or the Flight service. auth on Flight is purely per-handler via
`get_session_from_request`, and the web/health server has none. The XSS escaping (`esc()`) is applied at
every engine-data sink and is correct (WEB-06). CORS absence is secure-by-default and not a finding.

> **WEB-03 independently verified by the dispatcher** against `flight_sql.rs:1692-1706` (no
> `get_session_from_request` before the unauth `prepared_params.insert`).

---

### WEB-01 — high — Web UI dashboard + all `/api/v1/*` JSON endpoints served unauthenticated on `0.0.0.0`, default ON

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/bin/sqe_server.rs:288-312`, `:307`, `crates/sqe-core/src/config.rs:1601-1602`, `:1617`
- **Evidence:**
  ```rust
  // sqe_server.rs:288-304 — no .layer()/interceptor, no auth on any route
  fn start_health_server(port: u16, state: Arc<HealthState>) {
      let mut app = Router::new()
          .route("/healthz", get(healthz))
          .route("/readyz", get(readyz))
          .route("/api/v1/status", get(cluster_status));
      if state.web_ui {
          app = app
              .route("/", get(dashboard))
              .route("/api/v1/overview", get(api_overview))
              .route("/api/v1/queries", get(api_queries))
              .route("/api/v1/queries/{id}", get(api_query_detail))
              .route("/api/v1/workers", get(api_workers))
              .route("/api/v1/metrics/history", get(api_metrics_history));
      }
  // sqe_server.rs:307
      let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
  ```
  ```rust
  // config.rs:1601-1602 — default ON; comment admits "The UI has no auth."
  #[serde(default = "default_true")]
  pub web_ui: bool,
  ```
- **Impact:** With `web_ui = true` by default, anyone who can reach `0.0.0.0:<prometheus_port+1>` (9091 by
  default) over the network gets the full dashboard plus every JSON API with no credential. The design accepts
  "network-gated, no auth," but the defaults make that gating fragile: binds to all interfaces, on by default,
  exposes every user's query SQL, usernames, worker topology, and cluster metrics. Any Docker/K8s deployment
  that maps or exposes that port (or any flat internal network) leaks cross-tenant operational data without
  authenticating.
- **Fix:** Default `web_ui = false`. When enabled, require an auth layer (reuse the bearer/JWT chain via an
  axum middleware that calls the same `AuthProvider`), or at minimum bind the web-UI routes to `127.0.0.1` by
  default with an explicit `bind_address` config and a startup WARN when bound to `0.0.0.0`.
- **Effort:** medium

---

### WEB-02 — high — Full query SQL (may embed literal PII/secrets) + usernames exposed on the unauthenticated dashboard

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/web_ui.rs:226` (and `:186-195`, `:222`), `:309-314`
- **Evidence:**
  ```rust
  // web_ui.rs:222-226 — list item carries user + (truncated) full SQL
  user: r.user.clone(),
  source: r.source.clone(),
  sql: truncate_sql(&r.sql),
  ```
  Commit `65f92cb` ("drop PII from query detail") removed `session_id` / `client_ip` / `roles` from
  `QueryDetail` (web_ui.rs:285-287 comment), but `user` and the raw `sql` remain in both `QueryListItem` and
  `QueryDetail` (SQL truncated to 512 chars, `web_ui.rs:184`).
- **Impact:** The `65f92cb` mitigation is incomplete. the most sensitive field, the SQL statement itself, is
  still surfaced to the unauthenticated viewer. SQL routinely embeds literal PII and secrets in predicates and
  inserts (`WHERE email = 'jane@x.com'`, `WHERE national_id = '...'`, presigned URLs, tokens passed as literals).
  An attacker on the web-UI port harvests these across all users via `/api/v1/queries` and
  `/api/v1/queries/{id}`. Stands as its own issue even if WEB-01 auth is added, because a low-privilege
  authenticated viewer would still see every other user's SQL.
- **Fix:** Do not expose raw SQL on this no-auth/low-privilege endpoint. Either redact literals, expose only the
  query-shape/digest already stored as the audit SHA-256 hash, or gate full-SQL view behind real per-user
  authorization (only the submitting user or an admin role). At minimum, drop `sql` from the list endpoint and
  require ownership/admin for detail.
- **Effort:** medium

---

### WEB-03 — high — `do_put_prepared_statement_query` and `do_action_close_prepared_statement` skip authentication

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/flight_sql.rs:1692-1722` (put), `:1787-1795` (close), contrast `:1729`, `:1352`
- **Evidence:**
  ```rust
  // flight_sql.rs:1692-1706 — no get_session_from_request; unauth write to shared map
  async fn do_put_prepared_statement_query(
      &self,
      query: CommandPreparedStatementQuery,
      request: Request<PeekableFlightDataStream>,
  ) -> Result<DoPutPreparedStatementResult, Status> {
      let handle_bytes: Vec<u8> = query.prepared_statement_handle.to_vec();
      let stream = request.into_inner();
      match decode_parameter_stream(stream).await {
          Ok(params) if !params.is_empty() => {
              self.prepared_params.insert(handle_bytes.clone(), params);
  ```
  Every other do_get/do_put/do_action handler authenticates (e.g. `:1729 let session =
  self.get_session_from_request(&request).await?;`). There is **no** global tonic interceptor or `.layer()` auth
  on the Flight service, so the missing per-handler call is a real gap.
- **Impact:** Parameter poisoning across sessions. The prepared-statement handle is deterministic:
  `do_action_create_prepared_statement` builds it as `FetchResults { handle: sql }.encode_to_vec()`
  (flight_sql.rs:1769-1772), with no nonce and no session binding. For any known/guessable victim query (common
  app or dbt SQL), an attacker computes the exact handle bytes and, **without authenticating**, writes
  attacker-chosen parameter literals into `self.prepared_params`. When the victim's authenticated
  `do_get_prepared_statement` runs, it does `prepared_params.remove(&key)` and substitutes those attacker values
  into the victim's SQL, executed under the victim's session (flight_sql.rs:1362-1375). Single-quote escaping
  prevents literal breakout, but the attacker still controls predicate/insert values, so the victim runs with
  attacker-chosen filters or writes attacker-chosen rows. The close handler likewise lets any unauthenticated
  peer evict any handle.
- **Fix:** Call `self.get_session_from_request(&request).await?` at the top of both handlers, and bind
  prepared-statement params to the authenticated session: namespace the `prepared_params` key by `session` (or a
  server-generated random handle id stored per session) instead of by client-supplied handle bytes.
- **Effort:** small

---

### WEB-04 — medium — `prepared_params` is an unbounded `DashMap` writable by unauthenticated clients

- **Dimension:** reliability
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/flight_sql.rs:375`, `:397`, `:1706`
- **Evidence:**
  ```rust
  // flight_sql.rs:375 — no cap, no TTL
  prepared_params: Arc<dashmap::DashMap<Vec<u8>, Vec<String>>>,
  // flight_sql.rs:1706 — unauth insert (see WEB-03), key = client-supplied handle bytes
  self.prepared_params.insert(handle_bytes.clone(), params);
  ```
- **Impact:** Entries are only removed on a matching `do_get_prepared_statement` or
  `do_action_close_prepared_statement` for the exact handle. Because the insert is unauthenticated (WEB-03) and
  keyed by arbitrary client-supplied bytes, an attacker loops `do_put_prepared_statement_query` with unique
  handles + large parameter vectors and never fetches them. The map grows without bound, no cap/TTL/eviction,
  leading to coordinator memory exhaustion / OOM. Survives even after WEB-03 adds auth, because any authenticated
  user (or buggy client that abandons prepared statements) leaks entries indefinitely.
- **Fix:** Replace the raw `DashMap` with a bounded, TTL'd cache (the codebase already uses `moka`) keyed per
  session; expire abandoned binds after a short TTL and cap total entries.
- **Effort:** small

---

### WEB-05 — low — Web/health/metrics server has no rate limiting and no security response headers

- **Dimension:** security
- **Status:** NEW surface
- **Location:** `crates/sqe-coordinator/src/bin/sqe_server.rs:288-312`, `:280-286`
- **Evidence:**
  ```rust
  // sqe_server.rs:280-285 — dashboard sets only Content-Type
  async fn dashboard() -> Response {
      ([(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
       sqe_coordinator::web_ui::DASHBOARD_HTML).into_response()
  }
  ```
  The Flight path and Trino HTTP path both wire `governor`-based limiters; the web/health server wires none.
- **Impact:** Defense-in-depth gap. The JSON endpoints (`/api/v1/queries`, `/api/v1/metrics/history`) iterate
  the full query history per request with no per-client throttle, so an unauthenticated peer (given WEB-01
  exposure) can poll them to amplify CPU/allocation load. Missing `X-Content-Type-Options: nosniff` and a
  restrictive `Content-Security-Policy` are minor hardening gaps; the XSS sinks are already neutralized by
  `esc()`, so impact is low.
- **Fix:** Add a lightweight per-IP rate limit layer to the web/health router (or reuse the existing governor
  middleware), and attach `X-Content-Type-Options: nosniff` plus a tight `Content-Security-Policy` on the
  dashboard response.
- **Effort:** small

---

### WEB-06 — info — XSS escaping in the dashboard verified correct (no finding)

- **Dimension:** security
- **Status:** NEW surface (verified safe)
- **Location:** `crates/sqe-coordinator/src/web_ui/dashboard.html:158-159`, sinks at `:300`, `:355-357`, `:394-396`, `:449-453`, `:467-484`, `:499-500`
- **Evidence:**
  ```javascript
  // dashboard.html:158-159 — escapes the five HTML-significant chars
  function esc(s){ return String(s==null?"":s).replace(/[&<>"']/g, c=>(
    {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c])); }
  // dashboard.html:451 — SQL rendered into both text and title attribute via esc()
  `<td title="${esc(q.sql)}">${esc(q.sql)}</td>`+
  ```
- **Impact:** None. Every interpolation of engine-controlled data (SQL text, usernames, states-via `stClass`
  which strips to `[A-Z]`, worker URLs, task ids, error messages, query ids) passes through `esc()`. Unescaped
  `fill=`/`stroke=`/`style=width:` interpolations take only hardcoded CSS constants and numeric values coerced
  with `|0`/`.toFixed()`. Stored XSS (a query containing `<script>`) does not execute. Reported as verified-safe
  so triage does not chase it.
- **Fix:** None required. Keep `esc()` as the single sink-escaping path.
- **Effort:** trivial
