# Dashboard Auth Gate + Operational-Logging Polish (Sub-project C)

- Date: 2026-06-21
- Status: Approved design, pre-implementation
- Author: Jacob Verhoeks
- Scope: Sub-project C of the audit/logging effort. A (canonical OCSF audit) is merged on `main`; B (OTLP SIEM export) is in MR !400 and this branch stacks on it. C gates the operator dashboard and finishes the operational-logging threading.

## Context

The coordinator serves an operator dashboard and JSON API on the health port (`prometheus_port + 1`, bound `0.0.0.0`) from `crates/sqe-coordinator/src/bin/sqe_server.rs` + `web_ui.rs`. Routes split into two groups:

- Always served: `/healthz`, `/readyz`, `/api/v1/status`. Kubernetes liveness/readiness and load balancers depend on these.
- Served only when `[metrics] web_ui = true` (default false): `/`, `/api/v1/overview`, `/api/v1/queries`, `/api/v1/queries/{id}`, `/api/v1/workers`, `/api/v1/metrics/history`.

The `web_ui` group is fully unauthenticated. A startup WARN documents that it must be network-gated. The data exposed is operational metadata: `QueryListItem`/`QueryDetail` deliberately omit raw SQL, username, `client_ip`, and roles (the WEB-02 mitigation), exposing `query_id`, `state`, `sql_hash`, timings, stats, and `error_type`/`error_message`.

Premise correction (important): earlier sub-project-A documents justified WEB-01 as "A's identity enrichment exposes subject/email/groups on this endpoint." That is false. Sub-project A enriched `AuditEvent.Actor` and `SessionUser`, never `QueryRecord` or `web_ui.rs`. The endpoint never exposed identity. The real WEB-01 risk is unauthenticated cluster/query metadata (and `error_message`, which can leak schema or data) on `0.0.0.0` when explicitly enabled.

Operational logging: tracing is initialized in `sqe-metrics/src/otel.rs`; spans use `#[tracing::instrument]` with fields like `username`, `session_id`, `query_id`, `client_ip`. `client_ip` is extracted per-request in `flight_sql.rs::extract_client_ip` but is then dropped: `QueryTracker::start` receives `None` (`query_handler.rs:715,1934`), `StreamFinalizer` has no `client_ip` field and its audit events emit `None` (`streaming.rs:261,335,394`), and maintenance/startup events emit `None`. `QueryRecord` carries a `client_ip` field that is never populated.

Verified facts the design relies on:
- Admin notion already exists: a role name containing "admin"/"owner" (and "write" for write-capability) in `query_handler.rs::require_admin` and `maintenance.rs` (`authorize_or_deny` / `session_write_privilege`). C reuses this; it does not invent a new role.
- `sqe_auth::AuthProvider::authenticate(&FlightCredentials) -> Result<Identity, AuthError>`. `FlightCredentials` is constructable from a bearer token string, so an axum guard can build it from the `Authorization` header.
- `QueryHandler::execute` and `execute_stream` both take `&Session`, and the `bearer_provider`/`session_manager` are built in `run_coordinator` before the health server starts (available to wire into `HealthState`).

## Goals

1. Authenticate the `web_ui` dashboard routes: a valid bearer token plus the existing admin role. Health/readiness/status routes stay open.
2. Audit dashboard access (success and denial) so privileged-data viewing is itself recorded.
3. Once gated, expose the richer fields to authenticated admins: username, roles, `client_ip`, and the SQL (with the same audit redaction applied, so secrets/PII are not shown raw).
4. Thread `client_ip` from the request to the emit sites so audit events, the query tracker, and spans carry it.
5. Make the key structured-logging fields consistent (`username`, `session_id`, `query_id`, `client_ip`).

## Non-goals (deferred)

- Cookie/session-based browser login flows, CSRF, or a login page. The gate is bearer-token only (the same credential the Flight SQL clients use); a browser user supplies it via an `Authorization` header or a reverse proxy.
- TLS on the health port (kept as an infra/proxy concern, unchanged).
- A heavyweight logging-context struct. Field consistency is achieved by convention plus targeted fixes, not a new abstraction.
- Showing un-redacted raw SQL. Even admins see the audit-redacted SQL.

## Decisions

Settled during brainstorming:

- Gate the `web_ui` routes with bearer + the existing admin role. Do NOT gate `/healthz`, `/readyz`, `/api/v1/status`.
- Reuse the existing admin-role determination; factor it into one shared helper so the dashboard and `require_admin` agree.
- Show richer fields (username, roles, `client_ip`, redacted SQL) to authenticated admins. The SQL is shown with the same `redact_pii` pass the audit log uses; raw SQL is never shown.
- `client_ip` is threaded per-request as an explicit parameter into `execute`/`execute_stream` (forensically accurate, contained signature change), not stored on `Session`.
- One spec, ordered phases: gate first (security), then `client_ip` threading (so the data exists), then the enriched dashboard display, then field consistency.

## C1: Dashboard auth gate

- Add the admin-role check to one shared helper (for example `sqe_core` or a small `auth` util in the coordinator): `fn is_admin(roles: &[String]) -> bool` matching the existing notion (a role name contains "admin" or "owner", case-insensitive). Refactor `require_admin` to call it so there is a single definition.
- Wire `bearer_provider: Arc<dyn AuthProvider>` (and the `is_admin` helper) into `HealthState` (currently it carries no auth primitives; the providers are already built in `run_coordinator`).
- Add an axum middleware/guard applied ONLY to the `web_ui`-gated route group. The guard: read the `Authorization` header; if absent or not `Bearer <token>`, return 401; build `FlightCredentials` from the token and call `bearer_provider.authenticate`; on `Err`, return 401; on `Ok(identity)`, check `is_admin(&identity.roles)`; if not admin, return 403; else proceed.
- Health/readiness/status routes are registered outside the guarded group and remain open.
- Emit an `AuditKind::Auth` event for dashboard access via A's audit logger: success carries the admin identity; 401/403 carry `Outcome::Failure` with a non-sensitive reason and no token material. (The audit logger is reachable from the coordinator; pass an `Option<Arc<AuditLogger>>` into `HealthState`.)

## C2: client_ip threading and field consistency

- Add a `client_ip: Option<String>` parameter to `QueryHandler::execute` and `execute_stream`. The Flight handlers (`do_get`, `do_action`, and the prepared/ticket paths) pass the value from `extract_client_ip`. Trino/quack adapters and tests pass `None` where no peer is available.
- Populate `client_ip` into: `QueryTracker::start` (replacing the hardcoded `None` at `query_handler.rs:715,1934`), a new `client_ip` field on `StreamFinalizer` (set at construction, emitted by all three finalize branches), and the query/streaming audit events. Maintenance and the startup superdebug event pass `None` where no request IP exists.
- `QueryRecord.client_ip` is now populated, which feeds the enriched dashboard (C3).
- Field consistency: audit and trace emit sites use the canonical names `username`, `session_id`, `query_id`, `client_ip`. Fix obvious divergences; document the convention in the audit doc. No new context struct.

## C3: Enriched dashboard display (gated)

- Extend `QueryListItem`/`QueryDetail` (in `web_ui.rs`) with `username`, `roles`, `client_ip`, and a `sql` field that holds `redact_pii(record.sql)` (never the raw SQL). These fields are populated from `QueryRecord` (which carries username/roles, and now `client_ip` from C2).
- Because the routes are now admin-gated (C1), exposing these fields is safe. The WEB-02 omissions are lifted only behind the gate.
- The dashboard HTML/JSON renders the new columns. Keep `sql_hash` as well for correlation.

## Ordering and dependencies

Gate (C1) lands first as the security fix and is independently valuable. `client_ip` threading (C2) lands next so the data exists. The enriched display (C3) depends on both: it needs the gate (to be safe) and C2 (for a non-empty `client_ip` column). Field consistency rides with C2.

## Error handling

- Guard failures return clean 401/403 with a short JSON body; no token or internal detail echoed.
- A missing/disabled audit logger means dashboard-access events are simply not recorded (no failure).
- An auth provider error (not a rejection) returns 401 and is logged at WARN; it does not crash the server.

## Testing strategy

Test-driven throughout.

- Guard unit tests: no `Authorization` header -> 401; malformed scheme -> 401; valid bearer, non-admin role -> 403; valid bearer, admin role -> 200; health/readiness/status reachable WITHOUT a token (the guard must not apply to them).
- `is_admin` shared-helper tests, including that `require_admin` uses the same helper (one definition).
- Dashboard-access audit test: a successful admin view emits an `Auth` success event; a 403 emits a failure event; no token material in either.
- `client_ip` threading tests: a query driven with a client IP produces a `QueryRecord` and audit event carrying that IP; a streaming query carries it through `StreamFinalizer` to all finalize branches; default `None` paths stay `None`.
- Enriched-DTO test: the gated `/api/v1/queries` response includes username/roles/client_ip and the SQL field equals `redact_pii(sql)` (a secret literal does not appear).
- Regression: with `web_ui = false` (default), behavior is unchanged; health/readiness/status are unaffected by the guard.

## Risks and open items

- The axum guard must attach only to the `web_ui` route group. A wiring mistake that applies it to `/healthz` would break orchestration; the test that health routes need no token guards this.
- `FlightCredentials` construction from a bearer string must match what the Flight path builds; reuse the same constructor to avoid a divergent bearer-parsing path.
- Showing redacted SQL relies on `redact_pii` being sufficient for dashboard display; it is the same best-effort pass used by the audit native sink. GDPR-tag masking is not applied at the dashboard layer in C (the dashboard shows `redact_pii` output only); note this as a follow-up if tag-masked dashboard display is wanted.
- C stacks on B (MR !400, unmerged). If B changes in review, rebase C onto the updated B (or onto `main` once B merges).

## Deferred

Browser login/session/CSRF flows; TLS on the health port; GDPR-tag masking of dashboard SQL; a logging-context abstraction.
