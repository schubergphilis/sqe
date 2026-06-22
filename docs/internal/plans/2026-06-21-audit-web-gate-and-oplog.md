# Dashboard Auth Gate + Operational-Logging Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gate the operator dashboard routes behind bearer + admin auth, thread `client_ip` from the request to the audit/tracker/span emit sites, and surface the richer (redacted) fields to authenticated admins.

**Architecture:** An axum guard middleware wraps only the `web_ui` route group on the health server, validating an `Authorization: Bearer` token via the existing `sqe_auth` `bearer_provider` and requiring an admin role via the existing `config.auth.has_admin_role`. `client_ip` becomes an explicit per-request parameter to `execute`/`execute_stream`, populated into `QueryRecord`, `StreamFinalizer`, and audit events. Once gated, the query DTOs expose username/roles/client_ip and `redact_pii`-masked SQL.

**Tech Stack:** Rust, axum (health server), `sqe_auth` (AuthProvider/FlightCredentials), `sqe_metrics::audit`, tokio.

## Global Constraints

- Spec: `docs/internal/specs/2026-06-21-audit-web-gate-and-oplog-design.md`. This branch stacks on sub-project B (MR !400, unmerged).
- Gate ONLY the `web_ui` routes (`/`, `/api/v1/overview`, `/api/v1/queries`, `/api/v1/queries/{id}`, `/api/v1/workers`, `/api/v1/metrics/history`). NEVER gate `/healthz`, `/readyz`, `/api/v1/status` (k8s liveness/readiness + load balancers depend on them).
- Reuse the EXISTING admin notion: `config.auth.has_admin_role(&roles)` (the same one `require_admin` uses). Do NOT invent a new role or a second definition.
- Build the bearer credential with the SAME `FlightCredentials` the Flight path uses: `FlightCredentials { bearer_token: Some(SecretString::new(token)), ..Default::default() }`. Do not write a divergent bearer parser.
- Never show raw SQL on the dashboard. Show `redact_pii(sql)`. No token material in any auth/audit output.
- `client_ip` is a per-request parameter into `execute`/`execute_stream`; do not store it on `Session`.
- Default `web_ui = false` must remain behavior-identical (no guard, no new behavior).
- No emdash/endash/unicode arrows in code or docs (use `->`). Jacob's writing style in docs.
- `cargo clippy --all-targets --all-features -- -D warnings` clean before each commit.
- Existing tests keep passing.

---

## File Structure

- `crates/sqe-coordinator/src/bin/sqe_server.rs` (modify): add auth fields to `HealthState`; wrap the `web_ui` route group with an auth guard; wire `bearer_provider` + `auth` config + `audit` into `HealthState` in `run_coordinator`.
- `crates/sqe-coordinator/src/web_auth.rs` (new): the axum guard middleware fn + a small `bearer_admin_check` helper, plus its unit tests.
- `crates/sqe-coordinator/src/web_ui.rs` (modify): extend `QueryListItem`/`QueryDetail` with username/roles/client_ip/redacted `sql`; update `to_list_item`/the detail builder.
- `crates/sqe-coordinator/src/query_handler.rs` (modify): add `client_ip: Option<String>` to `execute`/`execute_stream`; pass to `QueryTracker::start` (lines ~715, ~1934) and audit events.
- `crates/sqe-coordinator/src/streaming.rs` (modify): add `client_ip` field to `StreamFinalizer`; emit it in the 3 finalize branches.
- `crates/sqe-coordinator/src/flight_sql.rs` (modify): pass `extract_client_ip(...)` into `execute`/`execute_stream` calls.
- `docs/site/book/src/operations/audit-logging.md` (modify): document the dashboard gate + client_ip; `docs/internal/roadmap-tracker.md` (modify): mark C done.

---

## Phase C1: Dashboard auth gate

### Task 1: Bearer + admin guard on the web_ui routes

**Files:**
- Create: `crates/sqe-coordinator/src/web_auth.rs`
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs` (`HealthState` struct ~line 59; `start_health_server` ~line 320; `HealthState` construction in `run_coordinator` ~line 757), `crates/sqe-coordinator/src/lib.rs` (`pub mod web_auth;`)
- Test: inline in `web_auth.rs`

**Interfaces:**
- Consumes: `sqe_auth::{AuthProvider, FlightCredentials}`, `sqe_core::SecretString`, `config.auth.has_admin_role(&[String]) -> bool`, `config.auth.admin_roles`.
- Produces: `pub async fn require_admin_bearer(State<Arc<HealthState>>, Request, Next) -> Response` (axum middleware) and `pub async fn bearer_admin_identity(provider: &Arc<dyn AuthProvider>, auth_cfg: &sqe_core::config::AuthConfig, header: Option<&str>) -> Result<sqe_auth::Identity, GuardReject>` where `GuardReject { Unauthorized, Forbidden }`. `HealthState` gains `bearer_provider: Option<Arc<dyn AuthProvider>>`, `auth_cfg: Option<sqe_core::config::AuthConfig>`, `audit: Option<Arc<sqe_metrics::audit::AuditLogger>>`.

- [ ] **Step 1: Write the failing guard unit tests.** Drive `bearer_admin_identity` (the pure-ish core) with a stub `AuthProvider`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    struct StubProvider { roles: Vec<String>, ok: bool }
    #[async_trait::async_trait]
    impl sqe_auth::AuthProvider for StubProvider {
        async fn authenticate(&self, creds: &sqe_auth::FlightCredentials) -> Result<sqe_auth::Identity, sqe_auth::AuthError> {
            assert!(creds.bearer_token.is_some(), "guard must pass the bearer token");
            if self.ok {
                Ok(sqe_auth::Identity { user_id: "u".into(), display_name: "u".into(), roles: self.roles.clone(), subject: None, email: None, groups: vec![], catalog_token: None, refresh_token: None, expires_at: None })
            } else {
                Err(sqe_auth::AuthError::AuthFailed("bad".into()))
            }
        }
    }
    fn auth_cfg(admin_roles: &[&str]) -> sqe_core::config::AuthConfig {
        let mut c = sqe_core::config::AuthConfig::default();
        c.admin_roles = admin_roles.iter().map(|s| s.to_string()).collect();
        c
    }

    #[tokio::test]
    async fn missing_header_is_unauthorized() {
        let p: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubProvider { roles: vec![], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), None).await;
        assert!(matches!(r, Err(GuardReject::Unauthorized)));
    }
    #[tokio::test]
    async fn non_bearer_scheme_is_unauthorized() {
        let p: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubProvider { roles: vec![], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Basic abc")).await;
        assert!(matches!(r, Err(GuardReject::Unauthorized)));
    }
    #[tokio::test]
    async fn valid_bearer_non_admin_is_forbidden() {
        let p: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubProvider { roles: vec!["analyst".into()], ok: true });
        let r = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Bearer tok")).await;
        assert!(matches!(r, Err(GuardReject::Forbidden)));
    }
    #[tokio::test]
    async fn valid_bearer_admin_is_ok() {
        let p: Arc<dyn sqe_auth::AuthProvider> = Arc::new(StubProvider { roles: vec!["admin".into()], ok: true });
        let id = bearer_admin_identity(&p, &auth_cfg(&["admin"]), Some("Bearer tok")).await.unwrap();
        assert_eq!(id.roles, vec!["admin".to_string()]);
    }
}
```

Verify the exact `Identity` field set against `crates/sqe-auth/src/provider.rs` (sub-project A added `subject`/`email`/`groups`); adjust the struct literal in the stub to match. If `AuthError` variant names differ, match them.

- [ ] **Step 2: Run to verify failure.**

Run: `cargo test -p sqe-coordinator -- web_auth`
Expected: FAIL (module/functions absent).

- [ ] **Step 3: Implement `web_auth.rs`.**

```rust
use std::sync::Arc;
use axum::{extract::State, http::StatusCode, middleware::Next, response::{IntoResponse, Response}, body::Body, extract::Request};
use sqe_auth::{AuthProvider, FlightCredentials};
use sqe_core::SecretString;

#[derive(Debug, PartialEq, Eq)]
pub enum GuardReject { Unauthorized, Forbidden }

/// Validate an `Authorization: Bearer <token>` header against the bearer
/// provider and require an admin role. Reuses `config.auth.has_admin_role`.
pub async fn bearer_admin_identity(
    provider: &Arc<dyn AuthProvider>,
    auth_cfg: &sqe_core::config::AuthConfig,
    header: Option<&str>,
) -> Result<sqe_auth::Identity, GuardReject> {
    let token = match header.and_then(|h| h.strip_prefix("Bearer ")) {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => return Err(GuardReject::Unauthorized),
    };
    let creds = FlightCredentials { bearer_token: Some(SecretString::new(token)), ..Default::default() };
    let identity = provider.authenticate(&creds).await.map_err(|_| GuardReject::Unauthorized)?;
    if auth_cfg.has_admin_role(&identity.roles) {
        Ok(identity)
    } else {
        Err(GuardReject::Forbidden)
    }
}

/// Axum middleware: gate a route group behind bearer + admin. Attach via
/// `route_layer` to the web_ui routes only.
pub async fn require_admin_bearer(
    State(state): State<Arc<crate_health_state_path::HealthState>>, // see note
    request: Request,
    next: Next,
) -> Response {
    let provider = match &state.bearer_provider {
        Some(p) => p,
        None => return (StatusCode::UNAUTHORIZED, "auth not configured").into_response(),
    };
    let auth_cfg = match &state.auth_cfg { Some(c) => c, None => return (StatusCode::UNAUTHORIZED, "auth not configured").into_response() };
    let header = request.headers().get(axum::http::header::AUTHORIZATION).and_then(|v| v.to_str().ok());
    match bearer_admin_identity(provider, auth_cfg, header).await {
        Ok(_identity) => next.run(request).await,
        Err(GuardReject::Unauthorized) => (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
        Err(GuardReject::Forbidden) => (StatusCode::FORBIDDEN, "admin role required").into_response(),
    }
}
```

Note: `HealthState` is defined in `bin/sqe_server.rs` (a binary, not the lib). The middleware needs the state type. Two clean options: (a) move `HealthState` into the lib (e.g. `crate::web_ui` or a new `crate::health` module) so `web_auth` can name it, or (b) make the guard generic over a small trait `BearerAdminState { fn provider(&self) -> Option<&Arc<dyn AuthProvider>>; fn auth_cfg(&self) -> Option<&AuthConfig>; }` implemented for `HealthState` in the binary. Pick (b) if moving `HealthState` is invasive; pick (a) if it is small. The PURE `bearer_admin_identity` (tested above) stays in `web_auth.rs` regardless; the middleware wrapper can live in the binary if that is simpler. Decide based on what keeps `HealthState` construction in one place; report the choice.

- [ ] **Step 4: Add auth fields to `HealthState` and wire the guard.** In `bin/sqe_server.rs`:
  - Add to `HealthState`: `bearer_provider: Option<Arc<dyn sqe_auth::AuthProvider>>`, `auth_cfg: Option<sqe_core::config::AuthConfig>`, `audit: Option<Arc<sqe_metrics::audit::AuditLogger>>`.
  - In `run_coordinator`'s `HealthState { .. }` construction (~line 757), set `bearer_provider: bearer_provider.clone()` (it is built earlier in `run_coordinator`), `auth_cfg: Some(config.auth.clone())`, `audit: Some(audit.clone())` (the audit logger built for the coordinator). For `run_worker`/tests, these are `None`.
  - In `start_health_server`, split the router: keep `/healthz`, `/readyz`, `/api/v1/status` ungated; build the `web_ui` routes as a separate `Router` and attach `.route_layer(axum::middleware::from_fn_with_state(state.clone(), require_admin_bearer))` to THAT sub-router only, then `.merge(...)` it. Confirm the guard does not apply to the health routes.

- [ ] **Step 5: Write an integration test for route-level gating** (in `bin/sqe_server.rs` `#[cfg(test)]` or a coordinator it-test): build the router with `web_ui=true` and a stub bearer provider; assert `GET /healthz` -> 200 WITHOUT a token; `GET /api/v1/queries` without a token -> 401; with a non-admin bearer -> 403; with an admin bearer -> 200. Use `tower::ServiceExt::oneshot` to drive the router (axum's standard test pattern; grep the repo for `oneshot` to follow any existing example).

- [ ] **Step 6: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator -- web_auth
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/web_auth.rs crates/sqe-coordinator/src/bin/sqe_server.rs crates/sqe-coordinator/src/lib.rs
git commit -m "feat(web): gate web_ui dashboard routes behind bearer + admin auth"
```

### Task 2: Audit dashboard access

**Files:**
- Modify: `crates/sqe-coordinator/src/web_auth.rs` (emit on the guard outcome), `bin/sqe_server.rs` (pass `audit` through)
- Test: inline in `web_auth.rs`

**Interfaces:**
- Consumes: `state.audit: Option<Arc<AuditLogger>>`, `sqe_metrics::audit::{AuditEvent, AuditKind, Actor, Outcome}`, `Actor::from_parts`.

- [ ] **Step 1: Write the failing test** asserting the guard emits an `AuditKind::Auth` event: success -> `Outcome::Success` with the admin username; reject -> `Outcome::Failure` with a non-sensitive reason and NO token. Use a tempfile `AuditLogger` and read the line back. (Follow A's `audit_e2e_test.rs` reading pattern.)

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement.** In `require_admin_bearer`, after the `bearer_admin_identity` result, if `state.audit` is `Some`, `log_event` an `AuditEvent { kind: Auth, .. }`: on `Ok(identity)` build `Actor::from_parts(identity.user_id, identity.subject, identity.email, identity.roles, identity.groups)` and `Outcome::Success`; on `Err` use a placeholder actor (`Actor::from_parts("unknown".into(), None, None, vec![], vec![])`) and `Outcome::Failure { error_type: Some("DashboardAccessDenied"), error_code: None, message: Some("bearer/admin required") }`. Never include the token. Set a resource/message identifying the dashboard. Client IP from the request peer if readily available, else `None`.

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator -- web_auth
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/web_auth.rs crates/sqe-coordinator/src/bin/sqe_server.rs
git commit -m "feat(web): audit dashboard access (Auth event on grant and denial)"
```

---

## Phase C2: client_ip threading + field consistency

### Task 3: Thread client_ip into execute/execute_stream and the tracker/audit

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs` (`execute` ~line 562, `execute_stream` ~line 1801, the two `QueryTracker::start` calls passing `None` at ~715 and ~1934, and the query audit event), `crates/sqe-coordinator/src/flight_sql.rs` (callers pass `extract_client_ip`)
- Test: `crates/sqe-coordinator/tests/it/audit_e2e_test.rs` (extend)

**Interfaces:**
- Produces: `execute(&self, session: &Session, ..., client_ip: Option<String>)` and `execute_stream(&self, session: &Session, ..., client_ip: Option<String>)` (append the param at the end so other args are unchanged). The query `AuditEvent.client_ip` and `QueryRecord.client_ip` are populated from it.

- [ ] **Step 1: Write the failing test.** Drive a buffered `execute` with `client_ip: Some("10.1.2.3".into())` through a handler wired to a tempfile `AuditLogger`; flush; assert the written audit line has `client_ip == "10.1.2.3"`. (If the tracker is also readable in the test, assert the `QueryRecord` carries it too.)

- [ ] **Step 2: Run to verify failure** (param does not exist / audit emits None).

- [ ] **Step 3: Implement.** Add `client_ip: Option<String>` as the final parameter to `execute` and `execute_stream`. In the buffered path, pass it to `QueryTracker::start` (replace `None` at ~715 and ~1934) and into the `AuditEvent { client_ip: client_ip.clone(), .. }` for the Query emit (A Task 11 path). In `flight_sql.rs`, the `do_get`/`do_action`/prepared/ticket call sites pass `Some(self.extract_client_ip(&request))`. Other callers (Trino/quack adapters, tests) pass `None`. Update ALL call sites so it compiles (grep `\.execute(` and `\.execute_stream(`).

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator audit
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/flight_sql.rs crates/sqe-coordinator/tests/it/audit_e2e_test.rs
git commit -m "feat(audit): thread per-request client_ip into execute/execute_stream and audit"
```

### Task 4: client_ip through the streaming finalizer

**Files:**
- Modify: `crates/sqe-coordinator/src/streaming.rs` (`StreamFinalizer` struct + the 3 finalize branches), `query_handler.rs` (`execute_stream` sets it at `StreamFinalizer` construction)
- Test: `streaming.rs` inline or `audit_e2e_test.rs`

**Interfaces:**
- Produces: `StreamFinalizer.client_ip: Option<String>` set at construction; all 3 audit emit branches use it instead of `None`.

- [ ] **Step 1: Write the failing test.** Build a `StreamFinalizer` with `client_ip: Some("10.9.9.9".into())`, run the success finalize path with a tempfile `AuditLogger`, assert the audit line has `client_ip == "10.9.9.9"`. (Follow the `streaming_select_emits_canonical_query_event` test added in B.)

- [ ] **Step 2: Run to verify failure** (StreamFinalizer has no field / emits None).

- [ ] **Step 3: Implement.** Add `pub client_ip: Option<String>` to `StreamFinalizer`. Set it at the `StreamFinalizer` construction in `execute_stream` from the new `client_ip` param (Task 3). Replace the hardcoded `client_ip: None` in the 3 emit branches (~streaming.rs:261,335,394) with `self.client_ip.clone()`.

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator streaming
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/streaming.rs crates/sqe-coordinator/src/query_handler.rs
git commit -m "feat(audit): carry client_ip through the streaming finalizer to audit events"
```

---

## Phase C3: Enriched dashboard display (gated)

### Task 5: Expose username/roles/client_ip and redacted SQL in the query DTOs

**Files:**
- Modify: `crates/sqe-coordinator/src/web_ui.rs` (`QueryListItem` ~line 197, `to_list_item` ~line 220, and the `QueryDetail` builder), the dashboard HTML/JSON render if columns are listed there
- Test: inline in `web_ui.rs`

**Interfaces:**
- Consumes: `QueryRecord` (`user`, `roles`, `client_ip` now populated by C2, `sql`), `sqe_metrics::audit::redact_pii`.
- Produces: `QueryListItem`/`QueryDetail` gain `username: String`, `roles: Vec<String>`, `client_ip: Option<String>`, `sql: String` (= `redact_pii(&record.sql)`). Keep `sql_hash`.

- [ ] **Step 1: Write the failing test.** Build a `QueryRecord` with `user = "alice"`, `roles = ["admin"]`, `client_ip = Some("10.0.0.7")`, `sql = "SELECT * FROM users WHERE email = 'a@b.com'"`. Call `to_list_item(&record)` and assert: `username == "alice"`, `roles == ["admin"]`, `client_ip == Some("10.0.0.7")`, and `sql` contains `[EMAIL]` and NOT `a@b.com` (proving `redact_pii` is applied, not raw SQL).

- [ ] **Step 2: Run to verify failure** (fields absent).

- [ ] **Step 3: Implement.** Add the four fields to `QueryListItem` (and the `QueryDetail` DTO). In `to_list_item` (and the detail builder), populate `username: r.user.clone()`, `roles: r.roles.clone()`, `client_ip: r.client_ip.clone()`, `sql: sqe_metrics::audit::redact_pii(&r.sql)`. Update the WEB-02 doc-comment to note these are now exposed because the routes are admin-gated (C1), and that SQL is `redact_pii`-masked (never raw). If the dashboard HTML template enumerates columns, add the new ones.

- [ ] **Step 4: Run, clippy, commit.**

```bash
cargo test -p sqe-coordinator -- web_ui
cargo clippy --all-targets --all-features -- -D warnings
git add crates/sqe-coordinator/src/web_ui.rs
git commit -m "feat(web): expose username/roles/client_ip and redacted SQL to gated dashboard"
```

---

## Phase C-final: docs + regression

### Task 6: Docs, field-consistency note, roadmap, full regression

**Files:**
- Modify: `docs/site/book/src/operations/audit-logging.md`, `docs/internal/roadmap-tracker.md`
- Test: full workspace

- [ ] **Step 1: Document the dashboard gate** in the audit doc: the `web_ui` routes now require `Authorization: Bearer <token>` with an admin role (same `admin_roles` as DDL); health/readiness/status stay open; dashboard access is audited; the dashboard shows username/roles/client_ip and `redact_pii`-masked SQL (never raw) to authenticated admins. Note `client_ip` is now populated end to end. Add a one-line field-consistency note: audit and trace emit sites use `username`/`session_id`/`query_id`/`client_ip` as the canonical field names. Honor the no-emdash rule.
- [ ] **Step 2: Mark sub-project C done** in `docs/internal/roadmap-tracker.md` (follow its convention).
- [ ] **Step 3: Full regression.** `cargo test --all` (docker-gated tests are `#[ignore]`d; a pre-existing `channel_pool`/`oidc_m2m` network test may fail offline - report as pre-existing) and `cargo clippy --all-targets --all-features -- -D warnings`. Confirm `web_ui = false` default is unchanged.
- [ ] **Step 4: Emdash check.** `grep -rn '—' docs/site/book/src/operations/audit-logging.md docs/internal/roadmap-tracker.md | grep -v '`' || echo clean`.
- [ ] **Step 5: Commit.**

```bash
git add docs/site/book/src/operations/audit-logging.md docs/internal/roadmap-tracker.md
git commit -m "docs(web): document dashboard gate + client_ip; mark sub-project C done"
```

---

## Self-Review

**Spec coverage:** C1 gate (bearer + admin, web_ui routes only, health open): Task 1. Audit dashboard access: Task 2. client_ip threading (execute/execute_stream -> tracker + audit): Task 3; through the streaming finalizer: Task 4. Enriched gated display (username/roles/client_ip/redacted SQL): Task 5. Field consistency + docs + roadmap: Task 6. The "one shared admin helper" spec point is satisfied by reusing the EXISTING `config.auth.has_admin_role` (no new helper needed; noted in Global Constraints and Task 1) - a simplification over the spec's "factor into a helper" wording, same single-definition outcome.

**Placeholder scan:** Task 1 Step 3 leaves a deliberate decision (move `HealthState` to lib vs a small state trait) with both options spelled out and a decision rule - that is a real architectural choice the implementer resolves and reports, not a vague placeholder. Test code and implementation code are concrete. The `Identity` struct literal in the Task 1 stub is flagged to verify against provider.rs (A added fields) rather than assumed.

**Type consistency:** `bearer_admin_identity` / `GuardReject` / `require_admin_bearer` (Task 1) are used consistently in Task 2. `client_ip: Option<String>` appended to `execute`/`execute_stream` (Task 3) is the field set on `StreamFinalizer` (Task 4) and surfaced via `QueryRecord.client_ip` in the DTO (Task 5). `redact_pii` (from sqe-metrics, used by A's audit) is reused in Task 5. `Actor::from_parts` signature matches A. `config.auth.has_admin_role` / `config.auth.admin_roles` are the existing API.

**Open risk:** the `HealthState`-location decision (Task 1 Step 3) ripples to whether the middleware lives in the lib or the binary; the plan gives the decision rule and keeps the tested `bearer_admin_identity` in the lib either way. The exact axum 0.x middleware API (`from_fn_with_state`, `route_layer`) and `Identity`/`AuthError` field names are pinned by the implementer against the vendored versions during Task 1.
