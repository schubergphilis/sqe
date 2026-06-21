# Phase 2B: Session-context functions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add session-context SQL functions (`current_user()`, `is_role_in_session(role)`, `current_available_roles()`, `current_database()`, `current_schema()`) resolved from the authenticated session user, usable BOTH in user SQL AND inside Ranger policy expressions, enabling Snowflake-style role-conditional masking/filtering (e.g. a row filter `is_role_in_session('admin') OR region = 'EU'`).

**Architecture (the key decision):** The functions are `Volatility::Immutable` scalar UDFs **baked with the session identity** (username + roles + database + schema), resolved on the **coordinator**. Because they are Immutable with literal-or-no args, DataFusion's const-evaluator **folds them to literals during logical optimization on the coordinator, before physical fragments ship to workers**. So `is_role_in_session('admin')` becomes `true`/`false` and `current_user()` becomes `'alice'` BEFORE distribution. Workers never see session state or the UDF. This is fail-closed (value fixed at the authenticated coordinator) and makes distributed execution a non-issue. (Contrast: a config-extension UDF read at invoke time would fail-open on workers, since SessionConfig extensions are not serialized into shipped fragments. Const-fold avoids that entirely.)

**Always available, not gated** (user decision 2026-06-19): registered unconditionally, resolved from the session user (present on every query). Standard identity functions are SQL-compat (Trino/Snowflake/Postgres expose them always); the governance helpers are harmless self-introspection and must be present whenever a Ranger policy references them.

**Role model:** `SessionUser.roles` is the flat token role list (Keycloak `realm_access.roles`, already the effective set). `is_role_in_session(role)` = `roles.contains(role)`. This matches how SQE-side enforcement already matches (token roles directly, NOT Ranger membership which is only the Polaris gate). No `sqe-auth` refactor needed for MVP. Keep this consistent; do not "fix" it to Ranger membership.

**`current_role()` is OMITTED for MVP.** SQE has no primary/secondary-role concept; returning `roles[0]` is nondeterministic and a footgun. `is_role_in_session(role)` is the load-bearing primitive. (A future `current_role()` would need explicit primary-role semantics.)

**Sequencing:** 2B.2 (functions inside Ranger policy exprs, via const-fold in `parse_sql_predicate`) is the parity deliverable and is built first (Tasks 1-4). 2B.1 (bare `SELECT current_user()` in user SQL) is the SQL-compat adjunct (Task 5), which also just const-folds.

**Branch:** `feat/session-context-functions` off `main` (now has Phase 1 + 2A + TVF fix). Never push to main; open an MR.

**Gates:** `cargo build --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (only the 2 known env-flaky network tests may fail).

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `crates/sqe-policy/src/session_udf.rs` | The Immutable session-context UDFs, baked with `SessionIdentity` | Create |
| `crates/sqe-policy/src/lib.rs` | `pub mod session_udf;` + a `SessionIdentity` type (or reuse SessionUser) | Modify |
| `crates/sqe-policy/src/policy_expr.rs` | `parse_sql_predicate` registers the session UDFs (bound to the user) so policy exprs parse + bind | Modify |
| `crates/sqe-policy/src/ranger_store.rs` | pass the session identity into `parse_sql_predicate` | Modify |
| `crates/sqe-coordinator/src/session_context.rs` | register the session UDFs on the live session context (~line 485, by sha256) | Modify |
| `crates/sqe-policy/tests/rewriter_integration.rs` | fold-inspection + executable role-conditional tests | Modify |

---

## Task 1: Session-context UDFs (Immutable, baked with identity)

**Files:** Create `crates/sqe-policy/src/session_udf.rs`; modify `lib.rs`.

- [ ] **Step 1: Define a `SessionIdentity` carrier.** In `session_udf.rs` (or `lib.rs`):
```rust
#[derive(Debug, Clone, Default)]
pub struct SessionIdentity {
    pub username: String,
    pub roles: Vec<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}
```
(Build it from `sqe_core::SessionUser` + the session's warehouse/default-namespace where wired.)

- [ ] **Step 2 (TDD): tests first.** Model the UDFs on `crates/sqe-policy/src/sha256_udf.rs` (Immutable, baked field, manual PartialEq/Eq/Hash over the baked data). Write tests asserting, via `invoke_with_args` with a no-arg / literal-arg call:
  - `current_user()` -> `Utf8("alice")` for identity{username:"alice"}.
  - `is_role_in_session('admin')` -> `Boolean(true)` when roles contains "admin", `Boolean(false)` otherwise.
  - `is_role_in_session('x')` with empty roles -> `Boolean(false)`.
  - `current_available_roles()` -> a deterministic `Utf8` rendering of the sorted role set (e.g. JSON array string `["analyst","engineer"]` or comma-joined; pick one, document it).
  - `current_database()` / `current_schema()` -> `Utf8` of the baked value, or `Utf8(NULL)` when `None`.
  - Two UDFs baked with different identities are NOT equal (PartialEq includes the identity, so CSE can't conflate cross-session — like sha256's key).
  - Each UDF reports `Volatility::Immutable` (assert via `signature().volatility`).

- [ ] **Step 3: Implement.** One struct per function (or a small enum-tagged struct) holding an `Arc<SessionIdentity>`:
  - `current_user()`: `Signature::exact(vec![], Immutable)`, returns Utf8 = identity.username.
  - `is_role_in_session(role: Utf8)`: `Signature::exact(vec![Utf8], Immutable)`, returns Boolean = identity.roles.contains(arg). Handle the arg as a scalar Utf8 literal AND a Utf8 array (map element-wise) for completeness, NULL arg -> NULL/false (pick + document).
  - `current_available_roles()`: `Signature::exact(vec![], Immutable)`, returns Utf8 (sorted roles rendered).
  - `current_database()` / `current_schema()`: `Signature::exact(vec![], Immutable)`, returns Utf8 or Utf8 NULL.
  - Manual `PartialEq`/`Eq`/`Hash` including the function kind + the baked identity (mirror sha256). SQL names: `current_user`, `is_role_in_session`, `current_available_roles`, `current_database`, `current_schema`.
  - A `pub fn session_udfs(identity: Arc<SessionIdentity>) -> Vec<ScalarUDF>` returning all of them, for one-call registration.

- [ ] **Step 4: Register module + run.** `pub mod session_udf;` in lib.rs. `cargo test -p sqe-policy session_udf 2>&1 | tail -20` (pass), clippy clean.
- [ ] **Step 5: Commit.** `git add crates/sqe-policy/src/session_udf.rs crates/sqe-policy/src/lib.rs && git commit -m "feat(policy): session-context UDFs (Immutable, baked identity)"`

---

## Task 2: `parse_sql_predicate` registers the session UDFs (in-policy use)

**Files:** Modify `crates/sqe-policy/src/policy_expr.rs`.

- [ ] **Step 1 (TDD): tests first.** Add tests proving a policy expression that references a session function PARSES and binds:
```rust
    #[test]
    fn parses_is_role_in_session_in_policy() {
        let id = SessionIdentity { username: "bob".into(), roles: vec!["admin".into()], ..Default::default() };
        let e = parse_sql_predicate("is_role_in_session('admin') OR region = 'EU'", &id).unwrap();
        // a ScalarFunction referencing the bound UDF must be present
        let s = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string().to_lowercase();
        assert!(s.contains("is_role_in_session"));
        assert!(s.contains("region"));
    }
    #[test]
    fn parses_current_user_in_mask() {
        let id = SessionIdentity { username: "alice".into(), ..Default::default() };
        assert!(parse_sql_predicate("current_user()", &id).is_ok());
    }
```
Run; they FAIL (signature is `parse_sql_predicate(sql)` + functions unregistered).

- [ ] **Step 2: Change the signature** to `pub fn parse_sql_predicate(sql: &str, identity: &SessionIdentity) -> sqe_core::Result<Expr>`. Before `ctx.parse_sql_expr(...)`, register the session UDFs bound to `identity` on the throwaway `ctx`:
```rust
    let ctx = SessionContext::new();
    for udf in crate::session_udf::session_udfs(std::sync::Arc::new(identity.clone())) {
        ctx.register_udf(udf);
    }
```
The `IdentCollector` already ignores function names (they are `Expr::Function`, not `Identifier`), so the stub-schema column logic is unaffected. The parsed `Expr` embeds the user-bound UDFs (which const-fold later).
- [ ] **Step 3: Update all callers.** Find every `parse_sql_predicate(` call (grep). In `ranger_store.rs`, `resolve(user, ...)` has the user -> build a `SessionIdentity` from it and pass it (row filters AND CUSTOM masks). Update the existing `policy_expr` tests to pass a `&SessionIdentity::default()`.
- [ ] **Step 4: Run + commit.** `cargo test -p sqe-policy 2>&1 | grep "test result"` (all pass), clippy clean.
`git add crates/sqe-policy/src/policy_expr.rs crates/sqe-policy/src/ranger_store.rs && git commit -m "feat(policy): bind session functions when parsing policy exprs"`

---

## Task 3: Const-fold validation (the distribution gate)

**Files:** Modify `crates/sqe-policy/tests/rewriter_integration.rs`.

- [ ] **Step 1: Fold-inspection test (no live stack).** Using the existing qualified-multilevel-scan harness: a row filter `is_role_in_session('admin') OR region = 'EU'` (parsed with an `admin` identity) injected via the rewriter, then run through DataFusion's optimizer (build the plan, call `ctx.state().optimize(&plan)` or execute and inspect). Assert the optimized Filter predicate is a **literal/pruned form, NOT a residual `ScalarFunction`** for the session function. Concretely, two cases:
  - identity has `admin`: optimized filter folds the OR to `true` -> all 3 rows returned (no row filtering). Assert 3 rows.
  - identity lacks `admin` (e.g. roles `["analyst"]`): folds to `region = 'EU'` -> 2 rows. Assert 2 rows AND no `is_role_in_session` text remains in the optimized plan string.
  This proves session functions fold on the coordinator and never reach workers as UDFs. If they do NOT fold (residual ScalarFunction remains), STOP and report — the distributed-safety assumption is broken and must be resolved before shipping.
- [ ] **Step 2: Run + commit.** `cargo test -p sqe-policy --test rewriter_integration 2>&1 | tail -15`.
`git add crates/sqe-policy/tests/rewriter_integration.rs && git commit -m "test(policy): session functions const-fold to literals before distribution"`

---

## Task 4: Register on the live session context (user SQL + non-folded fallback)

**Files:** Modify `crates/sqe-coordinator/src/session_context.rs`.

- [ ] **Step 1:** At `create_session_context` (~line 485, where `sha256_udf` is registered, with `session.user` in scope), build a `SessionIdentity` from `session.user.username`, `session.user.roles`, and the session's database/schema (use the warehouse + default namespace if readily available; otherwise `None` for now and note it), then register all session UDFs:
```rust
    let identity = std::sync::Arc::new(sqe_policy::session_udf::SessionIdentity {
        username: session.user.username.clone(),
        roles: session.user.roles.clone(),
        database: /* session warehouse if available */ None,
        schema: /* session default namespace if available */ None,
    });
    for udf in sqe_policy::session_udf::session_udfs(identity) {
        ctx.register_udf(udf);
    }
```
This makes `SELECT current_user()` / `SELECT is_role_in_session('x')` work in user SQL, and is the resolution path if any session-function-bearing expr did not fold.
- [ ] **Step 2:** Build both coordinator binaries: `cargo build -p sqe-coordinator --bins 2>&1 | tail -8`. Clippy clean.
- [ ] **Step 3: Commit.** `git add crates/sqe-coordinator/src/session_context.rs && git commit -m "feat(coordinator): register session-context functions on the session"`

---

## Task 5: Gates + live demo + MR

- [ ] **Step 1: Full gates.** `cargo build --all`; `cargo clippy --all-targets --all-features -- -D warnings`; `cargo test --all` (only the 2 known env-flaky failures).
- [ ] **Step 2: Live demo (controller, optional but recommended).** On the polaris-ranger-keycloak stack: add a row-filter policy `is_role_in_session('engineer') OR region = 'EU'` (or a mask conditioned on a role) for a broad role, and show that an engineer sees all rows while an analyst-only user sees EU-only — one policy, role-conditional, the Snowflake pattern. Also `SELECT current_user(), is_role_in_session('analyst')` returns the session values.
- [ ] **Step 3: Docs + project state.** Note 2B in `docs/fine-grained-policy.md` (context-functions item done), `nextsteps.md`, README roadmap. Update `docs/ranger-fine-grained-service-type.md` mask table / context-function section if relevant.
- [ ] **Step 4: Commit + MR.** Push `feat/session-context-functions`; open MR -> `main` titled "feat: session-context functions (Phase 2B: role-conditional masking/filtering)", noting the const-fold distribution-safety and that `is_role_in_session` is the primitive (`current_role` deferred).

---

## Out of scope
- `current_role()` (needs explicit primary-role semantics).
- Richer `sqe-auth` role hierarchy (inherited/secondary roles) beyond the flat token set.
- Session functions with COLUMN arguments (e.g. `is_role_in_session(some_col)`) — those can't const-fold and would ship to workers; not supported (policies use literal role names).
- `current_database`/`current_schema` exact session-namespace wiring if the value isn't readily in scope at registration (start with `None`; wire later).
