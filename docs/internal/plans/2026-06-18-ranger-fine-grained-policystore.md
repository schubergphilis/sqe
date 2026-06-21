# Ranger Fine-Grained PolicyStore Implementation Plan (Phase 1 MVP)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `RangerStore: PolicyStore` that reads row-filter and column-mask policies from a `hive`-type Apache Ranger service and feeds SQE's existing `PolicyPlanRewriter`, plus wire the long-dormant policy-engine dispatcher (AUTH-01) so a configured engine actually runs.

**Architecture:** SQE already enforces row filters and column masks by rewriting the `LogicalPlan` (`PolicyPlanRewriter` -> `PolicyStore::resolve` -> `ResolvedPolicy`). Today the only real `PolicyStore` is `OpaStore`, and even it is not wired into the coordinator (both binaries hardcode `PassthroughEnforcer` behind a `TODO(AUTH-01)`). This plan adds a new policy SOURCE (Ranger) and finally connects the dispatcher. The Ranger store pulls the whole policy bundle from the plugin DOWNLOAD endpoint, flattens Iceberg `catalog/namespace/table` to hive `database/table`, matches the user + token roles against `dataMaskPolicyItems` / `rowFilterPolicyItems`, and returns a `ResolvedPolicy`. Sharing the `hive` service-def is what lets Spark/Kyuubi and SQE enforce one policy set.

**Tech Stack:** Rust, DataFusion 54, `reqwest` (basic auth), `moka` async cache, `wiremock` for HTTP tests. Mirrors `crates/sqe-policy/src/opa.rs` (cache + 3-state breaker + fail-closed posture) and `crates/sqe-policy/src/grants/ranger.rs` (Ranger HTTP client patterns).

**Spec source (design is settled, do not re-brainstorm):**
- `docs/fine-grained-policy.md` (why pull/rewrite, the vocabulary roadmap)
- `docs/ranger-fine-grained-service-type.md` (service-type decision, mask table, flattening sharp edge, effort estimate)
- `docs/prompts/ranger-policystore-prompt.md` (the original implementation prompt this plan supersedes)

**MVP scope decisions (locked):**
- **Mask types:** map only what the existing `MaskType` covers. `MASK_NULL -> Nullify`, `MASK_NONE -> exemption (no mask)`, `MASK_HASH -> Hash`, `CUSTOM (valueExpr) -> Custom(parsed Expr)`. The partial/substring/date Hive masks (`MASK`, `MASK_SHOW_LAST_4`, `MASK_SHOW_FIRST_4`, `MASK_DATE_SHOW_YEAR`) need NEW DataFusion UDFs and are **Phase 2**. In MVP they fail-closed: the column is added to `restricted_columns` (dropped, never leaked) and a warning is logged.
- **Row filters:** parsed with a REAL SQL parser (DataFusion `parse_sql_expr`), NOT OPA's toy `parse_filter_expr`. Compound `AND`/`OR`/`IN`/function filters must work. (See Task 2 — this is the single biggest correctness risk.)
- **Tag policies, session-context functions (`current_user()`, `is_role_in_session()`), richer `SessionUser` role model:** all **out of scope** (Phase 2/3). MVP matches on flat `user.username` + `user.roles` only.
- **Wiring (AUTH-01):** included as the final store task. The dispatcher wires ALL engines (OPA + InMemory have been unwired too), shared across both coordinator binaries so they cannot drift.

**Branch:** `feat/ranger-policy-store` off `main`. Never push to main; open an MR at the end.

**Project gates (run before opening the MR):**
```bash
cargo build --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `crates/sqe-policy/src/policy_breaker.rs` | Shared 3-state circuit breaker (extracted from `opa.rs`) | Create |
| `crates/sqe-policy/src/policy_expr.rs` | `parse_sql_predicate(sql) -> Result<Expr>` — real SQL boolean/expr parser shared by Ranger row-filters and CUSTOM masks | Create |
| `crates/sqe-policy/src/ranger_store.rs` | `RangerStore: PolicyStore` — download client + `ServicePolicies` model + `resolve()` | Create |
| `crates/sqe-policy/src/opa.rs` | Use the shared breaker instead of the private one | Modify |
| `crates/sqe-policy/src/lib.rs` | `pub mod policy_breaker; pub mod policy_expr; pub mod ranger_store;` | Modify |
| `crates/sqe-core/src/config.rs` | `PolicyEngine::Ranger`, `RangerPolicyConfig` nested in `PolicyConfig` | Modify |
| `crates/sqe-coordinator/src/policy_wiring.rs` | `build_policy_enforcer(&config)` shared dispatcher (AUTH-01) | Create |
| `crates/sqe-coordinator/src/lib.rs` | `pub mod policy_wiring;` | Modify |
| `crates/sqe-coordinator/src/main.rs` | Call `build_policy_enforcer`, pass store into `QueryHandler::new` | Modify |
| `crates/sqe-coordinator/src/bin/sqe_server.rs` | Same dispatcher call | Modify |
| `crates/sqe-policy/tests/ranger_store_wiremock.rs` | HTTP integration tests (wiremock) | Create |
| `quickstart/polaris-ranger-keycloak/` | Row-filter + mask demo, `engine = "ranger"` | Modify |

---

## Task 1: Extract the circuit breaker into a shared module

Pure refactor. `OpaCircuitBreaker` (private in `opa.rs`) becomes `PolicyCircuitBreaker` in a shared module so `RangerStore` reuses it. OPA's existing tests are the regression gate.

**Files:**
- Create: `crates/sqe-policy/src/policy_breaker.rs`
- Modify: `crates/sqe-policy/src/lib.rs`
- Modify: `crates/sqe-policy/src/opa.rs:43-147` (remove the local breaker, import the shared one)

- [ ] **Step 1: Create the shared breaker module**

Create `crates/sqe-policy/src/policy_breaker.rs` by moving the breaker verbatim from `opa.rs` (lines 43-147), renaming the type and generalizing the log strings. Full content:

```rust
//! Lightweight three-state circuit breaker shared by HTTP-backed policy stores
//! (OPA, Ranger). Extracted from `opa.rs` so both stores share one impl.
//!
//! Mirrors `sqe_catalog::CircuitBreaker`. The sqe-policy crate cannot depend on
//! sqe-catalog (the dependency direction is the other way around), so the
//! smaller implementation lives here.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

const STATE_CLOSED: u32 = 0;
const STATE_OPEN: u32 = 1;
const STATE_HALF_OPEN: u32 = 2;

/// Three-state circuit breaker around a remote policy backend call.
pub struct PolicyCircuitBreaker {
    /// Backend label used in log lines (e.g. "OPA", "Ranger").
    name: &'static str,
    failure_count: AtomicU32,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_ms: AtomicU64,
    state: AtomicU32,
}

impl PolicyCircuitBreaker {
    pub fn new(name: &'static str, failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            name,
            failure_count: AtomicU32::new(0),
            failure_threshold,
            recovery_timeout,
            last_failure_ms: AtomicU64::new(0),
            state: AtomicU32::new(STATE_CLOSED),
        }
    }

    /// Returns Err when the breaker is open (caller must fail closed).
    pub fn check(&self) -> Result<(), String> {
        let state = self.state.load(Ordering::Acquire);
        match state {
            STATE_CLOSED => Ok(()),
            STATE_OPEN => {
                let elapsed_ms =
                    now_millis().saturating_sub(self.last_failure_ms.load(Ordering::Relaxed));
                if elapsed_ms >= self.recovery_timeout.as_millis() as u64
                    && self
                        .state
                        .compare_exchange(
                            STATE_OPEN,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                {
                    info!("{} circuit breaker moving to half_open (probe allowed)", self.name);
                    return Ok(());
                }
                Err(format!("{} circuit breaker is open", self.name))
            }
            STATE_HALF_OPEN => Ok(()),
            _ => Ok(()),
        }
    }

    pub fn record_success(&self) {
        if self.state.load(Ordering::Acquire) != STATE_CLOSED {
            self.state.store(STATE_CLOSED, Ordering::Release);
            self.failure_count.store(0, Ordering::Release);
            info!("{} circuit breaker closed after successful probe", self.name);
        } else {
            self.failure_count.store(0, Ordering::Relaxed);
        }
    }

    pub fn record_failure(&self) {
        self.last_failure_ms.store(now_millis(), Ordering::Relaxed);
        let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
        if count >= self.failure_threshold
            && self
                .state
                .compare_exchange(STATE_CLOSED, STATE_OPEN, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            warn!(
                backend = self.name,
                failures = count,
                threshold = self.failure_threshold,
                "circuit breaker opened"
            );
        } else if self.state.load(Ordering::Acquire) == STATE_HALF_OPEN {
            self.state.store(STATE_OPEN, Ordering::Release);
        }
    }

    /// 0 = closed, 1 = half-open, 2 = open (matches the metrics gauge encoding).
    pub fn state_code(&self) -> u8 {
        match self.state.load(Ordering::Relaxed) {
            STATE_OPEN => 2,
            STATE_HALF_OPEN => 1,
            _ => 0,
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_breaker_allows() {
        let b = PolicyCircuitBreaker::new("Test", 3, Duration::from_secs(30));
        assert!(b.check().is_ok());
        assert_eq!(b.state_code(), 0);
    }

    #[test]
    fn opens_after_threshold() {
        let b = PolicyCircuitBreaker::new("Test", 2, Duration::from_secs(30));
        b.record_failure();
        b.record_failure();
        assert!(b.check().is_err());
        assert_eq!(b.state_code(), 2);
    }

    #[test]
    fn success_resets_failures() {
        let b = PolicyCircuitBreaker::new("Test", 2, Duration::from_secs(30));
        b.record_failure();
        b.record_success();
        b.record_failure();
        // Only one failure since the reset, breaker stays closed.
        assert!(b.check().is_ok());
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/sqe-policy/src/lib.rs`, add after the existing `pub mod opa;` line:

```rust
pub mod policy_breaker;
```

- [ ] **Step 3: Replace OPA's private breaker with the shared one**

In `crates/sqe-policy/src/opa.rs`:
- Delete the `STATE_*` consts (lines 43-45), the entire `struct OpaCircuitBreaker` + its `impl` (lines 47-140), and the local `fn now_millis` (lines 142-147).
- Add the import near the other `use crate::...` lines:

```rust
use crate::policy_breaker::PolicyCircuitBreaker;
```

- In `struct OpaStore`, change the field type:

```rust
    breaker: Arc<PolicyCircuitBreaker>,
```

- In `OpaStore::with_config`, change the constructor call:

```rust
            breaker: Arc::new(PolicyCircuitBreaker::new(
                "OPA",
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
```

The method names (`check`, `record_success`, `record_failure`, `state_code`) are unchanged, so the call sites in `resolve()` need no edits.

- [ ] **Step 4: Verify OPA still compiles and its tests pass**

Run: `cargo test -p sqe-policy 2>&1 | tail -30`
Expected: PASS. The OPA breaker behavior is unchanged; all existing `opa.rs` tests and `tests/opa_wiremock.rs` stay green.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/policy_breaker.rs crates/sqe-policy/src/lib.rs crates/sqe-policy/src/opa.rs
git commit -m "refactor(policy): extract shared PolicyCircuitBreaker from opa"
```

---

## Task 2: Real SQL predicate parser (the load-bearing risk)

OPA's `parse_filter_expr` only handles a single `col <op> literal`. Ranger `filterExpr` and `CUSTOM valueExpr` strings are arbitrary SQL: `region = 'EU' AND tier < 3`, `dept IN ('a','b')`, `mask_func(col)`. Feeding those to the toy parser produces a SILENTLY WRONG filter (a data-leak/over-restriction bug), not an error. This task builds a real parser using DataFusion's SQL planner and proves it on compound expressions.

**Files:**
- Create: `crates/sqe-policy/src/policy_expr.rs`
- Modify: `crates/sqe-policy/src/lib.rs`

- [ ] **Step 1: Write failing tests for the real parser**

Create `crates/sqe-policy/src/policy_expr.rs` with only the test module first (the function is `todo!()`):

```rust
//! Parse a SQL boolean / scalar expression string into a DataFusion `Expr`,
//! schema-free. Used for Ranger `filterExpr` (row filters) and CUSTOM
//! `valueExpr` (column masks). Unqualified identifiers become unresolved
//! `Expr::Column`; they resolve later when the rewriter injects the expr into
//! a `Filter`/projection above the matching `TableScan`.

use datafusion::logical_expr::Expr;

/// Parse `sql` (a single SQL expression, NOT a full statement) into an `Expr`.
/// Returns `Err` if the string is not a parseable expression. Callers MUST
/// fail closed on `Err` (reject the policy) rather than ignore the filter.
pub fn parse_sql_predicate(sql: &str) -> sqe_core::Result<Expr> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_comparison() {
        let e = parse_sql_predicate("clearance >= 3").unwrap();
        assert!(matches!(e, Expr::BinaryExpr(_)));
    }

    #[test]
    fn parses_compound_and() {
        // The case the toy parser silently corrupts.
        let e = parse_sql_predicate("region = 'EU' AND tier < 3").unwrap();
        assert!(matches!(e, Expr::BinaryExpr(_)));
        // Round-trip to SQL and confirm both clauses survived.
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.contains("region"));
        assert!(sql.contains("tier"));
        assert!(sql.to_uppercase().contains("AND"));
    }

    #[test]
    fn parses_in_list() {
        let e = parse_sql_predicate("dept IN ('hr', 'eng')").unwrap();
        let sql = datafusion::sql::unparser::expr_to_sql(&e).unwrap().to_string();
        assert!(sql.to_uppercase().contains("IN"));
    }

    #[test]
    fn parses_custom_mask_valueexpr() {
        // CUSTOM mask bodies are scalar exprs, often a function call.
        let e = parse_sql_predicate("concat('***', email)").unwrap();
        assert!(matches!(e, Expr::ScalarFunction(_)));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_sql_predicate("this is not sql !!!").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_sql_predicate("").is_err());
        assert!(parse_sql_predicate("   ").is_err());
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p sqe-policy policy_expr 2>&1 | tail -20`
Expected: FAIL (panics on `todo!()`).

- [ ] **Step 3: Implement using DataFusion's schema-free SQL-expr parser**

Replace the `todo!()` body. Primary approach: `SessionContext::parse_sql_expr` with an empty `DFSchema` — bare identifiers parse to `Expr::Column` without requiring the column to exist (resolution is deferred to planning).

```rust
use datafusion::common::DFSchema;
use datafusion::prelude::SessionContext;

pub fn parse_sql_predicate(sql: &str) -> sqe_core::Result<Expr> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(sqe_core::error::SqeError::Execution(
            "empty policy expression".to_string(),
        ));
    }
    // A fresh context is cheap and stateless; we only use its SQL->Expr planner.
    let ctx = SessionContext::new();
    let empty = DFSchema::empty();
    ctx.parse_sql_expr(trimmed, &empty).map_err(|e| {
        sqe_core::error::SqeError::Execution(format!(
            "failed to parse Ranger policy expression '{trimmed}': {e}"
        ))
    })
}
```

> Implementation note: `parse_sql_expr(sql, &DFSchema::empty())` is expected to yield unresolved `Expr::Column` for unknown identifiers in DataFusion 54. If Step 4 shows it instead ERRORS on unknown columns, switch the body to drive `datafusion::sql::planner::SqlToRel::sql_to_expr` over an empty schema provider (which does not resolve columns against a schema). Do NOT fall back to OPA's toy parser. Keep this function the single parse path for both row filters and CUSTOM masks.

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test -p sqe-policy policy_expr 2>&1 | tail -20`
Expected: PASS (all 6 tests). If `parses_single_comparison` or `parses_compound_and` fail with a column-resolution error, apply the `SqlToRel` fallback from the note, then re-run.

- [ ] **Step 5: Register and commit**

In `crates/sqe-policy/src/lib.rs` add:

```rust
pub mod policy_expr;
```

```bash
git add crates/sqe-policy/src/policy_expr.rs crates/sqe-policy/src/lib.rs
git commit -m "feat(policy): real SQL predicate parser for Ranger filterExpr/valueExpr"
```

---

## Task 3: Config — `PolicyEngine::Ranger` + `RangerPolicyConfig`

Add the fine-grained policy engine variant. This is SEPARATE from `access_control.ranger` (the GRANT/REVOKE write path against the `polaris` service). This config points at the `hive` service for enforcement.

**Files:**
- Modify: `crates/sqe-core/src/config.rs` (`PolicyEngine` enum ~2081, `FromStr` ~2094, `PolicyConfig` ~2110)

- [ ] **Step 1: Write failing config tests**

Add to the `#[cfg(test)] mod tests` block in `crates/sqe-core/src/config.rs` (near the existing `RangerConfig::default()` test at line ~5369):

```rust
    #[test]
    fn policy_engine_parses_ranger() {
        use std::str::FromStr;
        assert_eq!(
            crate::config::PolicyEngine::from_str("ranger").unwrap(),
            crate::config::PolicyEngine::Ranger
        );
    }

    #[test]
    fn policy_engine_unknown_lists_ranger() {
        use std::str::FromStr;
        let err = crate::config::PolicyEngine::from_str("nope").unwrap_err();
        assert!(err.contains("ranger"), "error must list ranger: {err}");
    }

    #[test]
    fn ranger_policy_config_defaults() {
        let c = crate::config::RangerPolicyConfig::default();
        assert_eq!(c.service_name, "hive");
        assert_eq!(c.admin_user, "admin");
        assert_eq!(c.cache_ttl_secs, 30);
    }
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p sqe-core policy_engine 2>&1 | tail -20`
Expected: FAIL (`PolicyEngine::Ranger` and `RangerPolicyConfig` do not exist).

- [ ] **Step 3: Add the `Ranger` variant**

In `crates/sqe-core/src/config.rs`, in `enum PolicyEngine` (after the `Cedar` variant at line ~2091):

```rust
    /// Apache Ranger fine-grained policies (hive service-def). Requires
    /// `[policy.ranger]`. Reads row-filter + data-mask policies and feeds the
    /// PlanRewriter. Separate from `access_control.backend = "ranger"`.
    Ranger,
```

In `FromStr` (the match in lines ~2098-2106), add the arm before `other =>` and update the error message:

```rust
            "ranger" => Ok(Self::Ranger),
            other => Err(format!(
                "unknown policy.engine {other:?}; expected one of passthrough, in-memory, opa, cedar, ranger"
            )),
```

- [ ] **Step 4: Add `RangerPolicyConfig` and nest it in `PolicyConfig`**

In `crates/sqe-core/src/config.rs`, add this struct immediately after the `OpaConfig` `Default` impl (after line ~2162). Reuse the existing `default_opa_*` free functions for the shared tuning knobs:

```rust
/// Fine-grained policy engine backed by a `hive`-type Apache Ranger service.
///
/// The Ranger Admin base URL is taken from `policy.ranger.url`. This is the
/// ENFORCEMENT path (row filters + column masks), distinct from
/// `access_control.ranger` (the GRANT/REVOKE write path on the `polaris`
/// service). `service_name` defaults to `hive` so policies are shared with
/// Apache Spark / Kyuubi.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RangerPolicyConfig {
    /// Ranger Admin base URL, e.g. `http://ranger-admin:6080`.
    #[serde(default)]
    pub url: String,
    /// The `hive` Ranger service instance to read. Shared with Spark/Kyuubi.
    #[serde(default = "default_ranger_policy_service_name")]
    pub service_name: String,
    /// Ranger Admin user for HTTP basic auth.
    #[serde(default = "default_ranger_admin_user")]
    pub admin_user: String,
    /// Ranger Admin password. Set via `SQE_POLICY__RANGER__ADMIN_PASSWORD`.
    #[serde(default)]
    pub admin_password: SecretString,
    /// HTTP timeout for a single Ranger download call, in seconds.
    #[serde(default = "default_opa_timeout_secs")]
    pub timeout_secs: u64,
    /// Maximum cached `ResolvedPolicy` entries.
    #[serde(default = "default_opa_cache_max_entries")]
    pub cache_max_entries: u64,
    /// Cache TTL in seconds.
    #[serde(default = "default_opa_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
    /// Consecutive failures before the circuit breaker opens.
    #[serde(default = "default_opa_breaker_failure_threshold")]
    pub breaker_failure_threshold: u32,
    /// How long to keep the breaker open before probing again, in seconds.
    #[serde(default = "default_opa_breaker_recovery_secs")]
    pub breaker_recovery_secs: u64,
    /// Accept self-signed TLS certs on the Ranger Admin endpoint.
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl Default for RangerPolicyConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            service_name: default_ranger_policy_service_name(),
            admin_user: default_ranger_admin_user(),
            admin_password: SecretString::default(),
            timeout_secs: default_opa_timeout_secs(),
            cache_max_entries: default_opa_cache_max_entries(),
            cache_ttl_secs: default_opa_cache_ttl_secs(),
            breaker_failure_threshold: default_opa_breaker_failure_threshold(),
            breaker_recovery_secs: default_opa_breaker_recovery_secs(),
            accept_invalid_certs: false,
        }
    }
}

fn default_ranger_policy_service_name() -> String {
    "hive".to_string()
}
```

In `struct PolicyConfig` (line ~2110), add the field after `opa`:

```rust
    /// Ranger fine-grained backend tuning. Used only when `engine = "ranger"`.
    #[serde(default)]
    pub ranger: RangerPolicyConfig,
}
```

> Note: `default_ranger_admin_user` already exists (defined for `access_control::RangerConfig` at line ~2069) and is reused here. `SecretString` is already imported in this file.

- [ ] **Step 5: Add the env override for the password**

Find the `env_override_str("SQE_POLICY__MASK_KEY", ...)` call (line ~3059) and add immediately after it, mirroring how `access_control.ranger.admin_password` is overridden elsewhere in this file (search `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD` for the exact pattern):

```rust
        if let Ok(v) = std::env::var("SQE_POLICY__RANGER__ADMIN_PASSWORD") {
            self.policy.ranger.admin_password = SecretString::from(v);
        }
```

> Verify the `SecretString` construction matches the existing access-control override (it may use `SecretString::new` / `.into()` — copy whichever that site uses so the two are identical).

- [ ] **Step 6: Run tests to confirm they pass**

Run: `cargo test -p sqe-core policy_engine 2>&1 | tail -20 && cargo test -p sqe-core ranger_policy_config 2>&1 | tail -10`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-core/src/config.rs
git commit -m "feat(config): add PolicyEngine::Ranger + RangerPolicyConfig (hive service)"
```

---

## Task 4: `ServicePolicies` model + download client

Build the `RangerStore` struct and the HTTP fetch against the plugin DOWNLOAD endpoint, which returns the whole bundle in one call. No `resolve()` logic yet (Task 5) — just fetch + deserialize + fail-closed + breaker.

**Files:**
- Create: `crates/sqe-policy/src/ranger_store.rs`
- Modify: `crates/sqe-policy/src/lib.rs`
- Reference (HTTP/auth patterns): `crates/sqe-policy/src/grants/ranger.rs:177-270`, `crates/sqe-policy/src/opa.rs:159-244`

- [ ] **Step 1: Write failing deserialization tests**

Create `crates/sqe-policy/src/ranger_store.rs`. Start with the model + a stub store and unit tests that parse a realistic `ServicePolicies` JSON. (HTTP-path tests live in Task 7's wiremock file.)

```rust
//! Apache Ranger fine-grained PolicyStore. Reads row-filter (policyType 2) and
//! data-mask (policyType 1) policies from a `hive`-type Ranger service and
//! returns a `ResolvedPolicy` for the PlanRewriter. Shares the policy set with
//! Apache Spark / Kyuubi. See docs/ranger-fine-grained-service-type.md.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use reqwest::Client;
use serde::Deserialize;
use tracing::{debug, warn};

use sqe_core::config::RangerPolicyConfig;
use sqe_core::SessionUser;

use crate::policy_breaker::PolicyCircuitBreaker;
use crate::policy_expr::parse_sql_predicate;
use crate::{MaskType, PolicyStore, ResolvedPolicy};

// --- Ranger policy bundle model (ServicePolicies) ---

#[derive(Debug, Deserialize, Default)]
pub struct ServicePolicies {
    #[serde(rename = "policyVersion", default)]
    pub policy_version: Option<i64>,
    #[serde(default)]
    pub policies: Vec<RangerPolicy>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RangerPolicy {
    #[serde(default)]
    pub id: i64,
    /// 0 = access, 1 = DATAMASK, 2 = ROWFILTER.
    #[serde(rename = "policyType", default)]
    pub policy_type: i32,
    #[serde(default)]
    pub is_enabled: bool,
    /// Resource map: keys are "database", "table", "column".
    #[serde(default)]
    pub resources: HashMap<String, RangerResource>,
    #[serde(rename = "dataMaskPolicyItems", default)]
    pub data_mask_policy_items: Vec<DataMaskPolicyItem>,
    #[serde(rename = "rowFilterPolicyItems", default)]
    pub row_filter_policy_items: Vec<RowFilterPolicyItem>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RangerResource {
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(rename = "isExcludes", default)]
    pub is_excludes: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct DataMaskPolicyItem {
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(rename = "dataMaskInfo", default)]
    pub data_mask_info: DataMaskInfo,
}

#[derive(Debug, Deserialize, Default)]
pub struct DataMaskInfo {
    #[serde(rename = "dataMaskType", default)]
    pub data_mask_type: String,
    #[serde(rename = "valueExpr", default)]
    pub value_expr: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct RowFilterPolicyItem {
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(rename = "rowFilterInfo", default)]
    pub row_filter_info: RowFilterInfo,
}

#[derive(Debug, Deserialize, Default)]
pub struct RowFilterInfo {
    #[serde(rename = "filterExpr", default)]
    pub filter_expr: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUNDLE: &str = r#"{
      "policyVersion": 7,
      "policies": [
        {
          "id": 1, "policyType": 1, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales_wh.sales"]},
            "table": {"values": ["orders"]},
            "column": {"values": ["amount"]}
          },
          "dataMaskPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}
          ]
        },
        {
          "id": 2, "policyType": 2, "isEnabled": true,
          "resources": {
            "database": {"values": ["sales_wh.sales"]},
            "table": {"values": ["orders"]}
          },
          "rowFilterPolicyItems": [
            {"users": [], "roles": ["analyst"],
             "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
          ]
        }
      ]
    }"#;

    #[test]
    fn parses_bundle() {
        let sp: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        assert_eq!(sp.policy_version, Some(7));
        assert_eq!(sp.policies.len(), 2);
        assert_eq!(sp.policies[0].policy_type, 1);
        assert_eq!(
            sp.policies[0].data_mask_policy_items[0].data_mask_info.data_mask_type,
            "MASK_NULL"
        );
        assert_eq!(
            sp.policies[1].row_filter_policy_items[0]
                .row_filter_info
                .filter_expr
                .as_deref(),
            Some("region = 'EU'")
        );
    }

    #[test]
    fn empty_bundle_is_default() {
        let sp: ServicePolicies = serde_json::from_str("{}").unwrap();
        assert!(sp.policies.is_empty());
        assert_eq!(sp.policy_version, None);
    }
}
```

- [ ] **Step 2: Run to confirm tests fail (module not registered)**

In `crates/sqe-policy/src/lib.rs` add:

```rust
pub mod ranger_store;
```

Run: `cargo test -p sqe-policy ranger_store 2>&1 | tail -20`
Expected: PASS for the two deserialization tests (this step is mostly the model). If it does not compile, fix field/serde-rename mismatches until `parses_bundle` and `empty_bundle_is_default` pass.

- [ ] **Step 3: Add the `RangerStore` struct + constructor + download fetch**

Append to `crates/sqe-policy/src/ranger_store.rs` (above the `#[cfg(test)]` block):

```rust
/// Fine-grained policy store backed by a `hive`-type Ranger service.
pub struct RangerStore {
    client: Client,
    /// Base download URL, e.g. ".../service/plugins/policies/download/hive".
    download_url: String,
    admin_user: String,
    admin_password: String,
    cache: Cache<String, ResolvedPolicy>,
    breaker: Arc<PolicyCircuitBreaker>,
}

impl RangerStore {
    pub fn from_config(cfg: &RangerPolicyConfig, admin_password: &str) -> Result<Self, reqwest::Error> {
        let base = cfg.url.trim_end_matches('/');
        let download_url = format!(
            "{base}/service/plugins/policies/download/{}",
            cfg.service_name
        );
        Ok(Self {
            client: Client::builder()
                .timeout(Duration::from_secs(cfg.timeout_secs))
                .danger_accept_invalid_certs(cfg.accept_invalid_certs)
                .build()?,
            download_url,
            admin_user: cfg.admin_user.clone(),
            admin_password: admin_password.to_string(),
            cache: Cache::builder()
                .time_to_live(Duration::from_secs(cfg.cache_ttl_secs))
                .max_capacity(cfg.cache_max_entries)
                .build(),
            breaker: Arc::new(PolicyCircuitBreaker::new(
                "Ranger",
                cfg.breaker_failure_threshold,
                Duration::from_secs(cfg.breaker_recovery_secs),
            )),
        })
    }

    /// Fetch the full policy bundle. Fail-closed: any transport/parse error
    /// trips the breaker and returns Err so the caller denies.
    async fn fetch_bundle(&self) -> sqe_core::Result<ServicePolicies> {
        self.breaker.check().map_err(|e| {
            sqe_core::error::SqeError::Execution(format!("Ranger unavailable: {e}"))
        })?;

        let resp = self
            .client
            .get(&self.download_url)
            .basic_auth(&self.admin_user, Some(&self.admin_password))
            .send()
            .await
            .map_err(|e| {
                self.breaker.record_failure();
                sqe_core::error::SqeError::Execution(format!("Ranger download failed: {e}"))
            })?;

        if !resp.status().is_success() {
            self.breaker.record_failure();
            return Err(sqe_core::error::SqeError::Execution(format!(
                "Ranger download returned status {}",
                resp.status()
            )));
        }

        let bundle: ServicePolicies = resp.json().await.map_err(|e| {
            self.breaker.record_failure();
            sqe_core::error::SqeError::Execution(format!("Failed to parse Ranger bundle: {e}"))
        })?;
        self.breaker.record_success();
        Ok(bundle)
    }
}
```

> 304/`lastKnownVersion` incremental refresh is intentionally NOT in the MVP — the moka TTL already bounds staleness, and a 304 needs a persisted last-version + cached bundle. Leave a `// TODO(phase2): lastKnownVersion + 304` comment at the fetch call. The download endpoint returns the full bundle every time, which is correct, just not maximally cheap.

- [ ] **Step 4: Confirm it compiles**

Run: `cargo build -p sqe-policy 2>&1 | tail -20`
Expected: builds. `RangerStore` is unused so far (no `PolicyStore` impl yet); that's fine — Task 5 adds `resolve`. If clippy later flags dead code, Task 5 resolves it.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-policy/src/ranger_store.rs crates/sqe-policy/src/lib.rs
git commit -m "feat(policy): RangerStore model + download-endpoint fetch (fail-closed)"
```

---

## Task 5: `resolve()` — flatten, match, map masks + row filters

The core logic. Flatten Iceberg `namespace`/`table` to hive `database`/`table`, select matching policies, map mask types, parse row filters, merge multiple items, apply deny/exemption ordering. Fail-closed throughout.

**Files:**
- Modify: `crates/sqe-policy/src/ranger_store.rs`

- [ ] **Step 1: Write failing tests for flattening + matching + mapping**

Add these tests inside the existing `#[cfg(test)] mod tests` block in `ranger_store.rs`:

```rust
    fn user(name: &str, roles: &[&str]) -> SessionUser {
        SessionUser {
            username: name.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn flattens_iceberg_to_hive_database() {
        // namespace "sales" in catalog-agnostic form -> hive database "sales".
        // Multi-level namespace "sales.eu" -> "sales.eu" (dotted, Kyuubi style).
        assert_eq!(hive_database("sales"), "sales");
        assert_eq!(hive_database("sales.eu"), "sales.eu");
    }

    #[test]
    fn mask_null_maps_to_nullify() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales_wh.sales");
        assert!(matches!(policy.column_masks.get("amount"), Some(MaskType::Nullify)));
    }

    #[test]
    fn row_filter_applied_for_matching_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales_wh.sales");
        assert_eq!(policy.row_filters.len(), 1);
    }

    #[test]
    fn no_match_for_other_role() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        let policy = resolve_from_bundle(&bundle, &user("bob", &["engineer"]), "orders", "sales_wh.sales");
        assert!(policy.column_masks.is_empty());
        assert!(policy.row_filters.is_empty());
    }

    #[test]
    fn user_match_works_too() {
        let bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        // alice by username even without the role
        let mut b = bundle;
        b.policies[0].data_mask_policy_items[0].roles.clear();
        b.policies[0].data_mask_policy_items[0].users = vec!["alice".to_string()];
        let policy = resolve_from_bundle(&b, &user("alice", &[]), "orders", "sales_wh.sales");
        assert!(policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn unsupported_mask_restricts_column_failclosed() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0].data_mask_info.data_mask_type =
            "MASK_SHOW_LAST_4".to_string();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales_wh.sales");
        // Not silently unmasked: the column is dropped instead.
        assert!(policy.restricted_columns.contains(&"amount".to_string()));
        assert!(!policy.column_masks.contains_key("amount"));
    }

    #[test]
    fn mask_none_is_exemption() {
        let mut bundle: ServicePolicies = serde_json::from_str(BUNDLE).unwrap();
        bundle.policies[0].data_mask_policy_items[0].data_mask_info.data_mask_type =
            "MASK_NONE".to_string();
        let policy = resolve_from_bundle(&bundle, &user("alice", &["analyst"]), "orders", "sales_wh.sales");
        assert!(!policy.column_masks.contains_key("amount"));
        assert!(!policy.restricted_columns.contains(&"amount".to_string()));
    }
```

- [ ] **Step 2: Run to confirm they fail**

Run: `cargo test -p sqe-policy ranger_store 2>&1 | tail -20`
Expected: FAIL (`hive_database`, `resolve_from_bundle` undefined).

- [ ] **Step 3: Implement the pure helpers**

Add to `ranger_store.rs` (above the test module, after the `impl RangerStore` block):

```rust
/// Flatten an Iceberg namespace to a hive `database` name. SQE namespaces are
/// already dotted multi-level strings; Kyuubi uses the same dotted convention,
/// so this is identity for now. Catalog is intentionally dropped (hive has no
/// catalog level); cross-engine policies must be written without a catalog
/// prefix. See docs/ranger-fine-grained-service-type.md (flattening sharp edge).
fn hive_database(namespace: &str) -> String {
    namespace.to_string()
}

/// True if a Ranger resource value list matches `target` (supports the `*`
/// wildcard and exact match; excludes invert the result).
fn resource_matches(res: &RangerResource, target: &str) -> bool {
    let hit = res
        .values
        .iter()
        .any(|v| v == "*" || v == target);
    hit ^ res.is_excludes
}

/// True if a policy's database+table resources match the target table.
fn policy_matches_table(p: &RangerPolicy, database: &str, table: &str) -> bool {
    let db_ok = p
        .resources
        .get("database")
        .map(|r| resource_matches(r, database))
        .unwrap_or(false);
    let tbl_ok = p
        .resources
        .get("table")
        .map(|r| resource_matches(r, table))
        .unwrap_or(false);
    db_ok && tbl_ok
}

/// True if a policy-item (mask or row-filter) applies to this user/roles.
fn item_matches(users: &[String], roles: &[String], user: &SessionUser) -> bool {
    users.iter().any(|u| u == &user.username)
        || roles.iter().any(|r| user.roles.contains(r))
}

/// Map a Ranger hive data-mask type to an SQE `MaskType`. Returns:
///  - `Ok(Some(mask))` for supported types,
///  - `Ok(None)` for MASK_NONE (explicit exemption: no mask, not restricted),
///  - `Err(())` for types not yet supported (caller restricts the column).
fn map_mask(info: &DataMaskInfo) -> Result<Option<MaskType>, ()> {
    match info.data_mask_type.as_str() {
        "MASK_NULL" => Ok(Some(MaskType::Nullify)),
        "MASK_NONE" => Ok(None),
        "MASK_HASH" => Ok(Some(MaskType::Hash)),
        "CUSTOM" => {
            let expr_str = info.value_expr.as_deref().ok_or(())?;
            // {col} is Ranger's placeholder; the rewriter substitutes the real
            // column, so a bare reference is fine here. Replace {col} with the
            // column name at resolve time (done by the caller via column ctx).
            match parse_sql_predicate(&expr_str.replace("{col}", "__col__")) {
                Ok(expr) => Ok(Some(MaskType::Custom(expr))),
                Err(_) => Err(()),
            }
        }
        // Phase 2: MASK, MASK_SHOW_LAST_4, MASK_SHOW_FIRST_4, MASK_DATE_SHOW_YEAR
        _ => Err(()),
    }
}

/// Build a `ResolvedPolicy` from an already-fetched bundle. Pure (no I/O), so
/// it is unit-tested directly and reused by `resolve()` after the cache miss.
fn resolve_from_bundle(
    bundle: &ServicePolicies,
    user: &SessionUser,
    table: &str,
    namespace: &str,
) -> ResolvedPolicy {
    let database = hive_database(namespace);
    let mut policy = ResolvedPolicy::default();

    for p in &bundle.policies {
        if !p.is_enabled || !policy_matches_table(p, &database, table) {
            continue;
        }

        // Data-mask policy (policyType 1)
        if p.policy_type == 1 {
            let column = p
                .resources
                .get("column")
                .and_then(|r| r.values.first())
                .cloned();
            let Some(column) = column else { continue };
            for item in &p.data_mask_policy_items {
                if !item_matches(&item.users, &item.roles, user) {
                    continue;
                }
                match map_mask(&item.data_mask_info) {
                    Ok(Some(mask)) => {
                        policy.column_masks.insert(column.clone(), mask);
                    }
                    Ok(None) => { /* MASK_NONE exemption: leave column visible */ }
                    Err(()) => {
                        warn!(
                            column = %column,
                            mask_type = %item.data_mask_info.data_mask_type,
                            "unsupported Ranger mask type; restricting column (fail-closed)"
                        );
                        if !policy.restricted_columns.contains(&column) {
                            policy.restricted_columns.push(column.clone());
                        }
                    }
                }
            }
        }

        // Row-filter policy (policyType 2)
        if p.policy_type == 2 {
            for item in &p.row_filter_policy_items {
                if !item_matches(&item.users, &item.roles, user) {
                    continue;
                }
                if let Some(expr_str) = &item.row_filter_info.filter_expr {
                    match parse_sql_predicate(expr_str) {
                        Ok(expr) => policy.row_filters.push(expr),
                        Err(e) => {
                            // Fail-closed: an unparseable filter denies the table
                            // rather than running unfiltered.
                            warn!(filter = %expr_str, error = %e,
                                "unparseable Ranger row filter; denying (fail-closed)");
                            policy.row_filters.push(datafusion::logical_expr::lit(false));
                        }
                    }
                }
            }
        }
    }

    debug!(
        user = %user.username, table = %table, db = %database,
        masks = policy.column_masks.len(),
        filters = policy.row_filters.len(),
        restricted = policy.restricted_columns.len(),
        "resolved Ranger policy"
    );
    policy
}
```

> CUSTOM-mask `{col}` handling: the `__col__` placeholder above is a stand-in. Confirm against `plan_rewriter.rs` how `MaskType::Custom(expr)` is spliced — if the rewriter substitutes the column by position/name it may need the real column reference. If so, change `map_mask` to take the column name and replace `{col}` with `col(column)` instead of a placeholder string. Add a test mirroring `plan_rewriter.rs`'s Custom-mask test once the splice contract is confirmed. (MVP can defer CUSTOM if the contract is unclear — `MASK_NULL`/`MASK_HASH` cover the demo.)

- [ ] **Step 4: Run helper tests to confirm they pass**

Run: `cargo test -p sqe-policy ranger_store 2>&1 | tail -25`
Expected: PASS (all flatten/match/map tests). Fix any mismatch until green.

- [ ] **Step 5: Wire `resolve()` into the `PolicyStore` impl with caching**

Add to `ranger_store.rs` (after the `impl RangerStore` block, before the pure helpers):

```rust
fn cache_key(user: &SessionUser, table: &str, namespace: &str) -> String {
    let mut roles = user.roles.clone();
    roles.sort();
    format!("{}:{}:{}:{}", user.username, namespace, table, roles.join(","))
}

#[async_trait]
impl PolicyStore for RangerStore {
    async fn resolve(
        &self,
        user: &SessionUser,
        table_name: &str,
        namespace: &str,
    ) -> sqe_core::Result<ResolvedPolicy> {
        let key = cache_key(user, table_name, namespace);
        if let Some(cached) = self.cache.get(&key).await {
            return Ok(cached);
        }
        let bundle = self.fetch_bundle().await?;
        let policy = resolve_from_bundle(&bundle, user, table_name, namespace);
        self.cache.insert(key, policy.clone()).await;
        Ok(policy)
    }

    fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }
}
```

- [ ] **Step 6: Run all sqe-policy tests + clippy**

Run: `cargo test -p sqe-policy 2>&1 | tail -20 && cargo clippy -p sqe-policy --all-targets -- -D warnings 2>&1 | tail -20`
Expected: PASS, no clippy warnings (dead-code on `RangerStore` is now resolved since it implements `PolicyStore`).

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-policy/src/ranger_store.rs
git commit -m "feat(policy): RangerStore::resolve — flatten, match, map masks + row filters"
```

---

## Task 6: wiremock HTTP integration test

Prove the full HTTP path: download endpoint -> parse -> resolve -> `ResolvedPolicy`, plus fail-closed on 5xx/garbage and breaker behavior.

**Files:**
- Create: `crates/sqe-policy/tests/ranger_store_wiremock.rs`
- Reference: `crates/sqe-policy/tests/opa_wiremock.rs` (mirror its harness + `wiremock` deps in `Cargo.toml` dev-deps — already present)

- [ ] **Step 1: Write the wiremock tests**

Create `crates/sqe-policy/tests/ranger_store_wiremock.rs`:

```rust
//! HTTP-path tests for RangerStore against a mock Ranger Admin (wiremock).

use sqe_core::config::RangerPolicyConfig;
use sqe_core::SessionUser;
use sqe_policy::ranger_store::RangerStore;
use sqe_policy::PolicyStore;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BUNDLE: &str = r#"{
  "policyVersion": 1,
  "policies": [
    {"id": 1, "policyType": 1, "isEnabled": true,
     "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}, "column": {"values": ["amount"]}},
     "dataMaskPolicyItems": [{"roles": ["analyst"], "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]},
    {"id": 2, "policyType": 2, "isEnabled": true,
     "resources": {"database": {"values": ["sales"]}, "table": {"values": ["orders"]}},
     "rowFilterPolicyItems": [{"roles": ["analyst"], "rowFilterInfo": {"filterExpr": "region = 'EU' AND tier < 3"}}]}
  ]
}"#;

fn cfg(url: &str) -> RangerPolicyConfig {
    RangerPolicyConfig {
        url: url.to_string(),
        service_name: "hive".to_string(),
        ..RangerPolicyConfig::default()
    }
}

fn analyst() -> SessionUser {
    SessionUser { username: "alice".into(), roles: vec!["analyst".into()] }
}

#[tokio::test]
async fn resolves_mask_and_filter_over_http() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(BUNDLE))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri()), "admin").unwrap();
    let policy = store.resolve(&analyst(), "orders", "sales").await.unwrap();

    assert!(policy.column_masks.contains_key("amount"));
    assert_eq!(policy.row_filters.len(), 1);
}

#[tokio::test]
async fn fail_closed_on_server_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri()), "admin").unwrap();
    let result = store.resolve(&analyst(), "orders", "sales").await;
    assert!(result.is_err(), "5xx must fail closed (Err), not allow-all");
}

#[tokio::test]
async fn fail_closed_on_garbage_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<<not json>>"))
        .mount(&server)
        .await;

    let store = RangerStore::from_config(&cfg(&server.uri()), "admin").unwrap();
    assert!(store.resolve(&analyst(), "orders", "sales").await.is_err());
}
```

- [ ] **Step 2: Run the wiremock tests**

Run: `cargo test -p sqe-policy --test ranger_store_wiremock 2>&1 | tail -25`
Expected: PASS (3 tests). If `RangerStore`/`ServicePolicies` field visibility blocks the test, make the needed items `pub` in `ranger_store.rs`.

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-policy/tests/ranger_store_wiremock.rs
git commit -m "test(policy): wiremock coverage for RangerStore HTTP path (fail-closed)"
```

---

## Task 7: AUTH-01 — wire the policy-engine dispatcher into both binaries

Both coordinator binaries hardcode `PassthroughEnforcer` behind a `TODO(AUTH-01)` and a loud `error!`. Build ONE shared dispatcher that constructs the right `PolicyStore` from `config.policy.engine` (including the new Ranger), wraps it in `PolicyPlanRewriter`, and returns both. Wire it in `main.rs` and `bin/sqe_server.rs` so they cannot drift (precedent: `mode::warns_unauthenticated_workers`).

**Files:**
- Create: `crates/sqe-coordinator/src/policy_wiring.rs`
- Modify: `crates/sqe-coordinator/src/lib.rs`
- Modify: `crates/sqe-coordinator/src/main.rs:199-205, 273-276`
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs:685-691` and its `QueryHandler::new` call (~line 938)

- [ ] **Step 1: Write the dispatcher with a unit test**

Create `crates/sqe-coordinator/src/policy_wiring.rs`:

```rust
//! AUTH-01: build the policy enforcer + store from `config.policy.engine`.
//!
//! Shared by both coordinator binaries (`main.rs`, `bin/sqe_server.rs`) so the
//! enforcement wiring cannot drift between them. Returns the enforcer that the
//! query pipeline runs AND the same `Arc<dyn PolicyStore>` so GRANT/REVOKE can
//! invalidate its cache.

use std::sync::Arc;

use sqe_core::config::PolicyEngine;
use sqe_core::SqeConfig;
use sqe_policy::plan_rewriter::PolicyPlanRewriter;
use sqe_policy::{PassthroughEnforcer, PolicyEnforcer, PolicyStore};

/// Construct the policy enforcer and (optionally) the backing store.
/// Passthrough returns `(PassthroughEnforcer, None)`.
pub fn build_policy_enforcer(
    config: &SqeConfig,
) -> anyhow::Result<(Arc<dyn PolicyEnforcer>, Option<Arc<dyn PolicyStore>>)> {
    let mask_key: Option<Arc<Vec<u8>>> = if config.policy.mask_key.is_empty() {
        None
    } else {
        Some(Arc::new(config.policy.mask_key.as_bytes().to_vec()))
    };

    let store: Option<Arc<dyn PolicyStore>> = match config.policy.engine {
        PolicyEngine::Passthrough => None,
        PolicyEngine::InMemory => {
            Some(Arc::new(sqe_policy::policy_store::InMemoryPolicyStore::new()))
        }
        PolicyEngine::Opa => {
            // OPA reads url from access_control.url historically; if a dedicated
            // policy.opa.url field exists use it. Mirror the existing OpaStore
            // construction site if one already exists; otherwise use the
            // documented OPA endpoint config.
            anyhow::bail!(
                "policy.engine = opa selected but OPA wiring is not part of this change; \
                 use ranger or in-memory"
            )
        }
        PolicyEngine::Cedar => {
            anyhow::bail!("policy.engine = cedar is not implemented")
        }
        PolicyEngine::Ranger => {
            let rc = &config.policy.ranger;
            if rc.url.is_empty() {
                anyhow::bail!("policy.engine = ranger requires policy.ranger.url");
            }
            let store = sqe_policy::ranger_store::RangerStore::from_config(
                rc,
                rc.admin_password.expose(),
            )?;
            Some(Arc::new(store))
        }
    };

    match store {
        None => Ok((Arc::new(PassthroughEnforcer), None)),
        Some(store) => {
            let rewriter = PolicyPlanRewriter::new(store.clone()).with_mask_key(mask_key);
            Ok((Arc::new(rewriter), Some(store)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_yields_no_store() {
        let config = SqeConfig::default();
        let (_enforcer, store) = build_policy_enforcer(&config).unwrap();
        assert!(store.is_none());
    }

    #[test]
    fn ranger_without_url_errors() {
        let mut config = SqeConfig::default();
        config.policy.engine = PolicyEngine::Ranger;
        assert!(build_policy_enforcer(&config).is_err());
    }
}
```

> Verify `SecretString::expose()` is the correct accessor (the access-control Ranger arm in `main.rs:536` uses `r.admin_password.expose()` — match it exactly). The top-level config type is `SqeConfig` (`use sqe_core::SqeConfig;`), already imported in both binaries. The OPA arm is intentionally a `bail!` stub: wiring OPA is not this change's scope, but the dispatcher exists so OPA can be filled in later without touching the binaries again.

- [ ] **Step 2: Register the module + run the unit test**

In `crates/sqe-coordinator/src/lib.rs`, add (near the other `pub mod` lines):

```rust
pub mod policy_wiring;
```

Run: `cargo test -p sqe-coordinator policy_wiring 2>&1 | tail -20`
Expected: PASS (2 tests).

- [ ] **Step 3: Wire `main.rs`**

In `crates/sqe-coordinator/src/main.rs`, replace the hardcoded passthrough block (lines 199-205) with:

```rust
    // AUTH-01: build the enforcer + store from config.policy.engine.
    let (policy_enforcer, policy_store) =
        sqe_coordinator::policy_wiring::build_policy_enforcer(&config)?;
    if config.policy.engine != sqe_core::config::PolicyEngine::Passthrough {
        tracing::info!(
            engine = ?config.policy.engine,
            "policy enforcement ACTIVE (row filters + column masks)"
        );
    }
```

Delete the now-stale startup `error!` block at lines 146-157 (the engine is wired; the warning is wrong). Then update the `QueryHandler::new` call (line 276) from `None, // policy_store ...` to:

```rust
            policy_store,
```

- [ ] **Step 4: Wire `bin/sqe_server.rs`**

In `crates/sqe-coordinator/src/bin/sqe_server.rs`, replace the hardcoded passthrough block (lines 685-691) with the same construction:

```rust
    // AUTH-01: build the enforcer + store from config.policy.engine.
    let (policy_enforcer, policy_store) =
        sqe_coordinator::policy_wiring::build_policy_enforcer(&config)?;
    if config.policy.engine != sqe_core::config::PolicyEngine::Passthrough {
        tracing::info!(
            engine = ?config.policy.engine,
            "policy enforcement ACTIVE (row filters + column masks)"
        );
    }
```

Delete the stale `error!` block at lines 549-559. Update the `QueryHandler::new` call (~line 938) `None, // policy_store ...` to `policy_store,`.

> Both binaries must pass `policy_store` (the SAME `Arc`) so GRANT/REVOKE cache invalidation reaches the live store (see `query_handler.rs:3246` comment about the shared `Arc<dyn PolicyStore>`).

- [ ] **Step 5: Build both binaries + full coordinator tests**

Run: `cargo build -p sqe-coordinator --bins 2>&1 | tail -20 && cargo test -p sqe-coordinator policy 2>&1 | tail -20`
Expected: both binaries build; policy_wiring tests pass. Fix any type mismatch in the `QueryHandler::new` argument position (the enforcer is the first arg, store the second — confirm against the existing call).

- [ ] **Step 6: Commit**

```bash
git add crates/sqe-coordinator/src/policy_wiring.rs crates/sqe-coordinator/src/lib.rs crates/sqe-coordinator/src/main.rs crates/sqe-coordinator/src/bin/sqe_server.rs
git commit -m "feat(coordinator): AUTH-01 wire policy engine dispatcher (incl. ranger)"
```

---

## Task 8: Quickstart end-to-end demo

Prove enforcement on the running `polaris-ranger-keycloak` stack: an `analyst` sees `orders.amount` masked to NULL and only EU rows; an admin sees everything. This is the demonstrable deliverable.

**Files:**
- Modify: `quickstart/polaris-ranger-keycloak/sqe.toml`
- Modify: `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh` (add a `hive` service + a mask + row-filter policy)
- Modify: `quickstart/polaris-ranger-keycloak/test.sh` (assert masked/filtered output)
- Modify: `quickstart/polaris-ranger-keycloak/OVERVIEW.md` (document the enforcement path)

- [ ] **Step 1: Add `[policy]` to `sqe.toml`**

Add to `quickstart/polaris-ranger-keycloak/sqe.toml`:

```toml
[policy]
engine = "ranger"

[policy.ranger]
url = "http://ranger-admin:6080"
service_name = "hive"
admin_user = "admin"
# admin_password via SQE_POLICY__RANGER__ADMIN_PASSWORD env in the compose file
```

Add the env var to the SQE coordinator service in `docker-compose.yml` (mirror how `SQE_ACCESS_CONTROL__RANGER__ADMIN_PASSWORD` is set).

- [ ] **Step 2: Create the hive service + fine-grained policies in bootstrap**

In `ranger/bootstrap-ranger.sh`, after the existing `polaris` service seed, add (mirror the existing CSRF-header + create-policy curl pattern already in the script):
- Create a `hive`-type service named `hive`.
- Create a DATAMASK policy (`policyType: 1`) on `database=sales_wh.sales table=orders column=amount`, item `roles:["analyst"] dataMaskInfo:{dataMaskType:"MASK_NULL"}`.
- Create a ROWFILTER policy (`policyType: 2`) on `database=sales_wh.sales table=orders`, item `roles:["analyst"] rowFilterInfo:{filterExpr:"region = 'EU'"}`.

> Use the exact `database` value SQE will send. The store flattens the namespace via `hive_database(namespace)`. Confirm the namespace string SQE passes for `sales_wh.sales` (check `plan_rewriter.rs` resolution / a debug log) and write the Ranger `database` resource to match it byte-for-byte, or the policy silently won't apply.

- [ ] **Step 3: Add enforcement assertions to `test.sh`**

Add a section after the existing grant tests:
- As `alice` (analyst): `SELECT region, amount FROM sales_wh.sales.orders` -> assert `amount` is NULL for returned rows AND every returned `region` is `EU`.
- As an admin/exempt user: same query -> assert non-NULL amounts and non-EU regions appear.

Pattern-match the existing `is_denial` / result-assertion helpers already in `test.sh`.

- [ ] **Step 4: Run the quickstart end-to-end**

Run: `cd quickstart/polaris-ranger-keycloak && ./run.sh && ./test.sh 2>&1 | tail -40`
Expected: the new mask + row-filter assertions PASS alongside the existing 13 grant tests.

> If the mask/filter does not apply: (a) check the SQE coordinator log for `resolved Ranger policy masks=.. filters=..` — `0/0` means the `database`/`table` resource didn't match (fix the bootstrap resource string to match the logged `db=`); (b) confirm `engine = "ranger"` took effect (startup logs `policy enforcement ACTIVE`).

- [ ] **Step 5: Document the enforcement path in OVERVIEW.md**

Add a short "Fine-grained enforcement (SQE-side)" section to `OVERVIEW.md`: SQE reads the `hive` Ranger service via the download endpoint and rewrites the plan; this is separate from the Polaris coarse gate and the GRANT/REVOKE write path; the same policies would be enforced by Spark/Kyuubi.

- [ ] **Step 6: Commit**

```bash
git add quickstart/polaris-ranger-keycloak/
git commit -m "feat(quickstart): ranger fine-grained mask + row-filter e2e demo"
```

---

## Final: gates, docs, MR

- [ ] **Run full project gates**

```bash
cargo build --all 2>&1 | tail -5
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
cargo test --all 2>&1 | tail -20
```
Expected: all green.

- [ ] **Update project state files** (per CLAUDE.md "After Completing Work")
- `README.md` roadmap: mark fine-grained Ranger row-filter/column-mask (Phase 1) done.
- `nextsteps.md`: shift the NEXT pointer; note Phase 2 (mask UDFs, session-context functions, tags) remains.
- Tick the relevant boxes in `docs/fine-grained-policy.md` "Phase shape" (items 3 partial, 4 done; 1/2/5 deferred).

- [ ] **Open the MR**

```bash
git push -u origin feat/ranger-policy-store
```
Then open an MR titled "feat: Ranger fine-grained PolicyStore (Phase 1: row-filter + column-mask)" summarizing: new `RangerStore`, real SQL predicate parser, AUTH-01 dispatcher wired in both binaries, quickstart demo, and explicit Phase-2 deferrals (mask UDFs, session-context functions, tag masking).

---

## Out of scope (Phase 2/3 — do NOT build here)

- Partial/substring/date mask UDFs (`MASK`, `MASK_SHOW_LAST_4`, `MASK_SHOW_FIRST_4`, `MASK_DATE_SHOW_YEAR`). MVP fail-closes these to `restricted_columns`.
- Session-context SQL functions (`current_user()`, `current_role()`, `is_role_in_session()`) and the richer `SessionUser` role model.
- Tag-based masking (`tagPolicies` + Iceberg/Atlas tag source).
- `lastKnownVersion` / HTTP 304 incremental refresh.
- OPA dispatcher arm (left as a documented `bail!` stub).
- Cross-engine name-match hardening with a live Kyuubi (the flattening is identity for now; Phase 2 locks it against Kyuubi's exact convention).
