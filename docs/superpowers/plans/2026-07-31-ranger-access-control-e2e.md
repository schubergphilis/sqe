# Ranger / Polaris Access Control E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move SQE's access-control coverage from a grep-based shell harness into the Rust integration tier, with exact-value assertions, and add the paths that have never run against a live Ranger: row filters, tag column masks, tag row filters, tag fail-closed, and keyed hash masks.

**Architecture:** A new module in the consolidated `sqe-coordinator` integration harness builds a real in-process `QueryHandler` wired to the Ranger policy enforcer and the Ranger grant backend, authenticates alice/bob/carol/dave through Keycloak ROPC, and asserts on decoded `RecordBatch` values. The stack is a subset of the existing `quickstart/polaris-ranger-keycloak` compose file. Ranger fixtures (services and policies) are created over the Admin REST API by the test itself, in test-owned services, so the demo fixtures and the Spark cross-compare stay untouched.

**Tech Stack:** Rust, tokio, arrow-array, reqwest (already a `sqe-coordinator` dev-dep), serde_json, Apache Ranger 2.8 Admin REST API, Polaris 1.5 with the embedded Ranger authorizer, Keycloak, docker compose.

**Spec:** `docs/superpowers/specs/2026-07-31-ranger-access-control-e2e-design.md`

## Global Constraints

- Fine-grained policies go in test-owned Ranger services `sqe_ac_hive` (type `hive`) and `sqe_ac_tag` (type `tag`). Never on the demo's shared `hive` service.
- Every policy the test creates is named with the prefix `sqe-ac-e2e-`. Setup deletes all policies with that prefix before creating any.
- The coarse gate stays on the shared `polaris` Ranger service. Its name is fixed by Polaris's authorizer config and must not be changed.
- Ranger state-changing requests require the header `X-XSRF-HEADER: x`. Without it Ranger returns 401.
- Ranger credentials: user `admin`, password `rangerR0cks!` (`.env` `RANGER_ADMIN_PASSWORD`).
- Host ports (from `quickstart/polaris-ranger-keycloak/.env.example`): Keycloak `38080`, Polaris `28181`, Ranger `26080`, RustFS `29000`.
- User passwords follow the demo convention `<user>123`: `alice123`, `bob123`, `carol123`, `dave123`. carol is the only `sqe_admin`.
- Every test is `#[tokio::test(flavor = "multi_thread")]` plus `#[ignore = "..."]` **and** gated on `SQE_AC_E2E=1`. When the env var is unset the test returns early. When it is set and the stack is unreachable the test **panics**. Never a silent pass.
- `policy.ranger.cache-ttl-secs = 2` in the test config. Waits use the `eventually` helper, never bare `sleep`.
- The `TableMetadataCache` instance passed to `build_policy_enforcer` must be the same one passed to `QueryHandler::with_table_cache`. A separate cache makes `CacheTagSource` report tag state unknown, which fails closed and turns every tag test red for the wrong reason.
- Ranger's hive `database` resource value is the LAST namespace component (`ranger_store.rs::hive_database`). For `sales_wh.ac.orders` that is `database = "ac"`, `table = "orders"`.
- Run tests with `--test-threads=1` and `RUST_MIN_STACK=33554432`.

---

### Task 1: Share `build_grant_backend` between the binaries and the test

`build_grant_backend` exists twice as a private function, byte-identical (52 lines each), in `crates/sqe-coordinator/src/main.rs:633` and `crates/sqe-coordinator/src/bin/sqe_server.rs:802`. The test needs it. Move it next to `build_policy_enforcer`.

**Files:**
- Modify: `crates/sqe-coordinator/src/policy_wiring.rs` (add the function)
- Modify: `crates/sqe-coordinator/src/main.rs:404,633-684` (call the shared one, delete the copy)
- Modify: `crates/sqe-coordinator/src/bin/sqe_server.rs:802-853,1392` (same)
- Test: `crates/sqe-coordinator/src/policy_wiring.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `pub fn sqe_coordinator::policy_wiring::build_grant_backend(config: &sqe_core::SqeConfig) -> anyhow::Result<Option<std::sync::Arc<dyn sqe_policy::grants::GrantBackend>>>`

- [ ] **Step 1: Confirm the two copies are identical before touching them**

```bash
cd /Users/jjverhoeks/git/schuberg/vpf-data-ai/chameleon/Applications/sqlengine
awk '/^fn build_grant_backend/,/^}/' crates/sqe-coordinator/src/main.rs > /tmp/gb-main.txt
awk '/^fn build_grant_backend/,/^}/' crates/sqe-coordinator/src/bin/sqe_server.rs > /tmp/gb-server.txt
diff /tmp/gb-main.txt /tmp/gb-server.txt && echo IDENTICAL
```

Expected: `IDENTICAL`. If it prints a diff, STOP and report it: a behavioral difference between the two coordinator binaries is a bug to be filed, not silently reconciled by this task.

- [ ] **Step 2: Write the failing test in `policy_wiring.rs`**

Note: this is the only test in this plan that joins the DEFAULT suite (`cargo test -p sqe-coordinator --lib`, which `make test` and the cargo gate run), so it must pass with nothing listening on port 26080. That is safe: `RangerGrantBackend::new` (`crates/sqe-policy/src/grants/ranger.rs:190`) only builds a `reqwest::Client` and copies strings. It performs no I/O. Re-confirm with `sed -n 190,212p crates/sqe-policy/src/grants/ranger.rs` before running; if that ever changes, keep only the `PASSTHROUGH_TOML` assertion in the default suite.

Append to `crates/sqe-coordinator/src/policy_wiring.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const RANGER_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59997"

[access_control]
backend = "ranger"
url = "http://localhost:26080"

[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
realm = "*"
"#;

    const PASSTHROUGH_TOML: &str = r#"
[coordinator]

[auth]

[catalog]
catalog_url = "http://localhost:59997"
"#;

    #[test]
    fn ranger_config_yields_a_grant_backend() {
        let config: sqe_core::SqeConfig = toml::from_str(RANGER_TOML).expect("parse ranger toml");
        let backend = build_grant_backend(&config).expect("build ranger grant backend");
        assert!(
            backend.is_some(),
            "access_control.backend = ranger with a url must yield a grant backend"
        );
    }

    #[test]
    fn no_access_control_config_yields_no_backend() {
        let config: sqe_core::SqeConfig =
            toml::from_str(PASSTHROUGH_TOML).expect("parse passthrough toml");
        let backend = build_grant_backend(&config).expect("build passthrough grant backend");
        assert!(
            backend.is_none(),
            "no access_control backend configured must yield None, not a live client"
        );
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p sqe-coordinator --lib policy_wiring 2>&1 | tail -20`
Expected: FAIL to compile, `cannot find function build_grant_backend in this scope`.

- [ ] **Step 4: Move the function**

Cut the 52-line `fn build_grant_backend(...)` block verbatim from `crates/sqe-coordinator/src/main.rs` and paste it into `crates/sqe-coordinator/src/policy_wiring.rs` after `build_policy_enforcer`. Change the signature line to `pub fn build_grant_backend(`. Add these imports at the top of `policy_wiring.rs`:

```rust
use sqe_core::SqeConfig;
use sqe_policy::grants::{
    chameleon::ChameleonGrantBackend, polaris::PolarisGrantBackend, ranger::RangerGrantBackend,
    GrantBackend,
};
```

If any import path differs from what `main.rs` used, copy `main.rs`'s exact `use` lines for those types instead of the block above. Add a doc comment above the function:

```rust
/// Construct the GRANT/REVOKE backend from `config.access_control`.
///
/// Shared by both coordinator binaries (`main.rs`, `bin/sqe_server.rs`) and by
/// the access-control e2e test, for the same reason `build_policy_enforcer` is
/// shared: three copies of this wiring would drift.
```

- [ ] **Step 5: Delete the copy in `sqe_server.rs` and update both call sites**

In `crates/sqe-coordinator/src/main.rs`, delete the now-moved function and change line 404 to:

```rust
    let grant_backend: Option<Arc<dyn GrantBackend>> =
        crate::policy_wiring::build_grant_backend(&config)?;
```

In `crates/sqe-coordinator/src/bin/sqe_server.rs`, delete its copy (lines 802-853) and change line 1392 to:

```rust
    let grant_backend: Option<Arc<dyn GrantBackend>> =
        sqe_coordinator::policy_wiring::build_grant_backend(&config)?;
```

Remove any `use` statements in the two binaries that are now unused (the compiler will name them as warnings; with `-D warnings` in clippy they are errors).

- [ ] **Step 6: Run the tests and the strict lint**

```bash
cargo test -p sqe-coordinator --lib policy_wiring
cargo clippy -p sqe-coordinator --all-targets -- -D warnings
```
Expected: both tests PASS, clippy clean.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/src/policy_wiring.rs crates/sqe-coordinator/src/main.rs crates/sqe-coordinator/src/bin/sqe_server.rs
git commit -m "refactor(coordinator): share build_grant_backend via policy_wiring

Both binaries carried a byte-identical private copy. The access-control e2e
test needs it too, and a third copy is the drift policy_wiring exists to
prevent."
```

---

### Task 2: Test config, stack script, make target, and a wiring smoke test

Deliverable: `make test-access-control` brings up the Ranger stack and proves the Ranger-wired in-process handler authenticates carol through Keycloak and executes SQL. No policy assertions yet.

**Files:**
- Create: `tests/sqe-ranger-test.toml`
- Create: `scripts/access-control-test.sh`
- Create: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`
- Modify: `crates/sqe-coordinator/tests/it/main.rs` (add `mod access_control_e2e;`)
- Modify: `crates/sqe-coordinator/tests/it/common/mod.rs` (add the ranger handler helpers)
- Modify: `Makefile` (`test-access-control` target plus a help line)

**Interfaces:**
- Produces: `common::ranger_config_path() -> String`
- Produces: `common::ac_enabled() -> bool`
- Produces: `common::setup_ranger_handler() -> (sqe_coordinator::QueryHandler, sqe_catalog::TableMetadataCache)`
- Produces: `common::ranger_session(user: &str) -> sqe_core::Session` (password derived as `format!("{user}123")`)
- Produces: `common::eventually<F, Fut, T>(what: &str, f: F) -> T`
- Produces: `common::serial() -> &'static tokio::sync::Mutex<()>`

- [ ] **Step 1: Write `tests/sqe-ranger-test.toml`**

```toml
# SQE config for the access-control e2e test (crates/sqe-coordinator/tests/it/
# access_control_e2e.rs). Mirrors quickstart/polaris-ranger-keycloak/sqe.toml
# with three deliberate differences, all marked below.
#
# Ports are the quickstart's host-published ones (.env.example).

[coordinator]
flight_sql_port = 60071
trino_http_port = 28081
mode = "hybrid"

[worker]
memory_limit = "4GB"
spill_dir = "/tmp/sqe-ac-spill"

[auth]
keycloak_url = "http://localhost:38080"
realm = "iceberg-ranger"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"
ssl_verification = false
admin_roles = ["sqe_admin"]

[[auth.providers]]
type = "oidc_password"
token_url = "http://localhost:38080/realms/iceberg-ranger/protocol/openid-connect/token"
client_id = "sqe-client"
client_secret = "sqe-secret-change-me"
roles_claim = "realm_access.roles"
accept_invalid_certs = true

[catalogs.sales_wh]
polaris_url = "http://localhost:28181/api/catalog"
warehouse = "sales_wh"
metadata_cache_ttl_secs = 30
default_table_format_version = 2

[catalogs.ops_wh]
polaris_url = "http://localhost:28181/api/catalog"
warehouse = "ops_wh"
metadata_cache_ttl_secs = 30
default_table_format_version = 2

[catalog]
polaris_url = "http://localhost:28181/api/catalog"
warehouse = "sales_wh"

[query]
catalog_discovery = "polaris-auto"

[storage]
s3_endpoint = "http://localhost:29000"
s3_region = "us-east-1"
s3_access_key = "s3admin"
s3_secret_key = "s3adminpw"
s3_path_style = true
s3_allow_http = true

# Difference 1: mask_key is set, so MASK_HASH runs as keyed HMAC-SHA256
# (issue #37) and the test can assert an exact digest.
[policy]
engine = "ranger"
mask_key = "sqe-ac-e2e-mask-key"

# Difference 2: service-name is the test-owned service, not the demo's `hive`.
# Difference 3: a 2s cache TTL so REST-created policies land quickly.
[policy.ranger]
url = "http://localhost:26080"
service-name = "sqe_ac_hive"
admin-user = "admin"
admin-password = "rangerR0cks!"
cache-ttl-secs = 2

[access_control]
backend = "ranger"
url = "http://localhost:26080"

# service-name MUST stay "polaris": it has to match Polaris's
# polaris.authorization.ranger.service-name. realm = "*" is not optional --
# Polaris sends a `root` resource on every authorization request, and without a
# matching value every policy SQE writes is accepted and then never matches.
[access_control.ranger]
service-name = "polaris"
admin-user = "admin"
admin-password = "rangerR0cks!"
realm = "*"

[session]
idle_timeout_secs = 900
```

- [ ] **Step 2: Add the helpers to `crates/sqe-coordinator/tests/it/common/mod.rs`**

Append:

```rust
// ── Access-control e2e helpers (see tests/it/access_control_e2e.rs) ─────────

/// Path to the Ranger e2e config, resolved from the workspace root like
/// `test_config_path()`.
pub fn ranger_config_path() -> String {
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .unwrap_or(std::path::Path::new("."));
    workspace_root
        .join("tests")
        .join("sqe-ranger-test.toml")
        .to_string_lossy()
        .to_string()
}

/// True when the caller opted into the access-control e2e suite.
///
/// The suite needs the `quickstart/polaris-ranger-keycloak` stack, which is NOT
/// the stack `scripts/integration-test.sh` brings up. `#[ignore]` alone is not
/// enough, because that script runs `cargo test -p sqe-coordinator -- --ignored`
/// and would force-run these. Opt in with `SQE_AC_E2E=1`
/// (`scripts/access-control-test.sh` sets it).
pub fn ac_enabled() -> bool {
    std::env::var("SQE_AC_E2E").as_deref() == Ok("1")
}

/// Process-wide serialization for the access-control tests. They share Ranger
/// state, the policy cache, and the fixture tables.
pub fn serial() -> &'static tokio::sync::Mutex<()> {
    static S: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    S.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Build a `QueryHandler` wired to the REAL Ranger enforcer and Ranger grant
/// backend from `tests/sqe-ranger-test.toml`.
///
/// Returns the handler and the `TableMetadataCache` it shares with the policy
/// enforcer. The SAME cache instance must reach both: `CacheTagSource` reads
/// column tags out of it, and a separate cache reports tag state as unknown,
/// which fails closed.
pub async fn setup_ranger_handler(
) -> (sqe_coordinator::QueryHandler, sqe_catalog::TableMetadataCache) {
    init_tracing();
    let config = sqe_core::SqeConfig::load(&ranger_config_path())
        .expect("load tests/sqe-ranger-test.toml");
    let table_cache = sqe_catalog::TableMetadataCache::new(30);
    let (enforcer, store) = sqe_coordinator::policy_wiring::build_policy_enforcer(
        &config.policy,
        Some(table_cache.clone()),
        None,
    )
    .expect("build ranger policy enforcer");
    let grant_backend = sqe_coordinator::policy_wiring::build_grant_backend(&config)
        .expect("build ranger grant backend");
    let query_tracker = Arc::new(sqe_coordinator::query_tracker::QueryTracker::new(
        &config.query_history,
    ));
    let handler = sqe_coordinator::QueryHandler::new(
        enforcer,
        store,
        config,
        None, // worker_registry
        None, // credential_tracker
        None, // metrics
        None, // audit
        query_tracker,
        None, // query_cache
        grant_backend,
        None, // lineage
        sqe_coordinator::RuntimeCatalogRegistry::default(),
        sqe_core::SecretStore::default(),
    )
    .expect("build QueryHandler")
    .with_table_cache(table_cache.clone());
    (handler, table_cache)
}

/// Authenticate a quickstart user through Keycloak ROPC. Password convention is
/// `<user>123` (alice123, bob123, carol123, dave123).
pub async fn ranger_session(user: &str) -> sqe_core::Session {
    let config = sqe_core::SqeConfig::load(&ranger_config_path())
        .expect("load tests/sqe-ranger-test.toml");
    let authenticator = sqe_auth::Authenticator::new(&config.auth)
        .await
        .expect("create authenticator");
    authenticator
        .authenticate(user, &format!("{user}123"))
        .await
        .unwrap_or_else(|e| panic!("Keycloak ROPC failed for {user}: {e}"))
}

/// Retry `f` until it returns `Ok` or 30s elapse. Panics with the last failure.
///
/// Policy changes made over the Ranger REST API become visible only after the
/// policy cache TTL (`policy.ranger.cache-ttl-secs = 2` in the test config), so
/// assertions need a bounded wait. Never use a bare sleep: it either flakes or
/// wastes wall clock, and it hides which assertion was still failing.
pub async fn eventually<F, Fut, T>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last = "never ran".to_string();
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) => last = e,
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out after 30s waiting for {what}; last failure: {last}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}
```

- [ ] **Step 3: Write the failing smoke test**

Create `crates/sqe-coordinator/tests/it/access_control_e2e.rs`:

```rust
//! Access-control end-to-end tests against a live Polaris + Ranger + Keycloak
//! stack (`quickstart/polaris-ranger-keycloak`).
//!
//! Run them with:
//!
//! ```text
//! scripts/access-control-test.sh
//! ```
//!
//! which brings up the stack subset these tests need and sets `SQE_AC_E2E=1`.
//! Every test is `#[ignore]`d AND gated on that variable: `scripts/
//! integration-test.sh` runs `cargo test -p sqe-coordinator -- --ignored`
//! against a DIFFERENT stack and must not force-run these. When the variable IS
//! set and the stack is unreachable the tests fail rather than skip. A gate that
//! passes when it never ran is worse than no gate.
//!
//! What is asserted here that the shell harness
//! (`quickstart/polaris-ranger-keycloak/test.sh`) cannot: exact masked values,
//! exact row counts, and denial-versus-typo discrimination (the same SQL is run
//! as a privileged user to prove the identifier is valid).

/// Skip-or-panic gate. Returns false when the suite was not opted into.
macro_rules! ac_gate {
    () => {
        if !crate::common::ac_enabled() {
            eprintln!(
                "skipping access_control_e2e: set SQE_AC_E2E=1 (use scripts/access-control-test.sh)"
            );
            return;
        }
    };
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn ranger_wiring_smoke_carol_can_query() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let (handler, _cache) = crate::common::setup_ranger_handler().await;
    let carol = crate::common::ranger_session("carol").await;

    assert!(
        carol.user.roles.iter().any(|r| r == "sqe_admin"),
        "carol must carry the sqe_admin realm role; got {:?}",
        carol.user.roles
    );

    let batches = handler
        .execute(&carol, "SELECT 1 AS one", None)
        .await
        .expect("SELECT 1 as carol through the Ranger-wired handler");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "SELECT 1 must return exactly one row");
}
```

Register it in `crates/sqe-coordinator/tests/it/main.rs` by adding, in alphabetical position after `mod analyze_statement_test;`:

```rust
mod access_control_e2e;
```

- [ ] **Step 4: Run it without the stack to verify the gate**

Run: `cargo test -p sqe-coordinator --test it -- --ignored ranger_wiring_smoke 2>&1 | tail -5`
Expected: PASS with the `skipping access_control_e2e` line on stderr. This proves `scripts/integration-test.sh` cannot be broken by this suite.

- [ ] **Step 5: Write `scripts/access-control-test.sh`**

```bash
#!/usr/bin/env bash
# Run the access-control e2e suite against the Polaris + Ranger + Keycloak
# quickstart stack.
#
#   scripts/access-control-test.sh                  # whole suite
#   scripts/access-control-test.sh tag_column_mask  # one test by substring
#   scripts/access-control-test.sh --down           # tear the stack down
#
# Only the services the suite needs are started. `sqe`, `data-seed`, and `spark`
# are NOT in any of those dependency chains, so no SQE image is built and the
# demo's seeded tables, grants, and hive policies are never created. That is
# what keeps this suite from disturbing quickstart test.sh / parity-test.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STACK_DIR="$ROOT_DIR/quickstart/polaris-ranger-keycloak"

RANGER_TIMEOUT="${AC_RANGER_TIMEOUT:-300}"
RANGER_URL="${AC_RANGER_URL:-http://localhost:26080}"
POLARIS_URL="${AC_POLARIS_URL:-http://localhost:28181}"
KEYCLOAK_URL="${AC_KEYCLOAK_URL:-http://localhost:38080}"

if [ "${1:-}" = "--down" ]; then
    cd "$STACK_DIR" && docker compose down -v
    echo "torn down"
    exit 0
fi

cd "$STACK_DIR"
[ -f .env ] || { echo "creating .env from .env.example"; cp .env.example .env; }
set -a; . ./.env; set +a

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Access-control stack (Ranger first boot takes 2-4 min)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker compose up -d --wait keycloak-config bucket-init ranger-setup polaris-setup

# --wait covers container health, not application readiness. Poll the three
# endpoints the tests actually call, with a bounded deadline, and report the
# last status instead of hanging.
wait_for() { # name url
    local name=$1 url=$2 deadline=$((SECONDS + RANGER_TIMEOUT)) code=000
    while [ $SECONDS -lt $deadline ]; do
        code="$(curl -s -o /dev/null -w '%{http_code}' "$url" || true)"
        case "$code" in 2??|3??|401) echo "  $name ready ($code)"; return 0 ;; esac
        sleep 5
    done
    echo "ERROR: $name not ready after ${RANGER_TIMEOUT}s (last HTTP $code): $url" >&2
    return 1
}

wait_for "ranger-admin" "$RANGER_URL/service/public/v2/api/servicedef"
wait_for "polaris"      "$POLARIS_URL/q/health"
wait_for "keycloak"     "$KEYCLOAK_URL/realms/iceberg-ranger/.well-known/openid-configuration"

cd "$ROOT_DIR"

# Scope the filter to this module. A bare substring (e.g. `tag_`) under
# `--ignored` would match ignored tests in OTHER modules of the same `it` binary
# and force-run them against this stack, which is not the one they need.
FILTER="access_control_e2e"
if [ "$#" -gt 0 ]; then
    FILTER="access_control_e2e::$1"
fi

echo ""
echo "Running access-control e2e suite (filter: $FILTER)..."
SQE_AC_E2E=1 \
RUST_LOG="${RUST_LOG:-sqe_coordinator=info,sqe_policy=debug,sqe_catalog=info,sqe_auth=info,warn}" \
RUST_MIN_STACK="${RUST_MIN_STACK:-33554432}" \
    cargo test -p sqe-coordinator --test it -- \
    --ignored --test-threads=1 --nocapture "$FILTER"
```

- [ ] **Step 6: Make it executable and add the make target**

```bash
chmod +x scripts/access-control-test.sh
```

In `Makefile`, add `test-access-control` to `.PHONY`. On `main` the relevant line (Makefile:81) reads:

```make
        benchmark-charts test clippy fmt fmt-check clean clean-rust clean-rustbook \
```

so change it to:

```make
        benchmark-charts test test-access-control clippy fmt fmt-check clean clean-rust clean-rustbook \
```

If the branch `chore/makefile-audit-bench-targets` has already merged, that line instead reads `benchmark-charts test audit audit-advisories audit-deny audit-licenses \` and `test-access-control` goes there. This plan does not otherwise depend on that branch.

Add this target after the `test:` target:

```make
# ── Access-control e2e (Polaris + Ranger + Keycloak) ──────────────────────
# Brings up a subset of quickstart/polaris-ranger-keycloak and runs the Rust
# access-control suite against it. Ranger Admin's first boot takes 2-4 minutes.
test-access-control:
	@scripts/access-control-test.sh
```

and this help line under the `Code:` block, after the `make test` line:

```make
	@echo "    make test-access-control  Ranger/Polaris access-control e2e (brings up the Ranger stack)"
```

- [ ] **Step 7: Run the smoke test against the real stack**

Run: `make test-access-control 2>&1 | tail -30`
Expected: the stack comes up, the three `ready` lines print, and `ranger_wiring_smoke_carol_can_query` PASSES. If ROPC fails, check that the realm name in `tests/sqe-ranger-test.toml` matches `iceberg-ranger` and that Keycloak finished importing the realm.

- [ ] **Step 8: Verify the other suite is unaffected**

Run: `make -n test-access-control && grep -c "SQE_AC_E2E" scripts/access-control-test.sh`
Expected: the target expands to the script, and the env var appears exactly once.

- [ ] **Step 9: Commit**

```bash
git add tests/sqe-ranger-test.toml scripts/access-control-test.sh Makefile \
        crates/sqe-coordinator/tests/it/access_control_e2e.rs \
        crates/sqe-coordinator/tests/it/main.rs \
        crates/sqe-coordinator/tests/it/common/mod.rs
git commit -m "test(policy): wire the access-control e2e stack and smoke test

In-process QueryHandler on the real Ranger enforcer + Ranger grant backend,
authenticating through Keycloak ROPC against the quickstart stack. Gated on
SQE_AC_E2E=1 so integration-test.sh cannot force-run it against the wrong
stack, and failing loudly when the gate is set but the stack is absent."
```

---

### Task 3: Ranger REST fixture helper

Deliverable: a helper that creates the two test-owned services, links them, and creates/deletes prefixed policies, verified by a round-trip test.

**Files:**
- Create: `crates/sqe-coordinator/tests/it/common/ranger_fixture.rs`
- Modify: `crates/sqe-coordinator/tests/it/common/mod.rs` (add `pub mod ranger_fixture;`)

**Interfaces:**
- Produces: `common::ranger_fixture::{PREFIX, HIVE_SERVICE, TAG_SERVICE}` string constants
- Produces: `RangerAdmin::from_env() -> RangerAdmin`
- Produces: `RangerAdmin::require_reachable(&self)` (panics with the HTTP status when down)
- Produces: `RangerAdmin::ensure_services(&self) -> anyhow::Result<()>`
- Produces: `RangerAdmin::delete_test_policies(&self) -> anyhow::Result<usize>`
- Produces: `RangerAdmin::create_policy(&self, body: serde_json::Value) -> anyhow::Result<i64>`
- Produces: `RangerAdmin::delete_policy(&self, service: &str, name: &str) -> anyhow::Result<()>`
- Produces: `RangerAdmin::get_policies(&self, service: &str) -> anyhow::Result<Vec<serde_json::Value>>`
- Produces: `RangerAdmin::update_policy(&self, id: i64, body: serde_json::Value) -> anyhow::Result<()>`
- Produces: `RangerAdmin::download_bundle(&self, service: &str) -> anyhow::Result<serde_json::Value>`

- [ ] **Step 1: Write the failing round-trip test**

Create `crates/sqe-coordinator/tests/it/common/ranger_fixture.rs` with only the test at the bottom for now (the impl comes in step 3):

```rust
#[cfg(test)]
mod tests {
    // Intentionally empty: the fixture is exercised by the e2e test below,
    // which needs the live stack. See `fixture_round_trip` in
    // tests/it/access_control_e2e.rs.
}
```

and add to `crates/sqe-coordinator/tests/it/access_control_e2e.rs`:

```rust
use crate::common::ranger_fixture::{RangerAdmin, HIVE_SERVICE, PREFIX, TAG_SERVICE};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn fixture_round_trip_creates_services_and_policies() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ranger = RangerAdmin::from_env();
    ranger.require_reachable().await;
    ranger.ensure_services().await.expect("ensure services");
    ranger.delete_test_policies().await.expect("clean prefix");

    // A minimal mask policy, created and then read back over REST.
    let name = format!("{PREFIX}roundtrip");
    ranger
        .create_policy(serde_json::json!({
            "service": HIVE_SERVICE,
            "name": name,
            "policyType": 1,
            "isEnabled": true,
            "resources": {
                "database": {"values": ["ac"]},
                "table": {"values": ["orders"]},
                "column": {"values": ["amount"]}
            },
            "dataMaskPolicyItems": [{
                "roles": ["engineer"],
                "accesses": [{"type": "select", "isAllowed": true}],
                "dataMaskInfo": {"dataMaskType": "MASK_NULL"}
            }]
        }))
        .await
        .expect("create policy");

    let policies = ranger.get_policies(HIVE_SERVICE).await.expect("list policies");
    assert!(
        policies
            .iter()
            .any(|p| p["name"].as_str() == Some(name.as_str())),
        "the created policy must be listed on {HIVE_SERVICE}"
    );

    // The tag service must be linked to the hive service, otherwise the
    // downloaded bundle carries no `tagPolicies` block and every tag test would
    // fail for the wrong reason.
    let bundle = ranger.download_bundle(HIVE_SERVICE).await.expect("download bundle");
    assert!(
        bundle.get("tagPolicies").is_some(),
        "bundle for {HIVE_SERVICE} must contain tagPolicies once {TAG_SERVICE} is linked; got keys {:?}",
        bundle.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );

    let removed = ranger.delete_test_policies().await.expect("cleanup");
    assert!(removed >= 1, "cleanup must delete at least the policy we made");
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `SQE_AC_E2E=1 cargo test -p sqe-coordinator --test it -- --ignored fixture_round_trip 2>&1 | tail -20`
Expected: FAIL to compile, `could not find ranger_fixture in common`.

- [ ] **Step 3: Implement the fixture**

Replace the contents of `crates/sqe-coordinator/tests/it/common/ranger_fixture.rs` with:

```rust
//! Ranger Admin REST fixtures for the access-control e2e suite.
//!
//! Two test-owned services are used so the suite never touches the demo's
//! shared `hive` service: linking a tag service or adding policies there would
//! change the downloaded bundle for Spark/Kyuubi, which
//! `quickstart/polaris-ranger-keycloak/parity-test.sh` cross-compares against.
//!
//! Ranger facts encoded here:
//!   - state-changing requests need the `X-XSRF-HEADER` header, else 401
//!   - tag-based policies live in a service of type `tag`, which must be linked
//!     to the resource service via its `tagService` field for the resource
//!     service's policy-download bundle to carry a `tagPolicies` block
//!   - SQE reads only mask (policyType 1) and row-filter (policyType 2)
//!     policies from the hive-type service; access (policyType 0) policies are
//!     ignored by SQE (its coarse gate is the `polaris` service)

use anyhow::{bail, Context};
use serde_json::Value;

/// Name prefix for every policy this suite creates. Setup deletes all policies
/// carrying it, so a crashed run cannot poison the next one.
pub const PREFIX: &str = "sqe-ac-e2e-";
/// Test-owned resource service (fine-grained masks + row filters).
pub const HIVE_SERVICE: &str = "sqe_ac_hive";
/// Test-owned tag service, linked to `HIVE_SERVICE`.
pub const TAG_SERVICE: &str = "sqe_ac_tag";

pub struct RangerAdmin {
    base: String,
    user: String,
    pass: String,
    client: reqwest::Client,
}

impl RangerAdmin {
    /// Ranger Admin at `AC_RANGER_URL` (default `http://localhost:26080`) with
    /// the quickstart's admin credentials.
    pub fn from_env() -> Self {
        Self {
            base: std::env::var("AC_RANGER_URL")
                .unwrap_or_else(|_| "http://localhost:26080".to_string()),
            user: std::env::var("AC_RANGER_USER").unwrap_or_else(|_| "admin".to_string()),
            pass: std::env::var("RANGER_ADMIN_PASSWORD")
                .unwrap_or_else(|_| "rangerR0cks!".to_string()),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.base, path))
            .basic_auth(&self.user, Some(&self.pass))
            .header("X-XSRF-HEADER", "x")
            .header("Content-Type", "application/json")
    }

    /// Panic with the HTTP status when Ranger Admin is not answering. Called by
    /// every test: with `SQE_AC_E2E=1` set, an absent stack must fail, not skip.
    pub async fn require_reachable(&self) {
        let url = "/service/public/v2/api/servicedef";
        match self.req(reqwest::Method::GET, url).send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => panic!(
                "Ranger Admin at {} answered HTTP {} for {url}; start the stack with \
                 scripts/access-control-test.sh",
                self.base,
                r.status()
            ),
            Err(e) => panic!(
                "Ranger Admin at {} is unreachable ({e}); start the stack with \
                 scripts/access-control-test.sh",
                self.base
            ),
        }
    }

    async fn service_by_name(&self, name: &str) -> anyhow::Result<Option<Value>> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/public/v2/api/service/name/{name}"),
            )
            .send()
            .await
            .context("GET service by name")?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            bail!("GET service {name} -> HTTP {}", resp.status());
        }
        Ok(Some(resp.json().await.context("parse service json")?))
    }

    /// Create `sqe_ac_hive` (type hive) and `sqe_ac_tag` (type tag) if absent,
    /// then link the tag service to the hive service. Idempotent.
    pub async fn ensure_services(&self) -> anyhow::Result<()> {
        if self.service_by_name(HIVE_SERVICE).await?.is_none() {
            let body = serde_json::json!({
                "name": HIVE_SERVICE,
                "type": "hive",
                "configs": {
                    "username": "admin",
                    "password": "none",
                    "jdbc.driverClassName": "org.apache.hive.jdbc.HiveDriver",
                    "jdbc.url": "none"
                },
                "isEnabled": true
            });
            let resp = self
                .req(reqwest::Method::POST, "/service/public/v2/api/service")
                .json(&body)
                .send()
                .await
                .context("POST hive service")?;
            if !resp.status().is_success() {
                bail!(
                    "create {HIVE_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }

        if self.service_by_name(TAG_SERVICE).await?.is_none() {
            let body = serde_json::json!({
                "name": TAG_SERVICE,
                "type": "tag",
                "configs": {},
                "isEnabled": true
            });
            let resp = self
                .req(reqwest::Method::POST, "/service/public/v2/api/service")
                .json(&body)
                .send()
                .await
                .context("POST tag service")?;
            if !resp.status().is_success() {
                bail!(
                    "create {TAG_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }

        // Link: set `tagService` on the hive service and PUT it back. Without
        // the link the hive bundle carries no tagPolicies block.
        let mut hive = self
            .service_by_name(HIVE_SERVICE)
            .await?
            .context("hive service must exist after creation")?;
        if hive.get("tagService").and_then(Value::as_str) != Some(TAG_SERVICE) {
            hive["tagService"] = Value::String(TAG_SERVICE.to_string());
            let id = hive["id"].as_i64().context("hive service id")?;
            let resp = self
                .req(
                    reqwest::Method::PUT,
                    &format!("/service/public/v2/api/service/{id}"),
                )
                .json(&hive)
                .send()
                .await
                .context("PUT hive service with tagService")?;
            if !resp.status().is_success() {
                bail!(
                    "link {TAG_SERVICE} to {HIVE_SERVICE} -> HTTP {}: {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                );
            }
        }
        Ok(())
    }

    /// All policies of a service.
    pub async fn get_policies(&self, service: &str) -> anyhow::Result<Vec<Value>> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/public/v2/api/policy?serviceName={service}"),
            )
            .send()
            .await
            .context("GET policies")?;
        if !resp.status().is_success() {
            bail!("GET policies for {service} -> HTTP {}", resp.status());
        }
        Ok(resp.json().await.context("parse policies json")?)
    }

    /// Create a policy. Returns its Ranger id.
    pub async fn create_policy(&self, body: Value) -> anyhow::Result<i64> {
        let resp = self
            .req(reqwest::Method::POST, "/service/public/v2/api/policy")
            .json(&body)
            .send()
            .await
            .context("POST policy")?;
        if !resp.status().is_success() {
            bail!(
                "create policy {} -> HTTP {}: {}",
                body["name"],
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        let created: Value = resp.json().await.context("parse created policy")?;
        created["id"].as_i64().context("created policy id")
    }

    /// Replace a policy by id (used to add denyPolicyItems to an existing one).
    pub async fn update_policy(&self, id: i64, body: Value) -> anyhow::Result<()> {
        let resp = self
            .req(
                reqwest::Method::PUT,
                &format!("/service/public/v2/api/policy/{id}"),
            )
            .json(&body)
            .send()
            .await
            .context("PUT policy")?;
        if !resp.status().is_success() {
            bail!(
                "update policy {id} -> HTTP {}: {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        Ok(())
    }

    /// Delete one policy by service + name. Missing is not an error.
    pub async fn delete_policy(&self, service: &str, name: &str) -> anyhow::Result<()> {
        let resp = self
            .req(
                reqwest::Method::DELETE,
                &format!("/service/public/v2/api/policy?servicename={service}&policyname={name}"),
            )
            .send()
            .await
            .context("DELETE policy")?;
        if resp.status().is_success() || resp.status().as_u16() == 404 {
            return Ok(());
        }
        bail!("delete policy {name} -> HTTP {}", resp.status());
    }

    /// Delete every `sqe-ac-e2e-` policy from both test-owned services.
    /// Returns how many were removed.
    pub async fn delete_test_policies(&self) -> anyhow::Result<usize> {
        let mut removed = 0;
        for service in [HIVE_SERVICE, TAG_SERVICE] {
            for p in self.get_policies(service).await? {
                let Some(name) = p["name"].as_str() else { continue };
                if name.starts_with(PREFIX) {
                    self.delete_policy(service, name).await?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    /// The policy bundle SQE downloads. Used to capture a real `tagPolicies`
    /// sample for `sqe-policy`'s unit test.
    pub async fn download_bundle(&self, service: &str) -> anyhow::Result<Value> {
        let resp = self
            .req(
                reqwest::Method::GET,
                &format!("/service/plugins/policies/download/{service}"),
            )
            .send()
            .await
            .context("GET policy bundle")?;
        if !resp.status().is_success() {
            bail!("download bundle for {service} -> HTTP {}", resp.status());
        }
        Ok(resp.json().await.context("parse bundle json")?)
    }
}
```

Add to `crates/sqe-coordinator/tests/it/common/mod.rs`, near the top:

```rust
pub mod ranger_fixture;
```

- [ ] **Step 4: Run the round-trip test against the stack**

Run: `scripts/access-control-test.sh fixture_round_trip 2>&1 | tail -20`
Expected: PASS. If `tagPolicies` is missing from the bundle, the link step did not take: re-read the hive service JSON and confirm `tagService` is `sqe_ac_tag`.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/tests/it/common/ranger_fixture.rs \
        crates/sqe-coordinator/tests/it/common/mod.rs \
        crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(policy): Ranger REST fixture for the access-control suite

Creates test-owned sqe_ac_hive + sqe_ac_tag services (linked, so the bundle
carries tagPolicies), and creates/deletes sqe-ac-e2e- prefixed policies.
Prefix cleanup runs before creation so a crashed run cannot poison the next."
```

---

### Task 4: Fixture data plus the first two cases (deny before grant, GRANT enables)

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`

**Interfaces:**
- Produces: `async fn ac_setup() -> AcCtx` where `struct AcCtx { handler: QueryHandler, ranger: RangerAdmin, carol: Session, alice: Session, bob: Session, dave: Session, _cache: TableMetadataCache }`
- Produces: `async fn exec_ok(ctx: &AcCtx, s: &Session, sql: &str) -> Vec<RecordBatch>`
- Produces: `async fn col_strings(batches: &[RecordBatch], column: &str) -> Vec<String>`
- Produces: `async fn assert_denied_but_valid(ctx: &AcCtx, denied: &Session, sql: &str)`
- Produces: `const ORDERS: &str = "sales_wh.ac.orders"`, `const AUDIT: &str = "ops_wh.ac.audit"`

- [ ] **Step 1: Write the failing tests**

Append to `crates/sqe-coordinator/tests/it/access_control_e2e.rs`:

```rust
use arrow_array::RecordBatch;
use sqe_coordinator::QueryHandler;
use sqe_core::Session;

/// Fully-qualified fixture tables. Namespace `ac` is used in both warehouses so
/// nothing collides with the demo's `sales` / `ops` namespaces, and so the
/// Ranger hive `database` resource is `ac` (SQE sends the LAST namespace
/// component as `database`; see ranger_store.rs::hive_database).
const ORDERS: &str = "sales_wh.ac.orders";
const AUDIT: &str = "ops_wh.ac.audit";

struct AcCtx {
    handler: QueryHandler,
    ranger: RangerAdmin,
    carol: Session,
    alice: Session,
    bob: Session,
    dave: Session,
    // Held so the cache the policy enforcer reads stays alive for the test.
    _cache: sqe_catalog::TableMetadataCache,
}

/// Clear `denyPolicyItems` from the polaris-service policy covering the audit
/// fixture table.
///
/// `ranger_deny_overrides_allow` adds a deny item to that policy, and nothing
/// else removes it: REVOKE strips ALLOW items, and the prefix cleanup only
/// covers the two test-owned services. Without this, the suite is not idempotent
/// -- on the second run that test starts with alice already denied and burns its
/// 30s `eventually` budget waiting for a baseline allow that can never arrive.
/// Uses the same resource matcher as `add_deny_item_to_audit_policy`.
async fn clear_audit_deny_items(ranger: &RangerAdmin) {
    let policies = ranger.get_policies("polaris").await.unwrap_or_default();
    for mut p in policies {
        let is_audit_policy = p["resources"]["table"]["values"] == serde_json::json!(["audit"])
            && p["resources"]["namespace"]["values"] == serde_json::json!(["ac"])
            && p["resources"]["catalog"]["values"] == serde_json::json!(["ops_wh"]);
        if !is_audit_policy {
            continue;
        }
        let has_denies = p["denyPolicyItems"]
            .as_array()
            .is_some_and(|items| !items.is_empty());
        if !has_denies {
            continue;
        }
        let Some(id) = p["id"].as_i64() else { continue };
        p["denyPolicyItems"] = serde_json::json!([]);
        ranger
            .update_policy(id, p)
            .await
            .expect("clear denyPolicyItems on the audit policy");
    }
}

/// Bring the suite to a known state: services present, no `sqe-ac-e2e-`
/// policies, no leftover deny items, no test grants, fixture tables holding
/// exactly three / two rows.
async fn ac_setup() -> AcCtx {
    let (handler, _cache) = crate::common::setup_ranger_handler().await;
    let ranger = RangerAdmin::from_env();
    ranger.require_reachable().await;
    ranger.ensure_services().await.expect("ensure services");
    ranger.delete_test_policies().await.expect("clean prefix");
    clear_audit_deny_items(&ranger).await;

    let carol = crate::common::ranger_session("carol").await;
    let alice = crate::common::ranger_session("alice").await;
    let bob = crate::common::ranger_session("bob").await;
    let dave = crate::common::ranger_session("dave").await;

    // Namespaces. CREATE SCHEMA is supported (sqe-sql classifier) and
    // catalog_discovery = "polaris-auto" routes the warehouse-qualified form.
    // If the two-part form does NOT route to the right warehouse on this stack,
    // fall back to the demo's existing namespaces with test-only table names:
    // `sales_wh.sales.orders_ac_e2e` and `ops_wh.ops.audit_ac_e2e`, and change
    // the Ranger `database` resource in every policy below from "ac" to
    // "sales" / "ops" accordingly.
    for ns in ["sales_wh.ac", "ops_wh.ac"] {
        let _ = handler
            .execute(&carol, &format!("CREATE SCHEMA IF NOT EXISTS {ns}"), None)
            .await;
    }

    // Fixture tables. Dropped and recreated so a leftover table from an aborted
    // run cannot skew row counts.
    for t in [ORDERS, AUDIT] {
        handler
            .execute(&carol, &format!("DROP TABLE IF EXISTS {t}"), None)
            .await
            .unwrap_or_else(|e| panic!("drop {t}: {e}"));
    }
    handler
        .execute(
            &carol,
            &format!(
                "CREATE TABLE {ORDERS} (id BIGINT, region VARCHAR, amount DOUBLE, \
                 ssn VARCHAR, email VARCHAR)"
            ),
            None,
        )
        .await
        .expect("create orders");
    handler
        .execute(
            &carol,
            &format!(
                "INSERT INTO {ORDERS} VALUES \
                 (1,'EU',10.0,'111-11-1111','a@x'), \
                 (2,'US',20.0,'222-22-2222','b@x'), \
                 (3,'EU',30.0,'333-33-3333','c@x')"
            ),
            None,
        )
        .await
        .expect("insert orders");
    handler
        .execute(&carol, &format!("CREATE TABLE {AUDIT} (id BIGINT, event VARCHAR)"), None)
        .await
        .expect("create audit");
    handler
        .execute(&carol, &format!("INSERT INTO {AUDIT} VALUES (1,'login'),(2,'logout')"), None)
        .await
        .expect("insert audit");

    // Remove any grants a previous run left on the fixture tables, so "denied
    // before grant" starts from a true denial.
    for stmt in [
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE INSERT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE SELECT ON {AUDIT} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {AUDIT} FROM USER \"bob\""),
    ] {
        let _ = handler.execute(&carol, &stmt, None).await;
    }

    AcCtx { handler, ranger, carol, alice, bob, dave, _cache }
}

/// Execute and unwrap with the SQL in the panic message.
async fn exec_ok(ctx: &AcCtx, s: &Session, sql: &str) -> Vec<RecordBatch> {
    ctx.handler
        .execute(s, sql, None)
        .await
        .unwrap_or_else(|e| panic!("[{}] {sql} failed: {e}", s.user.username))
}

/// Values of one column across all batches, rendered with `common::fmt_val`.
/// NULL renders as the empty string.
fn col_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let Ok(idx) = b.schema().index_of(column) else {
            panic!(
                "column `{column}` absent from result schema {:?}",
                b.schema().fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>()
            )
        };
        let arr = b.column(idx);
        for row in 0..b.num_rows() {
            out.push(crate::common::fmt_val(arr.as_ref(), row));
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Assert `sql` fails for `denied` while succeeding verbatim for carol.
///
/// This is the discrimination the shell harness cannot make: it greps for
/// `not found`, which a typo'd identifier also produces. Running the SAME text
/// as an admin proves the statement is valid, so the failure is authorization.
async fn assert_denied_but_valid(ctx: &AcCtx, denied: &Session, sql: &str) {
    let as_admin = ctx.handler.execute(&ctx.carol, sql, None).await;
    assert!(
        as_admin.is_ok(),
        "control failed: `{sql}` must succeed as carol, else the test cannot tell \
         denial from an invalid statement. Error: {:?}",
        as_admin.err()
    );
    let result = ctx.handler.execute(denied, sql, None).await;
    assert!(
        result.is_err(),
        "`{sql}` must be denied for {} but returned {} rows",
        denied.user.username,
        result.map(|b| total_rows(&b)).unwrap_or(0)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn denied_before_any_grant() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    assert_denied_but_valid(&ctx, &ctx.alice, &format!("SELECT region FROM {ORDERS}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn grant_select_to_role_enables_exact_rows() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;

    let regions = crate::common::eventually("alice's SELECT to be allowed", || async {
        match ctx
            .handler
            .execute(&ctx.alice, &format!("SELECT region, amount FROM {ORDERS} ORDER BY id"), None)
            .await
        {
            Ok(b) if total_rows(&b) == 3 => Ok(b),
            Ok(b) => Err(format!("expected 3 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;

    assert_eq!(col_strings(&regions, "region"), vec!["EU", "US", "EU"]);
    assert_eq!(col_strings(&regions, "amount"), vec!["10.0", "20.0", "30.0"]);
}
```

- [ ] **Step 2: Run to verify the first test fails for the right reason**

Run: `scripts/access-control-test.sh denied_before_any_grant 2>&1 | tail -30`
Expected: PASS if the revoke-then-deny setup works. If it FAILS with "control failed", carol lacks admin grants on the `ac` namespace: check that `bootstrap-ranger.sh` seeded `sqe_admin` with `{"root":"*","catalog":"*","namespace":"*","table":"*"}` ADMIN_ACCESS (it does) and that carol's token carries `sqe_admin`.

- [ ] **Step 3: Verify `amount` renders as expected**

Run: `scripts/access-control-test.sh grant_select_to_role 2>&1 | tail -30`
Expected: PASS. If the `amount` assertion fails on formatting (for example `10` instead of `10.0`), fix the EXPECTED values to match `common::fmt_val`'s rendering. Do not change `fmt_val`: other tests depend on it.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(policy): fixture setup plus deny-before-grant and grant-enables cases

Denials are asserted by running the same SQL as carol first, so a denial is
distinguishable from an invalid identifier -- the weakness in the shell
harness's grep for 'not found'."
```

---

### Task 5: Grant-path cases (role vs user, write privileges, DENY precedence, REVOKE)

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`

**Interfaces:**
- Consumes: `ac_setup`, `exec_ok`, `col_strings`, `total_rows`, `assert_denied_but_valid`, `ORDERS`, `AUDIT`
- Produces: `async fn add_deny_item_to_audit_policy(ctx: &AcCtx)`

- [ ] **Step 1: Write the failing tests**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn role_grant_and_user_grant_both_apply() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {AUDIT} TO USER \"bob\"")).await;

    // bob reads orders through the engineer ROLE.
    let orders = crate::common::eventually("bob's role grant on orders", || async {
        match ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT id FROM {ORDERS} ORDER BY id"), None)
            .await
        {
            Ok(b) if total_rows(&b) == 3 => Ok(b),
            Ok(b) => Err(format!("expected 3 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&orders, "id"), vec!["1", "2", "3"]);

    // bob reads audit through a direct USER grant, with no role involved.
    let audit = crate::common::eventually("bob's user grant on audit", || async {
        match ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT event FROM {AUDIT} ORDER BY id"), None)
            .await
        {
            Ok(b) if total_rows(&b) == 2 => Ok(b),
            Ok(b) => Err(format!("expected 2 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&audit, "event"), vec!["login", "logout"]);

    // dave holds no role and no user grant.
    assert_denied_but_valid(&ctx, &ctx.dave, &format!("SELECT id FROM {ORDERS}")).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn write_privileges_are_separate_from_read() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT INSERT ON {ORDERS} TO ROLE \"engineer\"")).await;

    // bob (engineer) can write, and the row is visible afterwards.
    crate::common::eventually("bob's INSERT to be allowed", || async {
        match ctx
            .handler
            .execute(
                &ctx.bob,
                &format!("INSERT INTO {ORDERS} VALUES (4,'EU',40.0,'444-44-4444','d@x')"),
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("INSERT still denied: {e}")),
        }
    })
    .await;
    let after = exec_ok(&ctx, &ctx.carol, &format!("SELECT id FROM {ORDERS} ORDER BY id")).await;
    assert_eq!(col_strings(&after, "id"), vec!["1", "2", "3", "4"]);

    // alice (analyst) holds SELECT only: no INSERT, no DROP.
    let insert = ctx
        .handler
        .execute(
            &ctx.alice,
            &format!("INSERT INTO {ORDERS} VALUES (9,'x',0.0,'000-00-0000','z@x')"),
            None,
        )
        .await;
    assert!(insert.is_err(), "alice holds SELECT only; INSERT must be denied");

    let drop = ctx
        .handler
        .execute(&ctx.alice, &format!("DROP TABLE {ORDERS}"), None)
        .await;
    assert!(drop.is_err(), "alice holds SELECT only; DROP must be denied");

    // Prove the DROP really did not happen.
    let still_there = exec_ok(&ctx, &ctx.carol, &format!("SELECT id FROM {ORDERS} ORDER BY id")).await;
    assert_eq!(total_rows(&still_there), 4, "the denied DROP must not have removed the table");
}

/// Add a `denyPolicyItems` entry for role `analyst` to the EXISTING Ranger
/// policy that SQE's `GRANT SELECT ON ops_wh.ac.audit` created.
///
/// Ranger keeps one policy per resource, so deny precedence has to be expressed
/// by editing that policy rather than creating a second one. Matching is on the
/// catalog / namespace / table resource values, exactly as the demo harness does
/// it (quickstart/polaris-ranger-keycloak/test.sh step 6).
async fn add_deny_item_to_audit_policy(ctx: &AcCtx) {
    let policies = ctx
        .ranger
        .get_policies("polaris")
        .await
        .expect("list polaris policies");
    let mut target = policies
        .into_iter()
        .find(|p| {
            p["resources"]["table"]["values"] == serde_json::json!(["audit"])
                && p["resources"]["namespace"]["values"] == serde_json::json!(["ac"])
                && p["resources"]["catalog"]["values"] == serde_json::json!(["ops_wh"])
        })
        .expect("SQE's GRANT must have created a polaris policy for ops_wh.ac.audit");

    let id = target["id"].as_i64().expect("policy id");
    let deny = serde_json::json!({
        "roles": ["analyst"],
        "accesses": [
            {"type": "table-properties-read", "isAllowed": true},
            {"type": "table-data-read", "isAllowed": true}
        ]
    });
    match target.get_mut("denyPolicyItems").and_then(|v| v.as_array_mut()) {
        Some(items) => items.push(deny),
        None => target["denyPolicyItems"] = serde_json::json!([deny]),
    }
    ctx.ranger
        .update_policy(id, target)
        .await
        .expect("add denyPolicyItems to the audit policy");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn ranger_deny_overrides_allow() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {AUDIT} TO ROLE \"analyst\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {AUDIT} TO ROLE \"engineer\"")).await;

    // alice can read before the deny lands.
    crate::common::eventually("alice's allow on audit", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT event FROM {AUDIT}"), None).await {
            Ok(b) if total_rows(&b) == 2 => Ok(()),
            Ok(b) => Err(format!("expected 2 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;

    add_deny_item_to_audit_policy(&ctx).await;

    // Deny beats allow for analyst...
    crate::common::eventually("the deny to take effect for alice", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT event FROM {AUDIT}"), None).await {
            Err(_) => Ok(()),
            Ok(b) => Err(format!("still allowed with {} rows", total_rows(&b))),
        }
    })
    .await;

    // ...and bob, who is engineer (not analyst), keeps his access. This is what
    // proves the deny is role-scoped rather than a blanket outage.
    let bob_rows = exec_ok(&ctx, &ctx.bob, &format!("SELECT event FROM {AUDIT}")).await;
    assert_eq!(total_rows(&bob_rows), 2, "the analyst deny must not affect engineer bob");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn revoke_disables_access() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;
    crate::common::eventually("alice's grant", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None).await {
            Ok(b) if total_rows(&b) == 3 => Ok(()),
            Ok(b) => Err(format!("expected 3 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;

    exec_ok(&ctx, &ctx.carol, &format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\"")).await;
    crate::common::eventually("the revoke to take effect", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None).await {
            Err(_) => Ok(()),
            Ok(b) => Err(format!("still allowed with {} rows", total_rows(&b))),
        }
    })
    .await;
}
```

- [ ] **Step 2: Run the four tests**

Run: `scripts/access-control-test.sh 2>&1 | tail -40`
Expected: all tests so far PASS. If `role_grant_and_user_grant_both_apply` fails on the dave assertion with "control failed", carol's admin grant is missing; see Task 4 Step 2.

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(policy): grant-path cases (role/user, writes, deny precedence, revoke)

Deny precedence edits the existing per-resource Ranger policy with a
denyPolicyItems entry, which is how Ranger models it -- a second policy would
not override. bob keeps access throughout, proving the deny is role-scoped."
```

---

### Task 6: Resource-based masks, keyed hash, and resource row filter

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`

**Interfaces:**
- Consumes: everything from Task 4 and 5
- Produces: `async fn grant_read_to_both_roles(ctx: &AcCtx)`
- Produces: `fn hive_mask_policy(name: &str, column: &str, mask: serde_json::Value) -> serde_json::Value`
- Produces: `fn hive_rowfilter_policy(name: &str, filter: &str) -> serde_json::Value`

- [ ] **Step 1: Write the failing tests**

Append:

```rust
/// Both roles get plain read access on orders, so the fine-grained cases differ
/// only in masking: alice (analyst only) is the unmasked baseline, bob
/// (analyst + engineer) is the masked subject.
async fn grant_read_to_both_roles(ctx: &AcCtx) {
    exec_ok(ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;
    exec_ok(ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\"")).await;
    crate::common::eventually("both roles to read orders", || async {
        let a = ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None).await;
        let b = ctx.handler.execute(&ctx.bob, &format!("SELECT id FROM {ORDERS}"), None).await;
        match (a, b) {
            (Ok(x), Ok(y)) if total_rows(&x) == 3 && total_rows(&y) == 3 => Ok(()),
            (a, b) => Err(format!("alice={:?} bob={:?}", a.map(|v| total_rows(&v)), b.map(|v| total_rows(&v)))),
        }
    })
    .await;
}

/// A datamask policy on the test-owned hive service for role `engineer`.
/// `database` is `ac` because SQE sends the LAST namespace component.
fn hive_mask_policy(name: &str, column: &str, mask: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "service": HIVE_SERVICE,
        "name": name,
        "policyType": 1,
        "isEnabled": true,
        "resources": {
            "database": {"values": ["ac"]},
            "table": {"values": ["orders"]},
            "column": {"values": [column]}
        },
        "dataMaskPolicyItems": [{
            "roles": ["engineer"],
            "accesses": [{"type": "select", "isAllowed": true}],
            "dataMaskInfo": mask
        }]
    })
}

/// A row-filter policy on the test-owned hive service for role `engineer`.
fn hive_rowfilter_policy(name: &str, filter: &str) -> serde_json::Value {
    serde_json::json!({
        "service": HIVE_SERVICE,
        "name": name,
        "policyType": 2,
        "isEnabled": true,
        "resources": {
            "database": {"values": ["ac"]},
            "table": {"values": ["orders"]}
        },
        "rowFilterPolicyItems": [{
            "roles": ["engineer"],
            "accesses": [{"type": "select", "isAllowed": true}],
            "rowFilterInfo": {"filterExpr": filter}
        }]
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn resource_column_masks_apply_to_engineer_only() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-amount"),
            "amount",
            serde_json::json!({"dataMaskType": "MASK_NULL"}),
        ))
        .await
        .expect("create amount mask");
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-ssn"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create ssn mask");

    let sql = format!("SELECT amount, ssn FROM {ORDERS} ORDER BY id");

    // bob: amount nulled, ssn show-last-4, all three rows still present (no row
    // filter in this case, so a short result would mean a mask failed closed).
    let bob = crate::common::eventually("bob's masks to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "amount").iter().all(|v| v.is_empty()) => Ok(b),
            Ok(b) => Err(format!("amount not masked: {:?}", col_strings(&b, "amount"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(total_rows(&bob), 3, "masking must not drop rows");
    assert_eq!(col_strings(&bob, "amount"), vec!["", "", ""], "MASK_NULL renders as NULL");
    assert_eq!(
        col_strings(&bob, "ssn"),
        vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"],
        "MASK_SHOW_LAST_4 keeps separators and the last four digits"
    );

    // alice is analyst-only: the engineer policies do not apply to her.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "amount"), vec!["10.0", "20.0", "30.0"]);
    assert_eq!(
        col_strings(&alice, "ssn"),
        vec!["111-11-1111", "222-22-2222", "333-33-3333"]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn hash_mask_is_keyed_hmac() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-hash"),
            "email",
            serde_json::json!({"dataMaskType": "MASK_HASH"}),
        ))
        .await
        .expect("create hash mask");

    // Expected digests are HMAC-SHA256(key = policy.mask_key) hex, computed
    // out-of-band so this is an independent oracle rather than the UDF checking
    // itself:
    //   printf 'a@x' | openssl dgst -sha256 -hmac 'sqe-ac-e2e-mask-key' -r
    const EXPECTED: [&str; 3] = [
        "491c535df5b10e029c37a1a2a49638fe8db57b96d0b83dac522fc0d6cf701109", // a@x
        "e38ff56157e4e2dd387e7e0fd085ba18dbe36132ee3e8ac0af93177f35813c85", // b@x
        "136bdc217df93c518ff03832f856d060be664ed5d22539151d4e10d6bd6ecd33", // c@x
    ];

    let sql = format!("SELECT email FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("bob's hash mask to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "email") == EXPECTED.to_vec() => Ok(b),
            Ok(b) => Err(format!("got {:?}", col_strings(&b, "email"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&bob, "email"), EXPECTED.to_vec());

    // Plain SHA-256 of "a@x" would be a DIFFERENT digest. Asserting the keyed
    // value is what proves policy.mask_key reached the UDF (issue #37).
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "email"), vec!["a@x", "b@x", "c@x"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn resource_row_filter_restricts_rows() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_rowfilter_policy(
            &format!("{PREFIX}rowfilter"),
            "region = 'EU'",
        ))
        .await
        .expect("create row filter");

    let sql = format!("SELECT id, region FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("bob's row filter to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if total_rows(&b) == 2 => Ok(b),
            Ok(b) => Err(format!("expected 2 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&bob, "id"), vec!["1", "3"], "only the EU rows survive");
    assert_eq!(col_strings(&bob, "region"), vec!["EU", "EU"]);

    // alice is unaffected: the filter targets role engineer.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "id"), vec!["1", "2", "3"]);
}
```

- [ ] **Step 2: Run the three tests**

Run: `scripts/access-control-test.sh 2>&1 | tail -40`
Expected: PASS. Two likely first-run adjustments, both in the EXPECTED constants rather than in engine code:
- if `MASK_SHOW_LAST_4` renders differently on this Ranger servicedef version, print `col_strings(&bob, "ssn")` and set the expectation to the observed value **only after confirming it still hides the first five characters**;
- if `amount` renders as something other than the empty string for NULL, match `common::fmt_val`.

- [ ] **Step 3: Mutation-check the mask test**

```bash
curl -s -u admin:'rangerR0cks!' -H 'X-XSRF-HEADER:x' -X DELETE \
  "http://localhost:26080/service/public/v2/api/policy?servicename=sqe_ac_hive&policyname=sqe-ac-e2e-mask-ssn"
scripts/access-control-test.sh resource_column_masks 2>&1 | tail -15
```
Expected: the test now FAILS on the ssn assertion (the mask is gone). This proves the assertion is load-bearing rather than trivially true. The next `ac_setup()` recreates the policy, so no cleanup is needed.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(policy): resource masks, keyed HMAC hash mask, row filter

Row filters had no end-to-end coverage: the demo drops its filter policy so the
Spark mask cross-compare stays byte-comparable. The hash case asserts an
out-of-band HMAC-SHA256 digest, proving policy.mask_key reaches the UDF (#37)."
```

---

### Task 7: Tag-based masks, tag row filter, tag fail-closed

This is the path with no prior live validation. `ranger_store.rs:27` carries `TODO(phase3): verify tagPolicies shape against a live tag-linked bundle`.

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`

**Interfaces:**
- Consumes: everything from Tasks 4 to 6
- Produces: `fn tag_mask_policy(name: &str, tag: &str, mask: serde_json::Value) -> serde_json::Value`
- Produces: `fn tag_rowfilter_policy(name: &str, tag: &str, filter: &str) -> serde_json::Value`
- Produces: `async fn set_column_tag(ctx: &AcCtx, column: &str, tag: &str)`

- [ ] **Step 1: Write the failing tests**

Append:

```rust
/// A datamask policy on the test-owned TAG service. `resolve_tag_policies`
/// matches on `is_enabled`, the `tag` resource values, and the policy item's
/// users/roles/groups. It does not filter on access types, so the `accesses`
/// entry here is realism, not a requirement.
fn tag_mask_policy(name: &str, tag: &str, mask: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "service": TAG_SERVICE,
        "name": name,
        "policyType": 1,
        "isEnabled": true,
        "resources": {"tag": {"values": [tag]}},
        "dataMaskPolicyItems": [{
            "roles": ["engineer"],
            "accesses": [{"type": "hive:select", "isAllowed": true}],
            "dataMaskInfo": mask
        }]
    })
}

fn tag_rowfilter_policy(name: &str, tag: &str, filter: &str) -> serde_json::Value {
    serde_json::json!({
        "service": TAG_SERVICE,
        "name": name,
        "policyType": 2,
        "isEnabled": true,
        "resources": {"tag": {"values": [tag]}},
        "rowFilterPolicyItems": [{
            "roles": ["engineer"],
            "accesses": [{"type": "hive:select", "isAllowed": true}],
            "rowFilterInfo": {"filterExpr": filter}
        }]
    })
}

/// Attach a column tag through SQL, so the DDL path is covered too. Tags land in
/// the Iceberg table property `sqe.column-tags` and are read back by
/// `CacheTagSource` from the shared `TableMetadataCache`.
async fn set_column_tag(ctx: &AcCtx, column: &str, tag: &str) {
    exec_ok(
        ctx,
        &ctx.carol,
        &format!("ALTER TABLE {ORDERS} MODIFY COLUMN {column} SET TAG {tag} = 'true'"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn tag_column_mask_applies_from_iceberg_property() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    // No RESOURCE mask on ssn in this test. If one existed, a passing result
    // could be the resource path doing the work and the tag path could be
    // entirely broken. ac_setup() already deleted every sqe-ac-e2e- policy.
    let hive_policies = ctx.ranger.get_policies(HIVE_SERVICE).await.expect("list hive policies");
    assert!(
        !hive_policies
            .iter()
            .any(|p| p["name"].as_str().is_some_and(|n| n.starts_with(PREFIX))),
        "no test resource policy may exist here, or this test cannot attribute the mask \
         to the tag path"
    );

    set_column_tag(&ctx, "ssn", "PII").await;
    ctx.ranger
        .create_policy(tag_mask_policy(
            &format!("{PREFIX}tag-mask-pii"),
            "PII",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create tag mask");

    let sql = format!("SELECT ssn FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("bob's tag mask to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "ssn") == vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"] => Ok(b),
            Ok(b) => Err(format!("got {:?}", col_strings(&b, "ssn"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(total_rows(&bob), 3, "a tag mask must not drop rows");

    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(
        col_strings(&alice, "ssn"),
        vec!["111-11-1111", "222-22-2222", "333-33-3333"],
        "the tag policy targets role engineer; analyst-only alice is unmasked"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn tag_row_filter_restricts_rows() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    set_column_tag(&ctx, "region", "RESTRICTED").await;
    ctx.ranger
        .create_policy(tag_rowfilter_policy(
            &format!("{PREFIX}tag-rowfilter"),
            "RESTRICTED",
            "region = 'EU'",
        ))
        .await
        .expect("create tag row filter");

    let sql = format!("SELECT id FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("bob's tag row filter to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if total_rows(&b) == 2 => Ok(b),
            Ok(b) => Err(format!("expected 2 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(col_strings(&bob, "id"), vec!["1", "3"]);

    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "id"), vec!["1", "2", "3"]);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn unmappable_tag_mask_fails_closed() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    set_column_tag(&ctx, "email", "SECRET").await;
    // CUSTOM with no valueExpr: nothing to substitute. resolve_tag_policies
    // marks the tag unmappable and the rewriter must RESTRICT every column
    // bearing it, rather than returning the raw value.
    ctx.ranger
        .create_policy(tag_mask_policy(
            &format!("{PREFIX}tag-mask-broken"),
            "SECRET",
            serde_json::json!({"dataMaskType": "CUSTOM"}),
        ))
        .await
        .expect("create broken tag mask");

    let sql = format!("SELECT id, email FROM {ORDERS} ORDER BY id");
    crate::common::eventually("the unmappable tag to restrict email", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            // Either the column is gone from the schema, or the whole statement
            // errors. Both are fail-closed; a raw email is not.
            Err(_) => Ok(()),
            Ok(batches) => {
                let leaked = batches.iter().any(|b| b.schema().index_of("email").is_ok());
                if leaked {
                    Err(format!(
                        "email still present: {:?}",
                        col_strings(&batches, "email")
                    ))
                } else {
                    Ok(())
                }
            }
        }
    })
    .await;

    // Whatever happened must not have leaked the value anywhere else. Iterate
    // each batch's own columns positionally: reading a name from one batch and
    // looking it up across all of them would panic if batch schemas ever differ.
    if let Ok(batches) = ctx.handler.execute(&ctx.bob, &sql, None).await {
        for b in &batches {
            for (idx, field) in b.schema().fields().iter().enumerate() {
                let arr = b.column(idx);
                for row in 0..b.num_rows() {
                    let v = crate::common::fmt_val(arr.as_ref(), row);
                    assert!(
                        !v.contains("@x"),
                        "raw email value `{v}` leaked in column `{}`",
                        field.name()
                    );
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run the three tag tests**

Run: `scripts/access-control-test.sh tag_ 2>&1 | tail -40`
Expected: PASS. This is the first time the `tagPolicies` deserialization has met a real bundle, so a failure here is a genuine finding. Debug order:
1. `curl -s -u admin:'rangerR0cks!' http://localhost:26080/service/plugins/policies/download/sqe_ac_hive | python3 -m json.tool | head -60` and confirm a `tagPolicies.policies` array with your policy in it.
2. Compare the JSON field names against the `serde` attributes in `crates/sqe-policy/src/ranger_store.rs` (the `TagPolicies` struct at line 28 onward). A mismatch there is the bug the TODO warned about: fix `ranger_store.rs`, and say so in the commit.
3. `RUST_LOG=sqe_policy=debug` output shows `tag_masks`, `tag_filters`, and `unmappable_tags` counts from `plan_rewriter.rs`. All zero with a non-empty bundle means matching failed (check the role name in the policy item), not parsing.

- [ ] **Step 3: Mutation-check the tag path**

```bash
curl -s -u admin:'rangerR0cks!' -H 'X-XSRF-HEADER:x' -X DELETE \
  "http://localhost:26080/service/public/v2/api/policy?servicename=sqe_ac_tag&policyname=sqe-ac-e2e-tag-mask-pii"
scripts/access-control-test.sh tag_column_mask 2>&1 | tail -15
```
Expected: FAIL with raw ssn values in the `got [...]` message, proving the assertion depends on the tag policy.

- [ ] **Step 4: Commit**

```bash
git add crates/sqe-coordinator/tests/it/access_control_e2e.rs
git commit -m "test(policy): tag-based masks, tag row filters, tag fail-closed

First live validation of the tag path: SET TAG DDL -> sqe.column-tags Iceberg
property -> CacheTagSource -> Ranger tagPolicies. The fail-closed case pins the
CUSTOM-without-valueExpr behaviour to column restriction, not a raw value."
```

---

### Task 8: Introspection cases, live bundle capture, CI job, and repo bookkeeping

**Files:**
- Modify: `crates/sqe-coordinator/tests/it/access_control_e2e.rs`
- Modify: `crates/sqe-policy/src/testdata/tag_bundle_live_sample.json` (replaced by the capture)
- Modify: `crates/sqe-policy/src/ranger_store.rs` (remove the `#[ignore]` at line 1389 and the `TODO(phase3)` at line 27)
- Modify: `.gitlab-ci.yml` (new job)
- Modify: `README.md`, `nextsteps.md`

**Interfaces:**
- Consumes: everything from Tasks 4 to 7

- [ ] **Step 1: Write the failing introspection tests**

Append:

```rust
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn show_grants_lists_both_roles() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\"")).await;

    let batches = crate::common::eventually("SHOW GRANTS to list both roles", || async {
        match ctx.handler.execute(&ctx.carol, &format!("SHOW GRANTS ON {ORDERS}"), None).await {
            Ok(b) if total_rows(&b) > 0 => Ok(b),
            Ok(_) => Err("no grant rows yet".to_string()),
            Err(e) => Err(format!("SHOW GRANTS failed: {e}")),
        }
    })
    .await;

    // Assert per column rather than on printed text: collect every string cell
    // of every row, then require both grantee names to be present.
    let mut cells: Vec<String> = Vec::new();
    for b in &batches {
        for f in b.schema().fields() {
            cells.extend(col_strings(&batches, f.name()));
        }
    }
    assert!(
        cells.iter().any(|c| c == "analyst"),
        "SHOW GRANTS must list grantee analyst; cells: {cells:?}"
    );
    assert!(
        cells.iter().any(|c| c == "engineer"),
        "SHOW GRANTS must list grantee engineer; cells: {cells:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn check_access_reflects_user_grants() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {AUDIT} TO USER \"bob\"")).await;

    let bob_cells = crate::common::eventually("CHECK ACCESS to report bob allowed", || async {
        match ctx
            .handler
            .execute(&ctx.carol, &format!("CHECK ACCESS SELECT ON {AUDIT} FOR USER \"bob\""), None)
            .await
        {
            Ok(b) => {
                let mut cells: Vec<String> = Vec::new();
                for batch in &b {
                    for f in batch.schema().fields() {
                        cells.extend(col_strings(&b, f.name()));
                    }
                }
                if cells.iter().any(|c| c.eq_ignore_ascii_case("true")) {
                    Ok(cells)
                } else {
                    Err(format!("no true cell yet: {cells:?}"))
                }
            }
            Err(e) => Err(format!("CHECK ACCESS failed: {e}")),
        }
    })
    .await;
    assert!(bob_cells.iter().any(|c| c.eq_ignore_ascii_case("true")));

    let dave = exec_ok(
        &ctx,
        &ctx.carol,
        &format!("CHECK ACCESS SELECT ON {AUDIT} FOR USER \"dave\""),
    )
    .await;
    let mut dave_cells: Vec<String> = Vec::new();
    for batch in &dave {
        for f in batch.schema().fields() {
            dave_cells.extend(col_strings(&dave, f.name()));
        }
    }
    assert!(
        dave_cells.iter().any(|c| c.eq_ignore_ascii_case("false")),
        "dave holds no grant on audit; CHECK ACCESS must report false. cells: {dave_cells:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn capture_live_tag_bundle() {
    ac_gate!();
    if std::env::var("SQE_AC_CAPTURE").as_deref() != Ok("1") {
        eprintln!("skipping capture: set SQE_AC_CAPTURE=1 to overwrite the testdata bundle");
        return;
    }
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // The capture must contain at least one tag-linked datamask AND one
    // tag-linked rowfilter policy, which is exactly what
    // testdata/tag_bundle_live_sample.json's placeholder text asks for.
    set_column_tag(&ctx, "ssn", "PII").await;
    set_column_tag(&ctx, "region", "RESTRICTED").await;
    ctx.ranger
        .create_policy(tag_mask_policy(
            &format!("{PREFIX}tag-mask-pii"),
            "PII",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create tag mask");
    ctx.ranger
        .create_policy(tag_rowfilter_policy(
            &format!("{PREFIX}tag-rowfilter"),
            "RESTRICTED",
            "region = 'EU'",
        ))
        .await
        .expect("create tag row filter");

    let bundle = crate::common::eventually("the bundle to carry both tag policies", || async {
        let b = ctx
            .ranger
            .download_bundle(HIVE_SERVICE)
            .await
            .map_err(|e| e.to_string())?;
        let policies = b["tagPolicies"]["policies"].as_array().cloned().unwrap_or_default();
        let has_mask = policies.iter().any(|p| p["policyType"] == 1);
        let has_filter = policies.iter().any(|p| p["policyType"] == 2);
        if has_mask && has_filter {
            Ok(b)
        } else {
            Err(format!("mask={has_mask} filter={has_filter} in {} policies", policies.len()))
        }
    })
    .await;

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .join("sqe-policy/src/testdata/tag_bundle_live_sample.json");
    std::fs::write(&path, serde_json::to_string_pretty(&bundle).expect("serialize bundle"))
        .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    eprintln!("captured live tag bundle -> {}", path.display());
}
```

- [ ] **Step 2: Run the introspection tests**

Run: `scripts/access-control-test.sh 2>&1 | tail -40`
Expected: all tests PASS, `capture_live_tag_bundle` prints its skip line.

- [ ] **Step 3: Capture the bundle and un-ignore the unit test**

```bash
SQE_AC_CAPTURE=1 scripts/access-control-test.sh capture_live_tag_bundle 2>&1 | tail -10
git diff --stat crates/sqe-policy/src/testdata/tag_bundle_live_sample.json
python3 -m json.tool < crates/sqe-policy/src/testdata/tag_bundle_live_sample.json | head -40
```
Expected: the placeholder `__placeholder__` key is gone and `tagPolicies.policies` holds the two policies.

Then in `crates/sqe-policy/src/ranger_store.rs`:
1. delete the `#[ignore = "pending a real tagPolicies capture; ..."]` line above `resolve_tag_policies_against_live_sample` (line 1389);
2. update that test's asserted user / roles / tags constants to match the capture (user `bob`, role `engineer`, tags `PII` and `RESTRICTED`);
3. delete the `TODO(phase3): verify tagPolicies shape against a live tag-linked bundle` comment at line 27, replacing it with:

```rust
/// Shape verified against a live Ranger 2.8 bundle captured by
/// `access_control_e2e::capture_live_tag_bundle` (see
/// `src/testdata/tag_bundle_live_sample.json`).
```

Run: `cargo test -p sqe-policy resolve_tag_policies_against_live_sample -- --nocapture`
Expected: PASS against the real capture.

- [ ] **Step 4: Add the CI job**

In `.gitlab-ci.yml`, after the `scenario-test` job, add:

```yaml
# ── Access-control e2e (Polaris + Ranger + Keycloak) ────────────────────────
# Rust access-control suite against the quickstart Ranger stack. Same
# docker-in-docker setup as scenario-test. NOTE: while the shared dind runner is
# unhealthy this job cannot fail meaningfully, so treat a green pipeline here as
# no evidence and gate policy changes on a local `make test-access-control`.
access-control-test:
  extends: .scenario-test-base
  timeout: 60m
  script:
    - ./scripts/access-control-test.sh
  rules:
    - changes:
        - crates/sqe-policy/**/*
        - crates/sqe-coordinator/src/policy_wiring.rs
        - crates/sqe-coordinator/tests/it/access_control_e2e.rs
        - crates/sqe-coordinator/tests/it/common/**/*
        - quickstart/polaris-ranger-keycloak/**/*
        - scripts/access-control-test.sh
        - tests/sqe-ranger-test.toml
```

If `.scenario-test-base` does not exist, copy the `image`, `services`, `variables`, and `before_script` blocks from the existing `scenario-test` job verbatim instead of extending. Validate the file parses:

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.gitlab-ci.yml')); print('yaml ok')"
```

- [ ] **Step 5: Update the repo bookkeeping files**

In `README.md`, in the roadmap checklist, add under the security/Phase 5 section:

```markdown
- [x] Access-control e2e tests (Ranger masks, row filters, tag masks, fail-closed) -- `make test-access-control`
```

In `nextsteps.md`, mark the access-control testing item done and add this line under the current status:

```markdown
- Access-control e2e: `make test-access-control` runs 13 cases against the
  Polaris + Ranger + Keycloak quickstart stack (exact masked values, row
  filters, tag masks, tag fail-closed). Follow-ups: tag-state-unknown deny,
  policy-breaker fail-closed, cache-TTL expiry, Flight SQL smoke.
```

- [ ] **Step 6: Full-suite verification and isolation proof**

```bash
scripts/access-control-test.sh 2>&1 | tail -25
cargo test -p sqe-policy 2>&1 | tail -5
( cd quickstart/polaris-ranger-keycloak && ./run.sh --check 2>&1 | tail -5 )
```
Expected: the e2e suite passes, `sqe-policy` unit tests pass including the newly un-ignored one, and the quickstart harness still reports `RESULT: N passed, 0 failed`. The last command is the isolation proof: the suite must not have disturbed the demo fixtures.

- [ ] **Step 7: Commit**

```bash
git add crates/sqe-coordinator/tests/it/access_control_e2e.rs \
        crates/sqe-policy/src/testdata/tag_bundle_live_sample.json \
        crates/sqe-policy/src/ranger_store.rs \
        .gitlab-ci.yml README.md nextsteps.md
git commit -m "test(policy): introspection cases, live tag bundle capture, CI job

SHOW GRANTS and CHECK ACCESS asserted per Arrow column. The capture replaces
testdata/tag_bundle_live_sample.json with a real Ranger 2.8 bundle and
un-ignores resolve_tag_policies_against_live_sample, retiring the TODO(phase3)
on the tagPolicies shape."
```

- [ ] **Step 8: Open the merge request**

```bash
git push -u origin test/ranger-access-control-e2e
glab mr create --fill --target-branch main
```

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: coordinator wiring and the config file (Task 2), the `build_grant_backend` refactor (Task 1), Ranger services and policy fixtures (Task 3), fixture data and cases 1 to 2 (Task 4), cases 3, 4, 11, 12 (Task 5), cases 5 to 7 (Task 6), cases 8 to 10 (Task 7), cases 13 to 15 plus CI and bookkeeping (Task 8). Gating, serialization, propagation waits, and stack size are in the Global Constraints and implemented in Task 2. The spec's four follow-ups are recorded in `nextsteps.md` in Task 8 Step 5 rather than implemented, as designed.

**Naming consistency.** `ac_setup`, `exec_ok`, `col_strings`, `total_rows`, `assert_denied_but_valid`, `grant_read_to_both_roles`, `hive_mask_policy`, `hive_rowfilter_policy`, `tag_mask_policy`, `tag_rowfilter_policy`, `set_column_tag`, `add_deny_item_to_audit_policy`, and the `RangerAdmin` methods are each defined once and used with the same signature everywhere. `PREFIX`, `HIVE_SERVICE`, `TAG_SERVICE`, `ORDERS`, `AUDIT` likewise.

**Known first-run risks, each with a concrete fallback in the step that hits it.** `CREATE SCHEMA sales_wh.ac` routing (Task 4 Step 1 names the exact fallback tables and the `database` resource change it implies); `MASK_SHOW_LAST_4` and NULL rendering (Task 6 Step 2); the `tagPolicies` shape (Task 7 Step 2 gives a three-step debug order and treats a mismatch as the finding it is); `.scenario-test-base` existing in CI (Task 8 Step 4).
