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

use arrow_array::RecordBatch;
use sqe_coordinator::QueryHandler;
use sqe_core::Session;

use crate::common::ranger_fixture::{RangerAdmin, HIVE_SERVICE, PREFIX, TAG_SERVICE};

/// Skip-or-run gate. Returns early when the suite was not opted into.
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

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

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
/// covers the two test-owned services. Without this the suite is not idempotent:
/// on a second run that test would start with alice already denied and burn its
/// 30s budget waiting for a baseline allow that can never arrive.
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

    // Namespaces. Both warehouses get an `ac` namespace of their own.
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
        .execute(
            &carol,
            &format!("CREATE TABLE {AUDIT} (id BIGINT, event VARCHAR)"),
            None,
        )
        .await
        .expect("create audit");
    handler
        .execute(
            &carol,
            &format!("INSERT INTO {AUDIT} VALUES (1,'login'),(2,'logout')"),
            None,
        )
        .await
        .expect("insert audit");

    // Remove any grants a previous run left on the fixture tables, so "denied
    // before grant" starts from a true denial.
    for stmt in [
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE INSERT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE SELECT ON {AUDIT} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {AUDIT} FROM ROLE \"engineer\""),
        format!("REVOKE SELECT ON {AUDIT} FROM USER \"bob\""),
    ] {
        let _ = handler.execute(&carol, &stmt, None).await;
    }

    AcCtx {
        handler,
        ranger,
        carol,
        alice,
        bob,
        dave,
        _cache,
    }
}

/// Execute and unwrap with the SQL in the panic message.
async fn exec_ok(ctx: &AcCtx, s: &Session, sql: &str) -> Vec<RecordBatch> {
    ctx.handler
        .execute(s, sql, None)
        .await
        .unwrap_or_else(|e| panic!("[{}] {sql} failed: {e}", s.user.username))
}

/// Values of one column across all batches, rendered with `common::fmt_val`.
/// NULL renders as the string "NULL"; Float64 renders with two decimals.
fn col_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let Ok(idx) = b.schema().index_of(column) else {
            panic!(
                "column `{column}` absent from result schema {:?}",
                b.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
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

// ─────────────────────────────────────────────────────────────────────────────
// Coarse gate: grant enables, revoke disables
// ─────────────────────────────────────────────────────────────────────────────

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

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
    )
    .await;

    let rows = crate::common::eventually("alice's SELECT to be allowed", || async {
        match ctx
            .handler
            .execute(
                &ctx.alice,
                &format!("SELECT region, amount FROM {ORDERS} ORDER BY id"),
                None,
            )
            .await
        {
            Ok(b) if total_rows(&b) == 3 => Ok(b),
            Ok(b) => Err(format!("expected 3 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;

    assert_eq!(col_strings(&rows, "region"), vec!["EU", "US", "EU"]);
    assert_eq!(
        col_strings(&rows, "amount"),
        vec!["10.00", "20.00", "30.00"],
        "fmt_val renders Float64 with two decimals"
    );
}
