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
    //
    // The audit table needs a bounded retry rather than a bare DROP. A deny item
    // left by `ranger_deny_overrides_allow` targets role `analyst`, and CAROL IS
    // A MEMBER of analyst (ranger-setup: analyst -> alice, bob, carol), so the
    // deny blocks the admin too. `clear_audit_deny_items` above removes it, but
    // Polaris caches Ranger policies, so the clear is not visible instantly.
    // The whole audit sequence retries as a unit. `DROP TABLE IF EXISTS` is NOT
    // a usable readiness probe on its own: while the table is policy-hidden the
    // DROP reports success (nothing to drop), and the following CREATE then
    // fails because the table does in fact exist. Retrying drop -> create ->
    // insert together converges as soon as the deny drains.
    crate::common::eventually("carol to (re)create the audit fixture", || async {
        for stmt in [
            format!("DROP TABLE IF EXISTS {AUDIT}"),
            format!("CREATE TABLE {AUDIT} (id BIGINT, event VARCHAR)"),
            format!("INSERT INTO {AUDIT} VALUES (1,'login'),(2,'logout')"),
        ] {
            if let Err(e) = handler.execute(&carol, &stmt, None).await {
                return Err(format!("`{stmt}` failed: {e}"));
            }
        }
        Ok(())
    })
    .await;
    handler
        .execute(&carol, &format!("DROP TABLE IF EXISTS {ORDERS}"), None)
        .await
        .unwrap_or_else(|e| panic!("drop {ORDERS}: {e}"));
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
    // Remove any grants a previous run left on the fixture tables, so "denied
    // before grant" starts from a true denial.
    for stmt in [
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE INSERT ON {ORDERS} FROM ROLE \"engineer\""),
        format!("REVOKE SELECT ON {AUDIT} FROM ROLE \"analyst\""),
        format!("REVOKE SELECT ON {AUDIT} FROM ROLE \"engineer\""),
        format!("REVOKE SELECT ON {AUDIT} FROM USER \"bob\""),
        format!("REVOKE SELECT ON {AUDIT} FROM USER \"dave\""),
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
/// The denial is waited for, not assumed: `ac_setup` REVOKEs any grants a
/// previous test left, and Polaris caches Ranger policies, so an allow can still
/// be served for a few seconds after the revoke lands in Ranger.
async fn assert_denied_but_valid(ctx: &AcCtx, denied: &Session, sql: &str) {
    let as_admin = ctx.handler.execute(&ctx.carol, sql, None).await;
    assert!(
        as_admin.is_ok(),
        "control failed: `{sql}` must succeed as carol, else the test cannot tell \
         denial from an invalid statement. Error: {:?}",
        as_admin.err()
    );
    let user = denied.user.username.clone();
    crate::common::eventually(&format!("`{sql}` to be denied for {user}"), || async {
        match ctx.handler.execute(denied, sql, None).await {
            Err(_) => Ok(()),
            Ok(b) => Err(format!(
                "still allowed for {user} with {} rows",
                total_rows(&b)
            )),
        }
    })
    .await;
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn role_grant_and_user_grant_both_apply() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {AUDIT} TO USER \"bob\""),
    )
    .await;

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

    for stmt in [
        format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
        format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
        format!("GRANT INSERT ON {ORDERS} TO ROLE \"engineer\""),
    ] {
        exec_ok(&ctx, &ctx.carol, &stmt).await;
    }

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
    assert!(
        insert.is_err(),
        "alice holds SELECT only; INSERT must be denied"
    );

    let drop = ctx
        .handler
        .execute(&ctx.alice, &format!("DROP TABLE {ORDERS}"), None)
        .await;
    assert!(drop.is_err(), "alice holds SELECT only; DROP must be denied");

    // Prove the DROP really did not happen.
    let still_there =
        exec_ok(&ctx, &ctx.carol, &format!("SELECT id FROM {ORDERS} ORDER BY id")).await;
    assert_eq!(
        total_rows(&still_there),
        4,
        "the denied DROP must not have removed the table"
    );
}

/// Add a `denyPolicyItems` entry for role `engineer` to the EXISTING Ranger
/// policy that SQE's `GRANT SELECT ON ops_wh.ac.audit` created.
///
/// The deny targets `engineer`, not `analyst`, so the test has a control user.
/// Ranger's role store (ranger-setup) is analyst -> alice, bob, carol and
/// engineer -> bob, carol, so denying engineer hits bob while leaving
/// analyst-only alice untouched. Denying analyst would hit alice, bob AND carol
/// (the admin), leaving nobody to compare against.
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
        "roles": ["engineer"],
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

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {AUDIT} TO ROLE \"analyst\""),
    )
    .await;
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {AUDIT} TO ROLE \"engineer\""),
    )
    .await;

    // Both can read before the deny lands.
    crate::common::eventually("alice and bob to read audit", || async {
        let a = ctx
            .handler
            .execute(&ctx.alice, &format!("SELECT event FROM {AUDIT}"), None)
            .await;
        let b = ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT event FROM {AUDIT}"), None)
            .await;
        match (a, b) {
            (Ok(x), Ok(y)) if total_rows(&x) == 2 && total_rows(&y) == 2 => Ok(()),
            (a, b) => Err(format!(
                "alice={:?} bob={:?}",
                a.map(|v| total_rows(&v)),
                b.map(|v| total_rows(&v))
            )),
        }
    })
    .await;

    add_deny_item_to_audit_policy(&ctx).await;

    // Deny beats allow for bob (engineer), even though his analyst membership
    // still carries an allow on the same resource.
    crate::common::eventually("the deny to take effect for bob", || async {
        match ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT event FROM {AUDIT}"), None)
            .await
        {
            Err(_) => Ok(()),
            Ok(b) => Err(format!("still allowed with {} rows", total_rows(&b))),
        }
    })
    .await;

    // ...and alice, analyst-only, keeps her access. This is what proves the deny
    // is scoped to the engineer role rather than a blanket outage of the table.
    let alice_rows = exec_ok(&ctx, &ctx.alice, &format!("SELECT event FROM {AUDIT}")).await;
    assert_eq!(
        total_rows(&alice_rows),
        2,
        "the engineer deny must not affect analyst-only alice"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn revoke_disables_access() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
    )
    .await;
    crate::common::eventually("alice's grant", || async {
        match ctx
            .handler
            .execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None)
            .await
        {
            Ok(b) if total_rows(&b) == 3 => Ok(()),
            Ok(b) => Err(format!("expected 3 rows, got {}", total_rows(&b))),
            Err(e) => Err(format!("still denied: {e}")),
        }
    })
    .await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\""),
    )
    .await;
    crate::common::eventually("the revoke to take effect", || async {
        match ctx
            .handler
            .execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None)
            .await
        {
            Err(_) => Ok(()),
            Ok(b) => Err(format!("still allowed with {} rows", total_rows(&b))),
        }
    })
    .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Fine-grained: resource-based masks and row filters (the `hive` service path)
// ─────────────────────────────────────────────────────────────────────────────

/// Both roles get plain read access on orders, so the fine-grained cases differ
/// only in masking: alice (analyst only) is the unmasked baseline, bob
/// (analyst + engineer) is the masked subject.
async fn grant_read_to_both_roles(ctx: &AcCtx) {
    exec_ok(
        ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
    )
    .await;
    exec_ok(
        ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    crate::common::eventually("both roles to read orders", || async {
        let a = ctx
            .handler
            .execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None)
            .await;
        let b = ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT id FROM {ORDERS}"), None)
            .await;
        match (a, b) {
            (Ok(x), Ok(y)) if total_rows(&x) == 3 && total_rows(&y) == 3 => Ok(()),
            (a, b) => Err(format!(
                "alice={:?} bob={:?}",
                a.map(|v| total_rows(&v)),
                b.map(|v| total_rows(&v))
            )),
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
            Ok(b) if col_strings(&b, "amount").iter().all(|v| v == "NULL") => Ok(b),
            Ok(b) => Err(format!("amount not masked: {:?}", col_strings(&b, "amount"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(total_rows(&bob), 3, "masking must not drop rows");
    assert_eq!(
        col_strings(&bob, "amount"),
        vec!["NULL", "NULL", "NULL"],
        "MASK_NULL nulls the column (fmt_val renders NULL as the string \"NULL\")"
    );
    assert_eq!(
        col_strings(&bob, "ssn"),
        vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"],
        "MASK_SHOW_LAST_4 keeps separators and the last four digits"
    );

    // alice is analyst-only: the engineer policies do not apply to her.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "amount"), vec!["10.00", "20.00", "30.00"]);
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
    assert_eq!(
        col_strings(&bob, "id"),
        vec!["1", "3"],
        "only the EU rows survive"
    );
    assert_eq!(col_strings(&bob, "region"), vec!["EU", "EU"]);

    // alice is unaffected: the filter targets role engineer.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "id"), vec!["1", "2", "3"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Fine-grained: TAG-based masks and row filters
//
// This is the least-validated path in the policy stack. ranger_store.rs carried
// `TODO(phase3): verify tagPolicies shape against a live tag-linked bundle`, and
// its testdata bundle was a placeholder, so the tagPolicies deserialization had
// never met a real Ranger response before these tests.
//
// Chain under test: SET TAG DDL -> `sqe.column-tags` Iceberg property ->
// CacheTagSource (reads the shared TableMetadataCache) -> Ranger tagPolicies ->
// mask / row filter / fail-closed restriction.
// ─────────────────────────────────────────────────────────────────────────────

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
    // could be the resource path doing the work while the tag path is broken.
    // ac_setup() already deleted every sqe-ac-e2e- policy; assert it.
    let hive_policies = ctx
        .ranger
        .get_policies(HIVE_SERVICE)
        .await
        .expect("list hive policies");
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
            // Tag-service mask types are component-qualified: the `tag`
            // servicedef only defines prefixed forms (hive:, trino:, presto:,
            // nestedstructure:). A bare "MASK_SHOW_LAST_4" is rejected by
            // Ranger with "is not a valid datamask-type".
            serde_json::json!({"dataMaskType": "hive:MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create tag mask");

    let sql = format!("SELECT ssn FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("bob's tag mask to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b)
                if col_strings(&b, "ssn")
                    == vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"] =>
            {
                Ok(b)
            }
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
async fn tag_row_filters_are_unsupported_by_ranger() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Ranger 2.8's `tag` servicedef defines NO row-filter resource hierarchy and
    // NO row-filter access types:
    //   GET /service/public/v2/api/servicedef/name/tag
    //   -> dataMaskDef.maskTypes = ["hive:MASK", "hive:MASK_SHOW_LAST_4", ...]
    //      rowFilterDef.accessTypes = []
    // So a tag-based row filter cannot be expressed at all, and SQE's
    // `resolve_tag_policies` rowfilter branch (POLICY_TYPE_ROWFILTER over the
    // tagPolicies bundle) is unreachable on this Ranger version. That is a
    // platform gap, not an SQE defect: tag MASKS work (see
    // tag_column_mask_applies_from_iceberg_property) and resource row filters
    // work (see resource_row_filter_restricts_rows).
    //
    // This test pins the limitation so the day Ranger gains the capability it
    // fails and tells us to write the real coverage, instead of the gap sitting
    // silently in a spec.
    let err = ctx
        .ranger
        .create_policy(tag_rowfilter_policy(
            &format!("{PREFIX}tag-rowfilter"),
            "RESTRICTED",
            "region = 'EU'",
        ))
        .await
        .expect_err(
            "Ranger accepted a tag row-filter policy: it has gained rowFilterDef support. \
             Replace this test with real tag row-filter coverage (bob sees only EU rows) and \
             re-check resolve_tag_policies' POLICY_TYPE_ROWFILTER branch against a live bundle.",
        );
    let msg = err.to_string();
    assert!(
        msg.contains("row filter") || msg.contains("resource hierarchies"),
        "expected a rowFilterDef validation failure from Ranger, got: {msg}"
    );
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
            serde_json::json!({"dataMaskType": "hive:CUSTOM"}),
        ))
        .await
        .expect("create broken tag mask");

    let sql = format!("SELECT id, email FROM {ORDERS} ORDER BY id");

    // Fail-closed here means NULLIFIED IN PLACE, not removed from the schema.
    // `plan_rewriter.rs` (the `restricted_columns` arm of the projection rewrite)
    // aliases a Nullify mask to the column's QUALIFIED name so an explicit
    // `SELECT id, email` keeps planning; dropping the field would break the
    // user's outer reference. So the contract is: the column survives, every
    // value is NULL, and no raw value appears anywhere in the result.
    let bob = crate::common::eventually("the unmappable tag to nullify email", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            // A hard error is also acceptable fail-closed behaviour.
            Err(_) => Ok(Vec::new()),
            Ok(batches) => {
                let emails = col_strings(&batches, "email");
                if emails.iter().all(|v| v == "NULL") {
                    Ok(batches)
                } else {
                    Err(format!("email not nullified: {emails:?}"))
                }
            }
        }
    })
    .await;

    // Nothing leaked in any column of any batch. Iterate each batch's own
    // columns positionally: reading a name from one batch and looking it up
    // across all of them would panic if batch schemas ever differ.
    for b in &bob {
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

    // The NULLs must be caused by the broken tag policy, not by the column being
    // empty or by some unrelated denial: alice is analyst-only, the tag policy
    // targets engineer, so she still sees the raw values.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(
        col_strings(&alice, "email"),
        vec!["a@x", "b@x", "c@x"],
        "analyst-only alice is unaffected by the engineer-scoped broken tag mask"
    );
}
