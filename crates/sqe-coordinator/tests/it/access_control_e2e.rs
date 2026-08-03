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
    ranger.bootstrap().await.expect("ranger bootstrap");

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
    /// The cache the policy enforcer reads. Held so it stays alive for the
    /// test, and shared with any second handler that must differ from
    /// `handler` in exactly one config value.
    cache: sqe_catalog::TableMetadataCache,
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
    let (handler, cache) = crate::common::setup_ranger_handler().await;
    let ranger = RangerAdmin::from_env();
    ranger.require_reachable().await;
    // One idempotent call does the whole Ranger-side bootstrap: tag-servicedef
    // row-filter capability, the two test-owned services and their link, and a
    // clean sqe-ac-e2e- policy slate.
    ranger.bootstrap().await.expect("ranger bootstrap");
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
                 ssn VARCHAR, email VARCHAR, signed_on DATE)"
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
                 (1,'EU',10.0,'111-11-1111','a@x',DATE '2021-05-04'), \
                 (2,'US',20.0,'222-22-2222','b@x',DATE '2022-06-05'), \
                 (3,'EU',30.0,'333-33-3333','c@x',DATE '2023-07-06')"
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
        cache,
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
                &format!(
                    "INSERT INTO {ORDERS} VALUES \
                     (4,'EU',40.0,'444-44-4444','d@x',DATE '2024-08-07')"
                ),
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
            &format!(
                "INSERT INTO {ORDERS} VALUES \
                 (9,'x',0.0,'000-00-0000','z@x',DATE '2020-01-01')"
            ),
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
async fn tag_row_filter_restricts_rows() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    // Precondition, stated rather than inferred from an error: Ranger can only
    // hold a tag ROW FILTER policy once the tag servicedef has a rowFilterDef.
    // Ranger propagates dataMaskDef into the tag servicedef unconditionally but
    // rowFilterDef only when Ranger Admin sets
    // `ranger.servicedef.autopropagate.rowfilterdef.to.tag=true` (default
    // false), so `RangerAdmin::bootstrap` patches it in. See
    // ranger_fixture::ensure_tag_rowfilter_support.
    assert!(
        ctx.ranger
            .tag_rowfilter_supported()
            .await
            .expect("query tag servicedef"),
        "tag servicedef has no rowFilterDef; bootstrap should have added it"
    );

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
    assert_eq!(
        col_strings(&bob, "id"),
        vec!["1", "3"],
        "the tag row filter keeps only the EU rows"
    );

    // alice is analyst-only: the tag policy targets engineer.
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

// ─────────────────────────────────────────────────────────────────────────────
// Introspection + live bundle capture
// ─────────────────────────────────────────────────────────────────────────────

/// Every string cell of every batch, for statements whose column layout is not
/// part of the contract under test (SHOW GRANTS, CHECK ACCESS). Iterates each
/// batch's own columns positionally so differing batch schemas cannot panic.
fn all_cells(batches: &[RecordBatch]) -> Vec<String> {
    let mut cells = Vec::new();
    for b in batches {
        for idx in 0..b.num_columns() {
            let arr = b.column(idx);
            for row in 0..b.num_rows() {
                cells.push(crate::common::fmt_val(arr.as_ref(), row));
            }
        }
    }
    cells
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn show_grants_lists_both_roles() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\""),
    )
    .await;
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;

    let cells = crate::common::eventually("SHOW GRANTS to list both roles", || async {
        match ctx
            .handler
            .execute(&ctx.carol, &format!("SHOW GRANTS ON {ORDERS}"), None)
            .await
        {
            Ok(b) => {
                let cells = all_cells(&b);
                let has_both = cells.iter().any(|c| c == "analyst")
                    && cells.iter().any(|c| c == "engineer");
                if has_both {
                    Ok(cells)
                } else {
                    Err(format!("grantees not both listed yet: {cells:?}"))
                }
            }
            Err(e) => Err(format!("SHOW GRANTS failed: {e}")),
        }
    })
    .await;

    // Asserted on decoded Arrow cells, not on printed text: the shell harness
    // greps the rendered table, which also matches a role name appearing in an
    // error message or an unrelated column.
    assert!(cells.iter().any(|c| c == "analyst"));
    assert!(cells.iter().any(|c| c == "engineer"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn check_access_reflects_user_grants() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {AUDIT} TO USER \"bob\""),
    )
    .await;

    let bob_cells = crate::common::eventually("CHECK ACCESS to report bob allowed", || async {
        match ctx
            .handler
            .execute(
                &ctx.carol,
                &format!("CHECK ACCESS SELECT ON {AUDIT} FOR USER \"bob\""),
                None,
            )
            .await
        {
            Ok(b) => {
                let cells = all_cells(&b);
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
    let dave_cells = all_cells(&dave);
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

    // The capture carries BOTH a tag-linked DATAMASK and a tag-linked ROWFILTER,
    // which is exactly what the placeholder testdata asked for. The row filter is
    // only creatable because `bootstrap` gave the tag servicedef a rowFilterDef
    // (Ranger does not propagate one by default -- see
    // ranger_fixture::ensure_tag_rowfilter_support).
    set_column_tag(&ctx, "ssn", "PII").await;
    set_column_tag(&ctx, "region", "RESTRICTED").await;
    ctx.ranger
        .create_policy(tag_mask_policy(
            &format!("{PREFIX}tag-mask-pii"),
            "PII",
            serde_json::json!({"dataMaskType": "hive:MASK_SHOW_LAST_4"}),
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

    let bundle = crate::common::eventually("the bundle to carry the tag datamask", || async {
        let b = ctx
            .ranger
            .download_bundle(HIVE_SERVICE)
            .await
            .map_err(|e| e.to_string())?;
        let policies = b["tagPolicies"]["policies"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        // policyType 1 == datamask on PII, 2 == rowfilter on RESTRICTED.
        let tagged = |p: &serde_json::Value, tag: &str| {
            p["resources"]["tag"]["values"]
                .as_array()
                .is_some_and(|v| v.iter().any(|t| t == tag))
        };
        let has_pii_mask = policies
            .iter()
            .any(|p| p["policyType"] == 1 && tagged(p, "PII"));
        let has_row_filter = policies
            .iter()
            .any(|p| p["policyType"] == 2 && tagged(p, "RESTRICTED"));
        if has_pii_mask && has_row_filter {
            Ok(b)
        } else {
            Err(format!(
                "mask={has_pii_mask} rowfilter={has_row_filter} among {} tag policies",
                policies.len()
            ))
        }
    })
    .await;

    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .join("sqe-policy/src/testdata/tag_bundle_live_sample.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&bundle).expect("serialize bundle"),
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    eprintln!("captured live tag bundle -> {}", path.display());
}

// ─────────────────────────────────────────────────────────────────────────────
// Operational edge: unknown tag state must deny, not leak
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn unknown_tag_state_denies() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    let sql = format!("SELECT id, ssn FROM {ORDERS} ORDER BY id");

    // Control: on the suite's handler bob reads all three rows. Its
    // TableMetadataCache saw this table during fixture DDL, so `CacheTagSource`
    // can answer "known: no tags" and the rewriter does no tag work.
    let warm = exec_ok(&ctx, &ctx.bob, &sql).await;
    assert_eq!(
        total_rows(&warm),
        3,
        "precondition: bob must read normally when tag state is known"
    );

    // Same user, same grant, same SQL -- but a handler whose TableMetadataCache
    // has never seen this table. `TagSource::column_tags` returns None
    // (UNKNOWN, not "no tags"), and the contract in tag_source.rs is that the
    // caller MUST fail closed: a mask or tag row filter might exist that we
    // cannot see, so treating unknown as untagged would silently skip a
    // security control. plan_rewriter logs
    // "Tag state unknown (cache miss or disabled); denying access" and injects a
    // deny-all row filter.
    //
    // This case rests on the cold handler staying cold for the ONE query below.
    // Do not "improve" `setup_ranger_handler` to pre-populate the table cache:
    // the deny would stop happening and this test would pass by no longer
    // exercising the contract. A test that needs a warm second handler should
    // ask for one explicitly with `setup_ranger_handler_sharing`.
    let (cold, _cache) = crate::common::setup_ranger_handler().await;
    let denied = cold
        .execute(&ctx.bob, &sql, None)
        .await
        .expect("the deny is a row filter, so the statement itself still succeeds");
    assert_eq!(
        total_rows(&denied),
        0,
        "unknown tag state must deny, got {:?}",
        col_strings(&denied, "ssn")
    );

    // The decisive check: nothing leaked through the deny.
    for b in &denied {
        for idx in 0..b.num_columns() {
            let arr = b.column(idx);
            for row in 0..b.num_rows() {
                let v = crate::common::fmt_val(arr.as_ref(), row);
                assert!(
                    !v.contains("111-11-1111"),
                    "raw ssn leaked while tag state was unknown: {v}"
                );
            }
        }
    }

    // The warm handler is unaffected: the deny is per-scan, not global state.
    let after = exec_ok(&ctx, &ctx.bob, &sql).await;
    assert_eq!(total_rows(&after), 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn ranger_outage_fails_closed() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    // A mask makes the outage consequential: while Ranger is reachable bob's ssn
    // is masked, so if an outage were to drop policy enforcement the raw value
    // would appear. Fail-closed must deny instead.
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-ssn"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create ssn mask");

    let sql = format!("SELECT id, ssn FROM {ORDERS} ORDER BY id");

    // Warm the handler while Ranger is up. This matters: the outage has to be
    // the ONLY thing that changes. A cold handler denies for an unrelated reason
    // (unknown tag state), which is what made an earlier version of this test
    // pass vacuously.
    let warm = crate::common::eventually("bob's mask to apply before the outage", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) => Ok(b),
            Ok(b) => Err(format!("mask not applied yet: {:?}", col_strings(&b, "ssn"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(total_rows(&warm), 3);

    {
        // Guard restarts ranger-admin on drop, including on panic, so a failure
        // here cannot poison the rest of the suite.
        let _outage = crate::common::ranger_fixture::RangerOutage::begin()
            .expect("stop ranger-admin");

        // The resolved-policy cache holds for policy.ranger.cache-ttl-secs (2s in
        // the test config), so the deny appears once the cache expires and the
        // next resolve cannot reach Ranger.
        let denied = crate::common::eventually("the outage to fail closed", || async {
            match ctx.handler.execute(&ctx.bob, &sql, None).await {
                Ok(b) if total_rows(&b) == 0 => Ok(b),
                Ok(b) => Err(format!(
                    "still returning {} rows: {:?}",
                    total_rows(&b),
                    col_strings(&b, "ssn")
                )),
                // A hard error is equally fail-closed.
                Err(_) => Ok(Vec::new()),
            }
        })
        .await;

        for b in &denied {
            for idx in 0..b.num_columns() {
                let arr = b.column(idx);
                for row in 0..b.num_rows() {
                    let v = crate::common::fmt_val(arr.as_ref(), row);
                    assert!(
                        !v.contains("111-11-1111"),
                        "raw ssn leaked while Ranger was down: {v}"
                    );
                }
            }
        }
    } // guard drops here: ranger-admin restarts

    // Recovery: once Ranger is back the same handler serves masked rows again,
    // which proves the deny was the outage and not a permanent breaker latch.
    // Ranger's restart is slower than the default budget allows.
    let recovered = crate::common::eventually_within(
        std::time::Duration::from_secs(300),
        "ranger to recover and masking to resume",
        || async {
            match ctx.handler.execute(&ctx.bob, &sql, None).await {
                Ok(b)
                    if total_rows(&b) == 3
                        && col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) =>
                {
                    Ok(b)
                }
                Ok(b) => Err(format!(
                    "{} rows, ssn {:?}",
                    total_rows(&b),
                    col_strings(&b, "ssn")
                )),
                Err(e) => Err(format!("query failed: {e}")),
            }
        },
    )
    .await;
    assert_eq!(
        col_strings(&recovered, "ssn"),
        vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"]
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Operational edge: the policy cache TTL bounds how stale a mask can be
// ─────────────────────────────────────────────────────────────────────────────

/// `policy.ranger.cache-ttl-secs` documents a bounded over-permissive window:
/// a mask authored directly in Ranger Admin is not honored until the cached
/// `ResolvedPolicy` expires. This pins BOTH edges of that window.
///
/// Two handlers differing in exactly one config value:
///
/// - `ctx.handler`, at the suite's 2s TTL, is the FRESH side.
/// - `slow`, at 30s, is the STALE side.
///
/// The mutation that discriminates, verified: drop `slow` to a 5s TTL, shorter
/// than `STALE_PROBE_SECS`, and edge 2 goes red with the masked values. Without
/// the two-handler pairing a single-handler version of this test would pass on
/// whichever timer happens to be slowest, which is exactly how an earlier
/// fail-closed test in this file passed vacuously.
///
/// A column mask on the test-owned `hive` service is the right mutation to
/// drive it with. SQE reads that service itself, over
/// `policies/download/{service}`, so the only cache in the path is the one
/// under test. A GRANT or REVOKE would instead route through Polaris's coarse
/// gate and its own plugin poll, and the result would measure that timer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn cache_ttl_bounds_policy_staleness() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    /// Long enough to hold across the probe with room for a slow box, short
    /// enough that the expiry edge still lands inside a sane budget.
    const STALE_TTL_SECS: u64 = 30;
    /// Cache age at which the stale read is taken: well past the 2s fresh TTL,
    /// well short of `STALE_TTL_SECS`.
    ///
    /// This wait is deliberate, not incidental. Elapsed time IS the variable
    /// under test. Without it the probe lands ~0.5s after the seeding read and
    /// passes for ANY ttl >= 1s, which is how the first draft of this test
    /// still passed with the stale handler mutated down to 1s.
    const STALE_PROBE_SECS: u64 = 10;

    // Shares ctx's warm TableMetadataCache, so the ONLY difference between the
    // two handlers is the policy-cache TTL. With a cold cache this handler
    // fails closed on unknown tag state for an indeterminate time (measured:
    // 60s), and a warm-up loop cannot then tell when the policy entry was
    // actually inserted -- it can succeed on a cache HIT for an entry already
    // most of a TTL old. That is precisely how an earlier draft went flaky.
    let (slow, _slow_cache) =
        crate::common::setup_ranger_handler_sharing(Some(ctx.cache.clone()), |c| {
            c.policy.ranger.cache_ttl_secs = STALE_TTL_SECS;
        })
        .await;

    let sql = format!("SELECT id, ssn FROM {ORDERS} ORDER BY id");
    let raw = vec!["111-11-1111", "222-22-2222", "333-33-3333"];

    // Seed both handlers before the policy exists, so each holds a cached
    // "no mask for bob" decision. `slow` has an empty policy cache, so this is
    // necessarily a miss-then-insert: the clock below starts on the insert.
    let warm_fresh = exec_ok(&ctx, &ctx.bob, &sql).await;
    assert_eq!(col_strings(&warm_fresh, "ssn"), raw, "precondition: no mask");
    let warm_slow = slow
        .execute(&ctx.bob, &sql, None)
        .await
        .expect("the long-TTL handler must read on its first try with a warm table cache");
    assert_eq!(
        col_strings(&warm_slow, "ssn"),
        raw,
        "precondition: the long-TTL handler cached an unmasked decision"
    );
    let warmed_at = std::time::Instant::now();

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-ssn"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create ssn mask");

    // Edge 1 -- expiry: the 2s TTL lets the new mask through.
    crate::common::eventually("the short-TTL handler to pick up the new mask", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) => Ok(()),
            Ok(b) => Err(format!("still raw: {:?}", col_strings(&b, "ssn"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;

    // Edge 2 -- staleness: the 30s handler must still be serving its cached
    // decision at an age where the fresh handler has long since refreshed.
    // Guarded on elapsed time, because if the probe somehow landed past the
    // stale TTL the claim below would be meaningless and a pass would be a lie.
    let probe_at = warmed_at + std::time::Duration::from_secs(STALE_PROBE_SECS);
    if let Some(remaining) = probe_at.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(remaining).await;
    }
    let elapsed = warmed_at.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_secs(STALE_PROBE_SECS),
        "probe taken too early to mean anything"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(STALE_TTL_SECS),
        "cannot assert staleness: the fresh handler took {}s, past the {STALE_TTL_SECS}s \
         stale TTL, so the long-TTL handler may legitimately have refreshed",
        elapsed.as_secs()
    );
    let stale = slow
        .execute(&ctx.bob, &sql, None)
        .await
        .expect("stale read must still succeed");
    assert_eq!(
        col_strings(&stale, "ssn"),
        raw,
        "a cached decision must be served for the whole TTL, unaffected by the \
         Ranger-side edit {}s ago",
        elapsed.as_secs()
    );

    // Edge 3 -- the window is BOUNDED. Staleness that never ends is not a cache,
    // it is a missed policy change. Budget = TTL plus slack for the fetch.
    crate::common::eventually_within(
        std::time::Duration::from_secs(STALE_TTL_SECS + 60),
        "the long-TTL handler to expire its cache and apply the mask",
        || async {
            match slow.execute(&ctx.bob, &sql, None).await {
                Ok(b) if col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) => Ok(()),
                Ok(b) => Err(format!("still stale: {:?}", col_strings(&b, "ssn"))),
                Err(e) => Err(format!("query failed: {e}")),
            }
        },
    )
    .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Closing the "documented but only unit-tested" rows of the support matrix
// ─────────────────────────────────────────────────────────────────────────────

/// The four mask types the matrix listed as unit-tested only, asserted against a
/// live Ranger in one pass: `MASK`, `MASK_SHOW_FIRST_4`, `MASK_DATE_SHOW_YEAR`
/// and `CUSTOM`.
///
/// One case rather than four, because they share the whole expensive part (stack,
/// grants, fixture) and differ only in the policy body. Each column carries a
/// different mask type, so a single query proves all four and also proves they do
/// not interfere.
///
/// `MASK_NONE` is deliberately absent: it is an exemption that depends on Ranger
/// policy EVALUATION ORDER, and ordering is a property of the policy set rather
/// than of one policy, so it needs its own case with explicit priorities. It
/// stays unit-tested and the matrix says so.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn remaining_mask_types_apply_live() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    // region 'EU' -> 'XX': MASK maps uppercase to X.
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-full"),
            "region",
            serde_json::json!({"dataMaskType": "MASK"}),
        ))
        .await
        .expect("create MASK policy");
    // ssn '111-11-1111' -> '111-xx-xxxx': first four kept, rest x.
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-first4"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_FIRST_4"}),
        ))
        .await
        .expect("create MASK_SHOW_FIRST_4 policy");
    // signed_on 2021-05-04 -> 2021-01-01: truncated to the year.
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-date"),
            "signed_on",
            serde_json::json!({"dataMaskType": "MASK_DATE_SHOW_YEAR"}),
        ))
        .await
        .expect("create MASK_DATE_SHOW_YEAR policy");
    // email 'a@x' -> 'redacted:x': a portable expression, no Hive UDF.
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-custom"),
            "email",
            serde_json::json!({
                "dataMaskType": "CUSTOM",
                "valueExpr": "concat('redacted:', substr({col}, 3, 1))"
            }),
        ))
        .await
        .expect("create CUSTOM policy");

    let sql =
        format!("SELECT region, ssn, signed_on, email FROM {ORDERS} ORDER BY id");

    let bob = crate::common::eventually("all four mask types to apply", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "region").first().map(String::as_str) == Some("XX") => Ok(b),
            Ok(b) => Err(format!("region not redacted yet: {:?}", col_strings(&b, "region"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;

    assert_eq!(total_rows(&bob), 3, "masking must not drop rows");
    assert_eq!(
        col_strings(&bob, "region"),
        vec!["XX", "XX", "XX"],
        "MASK maps every uppercase letter to X"
    );
    assert_eq!(
        col_strings(&bob, "ssn"),
        vec!["111-xx-xxxx", "222-xx-xxxx", "333-xx-xxxx"],
        "MASK_SHOW_FIRST_4 keeps the first four characters"
    );
    assert_eq!(
        col_strings(&bob, "signed_on"),
        vec!["2021-01-01", "2022-01-01", "2023-01-01"],
        "MASK_DATE_SHOW_YEAR truncates to the year"
    );
    assert_eq!(
        col_strings(&bob, "email"),
        vec!["redacted:x", "redacted:x", "redacted:x"],
        "CUSTOM substitutes the {{col}} placeholder and evaluates the expression"
    );

    // alice is analyst-only, so none of the engineer policies touch her. This is
    // the control that proves the values above are masks and not the fixture.
    let alice = exec_ok(&ctx, &ctx.alice, &sql).await;
    assert_eq!(col_strings(&alice, "region"), vec!["EU", "US", "EU"]);
    assert_eq!(col_strings(&alice, "ssn").first().map(String::as_str), Some("111-11-1111"));
    assert_eq!(col_strings(&alice, "signed_on").first().map(String::as_str), Some("2021-05-04"));
    assert_eq!(col_strings(&alice, "email"), vec!["a@x", "b@x", "c@x"]);
}

/// Precedence: a RESOURCE mask on a column beats a TAG mask on the same column.
///
/// The matrix listed precedence as unit-tested only. The two masks are chosen so
/// the winner is unambiguous: the resource mask shows the last four digits, the
/// tag mask would nullify. `xxx-xx-1111` can only come from the resource rule,
/// and NULL can only come from the tag rule, so the assertion cannot pass under
/// the wrong precedence.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn resource_mask_beats_tag_mask_live() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    set_column_tag(&ctx, "ssn", "PII").await;
    ctx.ranger
        .create_policy(tag_mask_policy(
            &format!("{PREFIX}tag-nullify-pii"),
            "PII",
            serde_json::json!({"dataMaskType": "hive:MASK_NULL"}),
        ))
        .await
        .expect("create tag mask");
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}mask-ssn-last4"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create resource mask");

    let sql = format!("SELECT id, ssn FROM {ORDERS} ORDER BY id");
    let bob = crate::common::eventually("the resource mask to win over the tag mask", || async {
        match ctx.handler.execute(&ctx.bob, &sql, None).await {
            Ok(b) if col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) => Ok(b),
            Ok(b) => Err(format!("got {:?}", col_strings(&b, "ssn"))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(
        col_strings(&bob, "ssn"),
        vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"],
        "the resource mask must win; NULL here would mean the tag mask won"
    );
}

/// `GRANT SELECT ON ALL TABLES IN SCHEMA` covers every table in the namespace
/// with one statement.
///
/// The matrix listed this as unit-tested only, and the unit test asserts the
/// resolved resource shape (table `"*"`). That shape was WRONG once in a way a
/// shape assertion alone would not have caught: `ON ALL` used to map to a
/// namespace-level resource, which parsed, reported success, and conferred
/// nothing on any table. This proves the read actually works, on two tables, one
/// of which is created AFTER the grant.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/access-control-test.sh"]
async fn all_tables_in_schema_grant_covers_the_namespace() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    let second = "sales_wh.ac.orders_extra";
    let _ = ctx
        .handler
        .execute(&ctx.carol, &format!("DROP TABLE IF EXISTS {second}"), None)
        .await;

    // alice cannot read the fixture table yet: ac_setup revoked every test grant.
    assert_denied_but_valid(&ctx, &ctx.alice, &format!("SELECT id FROM {ORDERS}")).await;

    exec_ok(
        &ctx,
        &ctx.carol,
        "GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.ac TO ROLE \"analyst\"",
    )
    .await;

    // Existing table.
    let first = crate::common::eventually("the wildcard grant to enable the existing table", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None).await {
            Ok(b) if total_rows(&b) == 3 => Ok(b),
            Ok(b) => Err(format!("{} rows", total_rows(&b))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(total_rows(&first), 3);

    // A table created AFTER the grant. Ranger has no future-only resource, so the
    // wildcard covers it; this is the documented difference from Snowflake, where
    // ON ALL and ON FUTURE are distinct.
    exec_ok(&ctx, &ctx.carol, &format!("CREATE TABLE {second} (id BIGINT)")).await;
    exec_ok(&ctx, &ctx.carol, &format!("INSERT INTO {second} VALUES (7)")).await;
    let later = crate::common::eventually("the wildcard grant to cover a new table", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {second}"), None).await {
            Ok(b) if total_rows(&b) == 1 => Ok(b),
            Ok(b) => Err(format!("{} rows", total_rows(&b))),
            Err(e) => Err(format!("query failed: {e}")),
        }
    })
    .await;
    assert_eq!(
        col_strings(&later, "id"),
        vec!["7"],
        "a table created after the wildcard grant is covered too"
    );

    let _ = ctx
        .handler
        .execute(
            &ctx.carol,
            "REVOKE SELECT ON ALL TABLES IN SCHEMA sales_wh.ac FROM ROLE \"analyst\"",
            None,
        )
        .await;
    let _ = ctx
        .handler
        .execute(&ctx.carol, &format!("DROP TABLE IF EXISTS {second}"), None)
        .await;
}

/// Revoking one privilege must not destroy another the grantee still holds.
///
/// Ranger permits ONE policy per resource, so every grant on a table lands in
/// the same policy item and the access types union. `WRITE_ACCESS` is a strict
/// superset of `READ_ACCESS`, so revoking INSERT verbatim removed every access
/// type SELECT needs. Reproduced live before the fix: after the write revoke
/// alice had no policy item at all, meaning an admin narrowing write access
/// silently took away read access too.
///
/// The fix records a provenance label per grant and holds back the access types
/// another labelled privilege still requires.
///
/// The write is proven by alice actually inserting a row while she holds INSERT,
/// rather than by `assert_denied_but_valid`, whose admin control would itself
/// insert one and move the row count this test asserts on.
#[tokio::test]
#[ignore]
async fn revoking_write_leaves_an_independent_read_grant_intact() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    assert_denied_but_valid(&ctx, &ctx.alice, &format!("SELECT id FROM {ORDERS}")).await;

    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO ROLE \"analyst\"")).await;
    exec_ok(&ctx, &ctx.carol, &format!("GRANT INSERT ON {ORDERS} TO ROLE \"analyst\"")).await;

    // Both grants live: alice can read, and can write.
    let probe = format!(
        "INSERT INTO {ORDERS} VALUES (91,'EU',1.0,'999-99-9999','z@x',DATE '2021-01-01')"
    );
    crate::common::eventually("both grants to land for alice", || async {
        match ctx.handler.execute(&ctx.alice, &probe, None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("insert failed: {e}")),
        }
    })
    .await;
    let baseline = total_rows(
        &ctx.handler
            .execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None)
            .await
            .expect("alice holds SELECT"),
    );
    assert!(baseline >= 4, "alice's own insert should be visible, got {baseline} rows");

    exec_ok(&ctx, &ctx.carol, &format!("REVOKE INSERT ON {ORDERS} FROM ROLE \"analyst\"")).await;

    // The write stops.
    crate::common::eventually("alice's INSERT to be denied after REVOKE INSERT", || async {
        match ctx.handler.execute(&ctx.alice, &probe, None).await {
            Err(_) => Ok(()),
            Ok(_) => Err("insert still allowed".to_string()),
        }
    })
    .await;

    // The read survives. THIS is the regression: before the fix alice had no
    // policy item at all here and this query failed outright.
    //
    // Deliberately NOT an exact row-count equality. The denial loop above keeps
    // retrying alice's INSERT until it fails, and every attempt made before the
    // revoke propagates succeeds, so the table legitimately grows by an
    // unpredictable amount. What matters is that SELECT still works.
    let after = ctx
        .handler
        .execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None)
        .await
        .expect("REVOKE INSERT must not remove the independent SELECT grant");
    assert!(
        total_rows(&after) >= baseline,
        "alice still holds SELECT; revoking INSERT must not have stripped the read \
         access types (got {} rows, baseline {baseline})",
        total_rows(&after)
    );

    // The control that keeps this honest: revoke must still REVOKE. Without it,
    // holding back every access type would satisfy the assertion above while
    // turning REVOKE into a no-op.
    exec_ok(&ctx, &ctx.carol, &format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"analyst\"")).await;
    crate::common::eventually("the SELECT revoke to take effect", || async {
        match ctx.handler.execute(&ctx.alice, &format!("SELECT id FROM {ORDERS}"), None).await {
            Err(_) => Ok(()),
            Ok(b) => Err(format!("still readable with {} rows", total_rows(&b))),
        }
    })
    .await;

    let _ = ctx
        .handler
        .execute(&ctx.carol, &format!("DELETE FROM {ORDERS} WHERE id = 91"), None)
        .await;
}


/// Access types recorded for `user` on the `polaris` policy whose resource is
/// exactly `{catalog}` + optional `{namespace}` + optional `{table}`.
async fn polaris_access_types_for(
    ctx: &AcCtx,
    user: &str,
    catalog: &str,
    namespace: Option<&str>,
    table: Option<&str>,
) -> Vec<String> {
    let policies = ctx.ranger.get_policies("polaris").await.expect("list polaris policies");
    for p in policies {
        let Some(res) = p.get("resources").and_then(|r| r.as_object()) else {
            continue;
        };
        let val = |k: &str| -> Option<String> {
            res.get(k)?
                .get("values")?
                .as_array()?
                .first()?
                .as_str()
                .map(str::to_string)
        };
        if val("catalog").as_deref() != Some(catalog)
            || val("namespace").as_deref() != namespace
            || val("table").as_deref() != table
        {
            continue;
        }
        let mut out = Vec::new();
        for item in p.get("policyItems").and_then(|v| v.as_array()).into_iter().flatten() {
            let named = item
                .get("users")
                .and_then(|u| u.as_array())
                .is_some_and(|us| us.iter().any(|u| u.as_str() == Some(user)));
            if !named {
                continue;
            }
            for a in item.get("accesses").and_then(|v| v.as_array()).into_iter().flatten() {
                if let Some(t) = a.get("type").and_then(|t| t.as_str()) {
                    out.push(t.to_string());
                }
            }
        }
        out.sort();
        return out;
    }
    Vec::new()
}

/// One `GRANT` on a table writes the namespace visibility it needs, and the
/// table becomes readable with no second statement.
///
/// A table-level grant used to be inert on its own. SQE resolves a table through
/// its catalog provider, which answers only for namespaces its per-namespace
/// probe (`LOAD_NAMESPACE_METADATA`) could load, so without namespace-level
/// `namespace-properties-read` the probe 403s, the namespace is hidden, and
/// planning ends at "table not found" without ever attempting `LOAD_TABLE`. The
/// grant reported success and the grantee still could not read.
///
/// `GRANT` now writes the namespace ancestor alongside the table. The CATALOG
/// level is deliberately still explicit: catalog-wide `namespace-list` exposes
/// sibling namespace NAMES unrelated to the granted table, so auto-adding it
/// would widen the blast radius of every table grant.
///
/// dave is the subject because he belongs to no role. alice and bob inherit
/// wildcard `{namespace: *}` discovery from the quickstart bootstrap, which would
/// make the ancestor grant invisible and this test vacuous.
///
/// This asserts the POLICY SQE writes, not a read by dave, and that limit is
/// forced rather than chosen. A principal who can list a catalog's namespaces
/// while every per-namespace probe 403s wedges `SqeCatalogProvider::schema()` on a
/// current-thread runtime (`#[tokio::test]`'s default): it bridges to async
/// through `runtime_bridge::block_on_compat`, which spawns a thread and joins it,
/// and the join never returns (`pthread_join` / `__ulock_wait`, captured with
/// `sample`). Same re-entrant-`block_on` family as #195. dave is in exactly that
/// state until the Polaris plugin polls, so the FIRST read attempt hangs and
/// `eventually` never gets to retry. That hang is a pre-existing read-path defect,
/// not something this change introduced, and it does not reproduce through the
/// container, whose runtime is multi-threaded.
///
/// What the read assertion would have added is covered instead by an isolated live
/// A/B recorded in `docs/internal/research/2026-08-02-catalog-traversal-gate.md`:
/// with catalog discovery and a table grant but no namespace visibility the read
/// failed `table not found`, and adding ONLY `namespace-properties-read` at
/// `{catalog, namespace}` returned rows. So this test pins that SQE writes exactly
/// that access type at exactly that resource, and the research note pins that the
/// access type is what unblocks the read.
#[tokio::test]
#[ignore]
async fn one_table_grant_writes_the_namespace_it_needs() {
    ac_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Strip BOTH halves so the grant under test is the only thing that can make
    // the table reachable. Leftovers are likely: an earlier run of this test, or
    // manual probing, writes exactly these.
    // Order matters: catalog discovery first. Revoking the schema while discovery
    // is still in place leaves dave able to list namespaces with none visible,
    // which is the state that wedges the read path.
    for stmt in [
        format!("REVOKE SELECT ON {ORDERS} FROM USER \"dave\""),
        "REVOKE USAGE ON DATABASE sales_wh FROM USER \"dave\"".to_string(),
        "REVOKE USAGE ON SCHEMA sales_wh.ac FROM USER \"dave\"".to_string(),
    ] {
        let _ = ctx.handler.execute(&ctx.carol, &stmt, None).await;
    }
    assert!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), None).await.is_empty(),
        "pre-state: dave must hold nothing on the ac namespace, or the ancestor \
         grant under test cannot be observed"
    );
    assert!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), Some("orders"))
            .await
            .is_empty(),
        "pre-state: dave must hold nothing on the orders table"
    );
    // The catalog level too. This assertion exists because its absence let a dirty
    // environment through once: leftover `catalog-list` / `catalog-properties-read`
    // from hand-editing a policy during an investigation survived the revokes
    // above (SQE never grants those, so no REVOKE removes them) and only surfaced
    // as a confusing failure on the post-grant equality check further down.
    let pre_catalog = polaris_access_types_for(&ctx, "dave", "sales_wh", None, None).await;
    assert!(
        pre_catalog.is_empty(),
        "pre-state: dave must hold nothing at the catalog level, found {pre_catalog:?} \
         -- most likely a hand-edited Ranger policy from an earlier investigation"
    );

    // ONE statement, naming only the table. No catalog grant by hand: writing it
    // is the behaviour under test.
    exec_ok(&ctx, &ctx.carol, &format!("GRANT SELECT ON {ORDERS} TO USER \"dave\"")).await;

    // All three levels of v4's SELECT plan landed, each carrying exactly its own
    // access type. Asserted as equality rather than `contains`: writing MORE than
    // the profile specifies is as much a drift from the control plane as less.
    assert_eq!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", None, None).await,
        vec!["namespace-list".to_string()],
        "catalog level: what LIST_NAMESPACES needs, and nothing more"
    );
    let ns = polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), None).await;
    assert_eq!(
        ns,
        vec!["namespace-properties-read".to_string()],
        "namespace level: visibility only, no table access"
    );

    // The table grant itself is unchanged.
    let tbl = polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), Some("orders")).await;
    assert!(
        tbl.contains(&"table-data-read".to_string()),
        "the table half of the plan must still be written, got {tbl:?}"
    );

    // And the payoff: dave reads, having been given one statement beyond catalog
    // discovery, through policies this code wrote.
    //
    // A plain wait rather than the usual `eventually` retry loop, for a specific
    // reason. Until the Polaris plugin polls, dave holds catalog discovery with
    // `ac` still invisible, and a read in that window wedges
    // `SqeCatalogProvider::schema()` (see the doc comment above) -- so the FIRST
    // retry would hang and the loop would never get a second attempt. Waiting out
    // the propagation window means the single read below happens after `ac` became
    // visible. The `timeout` is what keeps this honest: if the wedge is hit anyway
    // the test FAILS with a clear message instead of hanging the suite.
    tokio::time::sleep(std::time::Duration::from_secs(45)).await;
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        ctx.handler.execute(&ctx.dave, &format!("SELECT id FROM {ORDERS}"), None),
    )
    .await
    .expect(
        "dave's read neither succeeded nor failed within 60s: this is the \
         SqeCatalogProvider::schema() wedge, not a policy problem",
    )
    .expect("one GRANT on the table, plus catalog discovery, must make it readable");
    assert_eq!(
        total_rows(&read),
        3,
        "dave must see the fixture's 3 rows through a namespace policy that GRANT \
         wrote for him"
    );

    // Control: the namespace policy confers VISIBILITY, not data. If it leaked
    // read access, the other table in the namespace would be readable too.
    assert!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), Some("orders_sibling"))
            .await
            .is_empty(),
        "no grant was made on the sibling table"
    );

    // Control: revoke still revokes what was granted.
    exec_ok(&ctx, &ctx.carol, &format!("REVOKE SELECT ON {ORDERS} FROM USER \"dave\"")).await;
    let after = polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), Some("orders")).await;
    assert!(
        !after.contains(&"table-data-read".to_string()),
        "REVOKE must remove the table access types, got {after:?}"
    );
    // Documented asymmetry: the namespace policy survives the revoke on purpose,
    // because one namespace policy serves every table granted under it.
    assert_eq!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", Some("ac"), None).await,
        vec!["namespace-properties-read".to_string()],
        "namespace visibility is deliberately NOT released by a table revoke"
    );
    assert_eq!(
        polaris_access_types_for(&ctx, "dave", "sales_wh", None, None).await,
        vec!["namespace-list".to_string()],
        "nor is catalog discovery: both are shared with every other grant in the \
         catalog, so a table revoke must not strip them"
    );

    // Teardown drops CATALOG discovery and deliberately leaves the namespace
    // visibility behind.
    //
    // Revoking the schema too would leave dave holding catalog discovery with
    // every namespace probe denied, which is the state that wedges
    // `SqeCatalogProvider::schema()` (see this test's doc comment). Polaris
    // propagates the two revokes independently over 5 to 30s, so a teardown that
    // issued both would pass through that state for an unbounded window, and any
    // later test doing a read as dave would hang instead of failing.
    // `role_grant_and_user_grant_both_apply` does exactly that.
    //
    // Without catalog discovery dave cannot list namespaces at all, which denies
    // cleanly and promptly. The residue is inert visibility, the same asymmetry
    // this test asserts is deliberate.
    let _ = ctx
        .handler
        .execute(&ctx.carol, "REVOKE USAGE ON DATABASE sales_wh FROM USER \"dave\"", None)
        .await;
}
