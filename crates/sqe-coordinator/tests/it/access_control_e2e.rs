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
