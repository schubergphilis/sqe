//! Object-level access control for **Spark**, against the same Polaris catalog
//! and the same Ranger services as the SQE suite.
//!
//! Grants are written through SQE's `GRANT` statement and asserted through Spark.
//! One grant path, two engines: that is the parity being tested.
//!
//! Object level is decided by POLARIS. Spark reaches it by presenting a per-user
//! Keycloak JWT to the Iceberg REST catalog, so Polaris authorizes the end user
//! rather than a service account. Kyuubi defers via the blanket `policyType-0`
//! item on the frontend service (see
//! `RangerAdmin::grant_object_level_defer_item`).
//!
//! Every denial assertion names the TIER. A Kyuubi denial where Polaris was
//! expected means the defer item went missing and the test never reached the tier
//! under test, so it must fail rather than quietly pass.
//!
//! Run with:
//!
//! ```text
//! scripts/spark-access-control-test.sh
//! ```

use std::time::Duration;

use crate::access_control_e2e::{ac_setup, exec_ok, AcCtx, ORDERS};
use crate::common::spark_runner::{
    kyuubi_service_in_container, spark_sql, DenialTier, KYUUBI_SERVICE, SPARK_CATALOG,
};

/// Skip-or-run gate, same contract as the SQE suite: `#[ignore]` alone is not
/// enough because `scripts/integration-test.sh` force-runs ignored tests against
/// a different stack.
macro_rules! spark_gate {
    () => {
        if !crate::common::ac_enabled() {
            eprintln!(
                "skipping spark_access_control_e2e: set SQE_AC_E2E=1 \
                 (use scripts/spark-access-control-test.sh)"
            );
            return;
        }
    };
}

/// Polaris polls Ranger, so a grant or revoke is not visible instantly. The SQE
/// suite's reasoning applies here unchanged, plus a fresh JVM per query.
const SPARK_BUDGET: Duration = Duration::from_secs(180);

/// `sales_wh.ac.orders` as Spark addresses it: the suite's own catalog, then the
/// namespace, then the table.
fn spark_orders() -> String {
    let ns_and_table = ORDERS
        .strip_prefix("sales_wh.")
        .expect("ORDERS is qualified with the sales_wh warehouse");
    format!("{SPARK_CATALOG}.{ns_and_table}")
}

/// Prove the statement is VALID before asserting anyone is denied it, then wait
/// for the denial to propagate.
///
/// Without the control, a typo in a table name is indistinguishable from a
/// denial, and the test passes for the wrong reason. carol holds the wildcard
/// admin grants, so a failure for carol means the fixture is broken.
async fn assert_spark_denied_but_valid(ctx: &AcCtx, who: &str, sql: &str, op: &str) {
    let control = spark_sql(&ctx.carol, "carol", sql).await;
    control.expect_ok(&format!(
        "control: `{sql}` must succeed as carol, else the test cannot tell a \
         denial from an invalid statement"
    ));

    let session = match who {
        "alice" => &ctx.alice,
        "bob" => &ctx.bob,
        "dave" => &ctx.dave,
        other => panic!("unknown fixture user {other}"),
    };
    crate::common::eventually_within(
        SPARK_BUDGET,
        &format!("`{sql}` to be denied for {who} in Spark"),
        || async {
            let out = spark_sql(session, who, sql).await;
            match &out.tier {
                DenialTier::Polaris { op: got, .. } if got == op => Ok(()),
                DenialTier::None => Err(format!("still allowed, rows={:?}", out.rows)),
                other => Err(format!("wrong tier or op: {other:?}")),
            }
        },
    )
    .await;
}

/// Wait until `who` can read `sql` through Spark, returning the rows.
async fn spark_eventually_ok(ctx: &AcCtx, who: &str, sql: &str) -> Vec<Vec<String>> {
    let session = match who {
        "alice" => &ctx.alice,
        "bob" => &ctx.bob,
        "dave" => &ctx.dave,
        "carol" => &ctx.carol,
        other => panic!("unknown fixture user {other}"),
    };
    let _ = ctx;
    crate::common::eventually_within(
        SPARK_BUDGET,
        &format!("`{sql}` to be allowed for {who} in Spark"),
        || async {
            let out = spark_sql(session, who, sql).await;
            match &out.tier {
                DenialTier::None => Ok(out.rows.clone()),
                other => Err(format!("{other:?}")),
            }
        },
    )
    .await
}

// ─────────────────────────────────────────────────────────────────────────────
// Object level: the grant enables, the revoke disables, and Polaris is the one
// deciding
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_denied_before_any_grant() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    assert_spark_denied_but_valid(
        &ctx,
        "alice",
        &format!("SELECT region FROM {}", spark_orders()),
        "LOAD_TABLE",
    )
    .await;
}

/// The guard on the defer item.
///
/// The item grants `select` on `database=*`/`table=*`/`column=*` to group
/// `public`. Read out of context that says "everyone may select everything", and
/// an operator who does not know why will either delete it (breaking Spark) or
/// copy it somewhere it does not belong. It is safe only because Polaris still
/// decides, and that is what this proves: with the item in place and no Polaris
/// grant, the read is still refused, BY POLARIS.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn object_denial_survives_the_frontend_defer_policy() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Assert the precondition against the service KYUUBI READS, which is the
    // quickstart's, NOT the test-owned one SQE reads. Checking the wrong service
    // is the failure mode this whole block exists to rule out: it would pass
    // while Kyuubi, reading a service with no defer item, did the denying.
    let in_container = kyuubi_service_in_container()
        .await
        .expect("read ranger.plugin.spark.service.name from the spark container");
    assert_eq!(
        in_container, KYUUBI_SERVICE,
        "the Spark container's Ranger plugin names service `{in_container}`, but this \
         suite checks `{KYUUBI_SERVICE}`. One of them is wrong, and until they agree \
         no Spark assertion here means what it says."
    );
    let present = ctx
        .ranger
        .object_level_defer_item_present(KYUUBI_SERVICE)
        .await
        .expect("read the defer item");
    assert!(
        present,
        "the blanket policyType-0 item for group `public` is absent from `{KYUUBI_SERVICE}`, \
         so this test would prove nothing: Kyuubi would refuse before Polaris was consulted"
    );

    let out = spark_sql(&ctx.bob, "bob", &format!("SELECT * FROM {}", spark_orders())).await;
    out.expect_polaris_denial(
        "LOAD_TABLE",
        "the defer item must grant no data access of its own",
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_grant_select_to_role_enables_exact_rows() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;

    // bob is an engineer; the fixture seeds exactly 3 rows.
    let rows = spark_eventually_ok(
        &ctx,
        "bob",
        &format!("SELECT count(*) FROM {}", spark_orders()),
    )
    .await;
    assert_eq!(rows, vec![vec!["3".to_string()]], "row count after the grant");

    // alice is an analyst only, so the same grant must not admit her.
    let out = spark_sql(
        &ctx.alice,
        "alice",
        &format!("SELECT count(*) FROM {}", spark_orders()),
    )
    .await;
    out.expect_polaris_denial("LOAD_TABLE", "a grant to engineer must not admit an analyst");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_role_grant_and_user_grant_both_apply() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // dave holds no role at all, so a user grant is the only thing that can
    // admit him. bob comes in through the role.
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO USER \"dave\""),
    )
    .await;

    for who in ["bob", "dave"] {
        let rows = spark_eventually_ok(
            &ctx,
            who,
            &format!("SELECT count(*) FROM {}", spark_orders()),
        )
        .await;
        assert_eq!(rows, vec![vec!["3".to_string()]], "{who} row count");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_revoke_disables_access() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    let rows = spark_eventually_ok(
        &ctx,
        "bob",
        &format!("SELECT count(*) FROM {}", spark_orders()),
    )
    .await;
    assert_eq!(rows, vec![vec!["3".to_string()]], "read before the revoke");

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("REVOKE SELECT ON {ORDERS} FROM ROLE \"engineer\""),
    )
    .await;
    assert_spark_denied_but_valid(
        &ctx,
        "bob",
        &format!("SELECT count(*) FROM {}", spark_orders()),
        "LOAD_TABLE",
    )
    .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Write path
// ─────────────────────────────────────────────────────────────────────────────

/// Read the row count through SQE as carol, so the count itself is never
/// subject to the grant under test.
async fn row_count_via_sqe(ctx: &AcCtx) -> usize {
    let batches = exec_ok(ctx, &ctx.carol, &format!("SELECT id FROM {ORDERS}")).await;
    batches.iter().map(|b| b.num_rows()).sum()
}

/// A read grant must not confer a write.
///
/// Polaris refuses at the snapshot COMMIT (`ADD_TABLE_SNAPSHOT`), not at
/// `LOAD_TABLE`, so the data may already be staged when the refusal lands. The
/// row count is therefore asserted as well: authorization is only real if nothing
/// landed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_write_privileges_are_separate_from_read() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    spark_eventually_ok(
        &ctx,
        "bob",
        &format!("SELECT count(*) FROM {}", spark_orders()),
    )
    .await;

    let before = row_count_via_sqe(&ctx).await;
    let insert = format!(
        "INSERT INTO {} VALUES \
         (99,'EU',1.0,'999-99-9999','z@x',DATE '2024-01-01')",
        spark_orders()
    );
    let out = spark_sql(&ctx.bob, "bob", &insert).await;
    out.expect_polaris_denial(
        "ADD_TABLE_SNAPSHOT",
        "a read grant must not confer a write",
    );
    assert_eq!(
        before,
        row_count_via_sqe(&ctx).await,
        "a refused INSERT changed the table: the denial is not real"
    );

    // Granting the write admits it.
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT INSERT ON {ORDERS} TO ROLE \"engineer\""),
    )
    .await;
    let bob = &ctx.bob;
    crate::common::eventually_within(SPARK_BUDGET, "bob to write after the INSERT grant", || {
        let insert = insert.clone();
        async move {
            let out = spark_sql(bob, "bob", &insert).await;
            match &out.tier {
                DenialTier::None => Ok(()),
                other => Err(format!("{other:?}")),
            }
        }
    })
    .await;
    // The count is read through SQE, whose TableMetadataCache has a 30s TTL, so
    // the new snapshot is not visible the instant the write commits. Poll rather
    // than assert once: a bare assert here read the pre-write count and reported
    // "the INSERT did not land" for a write that had.
    crate::common::eventually_within(SPARK_BUDGET, "the granted INSERT to be visible", || async {
        let now = row_count_via_sqe(&ctx).await;
        if now == before + 1 {
            Ok(())
        } else {
            Err(format!("row count {now}, expected {}", before + 1))
        }
    })
    .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// The two-tier trust split
// ─────────────────────────────────────────────────────────────────────────────

/// Documents a real property of the Spark path, not a defect to fix here.
///
/// The object tier verifies a JWT signature. The fine-grained tier trusts
/// `HADOOP_USER_NAME`, an unauthenticated string the client chooses. A mismatched
/// pair therefore gets one user's OBJECT rights and another's MASKS.
///
/// In a deployment the platform controls `spark-submit`, so the assertion is that
/// the object tier follows the TOKEN and ignores the asserted name. Closing the
/// split means running Spark behind a Kyuubi server with real authentication,
/// which is separate work.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn mismatched_identity_reveals_the_two_tier_trust_split() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Grant to analyst only. alice is an analyst; bob is an analyst too, so pick
    // a grantee only alice matches: a USER grant to alice.
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("GRANT SELECT ON {ORDERS} TO USER \"alice\""),
    )
    .await;
    let sql = format!("SELECT count(*) FROM {}", spark_orders());

    // alice's token, alice asserted: allowed.
    let alice = &ctx.alice;
    crate::common::eventually_within(SPARK_BUDGET, "alice to read with her own token", || {
        let sql = sql.clone();
        async move {
            let out = spark_sql(alice, "alice", &sql).await;
            match &out.tier {
                DenialTier::None => Ok(()),
                other => Err(format!("{other:?}")),
            }
        }
    })
    .await;

    // alice's TOKEN with bob asserted to Kyuubi: still allowed, because the
    // object tier follows the token. The asserted name buys no object rights,
    // and confers no protection either.
    let mixed = spark_sql(&ctx.alice, "bob", &sql).await;
    mixed.expect_ok(
        "alice's token with HADOOP_USER_NAME=bob: the object tier follows the \
         TOKEN, so the asserted name does not change what may be loaded",
    );

    // bob's token with alice asserted: refused, again on the token, even though
    // the asserted name is the granted user.
    let reversed = spark_sql(&ctx.bob, "alice", &sql).await;
    reversed.expect_polaris_denial(
        "LOAD_TABLE",
        "bob's token must be refused even while asserting alice's name",
    );
}
