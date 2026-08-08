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

/// Add a Ranger DENY item for role `engineer` to the policy SQE wrote for ORDERS.
///
/// Ranger keeps ONE policy per resource, so deny precedence has to be expressed by
/// editing that policy rather than adding a second one. Targets ORDERS rather than
/// the audit table the SQE suite uses, because the Spark catalog is bound to the
/// `sales_wh` warehouse.
///
/// `analyst` is deliberately NOT denied: alice stays as the control proving the
/// table is still readable, so a denial cannot be confused with a broken fixture.
async fn add_deny_item_to_orders_policy(ctx: &AcCtx) {
    let policies = ctx
        .ranger
        .get_policies("polaris")
        .await
        .expect("list polaris policies");
    let mut target = policies
        .into_iter()
        .find(|p| {
            p["resources"]["table"]["values"] == serde_json::json!(["orders"])
                && p["resources"]["namespace"]["values"] == serde_json::json!(["ac"])
                && p["resources"]["catalog"]["values"] == serde_json::json!(["sales_wh"])
        })
        .expect("SQE's GRANT must have created a polaris policy for sales_wh.ac.orders");
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
        .expect("add denyPolicyItems to the orders policy");
}

/// A Ranger DENY beats an ALLOW on the Spark path too.
///
/// Both users are granted, then `engineer` is denied. bob (engineer) must lose
/// access while alice (analyst only) keeps it, which is what proves the deny is
/// precedence rather than the grant having failed.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_ranger_deny_overrides_allow() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    for role in ["analyst", "engineer"] {
        exec_ok(
            &ctx,
            &ctx.carol,
            &format!("GRANT SELECT ON {ORDERS} TO ROLE \"{role}\""),
        )
        .await;
    }
    let sql = format!("SELECT count(*) FROM {}", spark_orders());
    spark_eventually_ok(&ctx, "bob", &sql).await;

    add_deny_item_to_orders_policy(&ctx).await;

    // NOT assert_spark_denied_but_valid: that controls with carol, and carol is a
    // MEMBER of engineer, so the deny denies the control too and the helper reports a
    // broken fixture. alice is the control here, asserted straight after.
    crate::common::eventually_within(
        SPARK_BUDGET,
        "bob to be denied once the deny item propagates",
        || async {
            let out = spark_sql(&ctx.bob, "bob", &sql).await;
            match &out.tier {
                DenialTier::Polaris { op, .. } if op == "LOAD_TABLE" => Ok(()),
                DenialTier::None => Err(format!("still allowed: {:?}", out.rows)),
                other => Err(format!("wrong tier: {other:?}")),
            }
        },
    )
    .await;

    // alice still reads, so the deny is precedence over an allow rather than the
    // whole grant having gone away. She is analyst-only, so the engineer deny misses
    // her; that asymmetry is the entire control.
    let rows = spark_eventually_ok(&ctx, "alice", &sql).await;
    assert_eq!(
        rows,
        vec![vec!["3".to_string()]],
        "an analyst-only user must be unaffected by a deny on engineer"
    );
}

/// A schema-wide grant covers a table it never names, in Spark as in SQE.
///
/// The wildcard is what makes one grant cover tables created later, so a Spark path
/// that only honored explicitly named tables would silently under-grant.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn spark_all_tables_in_schema_grant_covers_the_namespace() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // A second table in the same namespace, created AFTER nothing is granted.
    let extra = format!("{ORDERS}_extra");
    exec_ok(&ctx, &ctx.carol, &format!("DROP TABLE IF EXISTS {extra}")).await;
    exec_ok(
        &ctx,
        &ctx.carol,
        &format!("CREATE TABLE {extra} (id BIGINT, note VARCHAR)"),
    )
    .await;
    exec_ok(&ctx, &ctx.carol, &format!("INSERT INTO {extra} VALUES (1,'a')")).await;

    exec_ok(
        &ctx,
        &ctx.carol,
        "GRANT SELECT ON ALL TABLES IN SCHEMA sales_wh.ac TO ROLE \"engineer\"",
    )
    .await;

    // The grant names no table, yet both are readable through Spark.
    let extra_in_spark = format!("{}_extra", spark_orders());
    for table in [spark_orders(), extra_in_spark] {
        let rows = spark_eventually_ok(&ctx, "bob", &format!("SELECT count(*) FROM {table}")).await;
        assert_eq!(rows.len(), 1, "{table} must be readable under the wildcard grant");
    }
}

/// SECURITY GAP, pinned: a service-account catalog left in the Spark session
/// defeats per-user identity entirely.
///
/// Handing Spark a per-user token governs ONLY the catalog that token is attached
/// to. Every other catalog in the session is a separate identity, and the user picks
/// which one by choosing the catalog name. `spark-defaults.conf` defines
/// `spark.sql.catalog.sales_wh.credential = root:...`, so a session that ADDS a
/// per-user catalog still carries a root-credentialed alias for the same warehouse.
///
/// Measured, and worth stating plainly: bob is denied on the table through his own
/// catalog and reads it through the other alias in the same breath. No view, no
/// trick, just a different name for the same table.
///
/// A session CANNOT defend itself. Overriding `spark.sql.catalog.sales_wh.token`
/// with the user's JWT does not help, because Iceberg prefers `credential` when both
/// are set (measured). The only fix is to not configure the service-account catalog
/// at all, which is a deployment change rather than an engine one.
///
/// The assertion encodes today's behaviour so the hazard is executable rather than
/// prose. When the quickstart drops the `credential` line, the second half of this
/// test starts failing, which is the signal to invert it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn a_service_account_catalog_in_the_session_defeats_per_user_identity() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Through bob's OWN catalog: denied, because nothing is granted.
    let own = spark_sql(
        &ctx.bob,
        "bob",
        &format!("SELECT count(*) FROM {}", spark_orders()),
    )
    .await;
    own.expect_polaris_denial("LOAD_TABLE", "bob holds no grant on the fixture table");

    // The SAME table through the service-account alias from spark-defaults.conf,
    // in the SAME session. This succeeds, which is the gap.
    let via_alias = spark_sql(
        &ctx.bob,
        "bob",
        &format!("SELECT count(*) FROM {ORDERS}"),
    )
    .await;
    let rows = via_alias.expect_ok(
        "EXPECTED the documented gap: a root-credentialed `sales_wh` catalog is \
         configured in spark-defaults.conf, so naming it reads as the service \
         account. If this now DENIES, the service-account catalog is gone from the \
         session and the gap is closed: invert this assertion and update the \
         access-control matrix.",
    );
    assert_eq!(
        rows,
        &vec![vec!["3".to_string()]],
        "the alias returns the real row count, so it is genuinely reading the data \
         bob was just denied"
    );
}
