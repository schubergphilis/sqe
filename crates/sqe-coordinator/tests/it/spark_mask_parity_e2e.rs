//! Cross-engine parity for the fine-grained tier: ONE policy, TWO engines, output
//! compared directly.
//!
//! Phase 1 (`spark_access_control_e2e`) proved Spark is subject to the same
//! OBJECT-level gates. These assert the other half: a column mask or row filter
//! written once into the shared frontend service produces the same result whether
//! SQE's plan rewriter or Kyuubi's `RangerSparkExtension` applied it.
//!
//! The assertion is a direct comparison of the two engines' rows, not "each engine
//! masked something". Per-engine checks pass while the engines disagree, which is
//! the whole failure mode worth catching: a policy that renders `xxx-xx-1111` in
//! SQE and `nnnUnnU1111` in Spark is not one policy, it is two.
//!
//! Both engines are pointed at the TEST-OWNED frontend service. SQE reads it by
//! config; Kyuubi is redirected to it per invocation, because the container names
//! the quickstart's `query` and writing mask policies there would change the bundle
//! the demo's `parity-test.sh` cross-compares against.
//!
//! Run with:
//!
//! ```text
//! scripts/spark-access-control-test.sh
//! ```

use crate::access_control_e2e::{
    ac_setup, grant_read_to_both_roles, hive_mask_policy, hive_rowfilter_policy, total_rows,
    AcCtx, ORDERS,
};
use crate::common::ranger_fixture::{HIVE_SERVICE, PREFIX};
use crate::common::spark_runner::{spark_sql_on_service, SPARK_CATALOG};

macro_rules! spark_gate {
    () => {
        if !crate::common::ac_enabled() {
            eprintln!(
                "skipping spark_mask_parity_e2e: set SQE_AC_E2E=1 \
                 (use scripts/spark-access-control-test.sh)"
            );
            return;
        }
    };
}

/// The portable CUSTOM mask expression.
///
/// `concat` and `substr` are built-ins in BOTH DataFusion and Spark, so each engine
/// injects the same expression verbatim and both render `xxx-xx-1111`. The named
/// `MASK_SHOW_LAST_4` type is NOT portable, which
/// `a_named_mask_type_is_not_byte_portable` pins.
const PORTABLE_SSN_MASK: &str = "concat('xxx-xx-', substr({col},8,4))";

fn spark_orders() -> String {
    let ns_and_table = ORDERS
        .strip_prefix("sales_wh.")
        .expect("ORDERS is qualified with the sales_wh warehouse");
    format!("{SPARK_CATALOG}.{ns_and_table}")
}

/// Run the same projection through both engines as `who`, and return
/// (sqe_rows, spark_rows) with one string per cell.
async fn both_engines(
    ctx: &AcCtx,
    who: &str,
    projection: &str,
    order_by: &str,
) -> (Vec<Vec<String>>, Vec<Vec<String>>) {
    let session = match who {
        "alice" => &ctx.alice,
        "bob" => &ctx.bob,
        "carol" => &ctx.carol,
        other => panic!("unknown fixture user {other}"),
    };

    let sqe_sql = format!("SELECT {projection} FROM {ORDERS} ORDER BY {order_by}");
    let batches = ctx
        .handler
        .execute(session, &sqe_sql, None)
        .await
        .unwrap_or_else(|e| panic!("[sqe/{who}] {sqe_sql} failed: {e}"));
    let sqe_rows = {
        let mut out = Vec::with_capacity(total_rows(&batches));
        for batch in &batches {
            for row in 0..batch.num_rows() {
                out.push(
                    (0..batch.num_columns())
                        .map(|c| crate::common::fmt_val(batch.column(c).as_ref(), row))
                        .collect(),
                );
            }
        }
        out
    };

    let spark_sql_text = format!(
        "SELECT {projection} FROM {} ORDER BY {order_by}",
        spark_orders()
    );
    let out = spark_sql_on_service(session, who, HIVE_SERVICE, &spark_sql_text).await;
    let spark_rows = out
        .expect_ok(&format!("[spark/{who}] {spark_sql_text}"))
        .clone();

    (sqe_rows, spark_rows)
}

/// Wait until BOTH engines agree, then hand back the agreed rows.
///
/// The two engines refresh policy on independent schedules: SQE has its own cache
/// TTL, Kyuubi downloads a fresh bundle per invocation. A single-shot comparison
/// straight after creating a policy compares one engine's new answer with the
/// other's old one and reports a parity failure that is really a race.
async fn agreed_rows(
    ctx: &AcCtx,
    who: &str,
    projection: &str,
    order_by: &str,
    settled: impl Fn(&[Vec<String>]) -> bool + Copy,
) -> Vec<Vec<String>> {
    crate::common::eventually_within(
        std::time::Duration::from_secs(180),
        &format!("both engines to agree on `{projection}` for {who}"),
        || async {
            let (sqe, spark) = both_engines(ctx, who, projection, order_by).await;
            if !settled(&sqe) {
                return Err(format!("sqe has not applied the policy yet: {sqe:?}"));
            }
            if !settled(&spark) {
                return Err(format!("spark has not applied the policy yet: {spark:?}"));
            }
            if sqe != spark {
                return Err(format!("engines disagree: sqe={sqe:?} spark={spark:?}"));
            }
            Ok(sqe)
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn column_mask_is_byte_identical_across_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}parity-mask-ssn"),
            "ssn",
            serde_json::json!({
                "dataMaskType": "CUSTOM",
                "valueExpr": PORTABLE_SSN_MASK,
            }),
        ))
        .await
        .expect("create the portable ssn mask");

    // bob is an engineer, so the mask applies to him.
    let rows = agreed_rows(&ctx, "bob", "id, ssn", "id", |rows| {
        rows.len() == 3 && rows.iter().all(|r| r.get(1).is_some_and(|v| v.starts_with("xxx-xx-")))
    })
    .await;

    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "xxx-xx-1111".to_string()],
            vec!["2".to_string(), "xxx-xx-2222".to_string()],
            vec!["3".to_string(), "xxx-xx-3333".to_string()],
        ],
        "one policy must render identically in both engines"
    );

    // Masking must not silently drop rows, and the raw value must be gone. A mask
    // that returned nothing would satisfy an equality check on two empty results.
    let flat = rows.concat().join(" ");
    assert!(
        !flat.contains("111-11-1111"),
        "the raw ssn leaked: {flat}"
    );
}

/// alice is an analyst and NOT an engineer, so the same policy must leave her
/// unmasked. Without this, a mask applied to everyone would pass the parity test.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn an_unmasked_role_is_unmasked_in_both_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}parity-mask-ssn"),
            "ssn",
            serde_json::json!({
                "dataMaskType": "CUSTOM",
                "valueExpr": PORTABLE_SSN_MASK,
            }),
        ))
        .await
        .expect("create the portable ssn mask");

    let rows = agreed_rows(&ctx, "alice", "id, ssn", "id", |rows| {
        rows.len() == 3 && rows.iter().all(|r| r.get(1).is_some_and(|v| v.contains("-11-") || !v.starts_with("xxx")))
    })
    .await;

    assert_eq!(
        rows.first().and_then(|r| r.get(1)).map(String::as_str),
        Some("111-11-1111"),
        "an analyst is not in the masked role and must see the raw value in BOTH engines"
    );
}

/// Row-filter parity.
///
/// `region` is PROJECTED deliberately. Kyuubi on Spark 3.5 throws
/// `MISSING_ATTRIBUTES` (Kyuubi #6889) when a row filter references a column the
/// query does not select, so an unprojected filter column is a Kyuubi bug rather
/// than a parity result. The runner classifies that separately for exactly this
/// reason.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn row_filter_returns_identical_rows_across_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_rowfilter_policy(
            &format!("{PREFIX}parity-rowfilter"),
            "region = 'EU'",
        ))
        .await
        .expect("create the region row filter");

    let rows = agreed_rows(&ctx, "bob", "id, region", "id", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.get(1).is_some_and(|v| v == "EU"))
    })
    .await;

    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "EU".to_string()],
            vec!["3".to_string(), "EU".to_string()],
        ],
        "the same row filter must select the same rows in both engines"
    );

    // The unfiltered user still sees everything, so the filter is not a global
    // truncation that would trivially agree.
    let (sqe_all, spark_all) = both_engines(&ctx, "alice", "id, region", "id").await;
    assert_eq!(sqe_all.len(), 3, "alice is unfiltered in SQE");
    assert_eq!(spark_all.len(), 3, "alice is unfiltered in Spark");
}

/// A DOCUMENTED DIVERGENCE, asserted so it cannot rot silently.
///
/// Ranger's named `MASK_SHOW_LAST_4` is not portable: SQE honors the servicedef
/// transformer and renders `xxx-xx-1111`, while Kyuubi ignores it and applies its
/// own mask characters (`nnnUnnU1111`). Both hide the raw value and both expose the
/// last four, so the SEMANTICS agree and only the rendering differs.
///
/// A failure here is not necessarily a regression. If the two outputs become equal,
/// Kyuubi started honoring the transformer, which is good news: drop this test and
/// update the access-control matrix, because named types would then be portable.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn a_named_mask_type_is_not_byte_portable() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}parity-named-mask"),
            "ssn",
            serde_json::json!({"dataMaskType": "MASK_SHOW_LAST_4"}),
        ))
        .await
        .expect("create the named show-last-4 mask");

    // Wait for each engine to apply the mask on its own schedule, without
    // requiring them to agree.
    let masked = |rows: &[Vec<String>]| {
        rows.len() == 3
            && rows.iter().all(|r| {
                r.get(1)
                    .is_some_and(|v| v.ends_with("1111") || v.ends_with("2222") || v.ends_with("3333"))
                    && !r.get(1).is_some_and(|v| v.starts_with("111-11"))
            })
    };
    let (sqe, spark) = crate::common::eventually_within(
        std::time::Duration::from_secs(180),
        "both engines to apply the named mask",
        || async {
            let (sqe, spark) = both_engines(&ctx, "bob", "id, ssn", "id").await;
            if masked(&sqe) && masked(&spark) {
                Ok((sqe, spark))
            } else {
                Err(format!("sqe={sqe:?} spark={spark:?}"))
            }
        },
    )
    .await;

    // Semantics agree: raw hidden, last four visible in both.
    for (label, rows) in [("sqe", &sqe), ("spark", &spark)] {
        let flat = rows.concat().join(" ");
        assert!(!flat.contains("111-11-1111"), "{label} leaked the raw ssn: {flat}");
        assert!(flat.contains("1111"), "{label} hid the last four: {flat}");
    }

    // Rendering does NOT agree, which is the divergence being recorded.
    assert_ne!(
        sqe, spark,
        "the named mask type rendered identically in both engines. If Kyuubi now \
         honors the servicedef transformer that is an improvement: delete this test \
         and update the `named Ranger mask types` row in the access-control matrix."
    );
    assert_eq!(
        col_strings_like(&sqe, 1),
        vec!["xxx-xx-1111", "xxx-xx-2222", "xxx-xx-3333"],
        "SQE renders the servicedef transformer's mask characters"
    );
}

/// Column `i` of every row, as `&str`, for a readable assertion.
fn col_strings_like(rows: &[Vec<String>], i: usize) -> Vec<&str> {
    rows.iter().map(|r| r[i].as_str()).collect()
}
