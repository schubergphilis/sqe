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
    ac_setup, col_strings, grant_read_to_both_roles, hive_mask_policy, hive_rowfilter_policy,
    total_rows, AcCtx, ORDERS,
};
use crate::common::ranger_fixture::{HIVE_SERVICE, PREFIX};
use crate::common::spark_runner::{spark_sql_on_service, DenialTier, SPARK_CATALOG};

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
) -> Result<(Vec<Vec<String>>, Vec<Vec<String>>), String> {
    let session = match who {
        "alice" => &ctx.alice,
        "bob" => &ctx.bob,
        "carol" => &ctx.carol,
        other => panic!("unknown fixture user {other}"),
    };

    let sqe_sql = format!("SELECT {projection} FROM {ORDERS} ORDER BY {order_by}");
    let batches = match ctx.handler.execute(session, &sqe_sql, None).await {
        Ok(b) => b,
        // Retryable, not fatal. After a schema change SQE can still be serving a
        // cached schema (30s metadata TTL) and answer "No field named ...". Panicking
        // here defeated the retry the caller wraps this in.
        Err(e) => return Err(format!("[sqe/{who}] {sqe_sql} failed: {e}")),
    };
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
    if out.tier != DenialTier::None {
        return Err(format!("[spark/{who}] {spark_sql_text}: {:?}", out.tier));
    }
    Ok((sqe_rows, out.rows))
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
            let (sqe, spark) = both_engines(ctx, who, projection, order_by).await?;
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
    let (sqe_all, spark_all) = both_engines(&ctx, "alice", "id, region", "id")
        .await
        .expect("alice's unfiltered read");
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
            let (sqe, spark) = both_engines(&ctx, "bob", "id, ssn", "id").await?;
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

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2b: tag associations projected into Ranger's tag store
// ─────────────────────────────────────────────────────────────────────────────

/// A projector that always fails, so the rollback path can be asserted.
///
/// A live Ranger cannot be made to fail on demand without breaking every other
/// test sharing the stack, and the rollback is the whole reason the projection is
/// safe to enable. So it is injected.
struct AlwaysFailingProjector;

#[async_trait::async_trait]
impl sqe_policy::tag_projector::TagProjector for AlwaysFailingProjector {
    async fn project(
        &self,
        _table: &sqe_policy::tag_projector::TagTableKey,
        _previous: &sqe_policy::tag_projector::ColumnTags,
        _tags: &sqe_policy::tag_projector::ColumnTags,
    ) -> sqe_core::Result<()> {
        Err(sqe_core::SqeError::Execution(
            "injected projection failure".to_string(),
        ))
    }
    fn enabled(&self) -> bool {
        true
    }
}

/// When the Ranger projection fails, `SET TAG` must leave the Iceberg property
/// UNCHANGED and fail.
///
/// Keeping the property would mean SQE masks the column while Spark returns it raw,
/// which is exactly the fail-open the projection exists to close, and it would be
/// invisible because the statement reported success.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak; run scripts/spark-access-control-test.sh"]
async fn a_failed_projection_rolls_back_the_tag() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;

    // Baseline: whatever tags the table carries before the attempt.
    let before = ctx
        .handler
        .execute(&ctx.carol, &format!("SHOW TAGS ON {ORDERS}"), None)
        .await
        .expect("SHOW TAGS before");
    let before_rows = total_rows(&before);

    // A second handler differing in exactly one thing: the projector fails.
    let (failing, _cache) = crate::common::setup_ranger_handler_sharing(
        Some(ctx.cache.clone()),
        |_cfg| {},
    )
    .await;
    let failing = failing.with_tag_projector(std::sync::Arc::new(AlwaysFailingProjector));

    let err = failing
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn SET TAG rollback_probe = 'true'"),
            None,
        )
        .await
        .expect_err("the statement must fail when the projection fails");
    let msg = err.to_string();
    assert!(
        msg.contains("rolled back") || msg.contains("inconsistent"),
        "the error must say what happened to the tag, got: {msg}"
    );

    // The property must be untouched. A leftover tag here is the fail-open.
    let after = ctx
        .handler
        .execute(&ctx.carol, &format!("SHOW TAGS ON {ORDERS}"), None)
        .await
        .expect("SHOW TAGS after");
    assert_eq!(
        before_rows,
        total_rows(&after),
        "a failed projection left the tag behind, so SQE would mask a column Spark \
         returns raw: exactly the gap the projector exists to close"
    );
}

/// A tag mask policy on the test-owned TAG service.
///
/// The mask type MUST be component-qualified (`hive:CUSTOM`). Ranger's tag
/// servicedef aggregates each component's mask vocabulary rather than defining bare
/// names, and a bare `CUSTOM` is refused with
/// `CUSTOM: is not a valid datamask-type ... service='tag'`.
fn tag_mask_policy_portable(name: &str, tag: &str) -> serde_json::Value {
    serde_json::json!({
        "service": crate::common::ranger_fixture::TAG_SERVICE,
        "name": name,
        "policyType": 1,
        "isEnabled": true,
        "resources": {"tag": {"values": [tag]}},
        "dataMaskPolicyItems": [{
            "roles": ["engineer"],
            "accesses": [{"type": "hive:select", "isAllowed": true}],
            "dataMaskInfo": {
                "dataMaskType": "hive:CUSTOM",
                "valueExpr": PORTABLE_SSN_MASK,
            }
        }]
    })
}

/// THE phase 2b payoff: a tag authored through SQL masks the column in BOTH engines.
///
/// Chain under test: `SET TAG` writes the Iceberg property AND projects the
/// association into Ranger's tag store, SQE masks from the property, Kyuubi masks
/// from the projection, and the two render identically. Before the projector, this
/// column was masked in SQE and RAW in Spark.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn tag_column_mask_is_byte_identical_across_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    // No RESOURCE mask on ssn here: a passing result must come from the TAG.
    ctx.ranger
        .create_policy(tag_mask_policy_portable(
            &format!("{PREFIX}parity-tagmask"),
            "parity_pii",
        ))
        .await
        .expect("create the tag mask");

    // Author the tag through SQL. With project-tags on, this also writes the
    // association into Ranger's tag store.
    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn SET TAG parity_pii = 'true'"),
            None,
        )
        .await
        .expect("SET TAG must succeed, including its Ranger projection");

    let rows = agreed_rows(&ctx, "bob", "id, ssn", "id", |rows| {
        rows.len() == 3
            && rows
                .iter()
                .all(|r| r.get(1).is_some_and(|v| v.starts_with("xxx-xx-")))
    })
    .await;

    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "xxx-xx-1111".to_string()],
            vec!["2".to_string(), "xxx-xx-2222".to_string()],
            vec!["3".to_string(), "xxx-xx-3333".to_string()],
        ],
        "a tag authored in SQE must mask identically in Spark"
    );
    let flat = rows.concat().join(" ");
    assert!(!flat.contains("111-11-1111"), "the raw ssn leaked: {flat}");
}

/// `UNSET TAG` must stop Spark masking, not just SQE.
///
/// This covers the projector's DELETE path, which nothing else exercises. The
/// projection sends `op: delete` before `op: add_or_update`, and if the delete were
/// a no-op the association would survive: Spark would keep masking a column SQE no
/// longer tags. That direction is fail-CLOSED rather than a leak, but it is still
/// two engines disagreeing about the same table, which is what this suite exists to
/// prevent.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn unset_tag_stops_masking_in_both_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(tag_mask_policy_portable(
            &format!("{PREFIX}parity-unset-tagmask"),
            "parity_unset_pii",
        ))
        .await
        .expect("create the tag mask");

    // Tag it, and confirm BOTH engines mask. Without this the unset below would
    // pass trivially against a column that was never masked.
    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn SET TAG parity_unset_pii = 'true'"),
            None,
        )
        .await
        .expect("SET TAG");
    agreed_rows(&ctx, "bob", "id, ssn", "id", |rows| {
        rows.len() == 3
            && rows
                .iter()
                .all(|r| r.get(1).is_some_and(|v| v.starts_with("xxx-xx-")))
    })
    .await;

    // Now remove it. Both engines must go back to the raw value.
    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn UNSET TAG parity_unset_pii"),
            None,
        )
        .await
        .expect("UNSET TAG");

    let rows = agreed_rows(&ctx, "bob", "id, ssn", "id", |rows| {
        rows.len() == 3 && rows.iter().all(|r| r.get(1).is_some_and(|v| v.contains("-11-") || v.contains("-22-") || v.contains("-33-")))
    })
    .await;
    assert_eq!(
        rows.first().and_then(|r| r.get(1)).map(String::as_str),
        Some("111-11-1111"),
        "after UNSET TAG both engines must return the raw value; a still-masked \
         Spark means the projector's delete path did nothing"
    );
}

/// A DOCUMENTED DIVERGENCE, measured: the two engines resolve resource-mask versus
/// tag-mask precedence DIFFERENTLY.
///
/// With both a resource mask and a tag mask on the same column, SQE applies the
/// RESOURCE mask (pinned on its own side by `resource_mask_beats_tag_mask_live`)
/// while Kyuubi applies the TAG mask. Stock `RangerBasePlugin` evaluates tag policies
/// before resource policies, so Kyuubi is following Ranger's own ordering and SQE is
/// the one that differs.
///
/// It matters because whichever mask is WEAKER becomes the effective one for anyone
/// who picks that engine, so the same column is governed differently depending on how
/// it is read. Recorded here rather than papered over; aligning the two is a decision,
/// not a bug fix, because it means changing SQE's documented precedence.
///
/// Both masks hide the raw value, so this is a difference in WHICH protection applies,
/// not a leak. The assertion checks exactly that much, and that they differ.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn resource_and_tag_mask_precedence_diverges_across_engines() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(tag_mask_policy_portable(
            &format!("{PREFIX}precedence-tagmask"),
            "precedence_pii",
        ))
        .await
        .expect("create the tag mask");
    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}precedence-resource-mask"),
            "ssn",
            serde_json::json!({
                "dataMaskType": "CUSTOM",
                "valueExpr": "concat('RES-', substr({col},8,4))",
            }),
        ))
        .await
        .expect("create the resource mask");
    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn SET TAG precedence_pii = 'true'"),
            None,
        )
        .await
        .expect("SET TAG");

    // Wait for each engine to settle on SOME mask, without requiring agreement.
    let (sqe, spark) = crate::common::eventually_within(
        std::time::Duration::from_secs(180),
        "both engines to apply a mask to ssn",
        || async {
            let (sqe, spark) = both_engines(&ctx, "bob", "id, ssn", "id").await?;
            let masked = |rows: &[Vec<String>]| {
                rows.len() == 3
                    && rows.iter().all(|r| {
                        r.get(1)
                            .is_some_and(|v| v.starts_with("RES-") || v.starts_with("xxx-xx-"))
                    })
            };
            if masked(&sqe) && masked(&spark) {
                Ok((sqe, spark))
            } else {
                Err(format!("sqe={sqe:?} spark={spark:?}"))
            }
        },
    )
    .await;

    // Neither engine leaks, whichever mask won.
    for (label, rows) in [("sqe", &sqe), ("spark", &spark)] {
        let flat = rows.concat().join(" ");
        assert!(
            !flat.contains("111-11-1111"),
            "{label} leaked the raw ssn: {flat}"
        );
    }

    assert_eq!(
        sqe.first().and_then(|r| r.get(1)).map(String::as_str),
        Some("RES-1111"),
        "SQE applies the RESOURCE mask"
    );
    assert_eq!(
        spark.first().and_then(|r| r.get(1)).map(String::as_str),
        Some("xxx-xx-1111"),
        "Kyuubi applies the TAG mask, following stock Ranger's tag-first ordering. \
         If this now reads RES-1111 the engines have converged, which is an \
         improvement: rename this test and update the access-control matrix."
    );
}

/// DEFECT, pinned: adding a column to a table that has a column mask makes the
/// masked query FAIL in SQE.
///
/// Measured: with a mask on `ssn`, `ALTER TABLE ADD COLUMN nickname` then
/// `SELECT id, ssn, nickname` dies with
/// `PhysicalExpr Column references column 'nickname' at index 2 ... but input schema
/// only has 2 columns: ["id", "ssn"]`. The rewritten plan and the scan schema
/// disagree.
///
/// It fails CLOSED, so nothing leaks, but a governed table becomes unqueryable after a
/// routine schema change. Asserted as current behaviour so the defect is recorded and
/// a fix trips this test.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn adding_a_column_to_a_masked_table_breaks_the_query_in_sqe() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(hive_mask_policy(
            &format!("{PREFIX}ddl-mask-ssn"),
            "ssn",
            serde_json::json!({
                "dataMaskType": "CUSTOM",
                "valueExpr": PORTABLE_SSN_MASK,
            }),
        ))
        .await
        .expect("create the ssn mask");
    // Let the mask take effect before the schema changes, so the failure below is
    // about ADD COLUMN and not about an unapplied policy.
    crate::common::eventually("the ssn mask to apply in SQE", || async {
        match ctx
            .handler
            .execute(&ctx.bob, &format!("SELECT ssn FROM {ORDERS}"), None)
            .await
        {
            Ok(b) if col_strings(&b, "ssn").iter().all(|v| v.starts_with("xxx-xx-")) => Ok(()),
            Ok(b) => Err(format!("not masked yet: {:?}", col_strings(&b, "ssn"))),
            Err(e) => Err(format!("{e}")),
        }
    })
    .await;

    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} ADD COLUMN nickname VARCHAR"),
            None,
        )
        .await
        .expect("ADD COLUMN");

    // Projecting the new column ALONGSIDE the masked one fails. Retried, so a stale
    // schema cannot be mistaken for the defect.
    let err = crate::common::eventually(
        "SQE to fail on the added column beside a masked one",
        || async {
            match ctx
                .handler
                .execute(
                    &ctx.bob,
                    &format!("SELECT id, ssn, nickname FROM {ORDERS} ORDER BY id"),
                    None,
                )
                .await
            {
                Err(e) => Ok(e.to_string()),
                Ok(b) => Err(format!(
                    "the query SUCCEEDED with {} rows. If ADD COLUMN beside a mask now \
                     works, this defect is FIXED: delete this test and update the \
                     access-control matrix.",
                    total_rows(&b)
                )),
            }
        },
    )
    .await;
    assert!(
        err.contains("input schema only has") || err.contains("PhysicalExpr"),
        "expected the plan/scan schema mismatch, got: {err}"
    );
}

/// DEFECT, pinned: renaming a tagged column breaks it DIFFERENTLY in each engine.
///
/// `sqe.column-tags` is keyed by column NAME and no schema-change path rewrites it, so
/// after `RENAME COLUMN ssn TO tax_id` the association names a column that is gone.
///
/// Measured, and not what a reader would predict: SQE silently DROPS the column from
/// the result (`SELECT id, tax_id` returns ONE column, no error) while Spark returns it
/// RAW. The stricter engine hides a column that was asked for; the other hands over
/// unmasked data. The mechanism on the SQE side is not confirmed. The behaviour is.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs quickstart/polaris-ranger-keycloak plus spark; run scripts/spark-access-control-test.sh"]
async fn renaming_a_tagged_column_breaks_differently_in_each_engine() {
    spark_gate!();
    let _guard = crate::common::serial().lock().await;
    let ctx = ac_setup().await;
    grant_read_to_both_roles(&ctx).await;

    ctx.ranger
        .create_policy(tag_mask_policy_portable(
            &format!("{PREFIX}ddl-rename-tagmask"),
            "rename_pii",
        ))
        .await
        .expect("create the tag mask");
    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} MODIFY COLUMN ssn SET TAG rename_pii = 'true'"),
            None,
        )
        .await
        .expect("SET TAG");
    // Masked in both engines first, so the rename is what changes things.
    agreed_rows(&ctx, "bob", "id, ssn", "id", |rows| {
        rows.len() == 3
            && rows
                .iter()
                .all(|r| r.get(1).is_some_and(|v| v.starts_with("xxx-xx-")))
    })
    .await;

    ctx.handler
        .execute(
            &ctx.carol,
            &format!("ALTER TABLE {ORDERS} RENAME COLUMN ssn TO tax_id"),
            None,
        )
        .await
        .expect("RENAME COLUMN");

    let (sqe, spark) = crate::common::eventually_within(
        std::time::Duration::from_secs(180),
        "both engines to settle after the rename",
        || async {
            let (sqe, spark) = both_engines(&ctx, "bob", "id, tax_id", "id").await?;
            if sqe.len() == 3 && spark.len() == 3 {
                Ok((sqe, spark))
            } else {
                Err(format!("sqe={sqe:?} spark={spark:?}"))
            }
        },
    )
    .await;

    assert_eq!(
        sqe.first().map(Vec::len),
        Some(1),
        "SQE drops the renamed column from the result entirely, returning only `id` \
         for `SELECT id, tax_id` with no error. If it now returns two columns the \
         behaviour changed: check whether the value is masked or raw, and update the \
         access-control matrix either way. Got: {sqe:?}"
    );
    assert_eq!(
        spark.first().and_then(|r| r.get(1)).map(String::as_str),
        Some("111-11-1111"),
        "Spark returns the renamed column RAW, because the projected association still \
         names the old column. Got: {spark:?}"
    );
}
