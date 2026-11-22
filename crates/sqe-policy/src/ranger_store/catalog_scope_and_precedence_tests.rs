use super::*;
use serde_json::json;

fn bundle(policies: serde_json::Value) -> ServicePolicies {
    serde_json::from_value(json!({ "policyVersion": 1, "policies": policies })).unwrap()
}

fn member(name: &str, groups: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: Vec::new(),
        subject: None,
        email: None,
        groups: groups.iter().map(|g| g.to_string()).collect(),
    }
}

fn mask_policy(
    id: i64,
    database: &str,
    column: &str,
    items: serde_json::Value,
) -> serde_json::Value {
    json!({
        "id": id, "name": format!("mask-{id}"), "policyType": 1, "isEnabled": true,
        "resources": {
            "database": {"values": [database]},
            "table": {"values": ["orders"]},
            "column": {"values": [column]}
        },
        "dataMaskPolicyItems": items
    })
}

fn item(groups: &[&str], mask: &str) -> serde_json::Value {
    json!({"groups": groups, "accesses": [{"type": "select", "isAllowed": true}],
           "dataMaskInfo": {"dataMaskType": mask}})
}

/// The portal's shape: "mask for everyone except group E".
fn everyone_but_e() -> serde_json::Value {
    json!([
        item(&["SG-Exempt"], "MASK_NONE"),
        item(&["public"], "MASK_NULL")
    ])
}

fn resolve(b: &ServicePolicies, u: &SessionUser, namespace: &str, scoped: bool) -> ResolvedPolicy {
    resolve_from_bundle_with(b, u, "orders", namespace, false, scoped)
}

// ── catalog_scoped ─────────────────────────────────────────────────

#[test]
fn scoped_mask_applies_only_to_the_named_catalog() {
    let b = bundle(json!([mask_policy(
        1,
        "ws_a.finance",
        "ssn",
        json!([item(&["public"], "MASK_NULL")])
    )]));
    let u = member("ann", &[]);
    assert!(resolve(&b, &u, "ws_a.finance", true)
        .column_masks
        .contains_key("ssn"));
    assert!(
        resolve(&b, &u, "ws_b.finance", true)
            .column_masks
            .is_empty(),
        "same namespace.table in another catalog is not covered"
    );
}

#[test]
fn unscoped_a_catalogless_policy_covers_every_catalog() {
    // Today's behaviour, kept when the setting is off: both catalogs key on
    // `finance`, so one policy fires in both.
    let b = bundle(json!([mask_policy(
        1,
        "finance",
        "ssn",
        json!([item(&["public"], "MASK_NULL")])
    )]));
    let u = member("ann", &[]);
    assert!(resolve(&b, &u, "finance", false)
        .column_masks
        .contains_key("ssn"));
}

#[test]
fn scoped_disables_the_last_component_fallback() {
    let b = bundle(json!([mask_policy(
        1,
        "finance",
        "ssn",
        json!([item(&["public"], "MASK_NULL")])
    )]));
    let u = member("ann", &[]);
    assert!(
        resolve(&b, &u, "ws_a.finance", false)
            .column_masks
            .contains_key("ssn"),
        "unscoped: the migration fallback matches on the last component"
    );
    assert!(
        resolve(&b, &u, "ws_a.finance", true)
            .column_masks
            .is_empty(),
        "scoped: `finance` must not match `ws_a.finance` (or `ws_b.finance`)"
    );
    // A policy naming another catalog never matches through the fallback.
    let other = bundle(json!([mask_policy(
        1,
        "ws_b.finance",
        "ssn",
        json!([item(&["public"], "MASK_NULL")])
    )]));
    assert!(resolve(&other, &u, "ws_a.finance", true)
        .column_masks
        .is_empty());
}

/// The deny-item fallback is off under `catalog_scoped` too: a deny on
/// `finance` is another catalog's (or no catalog's) policy.
#[test]
fn scoped_access_deny_does_not_match_by_last_component() {
    let b = bundle(json!([
        {"id": 1, "name": "allow", "policyType": 0, "isEnabled": true,
         "resources": {"database": {"values": ["ws_a.finance"]}, "table": {"values": ["*"]}},
         "policyItems": [{"groups": ["public"], "accesses": [{"type": "select", "isAllowed": true}]}]},
        {"id": 2, "name": "deny", "policyType": 0, "isEnabled": true,
         "resources": {"database": {"values": ["finance"]}, "table": {"values": ["*"]}},
         "denyPolicyItems": [{"groups": ["public"], "accesses": [{"type": "select", "isAllowed": true}]}]}
    ]));
    let u = member("ann", &[]);
    let denied = |scoped| {
        crate::plan_rewriter::policy_denies_all(&resolve_from_bundle_with(
            &b,
            &u,
            "orders",
            "ws_a.finance",
            true,
            scoped,
        ))
    };
    assert!(
        denied(false),
        "unscoped: the deny on `finance` matches by last component"
    );
    assert!(!denied(true), "scoped: it does not");
}

#[test]
fn scoped_store_reports_the_flag_and_scoped_ddl_keeps_the_catalog() {
    let on = RangerStore::from_config(&RangerPolicyConfig {
        catalog_scoped: true,
        ..RangerPolicyConfig::default()
    })
    .unwrap();
    assert!(on.catalog_scoped());
    let off = RangerStore::from_config(&RangerPolicyConfig::default()).unwrap();
    assert!(!off.catalog_scoped());

    let parsed = sqe_sql::policy_ddl::try_parse_policy_ddl(
        "CREATE POLICY m ON TABLE ws_a.finance.orders COLUMN MASK MASK_NULL \
         TO GROUP analysts ON COLUMN ssn",
    )
    .expect("valid policy SQL")
    .expect("policy statement");
    let sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt) = parsed else {
        panic!("expected create")
    };
    let scoped = ranger_policy_body(&stmt, "query", "hive", true).unwrap();
    assert_eq!(
        scoped["resources"]["database"]["values"],
        json!(["ws_a.finance"])
    );
    let unscoped = ranger_policy_body(&stmt, "query", "hive", false).unwrap();
    assert_eq!(
        unscoped["resources"]["database"]["values"],
        json!(["finance"])
    );

    let two_part = sqe_sql::policy_ddl::try_parse_policy_ddl(
        "CREATE POLICY m ON TABLE finance.orders COLUMN MASK MASK_NULL TO GROUP analysts ON COLUMN ssn",
    )
    .unwrap()
    .unwrap();
    let sqe_sql::policy_ddl::PolicyDdlStatement::Create(two_part) = two_part else {
        panic!("expected create")
    };
    assert!(
        ranger_policy_body(&two_part, "query", "hive", true).is_err(),
        "scoped DDL without a catalog would write a policy that never matches"
    );
}

// ── first-match precedence ─────────────────────────────────────────

#[test]
fn first_matching_mask_item_wins_so_an_exemption_item_exempts() {
    let b = bundle(json!([mask_policy(1, "finance", "ssn", everyone_but_e())]));
    let exempt = member("eve", &["SG-Exempt"]);
    let other = member("ann", &["SG-DataAnalysts"]);
    let p = resolve(&b, &exempt, "finance", false);
    assert!(
        p.column_masks.is_empty() && p.restricted_columns.is_empty(),
        "E sees the raw value"
    );
    assert!(resolve(&b, &other, "finance", false)
        .column_masks
        .contains_key("ssn"));
}

#[test]
fn mask_none_for_a_user_leaves_the_column_unmasked() {
    let items = json!([
        {"users": ["owner"], "dataMaskInfo": {"dataMaskType": "MASK_NONE"}},
        item(&["public"], "MASK_NULL")
    ]);
    let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
    let p = resolve(&b, &member("owner", &[]), "finance", false);
    assert!(p.column_masks.is_empty() && p.restricted_columns.is_empty());
    assert!(resolve(&b, &member("ann", &[]), "finance", false)
        .column_masks
        .contains_key("ssn"));
}

#[test]
fn item_order_decides_so_public_first_masks_everyone() {
    let items = json!([
        item(&["public"], "MASK_NULL"),
        item(&["SG-Exempt"], "MASK_NONE")
    ]);
    let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
    assert!(
        resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false)
            .column_masks
            .contains_key("ssn"),
        "Ranger takes the first matching item: public, so E is masked too"
    );
}

#[test]
fn with_two_matching_mask_items_the_first_wins() {
    let items = json!([
        item(&["public"], "MASK_HASH"),
        item(&["public"], "MASK_NULL")
    ]);
    let b = bundle(json!([mask_policy(1, "finance", "ssn", items)]));
    assert!(matches!(
        resolve(&b, &member("ann", &[]), "finance", false)
            .column_masks
            .get("ssn"),
        Some(MaskType::Hash)
    ));
}

#[test]
fn first_policy_to_decide_a_column_is_final_and_override_priority_goes_first() {
    let exempt = member("eve", &["SG-Exempt"]);
    // Policy 1 exempts E; policy 2 masks everyone. Same priority: bundle
    // order, policy 1 decides for E and policy 2 cannot re-mask.
    let b = bundle(json!([
        mask_policy(
            1,
            "finance",
            "ssn",
            json!([item(&["SG-Exempt"], "MASK_NONE")])
        ),
        mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_HASH")])),
    ]));
    assert!(resolve(&b, &exempt, "finance", false)
        .column_masks
        .is_empty());
    assert!(matches!(
        resolve(&b, &member("ann", &[]), "finance", false)
            .column_masks
            .get("ssn"),
        Some(MaskType::Hash)
    ));

    // Override priority on the masking policy puts it first.
    let mut over = mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_HASH")]));
    over["policyPriority"] = json!(1);
    let b = bundle(json!([
        mask_policy(
            1,
            "finance",
            "ssn",
            json!([item(&["SG-Exempt"], "MASK_NONE")])
        ),
        over
    ]));
    assert!(matches!(
        resolve(&b, &exempt, "finance", false)
            .column_masks
            .get("ssn"),
        Some(MaskType::Hash)
    ));
}

/// A conditional first item decides its columns (restricted) for the
/// whole bundle: a later policy's `MASK_NONE` does not reopen them.
#[test]
fn a_conditional_first_item_is_a_final_decision() {
    let conditional = json!([{"groups": ["public"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
                              "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}]);
    let b = bundle(json!([
        mask_policy(1, "finance", "ssn", conditional),
        mask_policy(2, "finance", "ssn", json!([item(&["public"], "MASK_NONE")])),
    ]));
    let p = resolve(&b, &member("ann", &[]), "finance", false);
    assert!(p.restricted_columns.contains(&"ssn".to_string()));
}

fn filter_policy(id: i64, items: serde_json::Value) -> serde_json::Value {
    json!({
        "id": id, "name": format!("rf-{id}"), "policyType": 2, "isEnabled": true,
        "resources": {"database": {"values": ["finance"]}, "table": {"values": ["orders"]}},
        "rowFilterPolicyItems": items
    })
}

#[test]
fn first_matching_row_filter_item_wins_and_an_empty_filter_exempts() {
    let b = bundle(json!([filter_policy(
        1,
        json!([
            {"groups": ["SG-Exempt"], "rowFilterInfo": {"filterExpr": ""}},
            {"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
        ])
    )]));
    assert!(
        resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false)
            .row_filters
            .is_empty()
    );
    assert_eq!(
        resolve(&b, &member("ann", &[]), "finance", false)
            .row_filters
            .len(),
        1
    );
}

/// A conditional exemption item cannot exempt: SQE cannot tell whether
/// its condition holds, so the later matching item applies as well.
#[test]
fn a_conditional_row_filter_item_does_not_decide_alone() {
    let b = bundle(json!([filter_policy(
        1,
        json!([
            {"groups": ["SG-Exempt"], "conditions": [{"type": "ip-range", "values": ["10.0.0.0/8"]}],
             "rowFilterInfo": {"filterExpr": ""}},
            {"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}
        ])
    )]));
    assert_eq!(
        resolve(&b, &member("eve", &["SG-Exempt"]), "finance", false)
            .row_filters
            .len(),
        1
    );
}

#[test]
fn row_filters_of_separate_policies_are_anded() {
    let b = bundle(json!([
        filter_policy(
            1,
            json!([{"groups": ["public"], "rowFilterInfo": {"filterExpr": "region = 'EU'"}}])
        ),
        filter_policy(
            2,
            json!([{"groups": ["public"], "rowFilterInfo": {"filterExpr": "year > 2020"}}])
        ),
    ]));
    assert_eq!(
        resolve(&b, &member("ann", &[]), "finance", false)
            .row_filters
            .len(),
        2
    );
}

// ── tag path ───────────────────────────────────────────────────────

#[test]
fn tag_policies_take_the_first_item_and_the_first_deciding_policy() {
    let mut b = bundle(json!([]));
    b.tag_policies = Some(
        serde_json::from_value(json!({"policies": [
            {"id": 9, "name": "pii-exempt", "policyType": 1, "isEnabled": true,
             "resources": {"tag": {"values": ["PII"]}},
             "dataMaskPolicyItems": [
                 {"groups": ["SG-Exempt"], "dataMaskInfo": {"dataMaskType": "hive:MASK_NONE"}},
                 {"groups": ["SG-DataAnalysts"], "dataMaskInfo": {"dataMaskType": "hive:MASK_NULL"}}
             ]},
            {"id": 10, "name": "pii-all", "policyType": 1, "isEnabled": true,
             "resources": {"tag": {"values": ["PII"]}},
             "dataMaskPolicyItems": [
                 {"groups": ["public"], "dataMaskInfo": {"dataMaskType": "hive:MASK_HASH"}}
             ]}
        ]}))
        .unwrap(),
    );
    let tags: HashSet<String> = ["PII".to_string()].into();
    let (masks, _, _) = resolve_tag_policies(&b, &member("ann", &["SG-DataAnalysts"]), &tags);
    assert!(matches!(
        masks.get("PII"),
        Some(TagMaskSpec::Ready(MaskType::Nullify))
    ));
    let (masks, _, _) =
        resolve_tag_policies(&b, &member("eve", &["SG-Exempt", "SG-DataAnalysts"]), &tags);
    assert!(
        masks.is_empty(),
        "the exemption item comes first and its decision is final"
    );
    let (masks, _, _) = resolve_tag_policies(&b, &member("bob", &[]), &tags);
    assert!(matches!(
        masks.get("PII"),
        Some(TagMaskSpec::Ready(MaskType::Hash))
    ));
}

// ── #919: OR REPLACE never moves a tag policy ──────────────────────

#[test]
fn or_replace_refuses_to_move_a_tag_policy_to_another_tag() {
    let body = |sql: &str| {
        let Some(sqe_sql::policy_ddl::PolicyDdlStatement::Create(stmt)) =
            sqe_sql::policy_ddl::try_parse_policy_ddl(sql).unwrap()
        else {
            panic!("expected create")
        };
        ranger_policy_body(&stmt, "query_tag", "hive", true).unwrap()
    };
    let legacy = body("CREATE POLICY p ON TAG pii COLUMN MASK MASK_NULL TO GROUP analysts");
    let scoped =
        body("CREATE POLICY p ON TAG sales_wh.pii COLUMN MASK MASK_NULL TO GROUP analysts");
    let other = body("CREATE POLICY p ON TAG ops_wh.pii COLUMN MASK MASK_NULL TO GROUP analysts");
    let same = body("CREATE POLICY p ON TAG sales_wh.pii COLUMN MASK MASK_HASH TO GROUP analysts");

    // A legacy bare policy replaced by a catalog key: refused, not narrowed silently.
    assert_eq!(
        tag_policy_move(&legacy, &scoped),
        Some(("pii".into(), "sales_wh.pii".into()))
    );
    // One catalog to another: refused.
    assert!(tag_policy_move(&scoped, &other).is_some());
    // Same tag, new mask: an ordinary replace.
    assert_eq!(tag_policy_move(&scoped, &same), None);
    // Resource policies carry no tag resource and are never refused here.
    let table = json!({"resources": {"database": {"values": ["ws_a.finance"]}}});
    assert_eq!(tag_policy_move(&table, &table), None);
}
