use super::*;

/// Real Ranger bundle shape, including a row-filter policy that must NOT appear
/// and a disabled policy that MUST appear (listed, doing nothing).
const BUNDLE: &str = r#"{
  "policyVersion": 7,
  "policies": [
    {"id":1,"name":"mask-orders-ssn","policyType":1,"isEnabled":true,
     "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                  "column":{"values":["ssn"]}},
     "dataMaskPolicyItems":[{"roles":["engineer"],"users":["dave"],
       "dataMaskInfo":{"dataMaskType":"CUSTOM","valueExpr":"concat('x', {col})"}}]},
    {"id":2,"name":"mask-multi","policyType":1,"isEnabled":false,
     "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                  "column":{"values":["email","phone"]}},
     "dataMaskPolicyItems":[{"groups":["public"],
       "dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]},
    {"id":3,"name":"filter-orders-eu","policyType":2,"isEnabled":true,
     "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]}},
     "rowFilterPolicyItems":[{"roles":["engineer"],
       "rowFilterInfo":{"filterExpr":"region = 'EU'"}}]},
    {"id":4,"name":"access-orders","policyType":0,"isEnabled":true,
     "resources":{"database":{"values":["sales"]}}},
    {"id":5,"name":"rowfilter-carrying-a-mask-item","policyType":2,"isEnabled":true,
     "resources":{"database":{"values":["sales"]},"table":{"values":["orders"]},
                  "column":{"values":["ssn"]}},
     "dataMaskPolicyItems":[{"roles":["engineer"],
       "dataMaskInfo":{"dataMaskType":"MASK_NULL"}}]}
  ],
  "tagPolicies": {
    "serviceName": "sqe_tag",
    "policies": [
      {"id":10,"name":"mask-pii-tag","policyType":1,"isEnabled":true,
       "resources":{"tag":{"values":["PII"]}},
       "dataMaskPolicyItems":[{"roles":["engineer"],
         "dataMaskInfo":{"dataMaskType":"hive:CUSTOM",
                         "valueExpr":"concat('t', {col})"}}]},
      {"id":11,"name":"tag-rowfilter","policyType":2,"isEnabled":true,
       "resources":{"tag":{"values":["PII"]}},
       "rowFilterPolicyItems":[{"roles":["engineer"],
         "rowFilterInfo":{"filterExpr":"region = 'EU'"}}],
       "dataMaskPolicyItems":[{"roles":["engineer"],
         "dataMaskInfo":{"dataMaskType":"hive:MASK_NULL"}}]}
    ]
  }
}"#;

fn listed() -> Vec<crate::MaskPolicyInfo> {
    let bundle: ServicePolicies = serde_json::from_str(BUNDLE).expect("parse bundle");
    mask_policies_from_bundle(&bundle)
}

/// TAG-based masks must be listed too.
///
/// This is the one that was WRONG. `mask_policies_from_bundle` walked only
/// `bundle.policies`, so on a deployment that masks entirely by tag --
/// which is the pattern the tag projector exists to support --
/// `SHOW MASKING POLICIES` returned an empty listing. An operator reads that
/// as "nothing is masked here", which is exactly the failure the refusing
/// trait default was written to prevent, reached from the other side.
///
/// The tag row carries the TAG and no table; a resource row the reverse. So
/// the two kinds are distinguishable in the output instead of a reader having
/// to infer which store a row came from.
#[test]
fn tag_based_masks_are_listed_with_their_tag() {
    let all = listed();
    let tagged: Vec<_> = all.iter().filter(|p| p.name == "mask-pii-tag").collect();
    assert_eq!(
        tagged.len(),
        1,
        "the tag-service mask policy is missing from the listing, so a \
         tag-masked deployment would report nothing masked. Got: {:?}",
        all.iter().map(|p| &p.name).collect::<Vec<_>>()
    );
    let row = tagged[0];
    assert_eq!(row.tag, "PII");
    assert_eq!(row.mask_type, "hive:CUSTOM");
    assert_eq!(row.expression, "concat('t', {col})");
    assert_eq!(row.grantees, "ROLE engineer");
    // A tag policy names no table, and saying so is the point: a reader can
    // tell it applies wherever the tag is, not to one column.
    assert_eq!(
        (
            row.database.as_str(),
            row.table.as_str(),
            row.column.as_str()
        ),
        ("", "", "")
    );
}

/// A tag ROW FILTER is not a mask, in the tag store either.
///
/// The resource-side equivalent is covered by `only_data_mask_policies_are_listed`.
/// The tag store needs its own case because the policyType guard runs over a
/// chained iterator, and a guard that is correct for one arm is not
/// automatically correct for the other.
///
/// `tag-rowfilter` carries `dataMaskPolicyItems` as well as its row filter, and
/// that detail is the whole test. My first version left it with only a row
/// filter, and the mutation survived: with no mask items the loop emits nothing
/// for it whether the type is checked or not, so the assertion held for a reason
/// that had nothing to do with the guard. The comment on
/// `only_data_mask_policies_are_listed` warns about exactly this shape, in this
/// file, and I reproduced it anyway. Mutation-verified after the fixture change:
/// dropping the guard makes `tag-rowfilter` appear.
#[test]
fn a_tag_row_filter_is_not_listed_as_a_mask() {
    assert!(
        !listed().iter().any(|p| p.name == "tag-rowfilter"),
        "a policyType-2 tag policy is a row filter, not a mask"
    );
}

/// Only policyType 1. A row filter or an access policy showing up under
/// SHOW MASKING POLICIES would misreport what protects a column.
///
/// The fixture includes a policyType-2 policy that ALSO carries
/// `dataMaskPolicyItems`, which is the only shape where the type check is the
/// thing doing the work. Without that entry this assertion was vacuous: a plain
/// row-filter policy has no mask items, so the loop emitted nothing for it
/// whether the type was checked or not, and removing the check changed no output.
/// Mutation-verified after adding it: dropping the guard makes
/// `rowfilter-carrying-a-mask-item` appear.
/// `mask-pii-tag` is here because it IS a policyType-1 mask, just one living in
/// the tag store. `tag-rowfilter` is absent for the same reason
/// `filter-orders-eu` is.
#[test]
fn only_data_mask_policies_are_listed() {
    let all = listed();
    let names: Vec<&str> = all.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["mask-multi", "mask-orders-ssn", "mask-pii-tag"]);
}

#[test]
fn the_resource_and_expression_survive_the_mapping() {
    let p = listed()
        .into_iter()
        .find(|p| p.name == "mask-orders-ssn")
        .expect("the ssn policy");
    assert_eq!(p.database, "sales");
    assert_eq!(p.table, "orders");
    assert_eq!(p.column, "ssn");
    assert_eq!(p.mask_type, "CUSTOM");
    assert_eq!(p.expression, "concat('x', {col})");
    assert!(p.enabled);
}

/// Every grantee kind is shown, and labelled. "engineer" alone would not say
/// whether it is a user or a role, and Ranger allows a user and a role of the
/// same name.
#[test]
fn grantees_are_labelled_by_kind() {
    let p = listed()
        .into_iter()
        .find(|p| p.name == "mask-orders-ssn")
        .expect("the ssn policy");
    assert_eq!(p.grantees, "ROLE engineer, USER dave");
}

/// A policy disabled in the console is LISTED, with enabled = false. Hiding it
/// would make a mask that someone can re-enable with one click invisible to an
/// audit.
#[test]
fn a_disabled_policy_is_listed_and_flagged() {
    let p = listed()
        .into_iter()
        .find(|p| p.name == "mask-multi")
        .expect("the disabled policy");
    assert!(!p.enabled);
    assert_eq!(p.column, "email, phone", "several columns are joined");
    assert_eq!(p.grantees, "GROUP public");
}

/// Deterministic order, so two runs on an unchanged service produce identical
/// output and a diff means something changed.
#[test]
fn the_order_is_stable() {
    assert_eq!(listed(), listed());
}
