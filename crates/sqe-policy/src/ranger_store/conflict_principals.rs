//! The principals the tag write check resolves under `mask_precedence =
//! refuse` (#918): one synthetic user per user, role and group that a mask or
//! row-filter policy on the table, or on one of its tags, names.

use std::collections::{BTreeSet, HashSet};

use sqe_core::SessionUser;

use super::{matching_fine_grained_policies, ServicePolicies, PUBLIC_GROUP};

/// User name of the synthetic role, group and public principals. The NUL byte
/// keeps every `users:` item from matching it.
const SYNTHETIC_USER: &str = "\0sqe-conflict-check";

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Principal<'a> {
    User(&'a str),
    Role(&'a str),
    Group(&'a str),
}

/// Every user, role and group in a mask or row-filter item of a resource
/// policy on `database.table` or an enabled tag policy on one of `tags`, one
/// synthetic `SessionUser` each, after a first user with no name, roles or
/// groups: `item_matches` lets that one match `{PUBLIC}` items only, and every
/// other principal matches them too.
pub(super) fn conflict_principals_from_bundle(
    bundle: &ServicePolicies,
    database: &str,
    table: &str,
    last_component_fallback: bool,
    tags: &HashSet<String>,
) -> Vec<SessionUser> {
    let on_tags = bundle
        .tag_policies
        .iter()
        .flat_map(|tp| tp.policies.iter())
        .filter(|p| p.is_enabled)
        .filter(|p| {
            p.resources
                .get("tag")
                .is_some_and(|r| r.values.iter().any(|v| tags.contains(v)))
        });
    let policies = matching_fine_grained_policies(bundle, database, table, last_component_fallback)
        .into_iter()
        .chain(on_tags);

    let mut principals = BTreeSet::new();
    for p in policies {
        let masks = p
            .data_mask_policy_items
            .iter()
            .map(|i| (&i.users, &i.roles, &i.groups));
        let filters = p
            .row_filter_policy_items
            .iter()
            .map(|i| (&i.users, &i.roles, &i.groups));
        for (users, roles, groups) in masks.chain(filters) {
            principals.extend(users.iter().map(|u| Principal::User(u)));
            principals.extend(roles.iter().map(|r| Principal::Role(r)));
            principals.extend(
                groups
                    .iter()
                    .filter(|g| *g != PUBLIC_GROUP)
                    .map(|g| Principal::Group(g)),
            );
        }
    }

    let synthetic = |username: &str, roles: Vec<String>, groups: Vec<String>| SessionUser {
        username: username.to_string(),
        roles,
        subject: None,
        email: None,
        groups,
    };
    let mut users = vec![synthetic(SYNTHETIC_USER, vec![], vec![])];
    users.extend(principals.into_iter().map(|p| match p {
        Principal::User(name) => synthetic(name, vec![], vec![]),
        Principal::Role(role) => synthetic(SYNTHETIC_USER, vec![role.to_string()], vec![]),
        Principal::Group(group) => synthetic(SYNTHETIC_USER, vec![], vec![group.to_string()]),
    }));
    users
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn principals(policies: serde_json::Value, tag_policies: serde_json::Value) -> Vec<String> {
        let bundle: ServicePolicies = serde_json::from_value(json!({
            "policyVersion": 1,
            "policies": policies,
            "tagPolicies": {"policies": tag_policies}
        }))
        .unwrap();
        let tags = HashSet::from(["pii".to_string()]);
        conflict_principals_from_bundle(&bundle, "ac", "orders", true, &tags)
            .into_iter()
            .map(|u| {
                format!(
                    "{}|{}|{}",
                    u.username.replace('\0', "~"),
                    u.roles.join(","),
                    u.groups.join(",")
                )
            })
            .collect()
    }

    fn policy(
        kind: i64,
        resources: serde_json::Value,
        items: serde_json::Value,
    ) -> serde_json::Value {
        let key = if kind == 1 {
            "dataMaskPolicyItems"
        } else {
            "rowFilterPolicyItems"
        };
        json!({"id": 1, "name": "p", "policyType": kind, "isEnabled": true,
               "resources": resources, key: items})
    }

    fn orders() -> serde_json::Value {
        json!({"database": {"values": ["ac"]}, "table": {"values": ["orders"]},
               "column": {"values": ["amount"]}})
    }

    #[test]
    fn lists_each_principal_of_matching_resource_and_tag_policies_once() {
        let got = principals(
            json!([
                policy(
                    1,
                    orders(),
                    json!([{"roles": ["engineer"], "users": ["bob"],
                    "dataMaskInfo": {"dataMaskType": "MASK_HASH"}}])
                ),
                policy(
                    2,
                    orders(),
                    json!([{"groups": ["auditors", "public"],
                    "rowFilterInfo": {"filterExpr": "1 = 1"}}])
                ),
            ]),
            json!([policy(
                1,
                json!({"tag": {"values": ["pii"]}}),
                json!([{"roles": ["engineer", "analyst"],
                "dataMaskInfo": {"dataMaskType": "MASK_NULL"}}])
            )]),
        );
        assert_eq!(
            got,
            vec![
                "~sqe-conflict-check||",
                "bob||",
                "~sqe-conflict-check|analyst|",
                "~sqe-conflict-check|engineer|",
                "~sqe-conflict-check||auditors",
            ],
            "public user first; `public` itself is not a group principal"
        );
    }

    #[test]
    fn ignores_other_tables_other_tags_and_disabled_policies() {
        let other_table = json!({"database": {"values": ["ac"]}, "table": {"values": ["other"]},
                                 "column": {"values": ["amount"]}});
        let mut disabled = policy(
            1,
            json!({"tag": {"values": ["pii"]}}),
            json!([{"roles": ["off"]}]),
        );
        disabled["isEnabled"] = json!(false);
        let got = principals(
            json!([policy(1, other_table, json!([{"roles": ["elsewhere"]}]))]),
            json!([
                policy(
                    1,
                    json!({"tag": {"values": ["secret"]}}),
                    json!([{"roles": ["untagged"]}])
                ),
                disabled,
            ]),
        );
        assert_eq!(got, vec!["~sqe-conflict-check||"]);
    }
}
