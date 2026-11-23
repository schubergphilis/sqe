//! Privilege decisions from a Ranger access-policy bundle, asked through the
//! public `PolicyStore` API: DDL (`alter`/`drop`), `apply_tag` / `remove_tag`
//! (#917, #927) and schema `USE` for a tag writer (#941). The bundle is served
//! by a mock Ranger Admin (wiremock).

use sqe_core::config::RangerPolicyConfig;
use sqe_core::SessionUser;
use sqe_policy::ranger_store::RangerStore;
use sqe_policy::PolicyStore;
use sqe_policy::TablePrivilege::{Alter, ApplyTag, Drop, RemoveTag};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn user(name: &str, roles: &[&str]) -> SessionUser {
    user_with_groups(name, roles, &[])
}

fn user_with_groups(name: &str, roles: &[&str], groups: &[&str]) -> SessionUser {
    SessionUser {
        username: name.to_string(),
        roles: roles.iter().map(|s| s.to_string()).collect(),
        subject: None,
        email: None,
        groups: groups.iter().map(|s| s.to_string()).collect(),
    }
}

/// A store whose Ranger serves `bundle` (a `{"policies":[...]}` document).
/// The mock server is returned so it outlives the store's requests.
async fn store_over(bundle: String, catalog_scoped: bool) -> (MockServer, RangerStore) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/service/plugins/secure/policies/download/hive"))
        .respond_with(ResponseTemplate::new(200).set_body_string(bundle))
        .mount(&server)
        .await;
    let cfg = RangerPolicyConfig {
        url: server.uri(),
        service_name: "hive".to_string(),
        catalog_scoped,
        ..RangerPolicyConfig::default()
    };
    let store = RangerStore::from_config(&cfg).unwrap();
    (server, store)
}

/// `apply_tag` (#917) in the shapes the governance service writes, under
/// `catalog_scoped`: one policy per securable (catalog `lake.*`, schema
/// `lake.sales`, table), `column: *`, the access type alone. Plus the
/// catalog-wide tag guard: `apply_tag` denied to `public`, one user
/// excepted. `extra` policies are appended as given.
fn apply_tag_bundle(guard: bool, extra: &[&str]) -> String {
    let star = r#"{"values":["*"],"isExcludes":false,"isRecursive":false}"#;
    let tag = r#"[{"type":"apply_tag","isAllowed":true}]"#;
    let policy = |id: u32, db: &str, table: &str, items: &str| {
        format!(
            r#"{{"id":{id},"name":"p{id}","policyType":0,"isEnabled":true,
              "resources":{{"database":{{"values":["{db}"],"isExcludes":false,"isRecursive":false}},
              "table":{table},"column":{star}}},{items}}}"#
        )
    };
    let one = |t: &str| format!(r#"{{"values":["{t}"],"isExcludes":false,"isRecursive":false}}"#);
    let item = |who: &str| {
        format!(r#""policyItems":[{{{who},"roles":[],"accesses":{tag},"delegateAdmin":false}}]"#)
    };
    let mut policies = vec![
        policy(
            1,
            "lake.*",
            star,
            &item(r#""users":["cat_owner"],"groups":[]"#),
        ),
        policy(
            2,
            "lake.sales",
            star,
            &item(r#""users":[],"groups":["stewards"]"#),
        ),
        policy(
            3,
            "lake.sales",
            &one("orders"),
            &item(r#""users":["tbl_owner"],"groups":[]"#),
        ),
        // Writers: `all` and the write types, never `apply_tag`.
        policy(
            4,
            "lake.*",
            star,
            r#""policyItems":[{"users":["writer"],"groups":[],"roles":[],
               "accesses":[{"type":"all","isAllowed":true},{"type":"alter","isAllowed":true},
               {"type":"update","isAllowed":true}]}]"#,
        ),
    ];
    if guard {
        policies.push(policy(
            5,
            "lake.*",
            star,
            &format!(
                r#""policyItems":[],
                   "denyPolicyItems":[{{"users":[],"groups":["public"],"roles":[],"accesses":{tag}}}],
                   "denyExceptions":[{{"users":["svc"],"groups":[],"roles":[],"accesses":{tag}}}]"#
            ),
        ));
    }
    policies.extend(extra.iter().map(|p| p.to_string()));
    format!(r#"{{"policies":[{}]}}"#, policies.join(","))
}

#[tokio::test]
async fn apply_tag_comes_only_from_items_that_name_it() {
    let (_server, store) = store_over(apply_tag_bundle(false, &[]), true).await;
    let tag = |u: SessionUser, ns: &'static str, t: &'static str, p| {
        let store = &store;
        async move {
            store
                .grants_column_privilege(&u, t, ns, "ssn", p)
                .await
                .unwrap()
        }
    };
    // Catalog, schema and table grants each reach the column.
    assert!(tag(user("cat_owner", &[]), "lake.sales", "orders", ApplyTag).await);
    assert!(
        tag(user("cat_owner", &[]), "lake.a.b", "t", ApplyTag).await,
        "`lake.*` covers a nested namespace"
    );
    assert!(
        tag(
            user_with_groups("sue", &[], &["stewards"]),
            "lake.sales",
            "orders",
            ApplyTag
        )
        .await
    );
    assert!(
        !tag(
            user_with_groups("sue", &[], &["stewards"]),
            "lake.hr",
            "orders",
            ApplyTag
        )
        .await,
        "a schema grant stays in its schema"
    );
    assert!(tag(user("tbl_owner", &[]), "lake.sales", "orders", ApplyTag).await);
    assert!(!tag(user("tbl_owner", &[]), "lake.sales", "other", ApplyTag).await);
    assert!(
        !tag(user("cat_owner", &[]), "other.sales", "orders", ApplyTag).await,
        "another catalog"
    );
    // Neither `all` nor `alter` nor a write type carries `apply_tag`, and
    // `apply_tag` does not carry `alter`.
    assert!(!tag(user("writer", &[]), "lake.sales", "orders", ApplyTag).await);
    assert!(tag(user("writer", &[]), "lake.sales", "orders", Alter).await);
    assert!(!tag(user("tbl_owner", &[]), "lake.sales", "orders", Alter).await);
    // Nor does any of them, `apply_tag` included, carry `remove_tag` (#927).
    for u in ["writer", "tbl_owner", "cat_owner"] {
        assert!(!tag(user(u, &[]), "lake.sales", "orders", RemoveTag).await);
    }

    // The tag guard: a deny for `public` beats every grant; only its
    // exception keeps a grant (svc has none here, so a grant is added).
    let svc = r#"{"id":6,"name":"svc","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["lake.*"]},"table":{"values":["*"]},"column":{"values":["*"]}},
          "policyItems":[{"users":["svc"],"accesses":[{"type":"apply_tag","isAllowed":true}]}]}"#;
    let (_server, store) = store_over(apply_tag_bundle(true, &[svc]), true).await;
    let tag = |u: SessionUser| {
        let store = &store;
        async move {
            store
                .grants_column_privilege(&u, "orders", "lake.sales", "ssn", ApplyTag)
                .await
                .unwrap()
        }
    };
    assert!(!tag(user("cat_owner", &[])).await, "deny wins");
    assert!(!tag(user("tbl_owner", &[])).await, "deny wins");
    assert!(tag(user("svc", &[])).await, "the deny exception");
}

/// Schema `USE` for a tag writer (#941): an allow granting `select` (or
/// `all`) on any table of the schema; a schema-wide `select` deny wins.
#[tokio::test]
async fn schema_use_is_a_select_reaching_the_schema() {
    let policy = |id: u32, db: &str, extra: &str, items: &str| {
        format!(
            r#"{{"id":{id},"name":"p{id}","policyType":0,"isEnabled":true,
               "resources":{{"database":{{"values":["{db}"]}}{extra}}},{items}}}"#
        )
    };
    let t = |table: &str, column: &str| {
        format!(r#","table":{{"values":["{table}"]}},"column":{{"values":["{column}"]}}"#)
    };
    let allow = |who: &str, access: &str| {
        format!(
            r#""policyItems":[{{"users":["{who}"],"accesses":[{{"type":"{access}","isAllowed":true}}]}}]"#
        )
    };
    let deny = |who: &str| {
        format!(
            r#""denyPolicyItems":[{{"users":["{who}"],"accesses":[{{"type":"select","isAllowed":true}}]}}]"#
        )
    };
    let policies = [
        // A reader of one table may use its schema.
        policy(
            1,
            "lake.sales",
            &t("orders", "*"),
            &allow("reader", "select"),
        ),
        // `all` carries `select`; `lake.*` reaches a nested schema.
        policy(2, "lake.*", &t("*", "*"), &allow("owner", "all")),
        // Tag grants alone are not `select`.
        policy(
            3,
            "lake.sales",
            &t("*", "*"),
            &allow("steward", "apply_tag"),
        ),
        policy(
            4,
            "lake.sales",
            &t("*", "*"),
            &allow("steward", "remove_tag"),
        ),
        // A schema-wide deny beats a table allow.
        policy(
            5,
            "lake.sales",
            &t("orders", "*"),
            &allow("blocked", "select"),
        ),
        policy(6, "lake.sales", &t("*", "*"), &deny("blocked")),
        // A deny on one table, or on one column, leaves the schema usable.
        policy(7, "lake.sales", &t("*", "*"), &allow("partly", "select")),
        policy(8, "lake.sales", &t("orders", "*"), &deny("partly")),
        policy(9, "lake.sales", &t("*", "ssn"), &deny("partly")),
        // A catalog-wide deny reaches every schema of the catalog.
        policy(10, "lake.sales", &t("*", "*"), &allow("barred", "select")),
        policy(11, "lake.*", &t("*", "*"), &deny("barred")),
        // A udf policy reaches no table.
        policy(
            12,
            "lake.sales",
            r#","udf":{"values":["*"]}"#,
            &allow("udf_user", "select"),
        ),
        // Old-style keys: an allow and a deny on the last component only.
        policy(13, "sales", &t("*", "*"), &allow("legacy", "select")),
        policy(
            14,
            "lake.hr",
            &t("*", "*"),
            &allow("legacy_denied", "select"),
        ),
        policy(15, "hr", &t("*", "*"), &deny("legacy_denied")),
    ];
    let bundle = format!(r#"{{"policies":[{}]}}"#, policies.join(","));
    let (_s1, scoped_store) = store_over(bundle.clone(), true).await;
    let (_s2, unscoped_store) = store_over(bundle, false).await;
    async fn ask(store: &RangerStore, who: &str, db: &str) -> bool {
        store.grants_schema_use(&user(who, &[]), db).await.unwrap()
    }
    let scoped = |who: &'static str, db: &'static str| ask(&scoped_store, who, db);
    let unscoped = |who: &'static str, db: &'static str| ask(&unscoped_store, who, db);

    assert!(scoped("reader", "lake.sales").await);
    assert!(
        !scoped("reader", "lake.hr").await,
        "a select on another schema does not count"
    );
    assert!(
        !scoped("reader", "other.sales").await,
        "nor one in another catalog"
    );
    assert!(scoped("owner", "lake.sales").await && scoped("owner", "lake.a.b").await);
    assert!(
        !scoped("steward", "lake.sales").await,
        "apply_tag / remove_tag alone are refused"
    );
    assert!(
        !scoped("blocked", "lake.sales").await,
        "the schema-wide deny wins"
    );
    assert!(
        scoped("partly", "lake.sales").await,
        "a table or column deny leaves USE"
    );
    assert!(
        !scoped("barred", "lake.sales").await,
        "a catalog-wide deny wins"
    );
    assert!(!scoped("udf_user", "lake.sales").await);
    assert!(
        !scoped("nobody", "lake.sales").await,
        "nothing matching is refused"
    );

    // Without `catalog_scoped` the key is the bare namespace, and a deny
    // written on its last component still refuses (fail closed) while an
    // allow written that way grants only the bare key.
    assert!(unscoped("reader", "lake.sales").await);
    assert!(unscoped("legacy", "sales").await);
    assert!(
        !unscoped("legacy", "lake.sales").await,
        "an allow never matches by last component"
    );
    assert!(
        !unscoped("legacy_denied", "lake.hr").await,
        "a deny matches by last component"
    );
    assert!(
        scoped("legacy_denied", "lake.hr").await,
        "not under catalog_scoped"
    );
}

/// DDL authority comes from the same access policies as `select`, asked
/// for `alter`/`drop`: `all` carries both, a `select`-only grant carries
/// neither, and a deny on `all` or an allow exception still wins.
#[tokio::test]
async fn ddl_privileges_come_from_the_access_policies() {
    let ddl = r#"{"policies":[
         {"id":1,"name":"ops-alter","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["lake.sales"]},"table":{"values":["orders"]}},
          "policyItems":[{"roles":["ops"],"accesses":[{"type":"alter","isAllowed":true}]}]},
         {"id":2,"name":"owners-all","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["lake.sales"]},"table":{"values":["*"]}},
          "policyItems":[{"roles":["stewards"],"accesses":[{"type":"all","isAllowed":true}]}],
          "allowExceptions":[{"users":["eve"],"accesses":[{"type":"all","isAllowed":true}]}]},
         {"id":3,"name":"readers","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["lake.sales"]},"table":{"values":["*"]}},
          "policyItems":[{"groups":["public"],"accesses":[{"type":"select","isAllowed":true}]}]},
         {"id":4,"name":"no-drop-locked","policyType":0,"isEnabled":true,
          "resources":{"database":{"values":["lake.sales"]},"table":{"values":["locked"]}},
          "denyPolicyItems":[{"roles":["stewards"],"accesses":[{"type":"drop","isAllowed":true}]}]}]}"#;
    let (_server, store) = store_over(ddl.to_string(), false).await;
    let can = |u: SessionUser, t: &'static str, p| {
        let store = &store;
        async move {
            store
                .grants_table_privilege(&u, t, "lake.sales", p)
                .await
                .unwrap()
        }
    };

    assert!(can(user("olga", &["ops"]), "orders", Alter).await);
    assert!(
        !can(user("olga", &["ops"]), "orders", Drop).await,
        "alter does not carry drop"
    );
    assert!(
        can(user("sam", &["stewards"]), "orders", Drop).await,
        "all carries drop"
    );
    assert!(
        !can(user("eve", &["stewards"]), "orders", Drop).await,
        "allow exception"
    );
    assert!(
        !can(user("carol", &[]), "orders", Alter).await,
        "select is not DDL"
    );
    assert!(
        !can(user("sam", &["stewards"]), "locked", Drop).await,
        "deny wins"
    );
    assert!(
        can(user("sam", &["stewards"]), "locked", Alter).await,
        "the deny names drop only"
    );
}
