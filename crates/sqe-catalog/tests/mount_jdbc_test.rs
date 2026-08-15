//! Integration tests for `sqe_catalog::build_catalog` with `CatalogKind::Jdbc`.
//!
//! `backends_integration::jdbc_postgres_namespace_roundtrip` already drives
//! `SqlCatalogBuilder` DIRECTLY against live Postgres. What it does not cover is
//! the `ATTACH ... TYPE jdbc` surface in front of it: prefix stripping, the
//! `WAREHOUSE` contract, and splicing a `basic` secret into the connection URL.
//! Those are what this file tests.
//!
//! The contract cases need no database. They assert on errors raised BEFORE the
//! builder opens a connection, so they run under a normal
//! `cargo test -p sqe-catalog --features sql-sqlite`. The live roundtrip is
//! `#[ignore]`d:
//!
//! ```bash
//! docker compose -f docker-compose.test.yml up -d postgres
//! cargo test -p sqe-catalog --features sql-postgres --test mount_jdbc_test -- --ignored
//! ```

#![cfg(any(feature = "sql", feature = "sql-postgres", feature = "sql-sqlite"))]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::{Secret, SecretStore};
use sqe_sql::{CatalogKind, OptionValue};

fn warehouse_opt(path: &str) -> BTreeMap<String, OptionValue> {
    let mut o = BTreeMap::new();
    o.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String(path.to_string()),
    );
    o
}

/// A SQL catalog stores metadata pointers, not the data, so the data root cannot
/// be inferred from the connection URL. Missing `WAREHOUSE` has to be an error
/// rather than a guess, and it must be raised before any connection attempt.
#[tokio::test]
async fn warehouse_is_required() {
    let secrets = SecretStore::new();
    let err = build_catalog(
        "jdbc:postgresql://127.0.0.1:1/db",
        CatalogKind::Jdbc,
        &BTreeMap::new(),
        &secrets,
    )
    .await
    .expect_err("missing WAREHOUSE must fail");
    assert!(
        err.contains("WAREHOUSE"),
        "error should name the missing option, got: {err}"
    );
}

#[tokio::test]
async fn empty_location_is_rejected() {
    let secrets = SecretStore::new();
    let err = build_catalog(
        "   ",
        CatalogKind::Jdbc,
        &warehouse_opt("/tmp/sqe-jdbc-test"),
        &secrets,
    )
    .await
    .expect_err("blank location must fail");
    assert!(
        err.contains("location"),
        "error should mention the location, got: {err}"
    );
}

/// A secret of the wrong kind is a configuration mistake worth naming. Silently
/// ignoring it would connect unauthenticated and fail later with a confusing
/// database error instead.
#[tokio::test]
async fn a_non_basic_secret_is_refused_by_kind() {
    let secrets = SecretStore::new();
    secrets
        .create(
            "wrong_kind",
            Secret::Bearer {
                token: "not-a-basic-secret".to_string(),
            },
        )
        .expect("create secret");

    let mut options = warehouse_opt("/tmp/sqe-jdbc-test");
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("wrong_kind".to_string()),
    );

    let err = build_catalog(
        "jdbc:postgresql://127.0.0.1:1/db",
        CatalogKind::Jdbc,
        &options,
        &secrets,
    )
    .await
    .expect_err("a bearer secret must be refused for TYPE jdbc");
    assert!(
        err.contains("basic"),
        "error should say basic is expected, got: {err}"
    );
}

#[tokio::test]
async fn an_unknown_secret_reference_is_refused() {
    let secrets = SecretStore::new();
    let mut options = warehouse_opt("/tmp/sqe-jdbc-test");
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("no_such_secret".to_string()),
    );

    let err = build_catalog(
        "jdbc:postgresql://127.0.0.1:1/db",
        CatalogKind::Jdbc,
        &options,
        &secrets,
    )
    .await
    .expect_err("an unresolvable secret must fail");
    assert!(
        !err.is_empty(),
        "an unresolvable secret must produce a message"
    );
}

/// The ATTACH surface end to end against a real database: prefix stripping, the
/// spliced secret, and a namespace round-trip proving the handle actually works.
#[tokio::test]
#[ignore = "requires live Postgres; docker compose -f docker-compose.test.yml up -d postgres"]
async fn jdbc_attach_roundtrip_against_live_postgres() {
    // `Catalog` itself is not imported: build_catalog returns `Arc<dyn Catalog>`,
    // so the trait methods are already reachable through the trait object.
    use iceberg::NamespaceIdent;

    let host =
        std::env::var("SQE_TEST_PG_HOSTPORT").unwrap_or_else(|_| "localhost:15432".to_string());
    let warehouse = std::env::var("SQE_TEST_PG_WAREHOUSE")
        .unwrap_or_else(|_| "/tmp/sqe-jdbc-attach-warehouse".to_string());
    std::fs::create_dir_all(&warehouse).expect("warehouse dir");

    // Credentials arrive through the SECRET, not inline in the URL: that is the
    // path ATTACH users take and the one that does the splicing.
    let secrets = SecretStore::new();
    secrets
        .create(
            "pg_cat",
            Secret::Basic {
                username: "iceberg".to_string(),
                password: "iceberg".to_string(),
            },
        )
        .expect("create secret");

    let mut options = warehouse_opt(&warehouse);
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("pg_cat".to_string()),
    );

    // `jdbc:` prefix included deliberately: SQL users write the Java form.
    let catalog = build_catalog(
        &format!("jdbc:postgresql://{host}/iceberg_catalog"),
        CatalogKind::Jdbc,
        &options,
        &secrets,
    )
    .await
    .expect("ATTACH TYPE jdbc should build against live Postgres");

    let ns = NamespaceIdent::new(format!(
        "sqe_jdbc_attach_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("create_namespace through the attached catalog");
    assert!(
        catalog
            .namespace_exists(&ns)
            .await
            .expect("namespace_exists"),
        "the namespace just created must be visible"
    );
    catalog.drop_namespace(&ns).await.expect("drop_namespace");
}
