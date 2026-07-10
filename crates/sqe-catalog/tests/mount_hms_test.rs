//! Unit-level tests for `sqe_catalog::mount::build_catalog` with
//! `CatalogKind::Hms`. Live HMS coverage lives in
//! `backends_integration::hms::*` (--ignored, requires
//! docker-compose hms-standalone). These tests verify option
//! threading and the AUTH_MODE / SECRET validation contract only;
//! the upstream `HmsCatalogBuilder::load()` does not make a network
//! call until the first thrift method invocation, so an option
//! threading test does not need a live metastore.

#![cfg(feature = "hms")]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::{Secret, SecretStore};
use sqe_sql::{CatalogKind, OptionValue};

#[tokio::test]
async fn hms_accepts_basic_options_without_panic() {
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://my-bucket/wh".to_string()),
    );

    // The HMS builder validates name + address + warehouse and then
    // returns a usable catalog handle without dialing the thrift
    // endpoint. We're testing that all option threading runs without
    // panicking and the builder accepts the props.
    let result = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "HMS builder should accept basic options, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn hms_requires_warehouse() {
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();

    let err = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await
    .expect_err("missing WAREHOUSE must error");
    assert!(
        err.contains("WAREHOUSE"),
        "expected WAREHOUSE error, got: {err}"
    );
}

#[tokio::test]
async fn hms_rejects_empty_location() {
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );

    let err = build_catalog("", CatalogKind::Hms, &options, &secrets)
        .await
        .expect_err("empty location must error");
    assert!(
        err.contains("location must not be empty"),
        "expected empty-location error, got: {err}"
    );
}

#[tokio::test]
async fn hms_plain_auth_requires_secret() {
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );
    options.insert(
        "AUTH_MODE".to_string(),
        OptionValue::String("plain".to_string()),
    );

    let err = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await
    .expect_err("AUTH_MODE plain without SECRET must error");
    assert!(
        err.contains("SECRET") && err.contains("plain"),
        "expected plain-without-secret error, got: {err}"
    );
}

#[tokio::test]
async fn hms_plain_auth_rejects_wrong_secret_kind() {
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_creds",
            Secret::Aws {
                access_key: Some("AKIA".to_string()),
                secret_key: Some("sk".to_string()),
                session_token: None,
                region: None,
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );
    options.insert(
        "AUTH_MODE".to_string(),
        OptionValue::String("plain".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_creds".to_string()),
    );

    let err = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await
    .expect_err("AWS secret must be rejected for HMS plain auth");
    assert!(
        err.contains("type aws") && err.contains("basic"),
        "expected kind-mismatch message, got: {err}"
    );
}

#[tokio::test]
async fn hms_plain_auth_with_basic_secret_accepted() {
    let secrets = SecretStore::new();
    secrets
        .create(
            "hms_creds",
            Secret::Basic {
                username: "hive".to_string(),
                password: "hunter2".to_string(),
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );
    options.insert(
        "AUTH_MODE".to_string(),
        OptionValue::String("plain".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("hms_creds".to_string()),
    );

    let result = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "AUTH_MODE plain + Basic secret should be accepted, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn hms_thrift_url_prefix_supported() {
    // The spec writes the location as `thrift://host:port`; the
    // upstream builder expects a bare socket address. Confirm the
    // prefix-stripping lets SQL users write the natural URL form.
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );

    let result = build_catalog(
        "thrift://127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "thrift:// URL form should be accepted, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn hms_unknown_auth_mode_errors() {
    let secrets = SecretStore::new();
    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://wh".to_string()),
    );
    options.insert(
        "AUTH_MODE".to_string(),
        OptionValue::String("oauth2".to_string()),
    );

    let err = build_catalog(
        "127.0.0.1:9083",
        CatalogKind::Hms,
        &options,
        &secrets,
    )
    .await
    .expect_err("unknown AUTH_MODE must error");
    assert!(
        err.contains("oauth2") && err.contains("none"),
        "expected unsupported-auth-mode error, got: {err}"
    );
}
