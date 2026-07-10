//! Sanity tests for `sqe_catalog::mount::build_catalog` with
//! `CatalogKind::Glue`. Full end-to-end coverage against AWS or
//! LocalStack lives in `backends_integration::glue::*` (gated on
//! `--ignored` because it needs a live stack). These tests cover the
//! option threading and error paths only.

#![cfg(feature = "glue")]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::{Secret, SecretStore};
use sqe_sql::{CatalogKind, OptionValue};

/// Disable IMDS / EC2 metadata service lookups; test hosts vary in
/// whether IMDS responds at all, and a stalled IMDS lookup makes the
/// no-creds path appear to hang.
fn disable_imds() {
    // Safety: process-global env var, set identically by every test
    // in this file. Concurrent set is a no-op rather than a race.
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}

#[tokio::test]
async fn glue_accepts_options_without_panic() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_test",
            Secret::Aws {
                access_key: Some("AKIAFAKEKEY12345".to_string()),
                secret_key: Some("fake/secret".to_string()),
                session_token: None,
                region: Some("us-east-1".to_string()),
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://my-bucket/warehouse".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_test".to_string()),
    );

    // The builder constructs an AWS SDK config and Glue client. With
    // explicit credentials, no network call is required at this stage,
    // so the call should return a usable catalog handle.
    let result = build_catalog(
        "arn:aws:glue:us-east-1:123456789012:catalog/test",
        CatalogKind::Glue,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "Glue builder should accept the options, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn glue_rejects_bearer_secret() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "polaris_token",
            Secret::Bearer {
                token: "tok".to_string(),
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "WAREHOUSE".to_string(),
        OptionValue::String("s3://my-bucket/wh".to_string()),
    );
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("polaris_token".to_string()),
    );

    let err = build_catalog(
        "arn:aws:glue:us-east-1:123:catalog/test",
        CatalogKind::Glue,
        &options,
        &secrets,
    )
    .await
    .expect_err("Bearer secret must be rejected for Glue");
    assert!(
        err.contains("type bearer") && err.contains("aws"),
        "expected kind-mismatch message, got: {err}"
    );
}

#[tokio::test]
async fn glue_requires_warehouse() {
    disable_imds();
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();

    let err = build_catalog(
        "arn:aws:glue:us-east-1:123:catalog/test",
        CatalogKind::Glue,
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
async fn glue_region_option_threads_through() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_west",
            Secret::Aws {
                access_key: Some("AKIA-X".to_string()),
                secret_key: Some("sk-x".to_string()),
                session_token: None,
                region: Some("us-west-2".to_string()),
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
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_west".to_string()),
    );
    options.insert(
        "REGION".to_string(),
        OptionValue::String("eu-west-1".to_string()),
    );

    // We don't have a way to peek inside the catalog handle to verify
    // which region the SDK config landed on. The point of this test is
    // to confirm the explicit REGION option does not cause the builder
    // to error out (e.g. by being treated as an unrecognised key).
    let result = build_catalog(
        "arn:aws:glue:eu-west-1:123:catalog/test",
        CatalogKind::Glue,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "REGION + SECRET combo should be accepted, got: {:?}",
        result.err()
    );
}
