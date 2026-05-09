//! Sanity tests for `sqe_catalog::mount::build_catalog` with
//! `CatalogKind::S3Tables`. Live AWS coverage lives in
//! `backends_integration::s3_tables` (--ignored). These tests cover
//! option threading and error paths only.

#![cfg(feature = "s3tables")]

use std::collections::BTreeMap;

use sqe_catalog::build_catalog;
use sqe_core::{Secret, SecretStore};
use sqe_sql::{CatalogKind, OptionValue};

fn disable_imds() {
    // Safety: process-global env var, set identically by every test.
    unsafe {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
}

#[tokio::test]
async fn s3tables_accepts_options_without_panic() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_test",
            Secret::Aws {
                access_key: Some("AKIAFAKEKEY".to_string()),
                secret_key: Some("fake-secret".to_string()),
                session_token: None,
                region: Some("us-east-1".to_string()),
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_test".to_string()),
    );

    let result = build_catalog(
        "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
        CatalogKind::S3Tables,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "S3 Tables builder should accept the options, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn s3tables_rejects_basic_secret() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "db_creds",
            Secret::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("db_creds".to_string()),
    );

    let err = build_catalog(
        "arn:aws:s3tables:us-east-1:123:bucket/test",
        CatalogKind::S3Tables,
        &options,
        &secrets,
    )
    .await
    .expect_err("Basic secret must be rejected for S3 Tables");
    assert!(
        err.contains("type basic") && err.contains("aws"),
        "expected kind-mismatch message, got: {err}"
    );
}

#[tokio::test]
async fn s3tables_rejects_empty_location() {
    disable_imds();
    let secrets = SecretStore::new();
    let options: BTreeMap<String, OptionValue> = BTreeMap::new();

    let err = build_catalog("", CatalogKind::S3Tables, &options, &secrets)
        .await
        .expect_err("empty location must error");
    assert!(
        err.contains("location must not be empty"),
        "expected empty-location error, got: {err}"
    );
}

#[tokio::test]
async fn s3tables_endpoint_url_threads_through() {
    disable_imds();
    let secrets = SecretStore::new();
    secrets
        .create(
            "aws_local",
            Secret::Aws {
                access_key: Some("test".to_string()),
                secret_key: Some("test".to_string()),
                session_token: None,
                region: Some("us-east-1".to_string()),
                profile: None,
            },
        )
        .expect("create secret");

    let mut options = BTreeMap::new();
    options.insert(
        "SECRET".to_string(),
        OptionValue::SecretRef("aws_local".to_string()),
    );
    options.insert(
        "ENDPOINT_URL".to_string(),
        OptionValue::String("http://localhost:4566".to_string()),
    );

    // ENDPOINT_URL must not cause the builder to error out.
    let result = build_catalog(
        "arn:aws:s3tables:us-east-1:000000000000:bucket/local",
        CatalogKind::S3Tables,
        &options,
        &secrets,
    )
    .await;
    assert!(
        result.is_ok(),
        "ENDPOINT_URL option should be accepted, got: {:?}",
        result.err()
    );
}
